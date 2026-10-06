// Flow-level poll-backoff tests (Poll Backoff Contract, amendment v27): both
// .NET poll loops (ModerationClient.RunAsync + StreamingClient.PollBufferedAsync)
// are wired to the pacer — observed poll counts EQUAL the reference simulator's
// output for identical parameters (wiring + math in one assertion), plus the
// latency gate, hint flows, 429 flows, wall-clock stop, strict acceptance, and
// the error taxonomy. Mirrors sdk/python/tests/test_poll_backoff.py.

using System.Net;
using System.Reflection;
using System.Text.Json;
using CollieAi.Internal;
using Xunit;

namespace CollieAi.Client.Tests;

/// <summary>Canonical expected poll counts from the reference simulator — an
/// INDEPENDENT implementation of the contract — loaded from the GENERATED
/// sdk/conformance/poll_cases.json (single cross-SDK source; a Python test
/// guards the file against the live simulator; regenerate with
/// `python poll_sim.py --json > poll_cases.json`). Parameters: interval
/// 0.05 s, rtt 0.01 s, timeout 30 s.</summary>
internal sealed record CanonCase(
    int Polls, bool TimedOut, double? RttS, double[][]? HintSchedule, double[][]? RttSchedule);

internal static class Canon
{
    private static readonly Dictionary<string, CanonCase> Cases = Load();

    private static Dictionary<string, CanonCase> Load()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "poll_cases.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var dict = new Dictionary<string, CanonCase>(StringComparer.Ordinal);
        foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
        {
            double? rtt = c.TryGetProperty("rtt_s", out var r) ? r.GetDouble() : null;
            double[][]? Steps(string key) =>
                c.TryGetProperty(key, out var s)
                    ? s.EnumerateArray()
                        .Select(step => step.EnumerateArray().Select(v => v.GetDouble()).ToArray())
                        .ToArray()
                    : null;
            dict[c.GetProperty("name").GetString()!] = new CanonCase(
                c.GetProperty("polls").GetInt32(),
                c.GetProperty("timed_out").GetBoolean(),
                rtt,
                Steps("hint_schedule"),
                Steps("rtt_schedule"));
        }
        return dict;
    }

    public static CanonCase Case(string name) =>
        Cases.TryGetValue(name, out var c)
            ? c
            : throw new InvalidOperationException($"missing canonical case {name} in poll_cases.json");

    public static int Polls(string name) => Case(name).Polls;
}

public class PollBackoffTests
{
    private const double Rtt = 0.01;

    private sealed class VClock
    {
        public double Now;
    }

    /// <summary>Client on a virtual monotonic clock: Delay advances it; the
    /// responder adds RTT per GET.</summary>
    private static (CollieClient Client, FakeHandler Handler) Create(
        VClock clock, Func<RecordedRequest, int, ResponseSpec> responder, Func<double>? jitter = null)
    {
        var handler = new FakeHandler(responder);
        var http = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://test.collieai.io"),
            Timeout = Timeout.InfiniteTimeSpan,
        };
        var options = new CollieClientOptions
        {
            ApiKey = "clai_test",
            BaseUrl = new Uri("https://test.collieai.io"),
            ProjectId = "project_123",
        };
        var hooks = new CollieTestHooks
        {
            Monotonic = () => clock.Now,
            Delay = (d, _) => { clock.Now += d.TotalSeconds; return Task.CompletedTask; },
            // Pacer sleeps advance the virtual clock by the RAW seconds — the
            // exact doubles the simulator accumulates (bit-identical parity).
            PollDelay = (s, _) => { clock.Now += s; return Task.CompletedTask; },
            Jitter = jitter ?? (() => 0.5),
        };
        return (new CollieClient(options, http, ownsHttp: true, logger: null, hooks: hooks), handler);
    }

    /// <summary>Real monotonic clock (Stopwatch default), instant sleeps.</summary>
    private static (CollieClient Client, FakeHandler Handler) CreateReal(
        Func<RecordedRequest, int, ResponseSpec> responder,
        Action<CollieClientOptions>? configure = null)
    {
        var handler = new FakeHandler(responder);
        var http = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://test.collieai.io"),
            Timeout = Timeout.InfiniteTimeSpan,
        };
        var options = new CollieClientOptions
        {
            ApiKey = "clai_test",
            BaseUrl = new Uri("https://test.collieai.io"),
            ProjectId = "project_123",
        };
        configure?.Invoke(options);
        var hooks = new CollieTestHooks
        {
            Delay = (d, _) => Task.CompletedTask,
            PollDelay = (_, _) => Task.CompletedTask,
            Jitter = () => 0.5,
        };
        return (new CollieClient(options, http, ownsHttp: true, logger: null, hooks: hooks), handler);
    }

    private static int Gets(FakeHandler handler) =>
        handler.CountWhere(r => r.Method == HttpMethod.Get);

    /// <summary>Job that becomes terminal when the GET *response* lands at/after
    /// jobS — the same counting model as the simulator.</summary>
    private static Func<RecordedRequest, int, ResponseSpec> TimedJob(
        VClock clock, double jobS, int? hintMs = null, string? terminalBody = null,
        Func<double, int?>? hintFn = null, double? rtt = null,
        Func<int, double>? rttFn = null)
    {
        double rttS = rtt ?? Rtt;
        int gets = 0;
        return (rec, _) =>
        {
            if (rec.Method == HttpMethod.Post && rec.Path == "/v1/jobs")
                return ResponseSpec.Ok(Bodies.JobCreated(), requestId: "req_1");
            if (rec.Method == HttpMethod.Get && rec.Path.StartsWith("/v1/jobs/"))
            {
                // The server snapshots the job status when it PROCESSES the
                // GET — modeled at ISSUE time (v20), before the response
                // travels back — so it cannot report a completion that
                // happens in flight. The loop's strict acceptance check then
                // rejects a response landing at/after the deadline.
                gets++;
                double snapshot = clock.Now;
                // rttFn models a VARIABLE round trip (rtt_schedule) evaluated
                // at the 1-based poll index — same convention as the
                // simulator's callable rtt_s.
                clock.Now += rttFn is not null ? rttFn(gets) : rttS; // response lands
                if (snapshot >= jobS)
                    return ResponseSpec.Ok(terminalBody ?? Bodies.InboundAllowed());
                // hintFn models a DYNAMIC schedule (the backend age floor).
                // The server computes status AND hint in ONE response at the
                // SNAPSHOT time (v21), not at landing.
                int? hint = hintFn is not null ? hintFn(snapshot) : hintMs;
                string pending = hint is { } h
                    ? "{\"status\":\"processing_inbound\",\"suggested_poll_ms\":" + h + "}"
                    : "{\"status\":\"processing_inbound\"}";
                return ResponseSpec.Ok(pending);
            }
            return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
        };
    }

    private static StreamingClient StreamingCore(CollieClient client) => (StreamingClient)client.Streaming;

    // --------------------------------------------------- wiring + simulator parity

    [Theory]
    [InlineData(0.3, "job_0.3s_draw_0.5")]
    [InlineData(8.0, "job_8s_draw_0.5")]
    [InlineData(11.0, "job_11s_draw_0.5")]
    public async Task Moderation_loop_matches_the_simulator(double jobS, string caseName)
    {
        int expectedPolls = Canon.Polls(caseName);
        var clock = new VClock();
        var (client, handler) = Create(clock, TimedJob(clock, jobS));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Allowed);
        Assert.Equal(expectedPolls, Gets(handler));
    }

    [Theory]
    [InlineData(0.3, "job_0.3s_draw_0.5")]
    [InlineData(8.0, "job_8s_draw_0.5")]
    public async Task Buffered_loop_matches_the_simulator(double jobS, string caseName)
    {
        int expectedPolls = Canon.Polls(caseName);
        var clock = new VClock();
        var (client, handler) = Create(
            clock, TimedJob(clock, jobS, terminalBody: Bodies.OutboundCompleted("ok")));
        var job = await StreamingCore(client).PollBufferedAsync("job_1", "protect_buffered", CancellationToken.None);
        Assert.Equal("completed", job.Status);
        Assert.Equal(expectedPolls, Gets(handler));
    }

    [Fact]
    public async Task Fast_job_latency_gate()
    {
        // Ceilings alone can hide over-backoff: a 300 ms job must complete
        // within job + caller_interval + 2*RTT of virtual time.
        var clock = new VClock();
        var (client, handler) = Create(clock, TimedJob(clock, 0.3));
        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(clock.Now <= 0.3 + 0.05 + (2 * Rtt), $"latency {clock.Now}");
        Assert.True(Gets(handler) <= 8);
    }

    [Fact]
    public async Task Worst_jitter_stays_within_the_ceilings()
    {
        var clock = new VClock();
        var (client, handler) = Create(clock, TimedJob(clock, 8.0), jitter: () => 0.0);
        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.Equal(Canon.Polls("job_8s_draw_0.0"), Gets(handler));
        Assert.True(Gets(handler) <= 45);
    }

    // ------------------------------------------------------------------ hint flows

    [Fact]
    public async Task Hint_ignored_in_warm_honored_in_ramp()
    {
        var clock = new VClock();
        var (client, handler) = Create(clock, TimedJob(clock, 8.0, hintMs: 5000));
        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        // Warm unaffected by the hint; ramp sleeps stretch toward 5 s.
        Assert.Equal(Canon.Polls("job_8s_draw_0.5_hint_5000"), Gets(handler));
        Assert.True(Canon.Polls("job_8s_draw_0.5_hint_5000") < Canon.Polls("job_8s_draw_0.5"));
    }

    [Fact]
    public async Task A_job_finishing_just_inside_the_budget_with_a_large_hint_succeeds()
    {
        // v10 final-poll reserve: pre-v10 the final sleep reached the
        // deadline and the loop timed out without issuing the last GET.
        var clock = new VClock();
        var (client, handler) = Create(clock, TimedJob(clock, 29.0, hintMs: 5000));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Allowed);
        Assert.Equal(Canon.Polls("job_29s_draw_0.5_hint_5000"), Gets(handler));
    }

    [Fact]
    public async Task The_final_poll_reserve_boundary_is_strict()
    {
        // v12: the reserve guarantees the final GET is ISSUED with 100 ms
        // remaining; it is accepted iff the full round trip completes
        // STRICTLY within the reserve — at exactly 100 ms the response lands
        // AT the deadline and the strict acceptance boundary rejects it.
        // v15 pins it on the plain 29.89 s job: both runs are poll-for-poll
        // identical and differ only in the reserve GET's landing
        // (29.999 vs 30.0).
        var ok = Canon.Case("job_29.89s_plain_rtt_0.099");
        var okClock = new VClock();
        var (okClient, okHandler) = Create(
            okClock, TimedJob(okClock, 29.89, rtt: ok.RttS));
        var result = await okClient.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Allowed);
        Assert.False(ok.TimedOut);
        Assert.Equal(ok.Polls, Gets(okHandler));

        var slow = Canon.Case("job_29.89s_plain_rtt_0.1");
        var slowClock = new VClock();
        var (slowClient, slowHandler) = Create(
            slowClock, TimedJob(slowClock, 29.89, rtt: slow.RttS));
        var ex = await Assert.ThrowsAsync<ModerationException>(
            () => slowClient.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" }));
        Assert.Contains("timed out", ex.Message);
        Assert.True(slow.TimedOut);
        Assert.Equal(slow.Polls, Gets(slowHandler));
    }

    /// <summary>Schedule lookup per the canonical semantics (v13): value of
    /// the last [t_from, ms] step with t_from STRICTLY &lt; t — transitions
    /// are exclusive at the boundary, matching the backend ladder's
    /// `raw &lt;= step` (at exactly 16.0 s the age floor is still 2000).</summary>
    private static Func<double, int?> ScheduleHint(double[][] schedule)
    {
        return t =>
        {
            double value = schedule[0][1];
            foreach (double[] step in schedule)
                if (t > step[0])
                    value = step[1];
            return (int)value;
        };
    }

    /// <summary>Variable round trip (v18): rtt for poll index i (1-based) is
    /// the last step with pollFrom STRICTLY &lt; i, in seconds — mirrors the
    /// simulator's rtt_schedule_fn.</summary>
    private static Func<int, double> ScheduleRtt(double[][] schedule)
    {
        return i =>
        {
            double ms = schedule[0][1];
            foreach (double[] step in schedule)
                if (i > step[0])
                    ms = step[1];
            return ms / 1000.0;
        };
    }

    [Fact]
    public async Task A_dynamic_hint_schedule_through_the_real_loop()
    {
        // The hint CHANGES between polls (the backend age floor under a low
        // host median) — the loop must follow each response's own value, not
        // just a constant. The schedule comes from the canonical case itself
        // (single source).
        var spec = Canon.Case("job_11s_draw_0.5_age_schedule");
        double[][] schedule = spec.HintSchedule
            ?? throw new InvalidOperationException("age-schedule case lost its hint_schedule");
        var clock = new VClock();
        var (client, handler) = Create(clock, TimedJob(clock, 11.0, hintFn: ScheduleHint(schedule)));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Allowed);
        Assert.False(spec.TimedOut);
        Assert.Equal(spec.Polls, Gets(handler));
    }

    [Fact]
    public async Task Boundary_and_dynamic_cases_through_the_second_loop()
    {
        // Hint extraction and deadline handling are duplicated per loop —
        // the moderation-loop replays alone can't catch a divergence in
        // PollBufferedAsync (v13). Same canonical cases, other loop.
        var ok = Canon.Case("job_29.89s_plain_rtt_0.099");
        var okClock = new VClock();
        var (okClient, okHandler) = Create(okClock, TimedJob(
            okClock, 29.89,
            terminalBody: Bodies.OutboundCompleted("ok"), rtt: ok.RttS));
        var job = await StreamingCore(okClient).PollBufferedAsync("job_1", "protect_buffered", CancellationToken.None);
        Assert.Equal("completed", job.Status);
        Assert.Equal(ok.Polls, Gets(okHandler));

        var slow = Canon.Case("job_29.89s_plain_rtt_0.1");
        var slowClock = new VClock();
        var (slowClient, slowHandler) = Create(slowClock, TimedJob(
            slowClock, 29.89,
            terminalBody: Bodies.OutboundCompleted("ok"), rtt: slow.RttS));
        var ex = await Assert.ThrowsAsync<CollieApiException>(
            () => StreamingCore(slowClient).PollBufferedAsync("job_1", "protect_buffered", CancellationToken.None));
        Assert.Equal("poll_timeout", ex.Code);
        Assert.True(slow.TimedOut);
        Assert.Equal(slow.Polls, Gets(slowHandler));

        var dyn = Canon.Case("job_11s_draw_0.5_age_schedule");
        var dynClock = new VClock();
        var (dynClient, dynHandler) = Create(dynClock, TimedJob(
            dynClock, 11.0, hintFn: ScheduleHint(dyn.HintSchedule!),
            terminalBody: Bodies.OutboundCompleted("ok")));
        var dynJob = await StreamingCore(dynClient).PollBufferedAsync("job_1", "protect_buffered", CancellationToken.None);
        Assert.Equal("completed", dynJob.Status);
        Assert.Equal(dyn.Polls, Gets(dynHandler));
    }

    /// <summary>Run one loop over a handler and assert its observed poll
    /// count EQUALS the spec's — whether the spec says success or timeout (a
    /// timeout surfaces as ModerationException / CollieApiException, but the
    /// handler still counted the GETs).</summary>
    private async Task ReplayExpectingSpec(
        int loop, VClock clock, double jobS, CanonCase spec,
        Func<double, int?>? hintFn, double? rtt)
    {
        var (client, handler) = Create(clock, TimedJob(
            clock, jobS, hintFn: hintFn,
            terminalBody: loop == 0 ? null : Bodies.OutboundCompleted("ok"),
            rtt: rtt));
        Func<Task> run = loop == 0
            ? () => client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" })
            : () => StreamingCore(client).PollBufferedAsync("job_1", "protect_buffered", CancellationToken.None);
        if (spec.TimedOut)
            await Assert.ThrowsAnyAsync<CollieException>(run);
        else
            await run();
        Assert.Equal(spec.Polls, Gets(handler));
    }

    [Fact]
    public async Task Boundary_pairs_through_both_loops_v13_rescued_ninth_inside_W()
    {
        // Two boundary pairs under the honest server-snapshot model (v20).
        // The v13 pair (age schedule, rtt == reserve) completes 700 ms before
        // the deadline — OUTSIDE W — and is genuinely rescued (both succeed).
        // The ninth-review pair (max(host@6->5000, age), rtt 80 ms) completes
        // 30 ms before the deadline — INSIDE W (= 180 ms); v14's "rescue" was
        // an artifact of the pre-v20 landing-snapshot model, so under the
        // honest model BOTH schedules TIME OUT (the guarantee permits it —
        // inside W is a lottery). Poll counts match the simulator either way.
        var pairs = new[]
        {
            ("job_29.3s_rtt_0.1_plain", "job_29.3s_age_schedule_rtt_0.1", 29.3),
            ("job_29.97s_rtt_0.08_plain", "job_29.97s_host5000_at_6_rtt_0.08", 29.97),
        };
        foreach (var (plainName, hintedName, jobS) in pairs)
        {
            var plainSpec = Canon.Case(plainName);
            var hintedSpec = Canon.Case(hintedName);
            double[][] schedule = hintedSpec.HintSchedule
                ?? throw new InvalidOperationException($"{hintedName} lost its hint_schedule");

            for (int loop = 0; loop < 2; loop++)
            {
                await ReplayExpectingSpec(loop, new VClock(), jobS, plainSpec, null, plainSpec.RttS);
                await ReplayExpectingSpec(
                    loop, new VClock(), jobS, hintedSpec, ScheduleHint(schedule), hintedSpec.RttS);
            }
        }
    }

    [Theory]
    [InlineData("job_5s_interval_0.2_draw_0.5", 5.0, 0.2, null)]    // custom interval, no hint (v16)
    [InlineData("job_8s_interval_0.2_hint_5000", 8.0, 0.2, 5000)]   // custom interval x hint (v17)
    [InlineData("job_25s_interval_10_draw_0.5", 25.0, 10.0, null)]  // interval above the 5 s cap (v17)
    public async Task Custom_interval_matches_the_simulator_through_both_loops(
        string caseName, double jobS, double intervalS, int? hintMs)
    {
        // Non-default caller intervals through the REAL loops: the warm
        // phase, ramp floor, jitter range and cap_eff all key off the caller
        // interval, so the interval sweep being simulator-only left the
        // loops' own interval plumbing unchecked (interval, interval x hint,
        // interval > cap).
        var spec = Canon.Case(caseName);
        Assert.False(spec.TimedOut);

        var modClock = new VClock();
        var (modClient, modHandler) = Create(modClock, TimedJob(modClock, jobS, hintMs: hintMs));
        var result = await modClient.ModerationCore.RunAsync(
            "hi", null, null, null, null, "moderate.input", CancellationToken.None,
            pollIntervalS: intervalS);
        Assert.True(result.Allowed);
        Assert.Equal(spec.Polls, Gets(modHandler));

        var secondClock = new VClock();
        var (secondClient, secondHandler) = Create(
            secondClock, TimedJob(secondClock, jobS, hintMs: hintMs,
                terminalBody: Bodies.OutboundCompleted("ok")));
        var job = await StreamingCore(secondClient).PollBufferedAsync(
            "job_1", "protect_buffered", CancellationToken.None, pollIntervalS: intervalS);
        Assert.Equal("completed", job.Status);
        Assert.Equal(spec.Polls, Gets(secondHandler));
    }

    [Theory]
    [InlineData("job_8s_rtt_schedule", 8.0, false)]                  // variable rtt, no hint (v18)
    [InlineData("job_29.5s_rtt_schedule_age_hint", 29.5, true)]      // tail: variable rtt + age hint (v20)
    public async Task Variable_rtt_schedule_matches_the_simulator_through_both_loops(
        string caseName, double jobS, bool hasHint)
    {
        // A per-poll VARYING round trip (bounded under the reserve) through
        // the REAL loops — a constant rtt models zero variance and can't
        // catch a loop that mishandles a changing round trip. The tail case
        // (v20) adds a changing production age hint at once.
        var spec = Canon.Case(caseName);
        double[][] schedule = spec.RttSchedule
            ?? throw new InvalidOperationException($"{caseName} lost its rtt_schedule");
        Assert.False(spec.TimedOut);
        var rttFn = ScheduleRtt(schedule);
        Func<double, int?>? hintFn =
            hasHint && spec.HintSchedule is not null ? ScheduleHint(spec.HintSchedule) : null;

        var modClock = new VClock();
        var (modClient, modHandler) = Create(
            modClock, TimedJob(modClock, jobS, hintFn: hintFn, rttFn: rttFn));
        var result = await modClient.Moderation.CheckInputAsync(
            new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Allowed);
        Assert.Equal(spec.Polls, Gets(modHandler));

        var secondClock = new VClock();
        var (secondClient, secondHandler) = Create(
            secondClock, TimedJob(secondClock, jobS, hintFn: hintFn, rttFn: rttFn,
                terminalBody: Bodies.OutboundCompleted("ok")));
        var job = await StreamingCore(secondClient).PollBufferedAsync(
            "job_1", "protect_buffered", CancellationToken.None);
        Assert.Equal("completed", job.Status);
        Assert.Equal(spec.Polls, Gets(secondHandler));
    }

    [Fact]
    public async Task Eleventh_review_counterexamples_are_rescued_through_both_loops()
    {
        // v15 (ramp-in bound + caller-paced tail) fixes the eleventh
        // review's pair:
        // - post-warm cliff: a host rung of 5000 jumped a 1.6 s job
        //   straight into a 4.5 s sleep (completion 6.02 s instead of
        //   1.62 s); the ramp-in bound observes it at 2.275 s with poll
        //   parity;
        // - custom timeout: a 2.914 s job under a 3 s budget with the age
        //   schedule timed out with 86 ms still on the clock; the
        //   caller-paced tail catches it in-budget with plain-parity polls.
        var cliff = Canon.Case("job_1.6s_host5000");
        Assert.False(cliff.TimedOut);
        for (int loop = 0; loop < 2; loop++)
        {
            var clock = new VClock();
            var (client, handler) = Create(clock, TimedJob(
                clock, 1.6, hintMs: 5000,
                terminalBody: loop == 0 ? null : Bodies.OutboundCompleted("ok")));
            if (loop == 0)
                await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
            else
                await StreamingCore(client).PollBufferedAsync("job_1", "protect_buffered", CancellationToken.None);
            Assert.Equal(cliff.Polls, Gets(handler));
            Assert.Equal(2.275, clock.Now, 9);
        }

        var ct = Canon.Case("job_2.914s_age_schedule_timeout_3");
        double[][] ctSchedule = ct.HintSchedule
            ?? throw new InvalidOperationException("custom-timeout case lost its hint_schedule");
        Assert.False(ct.TimedOut);

        var modClock = new VClock();
        var (modClient, modHandler) = Create(
            modClock, TimedJob(modClock, 2.914, hintFn: ScheduleHint(ctSchedule)),
            jitter: () => 0.0);
        var modResult = await modClient.ModerationCore.RunAsync(
            "hi", null, null, null, null, "moderate.input", CancellationToken.None,
            timeoutS: 3.0);
        Assert.True(modResult.Allowed);
        Assert.Equal(ct.Polls, Gets(modHandler));

        var secondClock = new VClock();
        var (secondClient, secondHandler) = Create(
            secondClock, TimedJob(
                secondClock, 2.914, hintFn: ScheduleHint(ctSchedule),
                terminalBody: Bodies.OutboundCompleted("ok")),
            jitter: () => 0.0);
        var job = await StreamingCore(secondClient).PollBufferedAsync(
            "job_1", "protect_buffered", CancellationToken.None, timeoutS: 3.0);
        Assert.Equal("completed", job.Status);
        Assert.Equal(ct.Polls, Gets(secondHandler));
    }

    [Fact]
    public async Task Bad_hint_type_never_fails_the_poll()
    {
        // .NET-survival (normative): "suggested_poll_ms": true must be
        // IGNORED by the strict parse — a plain int? DTO field would fail the
        // whole poll with a deserialization error.
        var clock = new VClock();
        var (client, _) = Create(clock, (rec, _) =>
        {
            if (rec.Method == HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated());
            clock.Now += Rtt;
            if (clock.Now >= 0.3)
                return ResponseSpec.Ok(Bodies.InboundAllowed());
            return ResponseSpec.Ok("{\"status\":\"processing_inbound\",\"suggested_poll_ms\":true}");
        });
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Allowed);
    }

    // --------------------------------------------------------------------- timeout

    [Fact]
    public async Task Wall_clock_timeout_in_the_virtual_model()
    {
        // Virtual-clock view of the budget: sleeps are bounded by the
        // remaining budget, so virtual time never overshoots by more than one
        // RTT. (Strict real-time enforcement — cancelling a GET that hangs
        // past the deadline — is covered by the real-clock test below.)
        var clock = new VClock();
        var (client, handler) = Create(clock, TimedJob(clock, 999.0));
        var ex = await Assert.ThrowsAsync<ModerationException>(
            () => client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" }));
        Assert.Contains("timed out", ex.Message);
        Assert.True(clock.Now <= 30.0 + Rtt, $"overshoot: {clock.Now}");
        Assert.Equal(Canon.Polls("job_999s_draw_0.5"), Gets(handler));
        Assert.True(Gets(handler) <= 75);
    }

    [Fact]
    public async Task Deadline_cancels_a_hung_get_real_clock()
    {
        // The budget is absolute wall clock: a GET that hangs past the
        // deadline is cancelled by the linked CTS and surfaces as this flow's
        // poll_timeout. Real clocks on purpose — the virtual clock can't
        // drive CancelAfter.
        var (client, handler) = CreateReal((rec, _) =>
            ResponseSpec.Ok(Bodies.OutboundCompleted("late")));
        handler.Gate = new TaskCompletionSource(); // every request hangs (cancellable)
        var start = System.Diagnostics.Stopwatch.StartNew();
        var ex = await Assert.ThrowsAsync<CollieApiException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None, timeoutS: 0.25));
        Assert.Equal("poll_timeout", ex.Code);
        Assert.True(start.Elapsed < TimeSpan.FromSeconds(5));
    }

    /// <summary>POST answers instantly; every GET hangs until cancelled —
    /// honoring the request token like a real transport.</summary>
    private sealed class HangingGetHandler : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method == HttpMethod.Post)
                return new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new System.Net.Http.StringContent(
                        Bodies.JobCreated(), System.Text.Encoding.UTF8, "application/json"),
                };
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            throw new InvalidOperationException("unreachable");
        }
    }

    /// <summary>Real-clock client over an arbitrary handler.</summary>
    private static CollieClient CreateRealWith(
        HttpMessageHandler handler, Action<CollieClientOptions>? configure = null)
    {
        var http = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://test.collieai.io"),
            Timeout = Timeout.InfiniteTimeSpan,
        };
        var options = new CollieClientOptions
        {
            ApiKey = "clai_test",
            BaseUrl = new Uri("https://test.collieai.io"),
            ProjectId = "project_123",
        };
        configure?.Invoke(options);
        var hooks = new CollieTestHooks
        {
            Delay = (d, _) => Task.CompletedTask,
            PollDelay = (_, _) => Task.CompletedTask,
            Jitter = () => 0.5,
        };
        return new CollieClient(options, http, ownsHttp: true, logger: null, hooks: hooks);
    }

    [Fact]
    public async Task Deadline_cancels_a_hung_get_real_clock_moderation()
    {
        // The per-loop hung-GET requirement: the moderation loop's deadline
        // bound must cancel a hung GET too, surfacing this flow's timeout.
        var client = CreateRealWith(new HangingGetHandler());
        var start = System.Diagnostics.Stopwatch.StartNew();
        var ex = await Assert.ThrowsAsync<ModerationException>(
            () => client.ModerationCore.RunAsync(
                "hi", null, null, null, null, "moderate.input", CancellationToken.None, timeoutS: 0.25));
        Assert.Contains("timed out", ex.Message);
        Assert.True(start.Elapsed < TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Transport_timeout_shorter_than_the_deadline_is_a_connection_error()
    {
        // Structural taxonomy under REAL clocks: Options.Timeout (the
        // transport bound) fires before the poll deadline — the standalone
        // transport timer identifies it deterministically, so the loop must
        // surface a connection error, not claim the deadline.
        var (client, handler) = CreateReal(
            (rec, _) => ResponseSpec.Ok(Bodies.OutboundCompleted("late")),
            o => o.Timeout = TimeSpan.FromMilliseconds(100));
        handler.Gate = new TaskCompletionSource(); // every request hangs (cancellable)
        var ex = await Assert.ThrowsAsync<CollieConnectionException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None, timeoutS: 30.0));
        Assert.Contains("timed out", ex.Message);
    }

    /// <summary>A transport whose own cancellation (none of our tokens) kills
    /// the request — e.g. a custom delegating handler's internal timeout.</summary>
    private sealed class ForeignCancelHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken) =>
            throw new TaskCanceledException("transport's own cancellation");
    }

    [Fact]
    public async Task A_foreign_cancellation_is_a_connection_error()
    {
        // Priority matrix: caller > transport > deadline > FOREIGN. A foreign
        // OperationCanceledException with budget left must surface as a
        // connection error — never impersonate the deadline as poll_timeout.
        var client = CreateRealWith(new ForeignCancelHandler());
        await Assert.ThrowsAsync<CollieConnectionException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None));
    }

    /// <summary>Throws a TCE carrying its OWN (already-cancelled) token.</summary>
    private sealed class ForeignTokenCancelHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var own = new CancellationTokenSource();
            own.Cancel();
            throw new TaskCanceledException("transport's own cancellation", null, own.Token);
        }
    }

    [Fact]
    public async Task A_foreign_cancellation_with_its_own_token_is_a_connection_error()
    {
        // Provenance gate (v9): the OCE carries a token that is none of
        // ours — classified as a connection error by ORIGIN, not by state
        // elimination.
        var client = CreateRealWith(new ForeignTokenCancelHandler());
        await Assert.ThrowsAsync<CollieConnectionException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None));
    }

    private static double? ParseRetryAfter(string value)
    {
        var resp = new HttpResponseMessage((HttpStatusCode)429);
        resp.Headers.TryAddWithoutValidation("Retry-After", value);
        return CollieClient.ParseRetryAfterSeconds(resp);
    }

    [Theory]
    [InlineData("1.5")]
    [InlineData("1e2")]
    [InlineData("")]
    [InlineData("-1")]
    [InlineData("0x10")]
    public void Retry_after_malformed_values_are_unusable(string bad)
    {
        Assert.Null(ParseRetryAfter(bad));
    }

    [Fact]
    public void Retry_after_decimal_and_overflow_values_parse()
    {
        // Our OWN strict parser (v9): the framework's typed Delta silently
        // nulls a VALID-but-large delta (Int32 overflow), which would take
        // the pressure-floor path and violate the server's stated minimum —
        // diverging from Python/Node.
        Assert.Equal(2.0, ParseRetryAfter("2"));
        Assert.Equal(0.0, ParseRetryAfter("0"));
        Assert.Equal(2147483648.0, ParseRetryAfter("2147483648"));
        Assert.True(double.IsPositiveInfinity(ParseRetryAfter(new string('9', 400))!.Value));
    }

    [Fact]
    public async Task Poll_429_with_overflowing_retry_after_surfaces_typed()
    {
        // Retry-After: 2147483648 is a VALID huge minimum: surface the typed
        // 429 (it can't fit any budget) — never fall to the 1 s pressure
        // floor and re-poll against the server's stated minimum.
        var clock = new VClock();
        int gets = 0;
        var (client, _) = Create(clock, (rec, _) =>
        {
            gets++;
            clock.Now += Rtt;
            return new ResponseSpec((HttpStatusCode)429,
                "{\"error\":{\"type\":\"rate_limit\",\"message\":\"rl\"}}",
                RetryAfterRaw: "2147483648");
        });
        var ex = await Assert.ThrowsAsync<CollieApiException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None));
        Assert.Equal(429, ex.StatusCode);
        Assert.Equal(1, gets);
    }

    [Fact]
    public async Task Caller_cancellation_propagates_as_cancellation()
    {
        // Taxonomy: the caller's own token must surface as
        // OperationCanceledException — never rewrapped as poll_timeout or a
        // connection error (.NET distinguishes its linked-token firing from
        // the caller's via callerToken.IsCancellationRequested).
        var (client, handler) = CreateReal((rec, _) =>
            ResponseSpec.Ok(Bodies.OutboundCompleted("late")));
        handler.Gate = new TaskCompletionSource();
        using var callerCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", callerCts.Token, timeoutS: 30.0));
    }

    // ------------------------------------------------------------ strict acceptance

    [Fact]
    public async Task A_late_terminal_response_is_rejected_moderation()
    {
        // Strict acceptance boundary: a terminal response landing after the
        // deadline (a transport that outran cancellation) is this flow's
        // timeout, never a late success.
        var clock = new VClock();
        var (client, _) = Create(clock, (rec, _) =>
        {
            if (rec.Method == HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated());
            clock.Now += 1.0; // lands 0.5 s past the 0.5 s budget
            return ResponseSpec.Ok(Bodies.InboundAllowed());
        });
        var ex = await Assert.ThrowsAsync<ModerationException>(
            () => client.ModerationCore.RunAsync(
                "hi", null, null, null, null, "moderate.input", CancellationToken.None, timeoutS: 0.5));
        Assert.Contains("timed out", ex.Message);
    }

    [Fact]
    public async Task A_late_terminal_response_is_rejected_buffered()
    {
        var clock = new VClock();
        var (client, _) = Create(clock, (rec, _) =>
        {
            clock.Now += 1.0;
            return ResponseSpec.Ok(Bodies.OutboundCompleted("late"));
        });
        var ex = await Assert.ThrowsAsync<CollieApiException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None, timeoutS: 0.5));
        Assert.Equal("poll_timeout", ex.Code);
    }

    [Fact]
    public async Task A_late_http_error_response_is_rejected_moderation()
    {
        // The late-HTTP-error catch exists in BOTH loops — through moderation too.
        var clock = new VClock();
        var (client, _) = Create(clock, (rec, _) =>
        {
            if (rec.Method == HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated());
            clock.Now += 1.0;
            return ResponseSpec.Error(HttpStatusCode.InternalServerError, "server_error", "late");
        });
        var ex = await Assert.ThrowsAsync<ModerationException>(
            () => client.ModerationCore.RunAsync(
                "hi", null, null, null, null, "moderate.input", CancellationToken.None, timeoutS: 0.5));
        Assert.Contains("timed out", ex.Message);
    }

    [Fact]
    public async Task A_late_http_error_response_is_rejected()
    {
        // Strict acceptance applies to error responses too: a 500 landing
        // after the deadline is poll_timeout, not a late typed HTTP error.
        var clock = new VClock();
        var (client, _) = Create(clock, (rec, _) =>
        {
            clock.Now += 1.0;
            return ResponseSpec.Error(HttpStatusCode.InternalServerError, "server_error", "late");
        });
        var ex = await Assert.ThrowsAsync<CollieApiException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None, timeoutS: 0.5));
        Assert.Equal("poll_timeout", ex.Code);
    }

    // -------------------------------------------------------------------- taxonomy

    [Fact]
    public async Task Transport_failure_coinciding_with_deadline_stays_a_connection_error()
    {
        // Structural taxonomy: a transport failure is a connection problem
        // even when the budget happens to be exhausted at that moment — only
        // the deadline CTS itself firing maps to poll_timeout.
        var clock = new VClock();
        int gets = 0;
        var (client, _) = Create(clock, (rec, _) =>
        {
            gets++;
            if (gets == 1)
            {
                clock.Now += Rtt;
                return ResponseSpec.Ok("{\"status\":\"processing_outbound\"}");
            }
            clock.Now += 1.0; // virtual budget (0.5 s) is now exhausted
            throw new HttpRequestException("socket reset");
        });
        await Assert.ThrowsAsync<CollieConnectionException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None, timeoutS: 0.5));
    }

    [Fact]
    public async Task Transport_error_with_budget_left_stays_a_connection_error()
    {
        var clock = new VClock();
        var (client, _) = Create(clock, (rec, _) => throw new HttpRequestException("boom"));
        await Assert.ThrowsAsync<CollieConnectionException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None));
    }

    // ------------------------------------------------------------------------ 429s

    private static Func<RecordedRequest, int, ResponseSpec> Job429ThenDone(
        VClock clock, int? retryAfterSeconds)
    {
        int gets = 0;
        return (rec, _) =>
        {
            if (rec.Method == HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated());
            gets++;
            clock.Now += Rtt;
            if (gets == 1)
                return ResponseSpec.Error((HttpStatusCode)429, "rate_limit", "rl", retryAfter: retryAfterSeconds);
            return ResponseSpec.Ok(Bodies.InboundAllowed());
        };
    }

    [Fact]
    public async Task Poll_429_with_retry_after_waits_and_continues()
    {
        var clock = new VClock();
        var (client, handler) = Create(clock, Job429ThenDone(clock, retryAfterSeconds: 2));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Allowed);
        Assert.Equal(2, Gets(handler));
        // Retry-After is a minimum: the wait was >= 2 s (upward-only jitter).
        Assert.True(clock.Now >= 2.0);
    }

    [Fact]
    public async Task Poll_429_with_retry_after_beyond_budget_surfaces_typed()
    {
        var clock = new VClock();
        var (client, handler) = Create(clock, Job429ThenDone(clock, retryAfterSeconds: 60));
        var ex = await Assert.ThrowsAsync<CollieApiException>(
            () => client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" }));
        Assert.Equal(429, ex.StatusCode);
        Assert.Equal(1, Gets(handler)); // no pointless sleep-then-timeout
    }

    [Fact]
    public async Task Poll_429_headerless_ends_warm_at_the_pressure_floor()
    {
        var clock = new VClock();
        var (client, _) = Create(clock, Job429ThenDone(clock, retryAfterSeconds: null));
        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        // Pressure floor: the single wait was >= 1 s despite being inside warm.
        Assert.True(clock.Now >= 1.0);
    }

    [Fact]
    public async Task Poll_429_zero_retry_after_floors_not_tight_loops()
    {
        // Retry-After: 0 must not produce a zero-sleep re-poll loop — any 429
        // is rate pressure and the wait floors at 1 s.
        var clock = new VClock();
        var (client, handler) = Create(clock, Job429ThenDone(clock, retryAfterSeconds: 0));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Allowed);
        Assert.Equal(2, Gets(handler));
        Assert.True(clock.Now >= 1.0);
    }

    [Fact]
    public async Task Buffered_loop_429_with_retry_after_waits_and_continues()
    {
        // The 429 path through the second loop, not just moderation.
        var clock = new VClock();
        int gets = 0;
        var (client, _) = Create(clock, (rec, _) =>
        {
            gets++;
            clock.Now += Rtt;
            if (gets == 1)
                return ResponseSpec.Error((HttpStatusCode)429, "rate_limit", "rl", retryAfter: 2);
            return ResponseSpec.Ok(Bodies.OutboundCompleted("ok"));
        });
        var job = await StreamingCore(client).PollBufferedAsync(
            "job_1", "protect_buffered", CancellationToken.None);
        Assert.Equal("completed", job.Status);
        Assert.Equal(2, gets);
        Assert.True(clock.Now >= 2.0); // Retry-After is a minimum
    }

    [Fact]
    public async Task Poll_429_large_interval_not_sped_up_moderation_loop()
    {
        // Flow-level (v18): with a 10 s interval, a Retry-After: 0 429 must
        // sleep the caller's 10 s, NOT the 1 s pressure floor.
        var clock = new VClock();
        var (client, handler) = Create(clock, Job429ThenDone(clock, retryAfterSeconds: 0));
        var result = await client.ModerationCore.RunAsync(
            "hi", null, null, null, null, "moderate.input", CancellationToken.None,
            pollIntervalS: 10.0);
        Assert.True(result.Allowed);
        Assert.Equal(2, Gets(handler));
        Assert.True(clock.Now >= 10.0); // the 10 s interval floor, not 1 s
    }

    [Fact]
    public async Task Poll_429_large_interval_not_sped_up_buffered_loop()
    {
        // Same, through the SECOND loop (PollBufferedAsync).
        var clock = new VClock();
        int gets = 0;
        var (client, _) = Create(clock, (rec, _) =>
        {
            gets++;
            clock.Now += Rtt;
            if (gets == 1)
                return ResponseSpec.Error((HttpStatusCode)429, "rate_limit", "rl", retryAfter: 0);
            return ResponseSpec.Ok(Bodies.OutboundCompleted("ok"));
        });
        var job = await StreamingCore(client).PollBufferedAsync(
            "job_1", "protect_buffered", CancellationToken.None, pollIntervalS: 10.0);
        Assert.Equal("completed", job.Status);
        Assert.Equal(2, gets);
        Assert.True(clock.Now >= 10.0);
    }

    [Fact]
    public async Task Poll_429_interval_floor_beyond_budget_surfaces_typed_moderation_loop()
    {
        // A 10 s interval with Retry-After: 0 under a 5 s budget: the 10 s
        // floor doesn't fit, so the rate-limit error surfaces on the first GET.
        var clock = new VClock();
        var (client, handler) = Create(clock, Job429ThenDone(clock, retryAfterSeconds: 0));
        var ex = await Assert.ThrowsAsync<CollieApiException>(
            () => client.ModerationCore.RunAsync(
                "hi", null, null, null, null, "moderate.input", CancellationToken.None,
                pollIntervalS: 10.0, timeoutS: 5.0));
        Assert.Equal(429, ex.StatusCode);
        Assert.Equal(1, Gets(handler)); // surfaced immediately, no 1 s tight loop
    }

    [Fact]
    public async Task Poll_429_interval_floor_beyond_budget_surfaces_typed_buffered_loop()
    {
        // Same feasibility case through the SECOND loop (v20 — the v19 "both
        // loops" claim covered only the sleep-floor case, not this one).
        var clock = new VClock();
        int gets = 0;
        var (client, _) = Create(clock, (rec, _) =>
        {
            gets++;
            clock.Now += Rtt;
            if (gets == 1)
                return ResponseSpec.Error((HttpStatusCode)429, "rate_limit", "rl", retryAfter: 0);
            return ResponseSpec.Ok(Bodies.OutboundCompleted("ok"));
        });
        var ex = await Assert.ThrowsAsync<CollieApiException>(
            () => StreamingCore(client).PollBufferedAsync(
                "job_1", "protect_buffered", CancellationToken.None,
                pollIntervalS: 10.0, timeoutS: 5.0));
        Assert.Equal(429, ex.StatusCode);
        Assert.Equal(1, gets); // surfaced immediately, no 1 s tight loop
    }

    /// <summary>StringContent whose disposal is observable.</summary>
    private sealed class TrackingContent : StringContent
    {
        public bool Disposed;
        public TrackingContent(string body)
            : base(body, System.Text.Encoding.UTF8, "application/json") { }
        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }

    [Fact]
    public async Task A_continued_429_response_is_disposed()
    {
        // The primitive reads the whole body and disposes the response — the
        // .NET analogue of Node's body.cancel(): a continued 429 must not pin
        // a pooled connection. Asserted on the actual HttpContent disposal.
        var clock = new VClock();
        var content429 = new TrackingContent("{\"error\":{\"type\":\"rate_limit\",\"message\":\"rl\"}}");
        int gets = 0;
        var handler = new FakeHandler((rec, _) =>
        {
            if (rec.Method == HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated());
            gets++;
            clock.Now += Rtt;
            return gets == 1
                ? ResponseSpec.Error((HttpStatusCode)429, "rate_limit", "rl", retryAfter: 2)
                : ResponseSpec.Ok(Bodies.InboundAllowed());
        });
        // FakeHandler builds its own content; swap in the tracked one for the
        // 429 by wrapping SendAsync via a delegating handler.
        var delegating = new Swap429ContentHandler(handler, content429);
        var http = new HttpClient(delegating)
        {
            BaseAddress = new Uri("https://test.collieai.io"),
            Timeout = Timeout.InfiniteTimeSpan,
        };
        var options = new CollieClientOptions
        {
            ApiKey = "clai_test",
            BaseUrl = new Uri("https://test.collieai.io"),
            ProjectId = "project_123",
        };
        var hooks = new CollieTestHooks
        {
            Monotonic = () => clock.Now,
            Delay = (d, _) => { clock.Now += d.TotalSeconds; return Task.CompletedTask; },
            PollDelay = (s, _) => { clock.Now += s; return Task.CompletedTask; },
            Jitter = () => 0.5,
        };
        var client = new CollieClient(options, http, ownsHttp: true, logger: null, hooks: hooks);
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Allowed);
        Assert.True(content429.Disposed);
    }

    /// <summary>Replaces the first 429 response's content with a tracked one.</summary>
    private sealed class Swap429ContentHandler : DelegatingHandler
    {
        private readonly TrackingContent _content;
        public Swap429ContentHandler(HttpMessageHandler inner, TrackingContent content)
            : base(inner) => _content = content;
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var resp = await base.SendAsync(request, cancellationToken);
            if ((int)resp.StatusCode == 429)
                resp.Content = _content;
            return resp;
        }
    }

    // ---------------------------------------------------------------- independence

    [Fact]
    public async Task Sequential_operations_get_fresh_pacers()
    {
        var clock = new VClock();
        var (client, handler) = Create(clock, TimedJob(clock, 0.3));
        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "one" });
        int first = Gets(handler);
        // Reset the virtual job for a second, identical run on the SAME clock.
        double baseNow = clock.Now;
        var (client2, handler2) = Create(clock, TimedJob(clock, baseNow + 0.3));
        await client2.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "two" });
        // A carried-over ramp would poll far fewer times; a fresh pacer
        // repeats the warm-phase pattern.
        Assert.Equal(first, Gets(handler2));
    }

    [Fact]
    public async Task Sequential_operations_on_one_client_get_fresh_pacers()
    {
        // Same CLIENT (not just same clock): a pacer accidentally shared on
        // the client instance would carry ramp state into the second call.
        var clock = new VClock();
        double[] jobDoneAt = { 0.3 };
        int gets = 0;
        var (client, _) = Create(clock, (rec, _) =>
        {
            if (rec.Method == HttpMethod.Post)
                return ResponseSpec.Ok(Bodies.JobCreated());
            gets++;
            clock.Now += Rtt;
            return clock.Now >= jobDoneAt[0]
                ? ResponseSpec.Ok(Bodies.InboundAllowed())
                : ResponseSpec.Ok("{\"status\":\"processing_inbound\"}");
        });
        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "one" });
        int first = gets;
        jobDoneAt[0] = clock.Now + 0.3;
        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "two" });
        Assert.Equal(first, gets - first);
    }

    // ------------------------------------------------------------ version identity

    [Fact]
    public void SdkVersion_derives_from_the_assembly_and_reaches_the_user_agent()
    {
        // The wire identity is what the release gate checks (it reads the
        // User-Agent): it must derive from the assembly version — which
        // `dotnet pack` stamps from the csproj <Version> — with the "+commit"
        // build metadata stripped, never a hardcoded copy.
        string? info = typeof(Wire).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        Assert.False(string.IsNullOrEmpty(info));
        string expected = info!.Split('+')[0];
        Assert.Equal(expected, Wire.SdkVersion);
        Assert.DoesNotContain("+", Wire.SdkVersion);
        Assert.Equal($"CollieAi.Client/{Wire.SdkVersion}", Wire.UserAgent);
    }
}
