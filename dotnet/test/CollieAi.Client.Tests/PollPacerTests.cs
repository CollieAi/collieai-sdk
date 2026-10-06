// PollPacer unit matrix (Poll Backoff Contract, amendment v27): warm/ramp
// boundary, post-jitter caller floor, cap continuity, capEff, hint parsing and
// placement, 429 semantics, ramp advancement, budget bounding, knob
// validation. Mirrors sdk/python/tests/test_poll_pacer.py.

using System.Text.Json;
using CollieAi.Internal;
using Xunit;

namespace CollieAi.Client.Tests;

public class PollPacerTests
{
    private const int P = 10; // Assert.Equal double precision digits

    private sealed class Clock
    {
        public double Now;
    }

    /// <summary>Scripted U(0,1) draws; fails the test if consumed when it must not be.</summary>
    private sealed class Rand
    {
        private readonly Queue<double> _draws;
        private readonly bool _forbid;
        public Rand(double[]? draws = null, bool forbid = false)
        {
            _draws = new Queue<double>(draws ?? Array.Empty<double>());
            _forbid = forbid;
        }
        public double Next()
        {
            Assert.False(_forbid, "jitter rng consumed where none is allowed");
            return _draws.Count > 0 ? _draws.Dequeue() : 0.5;
        }
    }

    private static (PollPacer Pacer, Clock Clock) Make(
        double interval = 0.05, double timeout = 30.0, Rand? rand = null)
    {
        var clock = new Clock();
        var r = rand ?? new Rand();
        return (new PollPacer(interval, timeout, () => clock.Now, r.Next), clock);
    }

    /// <summary>A wire-shaped hint value, parsed from raw JSON.</summary>
    private static JsonElement Hint(string rawJson)
    {
        using var doc = JsonDocument.Parse(rawJson);
        return doc.RootElement.Clone();
    }

    // ---------------------------------------------------------------- warm phase

    [Fact]
    public void Warm_is_literal_no_jitter_no_hint()
    {
        var (pacer, clock) = Make(rand: new Rand(forbid: true));
        foreach (double t in new[] { 0.0, 0.5, 1.49 })
        {
            clock.Now = t;
            // Hint present but MUST be ignored in warm; rng must not be consumed.
            Assert.Equal(0.05, pacer.NextSleep(Hint("5000")), P);
        }
    }

    [Fact]
    public void Warm_to_ramp_boundary()
    {
        var (pacer, clock) = Make(rand: new Rand(new[] { 0.0, 0.0 }));
        clock.Now = 1.49;
        Assert.Equal(0.05, pacer.NextSleep(), P); // still warm
        clock.Now = 1.5;
        // Ramp: base=max(0.1, 0.05)=0.1 → lo=max(0.08, 0.05)=0.08 with draw 0.
        Assert.Equal(0.08, pacer.NextSleep(), P);
    }

    // ------------------------------------------------------------- floors & caps

    [Fact]
    public void Post_jitter_caller_floor()
    {
        // caller 500 ms: draw 0.0 must NOT produce 400 ms.
        var (pacer, clock) = Make(interval: 0.5, rand: new Rand(new[] { 0.0, 1.0 }));
        clock.Now = 2.0;
        Assert.Equal(0.5, pacer.NextSleep(), P); // base=max(0.1,0.5) → lo=max(0.4,0.5)
        Assert.Equal(0.6, pacer.NextSleep(), P); // draw 1.0 → hi=min(0.6, 5)
    }

    [Fact]
    public void Cap_continuity_no_point_mass()
    {
        // target pinned at cap via hint: range must be U(0.8*cap, cap), continuous.
        // elapsed 10 s so the v15 ramp-in bound (elapsed/2 = cap) does not bind.
        var (pacer, clock) = Make(rand: new Rand(new[] { 0.0, 0.5, 1.0 }));
        clock.Now = 10.0;
        Assert.Equal(0.8 * PollPacer.HardCapS, pacer.NextSleep(Hint("5000")), P);
        Assert.Equal(0.9 * PollPacer.HardCapS, pacer.NextSleep(Hint("5000")), P);
        Assert.Equal(PollPacer.HardCapS, pacer.NextSleep(Hint("5000")), P);
    }

    [Fact]
    public void CapEff_honors_large_caller_interval()
    {
        var (pacer, clock) = Make(interval: 10.0, timeout: 60.0, rand: new Rand(new[] { 0.0, 1.0 }));
        clock.Now = 2.0;
        Assert.Equal(10.0, pacer.NextSleep(), P);
        Assert.Equal(10.0, pacer.NextSleep(), P);
    }

    [Fact]
    public void CapEff_is_isolated_by_the_headerless_429_path()
    {
        // The NextSleep assertions above can't FAIL on a cap_eff regression:
        // with a 10 s interval the post-jitter caller floor reconstructs 10
        // even if cap_eff were wrongly 5. The one place cap_eff is observable
        // in isolation is the headerless-429 sleep
        // Min(Max(base, pressureFloor), cap_eff) — no caller floor follows,
        // so a cap_eff broken to 5 s returns 5 where the contract needs 10.
        var (pacer, clock) = Make(interval: 10.0, timeout: 300.0);
        clock.Now = 2.0;
        Assert.Equal(10.0, pacer.SleepFor429(null)!.Value, P);
    }

    // ----------------------------------------------------------------- the hint

    [Fact]
    public void Hint_parsing_strictness()
    {
        Assert.Equal(0.1, PollPacer.ParseSuggestedPollMs(Hint("100"))!.Value, P);
        Assert.Equal(0.05, PollPacer.ParseSuggestedPollMs(Hint("50"))!.Value, P);
        Assert.Equal(5.0, PollPacer.ParseSuggestedPollMs(Hint("5000"))!.Value, P);
        // Bad wire values are ignored, never steer pacing and never fail the
        // poll: out-of-range, bools, strings, fractions/exponents (Utf8 int
        // parse rejects "100.0" and "1e2" — same strictness as Python),
        // arrays, negatives, absent.
        foreach (string bad in new[] { "49", "5001", "true", "false", "\"100\"", "100.5", "100.0", "1e2", "[100]", "-100" })
            Assert.Null(PollPacer.ParseSuggestedPollMs(Hint(bad)));
        Assert.Null(PollPacer.ParseSuggestedPollMs(null));
    }

    [Fact]
    public void Hint_honored_in_ramp()
    {
        var (pacer, clock) = Make(rand: new Rand(new[] { 0.5 }));
        clock.Now = 4.0; // elapsed/2 = 2.0: the v15 ramp-in bound does not bind
        // target = min(max(0.1, 2.0), 5) = 2.0 → U(1.6, 2.4) at 0.5 → 2.0
        Assert.Equal(2.0, pacer.NextSleep(Hint("2000")), P);
    }

    [Fact]
    public void Ramp_in_bound_halves_the_target_after_warm()
    {
        // v15: the target never exceeds half the operation's own elapsed
        // time — a host rung of 5000 must not jump a job straight from the
        // warm phase into 5 s sleeps (the post-warm cliff: a 1.6 s job cost
        // +4.4 s).
        var (pacer, clock) = Make(rand: new Rand(new[] { 0.5, 0.5 }));
        pacer.EndWarm();
        clock.Now = 1.6; // elapsed 1.6 -> bound 0.8, U(0.64, 0.96) at 0.5
        Assert.Equal(0.8, pacer.NextSleep(Hint("5000")), P);
        clock.Now = 6.0; // elapsed 6.0 -> bound 3.0, U(2.4, 3.6) at 0.5
        Assert.Equal(3.0, pacer.NextSleep(Hint("5000")), P);
    }

    [Fact]
    public void Ramp_advances_once_per_sleep_including_hint_stretched()
    {
        var (pacer, clock) = Make(rand: new Rand(new[] { 0.0, 0.0, 0.0 }));
        clock.Now = 2.0;
        pacer.NextSleep(Hint("5000")); // base 0.1 consumed, i -> 1
        pacer.NextSleep(Hint("5000")); // base 0.2 consumed, i -> 2
        // No hint now: base must be 0.4 (advanced twice above, never reset).
        Assert.Equal(Math.Max(0.8 * 0.4, 0.05), pacer.NextSleep(), P);
    }

    [Fact]
    public void Concurrent_pacers_are_independent()
    {
        var a = Make(rand: new Rand(new[] { 0.0, 0.0 }));
        var b = Make(rand: new Rand(new[] { 0.0 }));
        a.Clock.Now = 2.0;
        a.Pacer.NextSleep(); // advances a's ramp only
        b.Clock.Now = 2.0;
        Assert.Equal(0.08, b.Pacer.NextSleep(), P); // b still at ramp[0]
    }

    // ------------------------------------------------------------------- budget

    [Fact]
    public void Final_poll_reserve_shortens_a_deadline_reaching_sleep()
    {
        // v10: a sleep that would cross into the reserve is shortened so one
        // last GET is issued with the reserve remaining. Post-v14 this
        // branch is reached via the warm/caller-interval path — the
        // densified ramp target is capped at half the remaining budget and
        // no longer crosses the tail on its own.
        var (pacer, clock) = Make(timeout: 1.0);
        clock.Now = 0.87; // warm; interval 0.05 > tail 0.03 -> shortened
        Assert.Equal(0.03, pacer.NextSleep(), P);
        // The shortened sleep SPENDS the tail poll; after it polling
        // continues at the CALLER'S OWN interval (v15) — not a sleep to the
        // deadline, which forfeited round trips that still fit the budget.
        clock.Now = 0.93; // remaining 0.07 <= reserve, tail spent
        Assert.Equal(0.05, pacer.NextSleep(), P);
    }

    [Fact]
    public void Densification_halves_the_target_near_the_deadline()
    {
        // v14: the target never exceeds half the remaining budget (floored
        // at the caller's interval) — a 5 s hint with 4 s remaining paces
        // at 2 s; the jitter range follows the densified target.
        var (pacer, clock) = Make(timeout: 30.0, rand: new Rand(new[] { 0.5, 0.5 }));
        pacer.EndWarm();
        clock.Now = 26.0; // remaining 4.0 -> target 2.0, U(1.6, 2.4) at 0.5
        Assert.Equal(2.0, pacer.NextSleep(Hint("5000")), P);
        clock.Now = 29.0; // remaining 1.0 -> target 0.5, U(0.4, 0.6) at 0.5
        Assert.Equal(0.5, pacer.NextSleep(Hint("5000")), P);
    }

    [Fact]
    public void First_inside_reserve_landing_polls_immediately_then_caller_paced()
    {
        // v14 stateful tail poll + v15 caller-paced tail: a pending
        // response LANDING inside the reserve (the sleep-shortened path
        // never ran, so the final poll is still unspent) polls again
        // immediately; afterwards polling continues at
        // min(interval, remaining) until the deadline.
        var (pacer, clock) = Make(timeout: 1.0);
        clock.Now = 0.96; // remaining 0.04 <= reserve; tail poll unspent
        Assert.Equal(0.0, pacer.NextSleep(), P);
        Assert.Equal(0.04, pacer.NextSleep(), P); // min(interval, 0.04)
        Assert.Equal(0.04, pacer.NextSleep(), P);
        clock.Now = 0.93; // remaining 0.07 > interval: full caller interval
        Assert.Equal(0.05, pacer.NextSleep(), P);
    }

    [Fact]
    public void Sleep_bounded_by_remaining()
    {
        var (pacer, clock) = Make(timeout: 1.0);
        clock.Now = 0.97;
        Assert.Equal(0.0, pacer.NextSleep(), P); // v14: the unspent tail poll fires
        Assert.Equal(0.03, pacer.NextSleep(), P);
        clock.Now = 1.5;
        Assert.Equal(0.0, pacer.NextSleep(), P);
        Assert.True(pacer.Remaining() < 0);
    }

    // --------------------------------------------------------------- validation

    [Theory]
    [InlineData(0.0)]
    [InlineData(-1.0)]
    [InlineData(0.009)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void Interval_validation_rejects(double interval) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => PollPacer.Validate(interval, 30.0));

    [Theory]
    [InlineData(0.0)]
    [InlineData(-5.0)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void Timeout_validation_rejects(double timeout) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => PollPacer.Validate(0.05, timeout));

    [Fact]
    public void Timeout_beyond_the_runtime_timer_bound_is_rejected_explicitly()
    {
        // CancelAfter's delay is bounded by the runtime timer (2^31-1 ms): a
        // bigger budget must fail validation loudly, not deep inside the loop.
        Assert.Throws<ArgumentOutOfRangeException>(() => PollPacer.Validate(0.05, PollPacer.MaxTimeoutS + 1));
        PollPacer.Validate(0.05, PollPacer.MaxTimeoutS); // does not throw
    }

    // --------------------------------------------------------------------- 429s

    [Fact]
    public void Retry_after_is_a_minimum_with_upward_jitter()
    {
        var (pacer, _) = Make(rand: new Rand(new[] { 0.0, 1.0 }));
        Assert.Equal(2.0, pacer.SleepFor429(2.0)!.Value, P);
        Assert.Equal(2.2, pacer.SleepFor429(2.0)!.Value, P);
    }

    [Fact]
    public void Retry_after_never_polls_faster_than_the_caller_interval()
    {
        // v18: a 429 is rate pressure — it must never make the caller poll
        // FASTER than their own interval. With a 10 s interval, a VALID
        // Retry-After: 0 used to collapse to the 1 s pressure floor. (The
        // headerless path was already correct — it floors at
        // Max(base, 1 s) with base >= interval; only the with-header path
        // lacked the interval term.) The with-header floor now includes the
        // caller interval; default-interval tests are unaffected.
        var (p0, _) = Make(interval: 10.0, timeout: 300.0, rand: new Rand(new[] { 0.0 }));
        Assert.Equal(10.0, p0.SleepFor429(0.0)!.Value, P);   // max(0, 1, 10)
        var (p1, _) = Make(interval: 10.0, timeout: 300.0, rand: new Rand(new[] { 0.0 }));
        Assert.Equal(10.0, p1.SleepFor429(1.0)!.Value, P);   // max(1, 1, 10)
        var (p2, _) = Make(interval: 10.0, timeout: 300.0, rand: new Rand(new[] { 0.0 }));
        Assert.Equal(30.0, p2.SleepFor429(30.0)!.Value, P);  // server minimum honored
    }

    [Fact]
    public void Retry_after_beyond_budget_surfaces()
    {
        var (pacer, _) = Make(timeout: 1.0);
        Assert.Null(pacer.SleepFor429(2.0));
    }

    [Fact]
    public void Wait_must_leave_the_final_poll_reserve()
    {
        // v11: feasibility includes the final-poll reserve. remaining=2.05,
        // Retry-After=2 leaves only 50 ms for the final GET — surface the
        // typed 429; with remaining=2.2 the floor fits and the jitter falls
        // back to it.
        var (pacer, _) = Make(timeout: 2.05, rand: new Rand(new[] { 1.0 }));
        Assert.Null(pacer.SleepFor429(2.0));
        var (pacer2, _) = Make(timeout: 2.2, rand: new Rand(new[] { 1.0 }));
        Assert.Equal(2.0, pacer2.SleepFor429(2.0)!.Value, P);
    }

    [Fact]
    public void Retry_after_equal_to_remaining_surfaces()
    {
        // The wait must complete STRICTLY within the budget: equality leaves
        // no room for the next GET, so the typed 429 surfaces now.
        var (pacer, _) = Make(timeout: 2.0);
        Assert.Null(pacer.SleepFor429(2.0));
    }

    [Fact]
    public void Headerless_429_ends_warm_and_floors_at_pressure()
    {
        var (pacer, clock) = Make(rand: new Rand(new[] { 0.0 }));
        clock.Now = 0.2; // inside warm
        Assert.Equal(PollPacer.PressureFloorS, pacer.SleepFor429(null)!.Value, P);
        // Warm has ended: the next sleep is ramp-phase (jittered, not 50 ms).
        Assert.False(pacer.InWarm());
        Assert.NotEqual(0.05, pacer.NextSleep(), P);
    }

    [Fact]
    public void Headerless_429_beyond_budget_surfaces()
    {
        var (pacer, _) = Make(timeout: 0.5);
        Assert.Null(pacer.SleepFor429(null));
    }

    [Fact]
    public void Toctou_floor_no_longer_fitting_surfaces()
    {
        // The feasibility decision is re-taken against a fresh reading AFTER
        // the jitter draw: if time advanced between the two readings and the
        // floor no longer fits, surface the typed 429 instead of returning a
        // floor that sleeps into a guaranteed poll-timeout.
        double[] ticks = { 0.0, 8.85, 9.0006 }; // ctor, first check, final check
        int i = 0;
        var pacer = new PollPacer(0.05, 10.0, () => ticks[i++], () => 1.0);
        Assert.Null(pacer.SleepFor429(0.0));
    }

    [Fact]
    public void Zero_retry_after_floors_at_pressure()
    {
        // Retry-After: 0 is a minimum, not a license to tight-loop: any 429
        // is rate pressure, so the wait floors at PressureFloorS (still
        // jittered upward-only).
        var (pacer, _) = Make(rand: new Rand(new[] { 0.0, 1.0 }));
        Assert.Equal(PollPacer.PressureFloorS, pacer.SleepFor429(0.0)!.Value, P);
        Assert.Equal(PollPacer.PressureFloorS * 1.1, pacer.SleepFor429(0.0)!.Value, P);
    }

    [Fact]
    public void Zero_retry_after_with_tiny_budget_surfaces()
    {
        // Under pressure with less than the floor remaining, an immediate
        // re-poll would violate the floor and a floored sleep would overrun
        // the budget — surface the typed 429.
        var (pacer, _) = Make(timeout: 0.5);
        Assert.Null(pacer.SleepFor429(0.0));
    }
}
