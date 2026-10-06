using System.Net;
using System.Text.Json;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

/// <summary>Twentieth-review follow-ups (v24): the SHARED cross-SDK verdict
/// canon replayed through .NET, the context-block message surfaced on the
/// verdict, a GATED concurrent-dispose test that actually distinguishes the
/// shared-completion fix, and the absolute SSE idle bound proven against a
/// non-cooperative (hanging) body.</summary>
public class HardeningV24Tests
{
    private static Func<RecordedRequest, int, ResponseSpec> ModerationRoute(string jobBody) => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated());
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get) return ResponseSpec.Ok(jobBody);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    // -- finding 1 (+6): shared cross-SDK verdict-resolution canon -----------

    public static IEnumerable<object?[]> VerdictCanon()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "verdict_cases.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
        {
            var expect = c.GetProperty("expect");
            yield return new object?[]
            {
                c.GetProperty("name").GetString()!,
                c.GetProperty("job").GetRawText(),
                expect.GetProperty("allowed").GetBoolean(),
                expect.GetProperty("blocked").GetBoolean(),
                expect.TryGetProperty("block_message", out var m) ? m.GetString() : null,
            };
        }
    }

    [Theory]
    [MemberData(nameof(VerdictCanon))]
    public async Task Verdict_canon_matches_the_shared_contract(
        string name, string jobBody, bool expectAllowed, bool expectBlocked, string? expectMessage)
    {
        var (client, _) = TestClient.Create(ModerationRoute(jobBody));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "x" });
        Assert.Equal(expectAllowed, result.Allowed);
        Assert.Equal(expectBlocked, result.Blocked);
        if (expectMessage is not null)
            Assert.Equal(expectMessage, result.BlockMessage);
        Assert.NotEqual("", name); // name is documentary — keeps xUnit's case id readable
    }

    // -- finding 6: a context block surfaces its message on the verdict -------

    [Fact]
    public async Task Context_block_over_allowed_inbound_surfaces_the_context_message()
    {
        const string job = "{\"status\":\"completed\",\"inbound_result\":{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[]},"
            + "\"blocked_by\":\"context\",\"context_result\":{\"status\":\"blocked\",\"blocked\":true,\"block_message\":\"context rule tripped\"}}";
        var (client, _) = TestClient.Create(ModerationRoute(job));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "x" });
        Assert.False(result.Allowed);
        Assert.True(result.Blocked);
        Assert.Equal("context rule tripped", result.BlockMessage);
    }

    // -- finding 7b: concurrent dispose awaits ONE gated terminal chunk ------

    [Fact]
    public async Task Concurrent_dispose_tasks_all_await_the_gated_terminal_chunk()
    {
        var (client, handler) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return Responders.EchoChunk(rec);
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "context" });
        await session.PushAsync("a"); // real output -> dispose submits a terminal is_final chunk

        // Gate every subsequent response: the terminal chunk POST from Abort
        // blocks until we release, so BOTH concurrent disposers await the ONE
        // shared completion. A weaker (instant-handler) test would pass even if
        // each disposer entered Abort separately.
        handler.Gate = new TaskCompletionSource();
        Task d1 = session.DisposeAsync().AsTask();
        Task d2 = session.DisposeAsync().AsTask();

        // Neither completes while the terminal chunk is gated.
        await Task.WhenAny(Task.WhenAll(d1, d2), Task.Delay(200));
        Assert.False(d1.IsCompleted);
        Assert.False(d2.IsCompleted);

        handler.Gate.SetResult();
        await Task.WhenAll(d1, d2);

        // Exactly one terminal (is_final) chunk was submitted — no double-abort.
        int finals = handler.Requests.Count(r => r.Path.EndsWith("/chunks") && r.Body.Contains("\"is_final\":true"));
        Assert.Equal(1, finals);
    }

    // -- finding 7c: the SSE idle bound is ABSOLUTE against a hanging body ----

    private static Func<RecordedRequest, int, ResponseSpec> StreamRoute(Func<RecordedRequest, ResponseSpec> onStream) => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
        if (rec.Path.EndsWith("/stream") && rec.Method == HttpMethod.Get) return onStream(rec);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    [Fact]
    public async Task Sse_body_open_that_hangs_is_bounded_by_the_absolute_idle_timeout()
    {
        // A 200 whose body OPEN hangs (non-cooperative content). The absolute
        // WaitAsync idle bound must fire — this test completes in ~100 ms; a
        // broken bound would hang on the 30 s content and time the suite out.
        var (client, _) = TestClient.Create(StreamRoute(_ => ResponseSpec.Hanging(HttpStatusCode.OK)));
        await using var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "ctx" });

        var events = new List<CollieStreamEvent>();
        await foreach (var ev in session.StreamEventsAsync(idleTimeout: TimeSpan.FromMilliseconds(50), maxReconnects: 1))
            events.Add(ev);

        Assert.Contains(events, e => e is StreamInterrupted si && si.Reason == "idle_timeout");
    }

    [Fact]
    public async Task Sse_error_body_that_hangs_is_bounded_by_the_absolute_idle_timeout()
    {
        // A 500 whose ERROR body read hangs (non-cooperative content). The
        // absolute idle bound surfaces a connection error rather than hanging
        // forever on the caller token.
        var (client, _) = TestClient.Create(StreamRoute(_ => ResponseSpec.Hanging(HttpStatusCode.InternalServerError)));
        await using var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "ctx" });

        await Assert.ThrowsAsync<CollieConnectionException>(async () =>
        {
            await foreach (var _ in session.StreamEventsAsync(idleTimeout: TimeSpan.FromMilliseconds(50)))
            {
            }
        });
    }
}
