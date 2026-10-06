using System.Net;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

/// <summary>Regressions for the nineteenth-review follow-ups (v23): verdict
/// fail-open sub-cases, late-success chunk ceiling, concurrent dispose, and
/// oversized-timeout validation.</summary>
public class HardeningV23Tests
{
    private static Func<RecordedRequest, int, ResponseSpec> ModerationRoute(string inboundBody) => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated());
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get) return ResponseSpec.Ok(inboundBody);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    private static Func<RecordedRequest, int, ResponseSpec> StreamRoute(string inboundBody) => (rec, _) =>
    {
        if (rec.Path == "/v1/streaming/preflight") return ResponseSpec.Ok(Bodies.PreflightStreaming());
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_x"));
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get) return ResponseSpec.Ok(inboundBody);
        if (rec.Path.EndsWith("/chunks")) return Responders.EchoChunk(rec);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    // -- finding 1: verdict fail-open sub-cases ------------------------------

    [Theory]
    // (a) completed with an EMPTY inbound_result — no explicit allowed.
    [InlineData("{\"status\":\"completed\",\"inbound_result\":{}}")]
    // (b) allowed input but a context_result BLOCK on the root.
    [InlineData("{\"status\":\"completed\",\"inbound_result\":{\"allowed\":true,\"blocked\":false},"
        + "\"context_result\":{\"status\":\"blocked\",\"blocked\":true,\"block_message\":\"ctx\"}}")]
    public async Task Ambiguous_or_context_blocked_verdict_is_not_allowed(string inboundBody)
    {
        var (client, _) = TestClient.Create(ModerationRoute(inboundBody));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.False(result.Allowed);
        Assert.True(result.Blocked);
    }

    [Theory]
    [InlineData("{\"status\":\"completed\",\"inbound_result\":{}}")]
    [InlineData("{\"status\":\"completed\",\"inbound_result\":{\"allowed\":true,\"blocked\":false},"
        + "\"context_result\":{\"status\":\"blocked\",\"blocked\":true,\"block_message\":\"ctx\"}}")]
    public async Task Ambiguous_or_context_blocked_verdict_does_not_run_the_factory(string inboundBody)
    {
        var (client, _) = TestClient.Create(StreamRoute(inboundBody));
        var factory = new CountingFactory("should", "not", "run");
        var events = new List<CollieStreamEvent>();
        await foreach (var ev in client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }))
            events.Add(ev);
        Assert.Equal(0, factory.Calls);
        Assert.IsType<InputBlocked>(Assert.Single(events));
    }

    [Fact]
    public async Task Precomputed_inconsistent_allowed_and_blocked_result_blocks_the_factory()
    {
        // A caller-supplied InputResult with Allowed=true AND Blocked=true is
        // contradictory — the wrapper must block, not run the provider.
        var (client, _) = TestClient.Create(StreamRoute(Bodies.InboundAllowed()));
        var factory = new CountingFactory("nope");
        var events = new List<CollieStreamEvent>();
        await foreach (var ev in client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            InputResult = new InputModerationResult { OriginalText = "prompt", Allowed = true, Blocked = true },
            RawStreamFactory = factory.Factory,
        }))
            events.Add(ev);
        Assert.Equal(0, factory.Calls);
        Assert.IsType<InputBlocked>(Assert.Single(events));
    }

    // -- finding 2: late-success chunk is rejected by the strict ceiling -----

    [Fact]
    public async Task A_2xx_chunk_that_lands_after_the_ceiling_is_rejected()
    {
        // The monotonic clock jumps PAST the ceiling DURING the request (the
        // responder bumps it), so the 2xx arrives late. The strict total
        // ceiling must reject it rather than accept a late success.
        double mono = 0;
        var (client, _) = TestClient.Create(
            (rec, _) =>
            {
                if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
                if (rec.Path.EndsWith("/chunks"))
                {
                    mono = 0.2; // elapsed jumps past the 0.1s ceiling mid-flight
                    return Responders.EchoChunk(rec);
                }
                return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
            },
            configure: o => o.RetryCeilingPerChunk = TimeSpan.FromMilliseconds(100),
            monotonic: () => mono);
        await using var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "context" });
        await Assert.ThrowsAsync<ChunkRetryExhaustedException>(() => session.PushAsync("x"));
    }

    // -- finding 6: concurrent DisposeAsync awaits the real completion -------

    [Fact]
    public async Task Concurrent_dispose_all_await_the_same_completion()
    {
        var (client, handler) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return Responders.EchoChunk(rec);
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "context" });
        await session.PushAsync("a"); // real output streamed -> Abort submits a terminal chunk on dispose

        // Two concurrent disposals: neither must throw, both complete only once
        // the real dispose (which submits the terminal chunk) has finished.
        Task d1 = session.DisposeAsync().AsTask();
        Task d2 = session.DisposeAsync().AsTask();
        await Task.WhenAll(d1, d2);

        // Exactly one terminal (is_final) chunk was submitted by Abort — no
        // double-abort from the racing disposer.
        int finals = handler.Requests.Count(r => r.Path.EndsWith("/chunks") && r.Body.Contains("\"is_final\":true"));
        Assert.Equal(1, finals);

        var ex = await Assert.ThrowsAsync<CollieException>(() => session.PushAsync("b"));
        Assert.IsNotType<ObjectDisposedException>(ex);
    }

    // -- finding 7: oversized timeout rejected at construction ---------------

    [Fact]
    public void Oversized_timeout_is_rejected_at_construction_not_deep_in_a_request()
    {
        Assert.Throws<ArgumentException>(() => TestClient.Create(
            (_, _) => ResponseSpec.Ok("{}"), configure: o => o.Timeout = TimeSpan.MaxValue));
    }
}
