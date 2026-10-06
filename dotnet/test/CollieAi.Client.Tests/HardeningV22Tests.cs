using System.Net;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

/// <summary>Regressions for the seventeenth-review (.NET SDK hardening, v22)
/// defects: fail-open verdict, missing terminal Finished, caller-cancellation
/// idempotency, Dispose race, strict retry ceiling, options validation, and the
/// SSE resume cursor.</summary>
public class HardeningV22Tests
{
    // A blocked TERMINAL STATUS whose inbound_result OMITS "blocked" (so it
    // would default to false) — the exact fail-open shape.
    private const string InboundBlockedNoBlockedField =
        "{\"status\":\"inbound_blocked\",\"inbound_result\":{\"allowed\":false,\"triggered_rules\":[]}}";

    private static Func<RecordedRequest, int, ResponseSpec> ModerationRoute(string inboundBody) => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            return ResponseSpec.Ok(Bodies.JobCreated());
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get)
            return ResponseSpec.Ok(inboundBody);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    private static Func<RecordedRequest, int, ResponseSpec> StreamRoute(string inboundBody, Func<RecordedRequest, ResponseSpec>? chunk = null) => (rec, _) =>
    {
        if (rec.Path == "/v1/streaming/preflight")
            return ResponseSpec.Ok(Bodies.PreflightStreaming());
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            return ResponseSpec.Ok(Bodies.JobCreated("job_x"));
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get)
            return ResponseSpec.Ok(inboundBody);
        if (rec.Path.EndsWith("/chunks"))
            return (chunk ?? Responders.EchoChunk)(rec);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    // -- finding 1: fail-open verdict ----------------------------------------

    [Fact]
    public async Task Inbound_blocked_status_with_missing_blocked_field_is_blocked_not_allowed()
    {
        var (client, _) = TestClient.Create(ModerationRoute(InboundBlockedNoBlockedField));
        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });
        Assert.True(result.Blocked);
        Assert.False(result.Allowed);
    }

    [Fact]
    public async Task Fail_open_verdict_does_not_run_the_provider_factory()
    {
        var (client, _) = TestClient.Create(StreamRoute(InboundBlockedNoBlockedField));
        var factory = new CountingFactory("should", "not", "run");
        var events = new List<CollieStreamEvent>();
        await foreach (var ev in client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }))
            events.Add(ev);

        Assert.Equal(0, factory.Calls); // NEVER spend on an unallowed input
        Assert.IsType<InputBlocked>(Assert.Single(events));
    }

    // -- finding 5: every stream ends with a terminal event ------------------

    [Fact]
    public async Task Server_initiated_finish_still_emits_a_terminal_finished_event()
    {
        // The first chunk push returns finished:true (server finalized). The
        // stream must still close with a Finished event, not a bare SafeDelta.
        var (client, _) = TestClient.Create(StreamRoute(
            Bodies.InboundAllowed(),
            chunk: rec =>
            {
                using var doc = System.Text.Json.JsonDocument.Parse(rec.Body);
                int seq = doc.RootElement.GetProperty("sequence").GetInt32();
                return ResponseSpec.Ok(Bodies.ChunkEmit(seq, "hi", finished: true));
            }));
        var factory = new CountingFactory("hello");
        var events = new List<CollieStreamEvent>();
        await foreach (var ev in client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        }))
            events.Add(ev);

        Assert.Contains(events, e => e is SafeDelta);
        Assert.IsType<Finished>(events[^1]); // invariant: terminal event
    }

    // -- finding 4: caller cancellation closes the session -------------------

    [Fact]
    public async Task Cancelled_push_closes_the_session_and_does_not_free_the_sequence()
    {
        var (client, handler) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return Responders.EchoChunk(rec);
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await client.Streaming.CreateSessionAsync(
            new StreamingSessionRequest { Input = "context" });

        // Gate the chunk POST, then cancel the push while it's parked in-flight.
        handler.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource();
        Task<ChunkResult> parked = session.PushAsync("a", cts.Token);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await parked);

        handler.Gate.SetResult();
        // The sequence's fate is unknown, so the session is closed — a further
        // push must NOT reuse the sequence with different content.
        await Assert.ThrowsAsync<CollieException>(() => session.PushAsync("b"));
    }

    // -- finding 8: DisposeAsync is idempotent and race-clean ----------------

    [Fact]
    public async Task DisposeAsync_is_idempotent_and_leaves_a_clean_closed_session()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return Responders.EchoChunk(rec);
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "context" });
        await session.PushAsync("a");

        await session.DisposeAsync();
        await session.DisposeAsync(); // idempotent — no ObjectDisposedException

        // A push after dispose is a clean CollieException, never a raw
        // ObjectDisposedException from a disposed semaphore.
        var ex = await Assert.ThrowsAsync<CollieException>(() => session.PushAsync("b"));
        Assert.IsNotType<ObjectDisposedException>(ex);
    }

    // -- finding 3: strict retry ceiling -------------------------------------

    [Fact]
    public async Task Chunk_retry_ceiling_is_a_strict_boundary_no_post_once_spent()
    {
        // First chunk POST returns 429 Retry-After: 1; the ceiling is exactly
        // 1 s. The pre-sleep >= check must refuse to sleep to the boundary and
        // issue a second POST — exactly ONE chunk POST happens.
        int chunkPosts = 0;
        var (client, handler) = TestClient.Create(
            (rec, _) =>
            {
                if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
                if (rec.Path.EndsWith("/chunks"))
                {
                    chunkPosts++;
                    return ResponseSpec.Error(HttpStatusCode.TooManyRequests, "rate_limit", retryAfter: 1);
                }
                return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
            },
            advanceClockOnDelay: true,
            configure: o => { o.RetryCeilingPerChunk = TimeSpan.FromSeconds(1); o.MaxRetriesPerChunk = 5; });
        await using var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "context" });

        await Assert.ThrowsAsync<ChunkRetryExhaustedException>(() => session.PushAsync("x"));
        Assert.Equal(1, chunkPosts);
    }

    // -- finding 10: options validation --------------------------------------

    public static IEnumerable<object[]> BadOptions()
    {
        yield return new object[] { (Action<CollieClientOptions>)(o => o.Timeout = TimeSpan.Zero) };
        yield return new object[] { (Action<CollieClientOptions>)(o => o.RetryCeilingPerChunk = TimeSpan.FromSeconds(-1)) };
        yield return new object[] { (Action<CollieClientOptions>)(o => o.MaxRetriesPerChunk = 0) };
        yield return new object[] { (Action<CollieClientOptions>)(o => o.BaseBackoff = TimeSpan.Zero) };
        yield return new object[] { (Action<CollieClientOptions>)(o => o.MaxBackoff = TimeSpan.FromMilliseconds(1)) }; // < BaseBackoff (250ms)
        yield return new object[] { (Action<CollieClientOptions>)(o => o.MaxConnectionsPerServer = 0) };
    }

    [Theory]
    [MemberData(nameof(BadOptions))]
    public void Bad_numeric_options_are_rejected_at_construction(Action<CollieClientOptions> mutate)
    {
        Assert.Throws<ArgumentException>(() => TestClient.Create(
            (_, _) => ResponseSpec.Ok("{}"), configure: mutate));
    }

    // -- finding 11: SSE relay resume cursor ---------------------------------

    [Fact]
    public void Interrupted_relay_frame_carries_the_resume_cursor()
    {
        string frame = CollieSse.ToFrame(new StreamInterrupted
        {
            Reason = "disconnect",
            Resumable = true,
            LastEventId = "evt-42",
        });
        Assert.Contains("event: interrupted", frame);
        Assert.Contains("\"last_event_id\":\"evt-42\"", frame);
    }

    [Fact]
    public void Interrupted_relay_frame_omits_the_cursor_when_absent()
    {
        string frame = CollieSse.ToFrame(new StreamInterrupted { Reason = "disconnect", Resumable = true });
        Assert.DoesNotContain("last_event_id", frame);
    }
}
