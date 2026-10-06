using System.Net;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

public class SseTests
{
    private static Task<IStreamingSession> NewSessionAsync(CollieClient client) =>
        client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "ctx" });

    private static async Task<List<CollieStreamEvent>> CollectAsync(IAsyncEnumerable<CollieStreamEvent> stream)
    {
        var list = new List<CollieStreamEvent>();
        await foreach (var ev in stream)
            list.Add(ev);
        return list;
    }

    private static Func<RecordedRequest, int, ResponseSpec> StreamRoute(Func<RecordedRequest, string> sseBody) => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
        if (rec.Path.EndsWith("/stream") && rec.Method == HttpMethod.Get) return ResponseSpec.Ok(sseBody(rec));
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    [Fact]
    public async Task Final_terminal_yields_deltas_then_finished()
    {
        const string sse = """
        id: 0
        event: chunk
        data: {"content":"Hello","sequence":0}

        id: 1
        event: chunk
        data: {"content":" world","sequence":1}

        event: end
        data: {"reason":"final"}
        """;
        var (client, _) = TestClient.Create(StreamRoute(_ => sse));
        await using var session = await NewSessionAsync(client);

        var events = await CollectAsync(session.StreamEventsAsync());

        Assert.Equal("Hello world", string.Concat(events.OfType<SafeDelta>().Select(d => d.Text)));
        Assert.IsType<Finished>(events[^1]);
        Assert.Empty(events.OfType<StreamInterrupted>());
    }

    [Fact]
    public async Task Reconnect_resumes_without_duplicate_visible_deltas()
    {
        const string firstConnection = """
        id: 0
        event: chunk
        data: {"content":"Hello","sequence":0}

        id: 1
        event: chunk
        data: {"content":" world","sequence":1}

        """;
        const string secondConnection = """
        id: 1
        event: chunk
        data: {"content":" world","sequence":1}

        id: 2
        event: chunk
        data: {"content":"!","sequence":2}

        event: end
        data: {"reason":"final"}
        """;
        var (client, _) = TestClient.Create(StreamRoute(rec => rec.HasLastEventId ? secondConnection : firstConnection));
        await using var session = await NewSessionAsync(client);

        var events = await CollectAsync(session.StreamEventsAsync());

        var texts = events.OfType<SafeDelta>().Select(d => d.Text).ToList();
        Assert.Equal(new[] { "Hello", " world", "!" }, texts); // " world" replayed but not re-emitted
        Assert.Single(events.OfType<StreamInterrupted>());
        Assert.IsType<Finished>(events[^1]);
    }

    [Fact]
    public async Task Blocked_frame_is_a_terminal_event_and_carries_block_message()
    {
        const string sse = """
        id: 0
        event: chunk
        data: {"content":"","blocked":true,"block_message":"Custom rule block message","sequence":0}

        event: end
        data: {"reason":"blocked"}
        """;
        var (client, _) = TestClient.Create(StreamRoute(_ => sse));
        await using var session = await NewSessionAsync(client);

        var events = await CollectAsync(session.StreamEventsAsync());

        var blockedEvent = Assert.IsType<Blocked>(Assert.Single(events));
        // F1 parity with Python/Node on the SSE path.
        Assert.Equal("Custom rule block message", blockedEvent.BlockMessage);
        Assert.Empty(events.OfType<Finished>());
    }

    [Fact]
    public async Task Malformed_frame_shape_is_a_typed_error_not_invalid_operation()
    {
        // Valid JSON, but a non-object chunk payload (`data: []`) — must surface as a
        // typed CollieApiException, not an InvalidOperationException from TryGetProperty.
        const string sse = """
        event: chunk
        data: []
        """;
        var (client, _) = TestClient.Create(StreamRoute(_ => sse));
        await using var session = await NewSessionAsync(client);

        var ex = await Assert.ThrowsAsync<CollieApiException>(() => CollectAsync(session.StreamEventsAsync()));
        Assert.Equal("invalid_response", ex.Code);
    }

    [Fact]
    public async Task Sse_chunk_with_null_triggered_rule_element_is_rejected()
    {
        const string sse = """
        event: chunk
        data: {"content":"hi","sequence":0,"triggered_rules":[null]}
        """;
        var (client, _) = TestClient.Create(StreamRoute(_ => sse));
        await using var session = await NewSessionAsync(client);

        var ex = await Assert.ThrowsAsync<CollieApiException>(() => CollectAsync(session.StreamEventsAsync()));
        Assert.Equal("invalid_response", ex.Code);
    }

    [Fact]
    public async Task Sse_chunk_with_non_array_triggered_rules_is_rejected()
    {
        const string sse = """
        event: chunk
        data: {"content":"hi","sequence":0,"triggered_rules":{}}
        """;
        var (client, _) = TestClient.Create(StreamRoute(_ => sse));
        await using var session = await NewSessionAsync(client);

        var ex = await Assert.ThrowsAsync<CollieApiException>(() => CollectAsync(session.StreamEventsAsync()));
        Assert.Equal("invalid_response", ex.Code);
    }

    [Fact]
    public async Task Session_unrecoverable_end_throws_typed_error()
    {
        const string sse = """
        event: end
        data: {"reason":"session_unrecoverable"}
        """;
        var (client, _) = TestClient.Create(StreamRoute(_ => sse));
        await using var session = await NewSessionAsync(client);

        await Assert.ThrowsAsync<ChunkSessionUnrecoverableException>(() => CollectAsync(session.StreamEventsAsync()));
    }

    // -- CollieSse.ToFrame: context verdict on relay frames (Slice 1f review) --

    [Fact]
    public void ToFrame_input_blocked_carries_context_verdict()
    {
        var frame = CollieSse.ToFrame(new InputBlocked
        {
            BlockMessage = "ctx",
            BlockedBy = "context",
            Context = new ContextModerationResult
            {
                Status = "blocked", Blocked = true,
                TriggeringPointer = "/transaction/title", TriggeringRuleType = "lightweight_model",
            },
        });

        Assert.StartsWith("event: input_blocked\n", frame);
        Assert.Contains("\"blocked_by\":\"context\"", frame);
        Assert.Contains("\"triggering_pointer\":\"/transaction/title\"", frame);
        Assert.Contains("\"status\":\"blocked\"", frame);
    }

    [Fact]
    public void ToFrame_finished_carries_context_verdict()
    {
        var frame = CollieSse.ToFrame(new Finished
        {
            FinishReason = "stop",
            BlockedBy = "none",
            Context = new ContextModerationResult { Status = "degraded", InferenceDegraded = true },
        });

        Assert.StartsWith("event: finished\n", frame);
        Assert.Contains("\"status\":\"degraded\"", frame);
        Assert.Contains("\"inference_degraded\":true", frame);
    }

    [Fact]
    public void ToFrame_finished_without_context_omits_keys()
    {
        var frame = CollieSse.ToFrame(new Finished { FinishReason = "stop" });

        Assert.DoesNotContain("context", frame);
        Assert.DoesNotContain("blocked_by", frame);
    }
}
