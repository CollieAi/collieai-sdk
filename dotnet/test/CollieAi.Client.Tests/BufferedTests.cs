using System.Net;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

public class BufferedTests
{
    private static Func<RecordedRequest, int, ResponseSpec> Route(string inboundBody, string? outboundBody) => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
        {
            if (rec.Body.Contains("\"inbound_only\":true")) return ResponseSpec.Ok(Bodies.JobCreated("job_in"));
            if (rec.Body.Contains("message_output")) return ResponseSpec.Ok(Bodies.JobCreated("job_out"));
            return ResponseSpec.Error(HttpStatusCode.BadRequest, "bad_request");
        }
        if (rec.Path == "/v1/jobs/job_in" && rec.Method == HttpMethod.Get) return ResponseSpec.Ok(inboundBody);
        if (rec.Path == "/v1/jobs/job_out" && rec.Method == HttpMethod.Get) return ResponseSpec.Ok(outboundBody ?? "{}");
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    [Fact]
    public async Task Input_block_returns_input_blocked_result_without_calling_provider()
    {
        var (client, _) = TestClient.Create(Route(Bodies.InboundBlocked("nope"), outboundBody: null));
        var factory = new CountingFactory("response text");

        var result = await client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        });

        Assert.True(result.Blocked);
        Assert.True(result.InputBlocked);
        Assert.Equal("nope", result.BlockMessage);
        Assert.Equal(0, factory.Calls);
    }

    // The v24 fail-safe table on the buffered path (tech-debt #22): NOT
    // blocked requires an EXPLICIT allowed=true and no hard-block signal;
    // ambiguity resolves to blocked — same rows as CheckOutputAsync's table,
    // mirrored per-SDK. The first row is the #22 fix itself: an EMPTY
    // outbound_result used to fail-OPEN (Blocked=false).
    [Theory]
    [InlineData("completed", "{}", true)]
    [InlineData("completed", "{\"blocked\":false}", true)]                       // absent allowed
    [InlineData("completed", "{\"allowed\":false}", true)]                       // deny, blocked absent
    [InlineData("completed", "{\"allowed\":true,\"blocked\":false}", false)]     // explicit allow
    [InlineData("completed", "{\"allowed\":true,\"blocked\":true}", true)]       // inconsistent
    [InlineData("outbound_blocked", "{\"allowed\":true,\"blocked\":false}", true)] // status wins
    public async Task Buffered_verdict_table_is_fail_safe(string status, string outbound, bool expectBlocked)
    {
        string body = "{\"status\":\"" + status + "\",\"outbound_result\":" + outbound + "}";
        var (client, _) = TestClient.Create(Route(Bodies.InboundAllowed(), body));
        var factory = new CountingFactory("text");

        var result = await client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
        {
            Input = "prompt",
            CheckInput = false,
            RawStreamFactory = factory.Factory,
        });

        Assert.Equal(expectBlocked, result.Blocked);
    }

    [Fact]
    public async Task Buffered_non_bool_allowed_is_a_typed_error_never_an_allow()
    {
        // Documented per-SDK divergence (sdk-plan contract notes): Python/Node
        // treat a non-bool `allowed` as not-true → blocked; .NET's typed DTO
        // fails deserialization → typed error. Both are fail-safe; what must
        // never happen is a silent allow.
        string body = "{\"status\":\"completed\",\"outbound_result\":{\"allowed\":\"yes\",\"blocked\":false}}";
        var (client, _) = TestClient.Create(Route(Bodies.InboundAllowed(), body));
        var factory = new CountingFactory("text");

        var exc = await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
            {
                Input = "prompt",
                CheckInput = false,
                RawStreamFactory = factory.Factory,
            }));
        Assert.Equal("invalid_response", exc.Code);
    }

    [Fact]
    public async Task External_result_is_a_trusted_client_reuse_no_inbound_job_no_claim()
    {
        // The positive wire pin of the buffered trusted-client contract
        // (round 10): an external allowed result — carrying a REAL JobId —
        // skips the input pass entirely and claims nothing. Provider runs
        // once, the ONLY job created is the message_output one, and
        // input_job_id is sent nowhere (the absence that lets the server
        // neither verify nor consume the result).
        var (client, handler) = TestClient.Create(Route(Bodies.InboundAllowed(), Bodies.OutboundCompleted("clean")));
        var factory = new CountingFactory("ok");
        var ir = new InputModerationResult
        {
            Allowed = true, Blocked = false, OriginalText = "prompt", JobId = "job_gate",
        };

        var result = await client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
            InputResult = ir,
        });

        Assert.False(result.Blocked);
        Assert.Equal(1, factory.Calls);
        var create = Assert.Single(handler.Requests,
            r => r.Path == "/v1/jobs" && r.Method == HttpMethod.Post); // no inbound moderation job
        Assert.Contains("message_output", create.Body);
        Assert.DoesNotContain("inbound_only", create.Body);
        Assert.DoesNotContain("input_job_id", create.Body);
    }

    [Fact]
    public async Task Completed_returns_filtered_text()
    {
        var (client, _) = TestClient.Create(Route(Bodies.InboundAllowed(), Bodies.OutboundCompleted("clean text")));
        var factory = new CountingFactory("raw ", "model ", "text");

        var result = await client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        });

        Assert.False(result.Blocked);
        Assert.False(result.InputBlocked);
        Assert.Equal("clean text", result.FilteredText);
        Assert.Equal(1, factory.Calls);
    }

    [Fact]
    public async Task Success_carries_input_context_verdict()
    {
        // A monitored/degraded-but-allowed context is observable on the (output)
        // result, not only on a block (Slice 1f review).
        var (client, _) = TestClient.Create(Route(
            Bodies.InboundWithContext("degraded"), Bodies.OutboundCompleted("clean text")));
        var factory = new CountingFactory("raw");

        var result = await client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
            Context = new Dictionary<string, object> { ["a"] = "b" },
        });

        Assert.False(result.Blocked);
        Assert.NotNull(result.Context);
        Assert.Equal("degraded", result.Context!.Status);
        Assert.True(result.Context.InferenceDegraded);
    }

    [Fact]
    public async Task Outbound_result_with_null_triggered_rule_is_rejected()
    {
        var (client, _) = TestClient.Create(Route(
            Bodies.InboundAllowed(),
            "{\"status\":\"completed\",\"outbound_result\":{\"blocked\":false,\"filtered_content\":\"x\",\"triggered_rules\":[null]}}"));
        var factory = new CountingFactory("raw");

        var ex = await Assert.ThrowsAsync<CollieApiException>(() => client.Streaming.ProtectBufferedAsync(
            new ProtectBufferedRequest { Input = "p", RawStreamFactory = factory.Factory }));

        Assert.Equal("invalid_response", ex.Code);
    }

    [Fact]
    public async Task Output_block_returns_blocked_result()
    {
        var (client, _) = TestClient.Create(Route(Bodies.InboundAllowed(), Bodies.OutboundBlocked("bad output")));
        var factory = new CountingFactory("raw text");

        var result = await client.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
        {
            Input = "prompt",
            RawStreamFactory = factory.Factory,
        });

        Assert.True(result.Blocked);
        Assert.False(result.InputBlocked);
        Assert.Equal("bad output", result.BlockMessage);
    }
}
