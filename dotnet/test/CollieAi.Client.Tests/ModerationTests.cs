using System.Net;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

public class ModerationTests
{
    private static Func<RecordedRequest, int, ResponseSpec> Route(string statusBody) => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            return ResponseSpec.Ok(Bodies.JobCreated(), requestId: "req_1");
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get)
            return ResponseSpec.Ok(statusBody);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    [Fact]
    public async Task Allowed_input_returns_allowed_result()
    {
        var (client, _) = TestClient.Create(Route(Bodies.InboundAllowed()));

        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hello" });

        Assert.True(result.Allowed);
        Assert.False(result.Blocked);
        Assert.Equal("hello", result.OriginalText);
        Assert.Equal("job_1", result.JobId);
        Assert.Equal("req_1", result.RequestId);
    }

    [Fact]
    public async Task Blocked_input_is_a_result_not_an_exception()
    {
        var (client, _) = TestClient.Create(Route(Bodies.InboundBlocked("nope")));

        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "bad" });

        Assert.True(result.Blocked);
        Assert.False(result.Allowed);
        Assert.Equal("nope", result.BlockMessage);
        Assert.Single(result.TriggeredRules);
        Assert.Equal("r1", result.TriggeredRules[0].RuleId);
    }

    [Fact]
    public async Task Propagates_conversation_correlation_ids_and_sdk_origin()
    {
        var (client, handler) = TestClient.Create(Route(Bodies.InboundAllowed()));

        await client.Moderation.CheckInputAsync(new InputModerationRequest
        {
            Prompt = "hi",
            ConversationId = "conv-1",
            CorrelationId = "corr-1",
        });

        var post = handler.Requests.First(r => r.Path == "/v1/jobs" && r.Method == HttpMethod.Post);
        Assert.Contains("\"conversation_id\":\"conv-1\"", post.Body);
        Assert.Contains("\"correlation_id\":\"corr-1\"", post.Body);
        Assert.Equal("moderate.input", post.Origin);
    }

    [Fact]
    public async Task Sends_structured_context_as_json_preserving_keys()
    {
        var (client, handler) = TestClient.Create(Route(Bodies.InboundAllowed()));

        await client.Moderation.CheckInputAsync(new InputModerationRequest
        {
            Prompt = "summarize my last payment",
            // camelCase key must survive verbatim (NOT snake_cased by the body
            // serializer's DictionaryKeyPolicy) — Slice-1 JSON-Pointer needs it.
            Context = new Dictionary<string, object>
            {
                ["transaction"] = new Dictionary<string, object> { ["titleNew"] = "hi" },
            },
            ContextFormat = "json",
        });

        var post = handler.Requests.First(r => r.Path == "/v1/jobs" && r.Method == HttpMethod.Post);
        Assert.Contains("\"context\":{\"transaction\":{\"titleNew\":\"hi\"}}", post.Body);
        Assert.Contains("\"context_format\":\"json\"", post.Body);
    }

    [Fact]
    public async Task Sends_string_context_with_format()
    {
        var (client, handler) = TestClient.Create(Route(Bodies.InboundAllowed()));

        await client.Moderation.CheckInputAsync(new InputModerationRequest
        {
            Prompt = "hi",
            Context = "raw blob",
            ContextFormat = "text",
        });

        var post = handler.Requests.First(r => r.Path == "/v1/jobs" && r.Method == HttpMethod.Post);
        Assert.Contains("\"context\":\"raw blob\"", post.Body);
        Assert.Contains("\"context_format\":\"text\"", post.Body);
    }

    [Fact]
    public async Task Omits_context_when_absent()
    {
        var (client, handler) = TestClient.Create(Route(Bodies.InboundAllowed()));

        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });

        var post = handler.Requests.First(r => r.Path == "/v1/jobs" && r.Method == HttpMethod.Post);
        Assert.DoesNotContain("\"context\"", post.Body);
        Assert.DoesNotContain("\"context_format\"", post.Body);
    }

    // -- Context analysis typed result --

    [Theory]
    [InlineData("not_provided")]
    [InlineData("disabled")]
    [InlineData("not_run")]
    [InlineData("clean")]
    [InlineData("monitored")]
    [InlineData("blocked")]
    [InlineData("degraded")]
    public async Task Context_status_parsed_for_every_enum(string status)
    {
        var (client, _) = TestClient.Create(Route(
            Bodies.InboundWithContext(status, blocked: status == "blocked",
                                      blockedBy: status == "blocked" ? "context" : "none")));

        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });

        Assert.NotNull(result.Context);
        Assert.Equal(status, result.Context!.Status);
        Assert.Equal(status == "blocked" ? "context" : "none", result.BlockedBy);
    }

    [Fact]
    public async Task Context_block_detail_is_mapped()
    {
        var (client, _) = TestClient.Create(Route(Bodies.InboundContextBlocked()));

        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "summarize" });

        Assert.True(result.Blocked);                 // aggregate inbound verdict
        Assert.Equal("context", result.BlockedBy);
        Assert.Equal("blocked", result.Context!.Status);
        Assert.Equal("/transaction/title", result.Context.TriggeringPointer);
        Assert.Equal("lightweight_model", result.Context.TriggeringRuleType);
    }

    [Fact]
    public async Task Context_degraded_markers_are_mapped()
    {
        var (client, _) = TestClient.Create(Route(Bodies.InboundWithContext("degraded")));

        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });

        Assert.Equal("degraded", result.Context!.Status);
        Assert.True(result.Context.LimitExceeded);
        Assert.True(result.Context.InferenceDegraded);
    }

    [Fact]
    public async Task No_context_result_means_null()
    {
        var (client, _) = TestClient.Create(Route(Bodies.InboundAllowed()));

        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });

        Assert.Null(result.Context);
        Assert.Null(result.BlockedBy);
    }

    [Fact]
    public async Task Rejects_non_finite_context()
    {
        // NaN isn't valid JSON -> deterministic ArgumentException (cross-SDK
        // parity), not a silently mangled body.
        var (client, _) = TestClient.Create(Route(Bodies.InboundAllowed()));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest
            {
                Prompt = "hi",
                Context = new Dictionary<string, object> { ["amount"] = double.NaN },
            }));
    }

    [Fact]
    public async Task Job_failed_throws_moderation_exception()
    {
        var (client, _) = TestClient.Create(Route("""{"status":"failed"}"""));

        await Assert.ThrowsAsync<ModerationException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "x" }));
    }

    [Fact]
    public async Task Inbound_result_with_null_triggered_rule_is_rejected()
    {
        var (client, _) = TestClient.Create(Route(
            "{\"status\":\"completed\",\"inbound_result\":{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[null]}}"));

        var ex = await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" }));

        Assert.Equal("invalid_response", ex.Code);
    }

    [Fact]
    public async Task Terminal_without_inbound_result_is_malformed()
    {
        var (client, _) = TestClient.Create(Route("""{"status":"completed"}"""));

        await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "x" }));
    }
}

// --- CheckOutputAsync -------------------------------------------------------

public class OutputModerationTests
{
    private static Func<RecordedRequest, int, ResponseSpec> Route(
        string statusBody, string createBody = "{\"job_id\":\"job_1\",\"status\":\"processing_outbound\"}") => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            return ResponseSpec.Ok(createBody, requestId: "req_1");
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get)
            return ResponseSpec.Ok(statusBody);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    private static string Terminal(string status, string outboundResult) =>
        "{\"status\":\"" + status + "\",\"outbound_result\":" + outboundResult + "}";

    [Fact]
    public async Task Allowed_output_carries_FilteredText_the_text_to_send()
    {
        var (client, _) = TestClient.Create(Route(Terminal("completed",
            "{\"allowed\":true,\"blocked\":false,\"filtered_content\":\"your card is [MASKED]\"," +
            "\"triggered_rules\":[{\"rule_id\":\"r9\",\"rule_name\":\"pii-mask\",\"rule_type\":\"regex\",\"decision\":\"mask\",\"monitoring\":false}]}")));

        var result = await client.Moderation.CheckOutputAsync(
            new OutputModerationRequest { Response = "your card is 4111-1111" });

        Assert.True(result.Allowed);
        Assert.False(result.Blocked);
        Assert.Equal("your card is 4111-1111", result.OriginalText);
        Assert.Equal("your card is [MASKED]", result.FilteredText);
        Assert.Equal("pii-mask", result.TriggeredRules[0].RuleName);
        Assert.Equal("job_1", result.JobId);
        Assert.Equal("req_1", result.RequestId);
    }

    [Fact]
    public async Task Blocked_output_is_a_result_not_an_exception()
    {
        var (client, _) = TestClient.Create(Route(Terminal("outbound_blocked",
            "{\"allowed\":false,\"blocked\":true,\"block_message\":\"Response blocked\",\"triggered_rules\":[]}")));

        var result = await client.Moderation.CheckOutputAsync(
            new OutputModerationRequest { Response = "the FORBIDDEN text" });

        Assert.True(result.Blocked);
        Assert.False(result.Allowed);
        Assert.Equal("Response blocked", result.BlockMessage);
    }

    // The v24 fail-safe table (sdk-plan § Output Moderation), row by row.
    [Theory]
    [InlineData("completed", "{\"allowed\":true,\"blocked\":false}", true)]
    [InlineData("completed", "{}", false)]                       // ambiguous -> blocked
    [InlineData("completed", "{\"blocked\":false}", false)]      // absent allowed -> blocked
    [InlineData("completed", "{\"allowed\":false}", false)]      // explicit deny
    [InlineData("outbound_blocked", "{\"allowed\":true,\"blocked\":false}", false)] // status wins
    public async Task Verdict_resolution_is_fail_safe(string status, string outbound, bool expectAllowed)
    {
        var (client, _) = TestClient.Create(Route(Terminal(status, outbound)));

        var result = await client.Moderation.CheckOutputAsync(
            new OutputModerationRequest { Response = "text" });

        Assert.Equal(expectAllowed, result.Allowed);
        Assert.Equal(!expectAllowed, result.Blocked);
    }

    [Fact]
    public async Task Terminal_job_without_outbound_result_is_a_typed_error()
    {
        var (client, _) = TestClient.Create(Route("{\"status\":\"completed\"}"));

        var ex = await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Moderation.CheckOutputAsync(new OutputModerationRequest { Response = "text" }));
        Assert.Equal("invalid_response", ex.Code);
    }

    [Theory]
    [InlineData("failed")]
    [InlineData("expired")]
    public async Task Dead_job_raises_ModerationException(string dead)
    {
        var (client, _) = TestClient.Create(Route("{\"status\":\"" + dead + "\"}"));

        await Assert.ThrowsAsync<ModerationException>(() =>
            client.Moderation.CheckOutputAsync(new OutputModerationRequest { Response = "text" }));
    }

    [Fact]
    public async Task Sends_message_output_only_with_the_new_origin()
    {
        var (client, handler) = TestClient.Create(Route(Terminal("completed",
            "{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[]}")));

        await client.Moderation.CheckOutputAsync(new OutputModerationRequest
        {
            Response = "hello",
            ConversationId = "conv-1",
            CorrelationId = "corr-1",
        });

        var post = handler.Requests.First(r => r.Path == "/v1/jobs" && r.Method == HttpMethod.Post);
        Assert.Contains("\"message_output\":\"hello\"", post.Body);
        Assert.DoesNotContain("message_input", post.Body);
        Assert.DoesNotContain("inbound_only", post.Body);
        Assert.DoesNotContain("context", post.Body);
        Assert.DoesNotContain("webhook_url", post.Body);
        Assert.Contains("\"conversation_id\":\"conv-1\"", post.Body);
        Assert.Contains("\"correlation_id\":\"corr-1\"", post.Body);
        Assert.Equal("moderate.output", post.Origin);
    }

    [Fact]
    public void The_request_has_structurally_no_context()
    {
        // The no-context decision (context is an input surface; the server
        // 400s an outbound-only job carrying one) is pinned against future
        // drift: adding the property is a conscious contract change, not a
        // copy-paste from InputModerationRequest.
        Assert.Null(typeof(OutputModerationRequest).GetProperty("Context"));
        Assert.Null(typeof(OutputModerationRequest).GetProperty("ContextFormat"));
        Assert.Null(typeof(OutputModerationResult).GetProperty("Context"));
        Assert.Null(typeof(OutputModerationResult).GetProperty("BlockedBy"));
    }

    [Fact]
    public async Task Project_mismatch_throws_before_any_request()
    {
        var (client, handler) = TestClient.Create(Route(Terminal("completed",
            "{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[]}")));

        await Assert.ThrowsAnyAsync<Exception>(() =>
            client.Moderation.CheckOutputAsync(new OutputModerationRequest
            {
                Response = "hello",
                ProjectId = "proj_other",
            }));
        Assert.DoesNotContain(handler.Requests, r => r.Method == HttpMethod.Post);
    }

    [Fact]
    public async Task Create_without_job_id_is_a_typed_error()
    {
        var (client, _) = TestClient.Create(Route(
            Terminal("completed", "{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[]}"),
            createBody: "{\"status\":\"accepted\"}"));

        var ex = await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Moderation.CheckOutputAsync(new OutputModerationRequest { Response = "hello" }));
        Assert.Equal("invalid_response", ex.Code);
    }
}

// --- TimeoutS (the public poll budget) --------------------------------------

public class ModerationTimeoutTests
{
    private static Func<RecordedRequest, int, ResponseSpec> NeverTerminal() => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            return ResponseSpec.Ok("{\"job_id\":\"job_t\",\"status\":\"processing_inbound\"}");
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get)
            return ResponseSpec.Ok("{\"job_id\":\"job_t\",\"status\":\"processing_inbound\"}");
        return ResponseSpec.Error(System.Net.HttpStatusCode.NotFound, "not_found");
    };

    [Fact]
    public async Task Input_TimeoutS_bounds_the_poll_budget()
    {
        // advanceClockOnDelay: without it the virtual monotonic clock is
        // FROZEN while PollDelay is instant — the deadline never arrives
        // logically and the test terminated only when a GET happened to
        // exceed the real 200ms CancelAfter (the true root of the flake).
        var (client, _) = TestClient.Create(NeverTerminal(), advanceClockOnDelay: true);

        // The MESSAGE pins the forwarding: the flow embeds its timeoutS in
        // the error text, so a broken wiring says "30" (the internal
        // default), not "0.2". A wall-clock bound proved flaky here — the
        // harness's PollDelay is virtual-instant, so elapsed time measures
        // scheduler contention, not the budget (reproduced at 14 s under a
        // loaded suite).
        var ex = await Assert.ThrowsAsync<ModerationException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest
            {
                Prompt = "hi",
                TimeoutS = 0.2,
            }));
        // Exact and culture-safe: the same interpolation the flow uses.
        Assert.Contains($"timed out after {0.2:0.###}s", ex.Message);
    }

    [Fact]
    public async Task Output_TimeoutS_bounds_the_poll_budget()
    {
        var (client, _) = TestClient.Create(NeverTerminal(), advanceClockOnDelay: true);

        var ex = await Assert.ThrowsAsync<ModerationException>(() =>
            client.Moderation.CheckOutputAsync(new OutputModerationRequest
            {
                Response = "hi",
                TimeoutS = 0.2,
            }));
        Assert.Contains($"timed out after {0.2:0.###}s", ex.Message);
    }

    [Theory]
    [InlineData(-1.0)]
    [InlineData(0.0)]
    [InlineData(double.NaN)]
    public async Task Invalid_TimeoutS_fails_before_any_request(double bad)
    {
        var (client, handler) = TestClient.Create(NeverTerminal());

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            client.Moderation.CheckInputAsync(new InputModerationRequest
            {
                Prompt = "hi",
                TimeoutS = bad,
            }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            client.Moderation.CheckOutputAsync(new OutputModerationRequest
            {
                Response = "hi",
                TimeoutS = bad,
            }));
        Assert.DoesNotContain(handler.Requests, r => r.Method == HttpMethod.Post);
    }

    [Fact]
    public async Task Null_TimeoutS_keeps_the_default_and_succeeds()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
                return ResponseSpec.Ok("{\"job_id\":\"job_t\",\"status\":\"processing_outbound\"}");
            return ResponseSpec.Ok(
                "{\"status\":\"completed\",\"outbound_result\":{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[]}}");
        });

        var result = await client.Moderation.CheckOutputAsync(
            new OutputModerationRequest { Response = "hi" });
        Assert.True(result.Allowed);
    }
}
