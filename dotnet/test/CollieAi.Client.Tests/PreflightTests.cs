using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

public class PreflightTests
{
    [Fact]
    public async Task Streaming_verdict_maps_to_stream_behavior()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightStreaming()));

        var cap = await client.Streaming.PreflightAsync(new StreamingPreflightRequest());

        Assert.Equal("streaming", cap.Mode);
        Assert.Equal(RecommendedClientBehavior.Stream, cap.RecommendedClientBehavior);
        Assert.Equal("project_123", cap.ProjectId);
    }

    [Fact]
    public async Task Buffered_verdict_maps_to_buffer_then_show()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightBuffered()));

        var cap = await client.Streaming.PreflightAsync(new StreamingPreflightRequest());

        Assert.Equal("buffered", cap.Mode);
        Assert.Equal(RecommendedClientBehavior.BufferThenShow, cap.RecommendedClientBehavior);
        Assert.Equal("preset_buffered", cap.Reason);
    }

    [Fact]
    public async Task Unsupported_verdict_maps_to_fail_fast()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightUnsupported()));

        var cap = await client.Streaming.PreflightAsync(new StreamingPreflightRequest());

        Assert.Equal("unsupported", cap.Mode);
        Assert.Equal(RecommendedClientBehavior.FailFast, cap.RecommendedClientBehavior);
    }

    [Fact]
    public async Task Result_is_cached_until_valid_until()
    {
        var (client, handler) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightStreaming()));

        await client.Streaming.PreflightAsync(new StreamingPreflightRequest());
        await client.Streaming.PreflightAsync(new StreamingPreflightRequest());

        Assert.Equal(1, handler.CountWhere(r => r.Path == "/v1/streaming/preflight"));
    }

    [Fact]
    public async Task Malformed_preflight_missing_project_id_is_rejected()
    {
        var (client, handler) = TestClient.Create((_, _) =>
            ResponseSpec.Ok("{\"mode\":\"streaming\",\"recommended_client_behavior\":\"stream\"}")); // no project_id

        var ex = await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Streaming.PreflightAsync(new StreamingPreflightRequest()));

        Assert.Equal("invalid_response", ex.Code);
        // A second call must not return a cached malformed verdict.
        await Assert.ThrowsAsync<CollieApiException>(() => client.Streaming.PreflightAsync(new StreamingPreflightRequest()));
        Assert.Equal(2, handler.CountWhere(r => r.Path == "/v1/streaming/preflight"));
    }

    [Fact]
    public async Task Preflight_with_unknown_recommended_behavior_is_rejected()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(
            "{\"mode\":\"streaming\",\"recommended_client_behavior\":\"teleport\",\"project_id\":\"project_123\"}"));

        await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Streaming.PreflightAsync(new StreamingPreflightRequest()));
    }

    [Fact]
    public async Task Preflight_with_null_rule_element_is_rejected()
    {
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(
            "{\"mode\":\"streaming\",\"recommended_client_behavior\":\"stream\",\"project_id\":\"project_123\",\"rules\":[null]}"));

        var ex = await Assert.ThrowsAsync<CollieApiException>(() =>
            client.Streaming.PreflightAsync(new StreamingPreflightRequest()));

        Assert.Equal("invalid_response", ex.Code);
    }

    [Fact]
    public async Task Force_refresh_bypasses_cache()
    {
        var (client, handler) = TestClient.Create((_, _) => ResponseSpec.Ok(Bodies.PreflightStreaming()));

        await client.Streaming.PreflightAsync(new StreamingPreflightRequest());
        await client.Streaming.PreflightAsync(new StreamingPreflightRequest { ForceRefresh = true });

        Assert.Equal(2, handler.CountWhere(r => r.Path == "/v1/streaming/preflight"));
    }
    // -----------------------------------------------------------------------
    // Forward compatibility of the preflight wire shape
    // -----------------------------------------------------------------------
    // The server adds fields to this response without an SDK release —
    // `execution_role` on `rules[]` shipped 2026-07-26 and was modelled here a
    // day later. Tolerance here is System.Text.Json's DEFAULT
    // UnmappedMemberHandling.Skip on reflection-based binding; in Python it is
    // pydantic's `extra="ignore"` and in Node a hand-written allowlist mapper.
    // Three different mechanisms, none previously pinned, so switching this
    // project to source-generated/AOT serialization — or setting
    // UnmappedMemberHandling.Disallow — would break only this one.
    //
    // Modelling `execution_role` MOVED the frontier rather than closing it: the
    // unknown thing is now a fifth ROLE. That is why ExecutionRole is a string
    // and not an enum, and why the unknown-VALUE case is pinned below.

    [Fact]
    public async Task Unknown_rule_field_does_not_break_deserialization()
    {
        const string body = """
        {
          "mode": "buffered",
          "recommended_client_behavior": "buffer_then_show",
          "project_id": "project_123",
          "streaming_mode": "auto",
          "reason": "preset_buffered",
          "rules": [{
            "rule_id": "r1", "rule_name": "n", "rule_type": "regex",
            "decision": "mask", "monitoring": true,
            "streaming_supported": false, "fallback_reason": "monitor_mode",
            "execution_role": "stream_observed",
            "some_future_field": { "nested": [1, 2, 3] }
          }]
        }
        """;
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(body));

        var cap = await client.Streaming.PreflightAsync(new StreamingPreflightRequest());

        Assert.Single(cap.Rules);
        Assert.Equal("r1", cap.Rules[0].RuleId);
        Assert.Equal("monitor_mode", cap.Rules[0].FallbackReason);
        Assert.Equal("stream_observed", cap.Rules[0].ExecutionRole);
    }

    [Fact]
    public async Task A_role_this_sdk_has_never_heard_of_still_deserializes()
    {
        // ExecutionRole is a string, not an enum. A fifth server-side role must
        // reach the caller verbatim so it can be handled as unknown.
        const string body = """
        {
          "mode": "buffered",
          "recommended_client_behavior": "buffer_then_show",
          "project_id": "project_123",
          "streaming_mode": "auto",
          "rules": [{
            "rule_id": "r1", "rule_name": "n", "rule_type": "regex",
            "decision": "mask", "monitoring": true,
            "streaming_supported": false, "fallback_reason": null,
            "execution_role": "enforce_midflight_v2"
          }]
        }
        """;
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(body));

        var cap = await client.Streaming.PreflightAsync(new StreamingPreflightRequest());

        Assert.Equal("enforce_midflight_v2", cap.Rules[0].ExecutionRole);
    }

    [Fact]
    public async Task A_rule_from_an_old_server_has_no_role_either()
    {
        // Round 3, T-04. The ABSENT-field case — an old server that has never
        // heard of execution_role — was exercised by NO test in this suite:
        // every fixture either set the field or had "rules":[], so a DTO
        // initializer of "stream_observed" would have survived everything.
        // Absence and explicit null must be the same answer.
        const string body = """
        {
          "mode": "buffered",
          "recommended_client_behavior": "buffer_then_show",
          "project_id": "project_123",
          "streaming_mode": "auto",
          "rules": [{
            "rule_id": "r1", "rule_name": "n", "rule_type": "regex",
            "decision": "mask", "monitoring": true,
            "streaming_supported": false, "fallback_reason": "monitor_mode"
          }]
        }
        """;
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(body));

        var cap = await client.Streaming.PreflightAsync(new StreamingPreflightRequest());

        Assert.Null(cap.Rules[0].ExecutionRole);
    }

    [Fact]
    public async Task An_unplannable_rule_reports_no_role()
    {
        // Null is a real answer, not a missing one: the server sends it for a
        // rule it could not plan at all. It must not become "".
        const string body = """
        {
          "mode": "buffered",
          "recommended_client_behavior": "buffer_then_show",
          "project_id": "project_123",
          "streaming_mode": "auto",
          "rules": [{
            "rule_id": "r1", "rule_name": "n", "rule_type": "who_knows",
            "decision": "mask", "monitoring": false,
            "streaming_supported": false, "fallback_reason": "unknown_rule_type",
            "execution_role": null
          }]
        }
        """;
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(body));

        var cap = await client.Streaming.PreflightAsync(new StreamingPreflightRequest());

        Assert.Null(cap.Rules[0].ExecutionRole);
    }

    [Fact]
    public async Task Unknown_top_level_field_does_not_break_it_either()
    {
        const string body = """
        {
          "mode": "streaming",
          "recommended_client_behavior": "stream",
          "project_id": "project_123",
          "streaming_mode": "auto",
          "rules": [],
          "observer_summary": { "stream_observed": 2 }
        }
        """;
        var (client, _) = TestClient.Create((_, _) => ResponseSpec.Ok(body));

        var cap = await client.Streaming.PreflightAsync(new StreamingPreflightRequest());

        Assert.Equal("streaming", cap.Mode);
    }

}
