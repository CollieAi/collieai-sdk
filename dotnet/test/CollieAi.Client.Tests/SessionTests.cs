using System.Net;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

public class SessionTests
{
    // Session job-create + echoing chunk endpoint.
    private static Func<RecordedRequest, int, ResponseSpec> EchoRoute() => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
            return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
        if (rec.Path.EndsWith("/chunks"))
            return Responders.EchoChunk(rec);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    private static Task<IStreamingSession> NewSessionAsync(CollieClient client) =>
        client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "context" });

    [Fact]
    public async Task Push_assigns_monotonic_sequences_and_returns_safe_emits()
    {
        var (client, _) = TestClient.Create(EchoRoute());
        await using var session = await NewSessionAsync(client);

        var r0 = await session.PushAsync("Hello");
        var r1 = await session.PushAsync(" world");

        Assert.Equal(0, r0.Sequence);
        Assert.Equal("Hello", Assert.Single(r0.Emits).Text);
        Assert.Equal(1, r1.Sequence);
        Assert.Equal(1, session.LastSequence);

        var final = await session.FinishAsync();
        Assert.True(final.Finished);
    }

    [Fact]
    public async Task Create_sends_input_job_id_when_provided_and_omits_it_otherwise()
    {
        var (client, handler) = TestClient.Create(EchoRoute());
        await using var withRef = await client.Streaming.CreateSessionAsync(
            new StreamingSessionRequest { Input = "context", InputJobId = "job_gate" });
        await using var without = await client.Streaming.CreateSessionAsync(
            new StreamingSessionRequest { Input = "context" });

        var creates = handler.Requests
            .Where(r => r.Path == "/v1/jobs" && r.Method == HttpMethod.Post)
            .ToList();
        Assert.Equal(2, creates.Count);
        using var first = System.Text.Json.JsonDocument.Parse(creates[0].Body);
        Assert.Equal("job_gate", first.RootElement.GetProperty("input_job_id").GetString());
        Assert.Equal("context", first.RootElement.GetProperty("message_input").GetString());
        using var second = System.Text.Json.JsonDocument.Parse(creates[1].Body);
        Assert.False(second.RootElement.TryGetProperty("input_job_id", out _));
    }

    [Fact]
    public async Task Finish_is_idempotent()
    {
        var (client, handler) = TestClient.Create(EchoRoute());
        await using var session = await NewSessionAsync(client);
        await session.PushAsync("hi");

        await session.FinishAsync();
        int chunksAfterFirstFinish = handler.CountWhere(r => r.Path.EndsWith("/chunks"));
        await session.FinishAsync();
        int chunksAfterSecondFinish = handler.CountWhere(r => r.Path.EndsWith("/chunks"));

        Assert.Equal(chunksAfterFirstFinish, chunksAfterSecondFinish); // no extra submit
    }

    [Fact]
    public async Task Push_after_finish_throws()
    {
        var (client, _) = TestClient.Create(EchoRoute());
        await using var session = await NewSessionAsync(client);
        await session.FinishAsync();

        await Assert.ThrowsAsync<ChunkSessionFinishedException>(() => session.PushAsync("x"));
    }

    [Fact]
    public async Task Transient_503_retries_same_sequence_without_duplicate_emit()
    {
        int chunkCalls = 0;
        var (client, handler) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks"))
            {
                chunkCalls++;
                return chunkCalls == 1
                    ? ResponseSpec.Error(HttpStatusCode.ServiceUnavailable, "transient")
                    : Responders.EchoChunk(rec);
            }
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        var r = await session.PushAsync("Hello");

        Assert.Equal("Hello", Assert.Single(r.Emits).Text); // exactly one emit, no duplicate
        var chunkBodies = handler.Requests.Where(x => x.Path.EndsWith("/chunks")).Select(x => x.Body).ToList();
        Assert.Equal(2, chunkBodies.Count);                  // retried
        Assert.All(chunkBodies, b => Assert.Contains("\"sequence\":0", b)); // same sequence replayed
    }

    [Fact]
    public async Task Rate_limited_with_retry_after_then_succeeds()
    {
        int chunkCalls = 0;
        var (client, handler) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks"))
            {
                chunkCalls++;
                return chunkCalls == 1
                    ? ResponseSpec.Error(HttpStatusCode.TooManyRequests, "rate_limited", retryAfter: 0)
                    : Responders.EchoChunk(rec);
            }
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        var r = await session.PushAsync("hi");

        Assert.Equal("hi", Assert.Single(r.Emits).Text);
        Assert.Equal(2, handler.CountWhere(x => x.Path.EndsWith("/chunks")));
    }

    [Fact]
    public async Task Rate_limited_without_retry_after_is_fatal()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return ResponseSpec.Error(HttpStatusCode.TooManyRequests, "rate_limited");
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        await Assert.ThrowsAsync<ChunkQuotaExceededException>(() => session.PushAsync("x"));
    }

    [Fact]
    public async Task Policy_changed_is_fatal()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return ResponseSpec.Error(HttpStatusCode.Conflict, "chunk_policy_changed");
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        await Assert.ThrowsAsync<ChunkPolicyChangedException>(() => session.PushAsync("x"));
    }

    [Fact]
    public async Task Retry_budget_exhaustion_throws_typed_error()
    {
        var (client, handler) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return ResponseSpec.Error(HttpStatusCode.ServiceUnavailable, "transient");
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        await Assert.ThrowsAsync<ChunkRetryExhaustedException>(() => session.PushAsync("x"));
        Assert.Equal(3, handler.CountWhere(r => r.Path.EndsWith("/chunks"))); // default MaxRetriesPerChunk
    }

    [Fact]
    public async Task Resolution_unavailable_retries_then_succeeds()
    {
        var chunkCalls = 0;
        var (client, handler) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks"))
            {
                chunkCalls++;
                return chunkCalls == 1
                    ? ResponseSpec.Error(HttpStatusCode.ServiceUnavailable, "chunk_resolution_unavailable")
                    : Responders.EchoChunk(rec);
            }
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        var r = await session.PushAsync("hi");

        Assert.Equal("hi", Assert.Single(r.Emits).Text);
        Assert.Equal(2, handler.CountWhere(x => x.Path.EndsWith("/chunks")));
    }

    [Fact]
    public async Task Resolution_unavailable_exhaustion_names_the_condition()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return ResponseSpec.Error(HttpStatusCode.ServiceUnavailable, "chunk_resolution_unavailable");
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        var ex = await Assert.ThrowsAsync<ChunkRetryExhaustedException>(() => session.PushAsync("x"));
        var cause = Assert.IsType<ChunkResolutionUnavailableException>(ex.InnerException);
        Assert.Equal("chunk_resolution_unavailable", cause.Code);
        Assert.Equal(503, cause.StatusCode);
    }

    [Fact]
    public async Task Overlapping_push_is_rejected()
    {
        var (client, handler) = TestClient.Create(EchoRoute());
        await using var session = await NewSessionAsync(client);

        // Gate the next request (the first push's chunk POST) so the lock stays held.
        handler.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<ChunkResult> first = session.PushAsync("a"); // acquires the lock, then parks on the gated POST

        await Assert.ThrowsAsync<ConcurrentSessionUseException>(async () => await session.PushAsync("b"));

        handler.Gate.SetResult();
        var r = await first;
        Assert.Equal("a", Assert.Single(r.Emits).Text);
    }

    [Fact]
    public async Task Malformed_chunk_response_missing_required_fields_is_rejected()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            // 200 OK but missing "finished" — must NOT silently deserialize to false.
            if (rec.Path.EndsWith("/chunks")) return ResponseSpec.Ok("{\"sequence\":0,\"accepted\":true,\"emits\":[]}");
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        var ex = await Assert.ThrowsAsync<CollieApiException>(() => session.PushAsync("x"));
        Assert.Equal("invalid_response", ex.Code);
    }

    [Fact]
    public async Task Chunk_response_with_null_emit_element_is_rejected_and_closes_session()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return ResponseSpec.Ok("{\"sequence\":0,\"accepted\":true,\"emits\":[null],\"finished\":false}");
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        var ex = await Assert.ThrowsAsync<CollieApiException>(() => session.PushAsync("x"));
        Assert.Equal("invalid_response", ex.Code);

        // The malformed accept must also have closed the session.
        var closed = await Assert.ThrowsAsync<CollieException>(() => session.PushAsync("y"));
        Assert.Contains("closed", closed.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Chunk_response_with_accepted_false_is_rejected()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/chunks")) return ResponseSpec.Ok("{\"sequence\":0,\"accepted\":false,\"emits\":[],\"finished\":false}");
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        var ex = await Assert.ThrowsAsync<CollieApiException>(() => session.PushAsync("x"));
        Assert.Equal("invalid_response", ex.Code);
    }

    [Fact]
    public async Task Chunk_response_with_mismatched_sequence_is_rejected()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            // Sent sequence 0; server echoes 7 — not a confirmed accept of our submit.
            if (rec.Path.EndsWith("/chunks")) return ResponseSpec.Ok("{\"sequence\":7,\"accepted\":true,\"emits\":[],\"finished\":false}");
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        var ex = await Assert.ThrowsAsync<CollieApiException>(() => session.PushAsync("x"));
        Assert.Equal("invalid_response", ex.Code);
    }

    [Fact]
    public async Task Mint_stream_token_builds_browser_url()
    {
        var (client, _) = TestClient.Create((rec, _) =>
        {
            if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
            if (rec.Path.EndsWith("/stream-token")) return ResponseSpec.Ok(Bodies.StreamToken("tok_xyz", 60));
            return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
        });
        await using var session = await NewSessionAsync(client);

        var token = await session.MintStreamTokenAsync();

        Assert.Equal("tok_xyz", token.Token);
        Assert.Equal(60, token.ExpiresIn);
        Assert.Equal("https://test.collieai.io/v1/jobs/job_s/stream?stream_token=tok_xyz", token.Url);
    }
}
