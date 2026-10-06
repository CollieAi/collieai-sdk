using System.Net;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

/// <summary>Twenty-second-review follow-up (v26): PBC-13 — a chunk whose error
/// body fails to read is a RETRYABLE transport failure, not a fatal HTTP status.
/// Closes the coverage gap the v25 matrix wrongly blamed on the fake harness.</summary>
public class HardeningV26Tests
{
    [Fact]
    public async Task Chunk_error_body_read_failure_is_retried_not_fatal()
    {
        int chunkPosts = 0;
        var (client, handler) = TestClient.Create(
            (rec, _) =>
            {
                if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
                if (rec.Path.EndsWith("/chunks"))
                {
                    chunkPosts++;
                    // 400 headers, then the body read throws (socket drop) — the
                    // response never fully arrived, so it must retry, not fail
                    // fatally on the HTTP 400.
                    return new ResponseSpec(HttpStatusCode.BadRequest, ContentFactory: () => new ThrowingContent());
                }
                return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
            },
            configure: o => { o.MaxRetriesPerChunk = 2; o.RetryCeilingPerChunk = TimeSpan.FromSeconds(100); });

        await using var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "context" });
        await Assert.ThrowsAsync<ChunkRetryExhaustedException>(() => session.PushAsync("x"));

        // Retried up to the budget, not one-and-done.
        Assert.Equal(2, chunkPosts);
    }

    // PBC-15a: the default time seam is the Stopwatch-based monotonic source.
    [Fact]
    public void Pbc15a_default_time_seam_is_monotonic()
    {
        // A client WITHOUT the test Monotonic hook uses the default Stopwatch seam.
        var http = new HttpClient(new FakeHandler((_, _) => ResponseSpec.Ok("{}")))
        {
            BaseAddress = new Uri("https://t.io"),
        };
        var client = new CollieClient(
            new CollieClientOptions { ApiKey = "clai_test", BaseUrl = new Uri("https://t.io") },
            http, ownsHttp: true, logger: null, hooks: null);
        double a = client.Monotonic();
        Assert.True(client.Monotonic() >= a);
        // A UtcNow-based seam would be ~1.7e9 s since the Unix epoch; the
        // Stopwatch seam is boot-relative (< 1e9 for any real uptime), so this
        // fails if the seam regresses to the wall clock.
        Assert.True(client.Monotonic() < 1e9);
    }

    // PBC-12b: the per-attempt cancellation bound is cooperative — a hung chunk
    // attempt is cancelled per-attempt (min(Timeout, ceiling)) and RETRIED.
    [Fact]
    public async Task Pbc12b_hung_chunk_attempt_is_cancelled_per_attempt_and_retried()
    {
        var (client, handler) = TestClient.Create(
            (rec, _) =>
            {
                if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
                if (rec.Path.EndsWith("/chunks")) return Responders.EchoChunk(rec); // never reached — gated
                return ResponseSpec.Error(HttpStatusCode.NotFound, "x");
            },
            configure: o =>
            {
                o.MaxRetriesPerChunk = 2;
                o.Timeout = TimeSpan.FromMilliseconds(40);       // per-attempt bound
                o.RetryCeilingPerChunk = TimeSpan.FromSeconds(100); // not the limiter
            });

        var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "context" });
        handler.Gate = new TaskCompletionSource(); // every chunk POST now hangs until cancelled

        await Assert.ThrowsAsync<ChunkRetryExhaustedException>(() => session.PushAsync("x"));

        // Each hung attempt was cancelled at the 40 ms per-attempt bound and retried.
        int chunkPosts = handler.Requests.Count(r => r.Path.EndsWith("/chunks"));
        Assert.Equal(2, chunkPosts);
        handler.Gate.SetResult(); // release any stragglers
    }
}
