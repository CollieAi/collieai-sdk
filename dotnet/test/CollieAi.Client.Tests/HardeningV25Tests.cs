using System.Net;
using System.Text.Json;
using CollieAi;
using Xunit;

namespace CollieAi.Client.Tests;

/// <summary>Twenty-first-review follow-ups (v25): a late-completing SSE body-open
/// stream is disposed (not leaked), and the shared verdict canon is driven
/// end-to-end through ProtectStreamAsync (does the wrapper run the provider).</summary>
public class HardeningV25Tests
{
    // -- finding 4: a late-completing SSE stream is disposed, not leaked ------

    private static Func<RecordedRequest, int, ResponseSpec> StreamOpenRoute(Func<ResponseSpec> onStream) => (rec, _) =>
    {
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
        if (rec.Path.EndsWith("/stream") && rec.Method == HttpMethod.Get) return onStream();
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    [Fact]
    public async Task Late_completing_sse_body_open_stream_is_disposed_not_leaked()
    {
        // The body OPEN completes ~150 ms AFTER the 50 ms idle bound abandons it.
        // ObserveStream must dispose the late-returned stream; the old Observe
        // (fault-only) leaked it.
        var content = new LateStreamContent(TimeSpan.FromMilliseconds(150));
        var (client, _) = TestClient.Create(StreamOpenRoute(() => new ResponseSpec(HttpStatusCode.OK, ContentFactory: () => content)));
        await using var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "ctx" });

        var events = new List<CollieStreamEvent>();
        await foreach (var ev in session.StreamEventsAsync(idleTimeout: TimeSpan.FromMilliseconds(50), maxReconnects: 0))
            events.Add(ev);

        Assert.Contains(events, e => e is StreamInterrupted si && si.Reason == "idle_timeout");

        // Wait for the abandoned open (150 ms) to complete and be disposed.
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (!content.Stream.Disposed && DateTime.UtcNow < deadline)
            await Task.Delay(20);
        Assert.True(content.Stream.Disposed, "the late-completing SSE stream was leaked");
    }

    // -- finding 5: the verdict canon gates the provider END-TO-END -----------

    public static IEnumerable<object?[]> VerdictProviderCanon()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "verdict_cases.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
        {
            yield return new object?[]
            {
                c.GetProperty("name").GetString()!,
                c.GetProperty("job").GetRawText(),
                c.GetProperty("expect").GetProperty("provider_runs").GetBoolean(),
            };
        }
    }

    private static Func<RecordedRequest, int, ResponseSpec> ProtectStreamRoute(string jobBody) => (rec, _) =>
    {
        if (rec.Path == "/v1/streaming/preflight") return ResponseSpec.Ok(Bodies.PreflightStreaming());
        if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post) return ResponseSpec.Ok(Bodies.JobCreated("job_x"));
        if (rec.Path.StartsWith("/v1/jobs/") && rec.Method == HttpMethod.Get) return ResponseSpec.Ok(jobBody);
        if (rec.Path.EndsWith("/chunks")) return Responders.EchoChunk(rec);
        return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
    };

    [Theory]
    [MemberData(nameof(VerdictProviderCanon))]
    public async Task Verdict_canon_gates_the_provider_end_to_end(string name, string jobBody, bool providerRuns)
    {
        var (client, _) = TestClient.Create(ProtectStreamRoute(jobBody));
        var factory = new CountingFactory("hello");
        var events = new List<CollieStreamEvent>();
        await foreach (var ev in client.Streaming.ProtectStreamAsync(new ProtectStreamRequest
        {
            Input = "x",
            RawStreamFactory = factory.Factory,
        }))
            events.Add(ev);

        if (providerRuns)
        {
            Assert.Equal(1, factory.Calls);
            Assert.DoesNotContain(events, e => e is InputBlocked);
        }
        else
        {
            Assert.Equal(0, factory.Calls);
            Assert.IsType<InputBlocked>(events[0]);
        }
        Assert.NotEqual("", name);
    }
}
