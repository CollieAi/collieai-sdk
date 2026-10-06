using System.Net;
using CollieAi;
using Microsoft.Extensions.Logging;
using Xunit;

namespace CollieAi.Client.Tests;

// F9: the SDK warns once per client when the server advertises a newer
// X-CollieAi-Streaming-Protocol than it supports, and ignores a missing or
// malformed header value (older servers omit it). CheckProtocol is invoked on
// the JSON (RequestAsync), chunk (PostChunkAsync), and SSE (OpenSseAsync) paths,
// each of which has a dedicated test below so a future refactor can't silently
// drop a hook. The moderation JSON path (job create + poll = two responses) also
// exercises the warn-once latch across multiple responses, and a throwing logger
// must never break a call that already succeeded (best-effort diagnostics).
public class ProtocolVersionTests
{
    // Minimal ILogger that records warning-level messages.
    private sealed class CapturingLogger : ILogger
    {
        public readonly List<string> Warnings = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }

    // Job-create + poll, both responses carrying the given protocol header
    // (null = header absent).
    private static Func<RecordedRequest, int, ResponseSpec> Route(string? protocol) => (rec, _) =>
    {
        string body = rec.Method == HttpMethod.Post && rec.Path == "/v1/jobs"
            ? Bodies.JobCreated()
            : Bodies.InboundAllowed();
        return new ResponseSpec(HttpStatusCode.OK, body, StreamingProtocol: protocol);
    };

    [Fact]
    public async Task Newer_server_protocol_warns_exactly_once()
    {
        var logger = new CapturingLogger();
        var (client, _) = TestClient.Create(Route("2"), logger: logger);

        // Two server responses (create + poll), both advertising protocol 2.
        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });

        Assert.Single(logger.Warnings);
        Assert.Contains("streaming protocol 2", logger.Warnings[0]);
    }

    [Fact]
    public async Task Equal_server_protocol_does_not_warn()
    {
        var logger = new CapturingLogger();
        var (client, _) = TestClient.Create(Route("1"), logger: logger);

        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });

        Assert.Empty(logger.Warnings);
    }

    [Fact]
    public async Task Missing_protocol_header_does_not_warn()
    {
        var logger = new CapturingLogger();
        var (client, _) = TestClient.Create(Route(null), logger: logger);

        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });

        Assert.Empty(logger.Warnings);
    }

    [Fact]
    public async Task Malformed_protocol_header_does_not_warn()
    {
        var logger = new CapturingLogger();
        var (client, _) = TestClient.Create(Route("banana"), logger: logger);

        await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });

        Assert.Empty(logger.Warnings);
    }

    [Fact]
    public async Task Warning_emission_failure_does_not_break_a_successful_call()
    {
        // A custom logger that throws must never turn a valid API response into a
        // logging failure (best-effort diagnostics, matching the Python SDK).
        var (client, _) = TestClient.Create(Route("2"), logger: new ThrowingLogger());

        var result = await client.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = "hi" });

        Assert.True(result.Allowed); // succeeded despite the logger throwing on the warning
    }

    [Fact]
    public async Task Chunk_path_triggers_the_protocol_check()
    {
        // Header only on the chunk response, so the warning can only come from the
        // PostChunkAsync hook (job-create carries no header).
        var logger = new CapturingLogger();
        var (client, _) = TestClient.Create(
            (rec, _) => rec.Path.EndsWith("/chunks")
                ? Responders.EchoChunk(rec) with { StreamingProtocol = "2" }
                : ResponseSpec.Ok(Bodies.JobCreated("job_c")),
            logger: logger);

        await using var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "ctx" });
        await session.PushAsync("hello");

        Assert.Single(logger.Warnings);
    }

    [Fact]
    public async Task Sse_path_triggers_the_protocol_check()
    {
        // Header only on the SSE (/stream) response, so the warning can only come
        // from the OpenSseAsync hook.
        const string sse = """
        id: 0
        event: chunk
        data: {"content":"hi","sequence":0}

        event: end
        data: {"reason":"final"}
        """;
        var logger = new CapturingLogger();
        var (client, _) = TestClient.Create(
            (rec, _) =>
            {
                if (rec.Path == "/v1/jobs" && rec.Method == HttpMethod.Post)
                    return ResponseSpec.Ok(Bodies.JobCreated("job_s"));
                if (rec.Path.EndsWith("/stream") && rec.Method == HttpMethod.Get)
                    return new ResponseSpec(HttpStatusCode.OK, sse, StreamingProtocol: "2");
                return ResponseSpec.Error(HttpStatusCode.NotFound, "not_found");
            },
            logger: logger);

        await using var session = await client.Streaming.CreateSessionAsync(new StreamingSessionRequest { Input = "ctx" });
        await foreach (var _ in session.StreamEventsAsync()) { }

        Assert.Single(logger.Warnings);
    }

    // Logger that always throws, to prove warning emission is best-effort.
    private sealed class ThrowingLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            throw new InvalidOperationException("logger boom");
    }
}
