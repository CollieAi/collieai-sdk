using System.Net;
using System.Text;
using System.Text.Json;
using CollieAi;
using Microsoft.Extensions.Logging;

namespace CollieAi.Client.Tests;

/// <summary>Common responder fragments.</summary>
internal static class Responders
{
    /// <summary>Echo a submitted chunk back as a single safe emit (or final-empty when is_final).</summary>
    public static ResponseSpec EchoChunk(RecordedRequest rec)
    {
        using var doc = JsonDocument.Parse(rec.Body);
        var root = doc.RootElement;
        int sequence = root.GetProperty("sequence").GetInt32();
        bool isFinal = root.GetProperty("is_final").GetBoolean();
        string content = root.TryGetProperty("content", out var c) ? (c.GetString() ?? "") : "";
        return isFinal
            ? ResponseSpec.Ok(Bodies.ChunkFinal(sequence))
            : ResponseSpec.Ok(Bodies.ChunkEmit(sequence, content, finished: false));
    }
}

/// <summary>A scripted HTTP response. <see cref="ContentFactory"/>, when set,
/// supplies the response content instead of a buffered string body — used to
/// inject a non-cooperative (hanging) body for the SSE idle-bound tests.</summary>
internal sealed record ResponseSpec(HttpStatusCode Status, string? Body = null, string? RequestId = null, int? RetryAfterSeconds = null, string? StreamingProtocol = null, string? RetryAfterRaw = null, Func<HttpContent>? ContentFactory = null)
{
    public static ResponseSpec Ok(string body, string? requestId = null) => new(HttpStatusCode.OK, body, requestId);

    public static ResponseSpec Error(HttpStatusCode status, string code, string message = "error", int? retryAfter = null) =>
        new(status, "{\"error\":{\"type\":\"" + code + "\",\"message\":\"" + message + "\"}}", RetryAfterSeconds: retryAfter);

    /// <summary>A response whose HEADERS arrive with <paramref name="status"/>
    /// but whose BODY read never completes (ignores the read token) — only the
    /// SDK's absolute WaitAsync idle bound can end it.</summary>
    public static ResponseSpec Hanging(HttpStatusCode status) => new(status, ContentFactory: () => new HangingContent());
}

/// <summary>A non-cooperative HttpContent: its body read ignores the read token
/// and completes only after a long, uncancellable delay, so a broken idle bound
/// would hang the read forever (and time the test out) rather than pass.</summary>
internal sealed class HangingContent : HttpContent
{
    private static readonly TimeSpan Block = TimeSpan.FromSeconds(30);
    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
        Task.Delay(Block, CancellationToken.None);
    protected override Task<Stream> CreateContentReadStreamAsync() =>
        Task.Delay(Block, CancellationToken.None).ContinueWith(
            static _ => (Stream)new MemoryStream(), TaskScheduler.Default);
    protected override bool TryComputeLength(out long length) { length = -1; return false; }
}

/// <summary>A body whose OPEN completes SUCCESSFULLY only after a delay, returning
/// a stream whose disposal is observable — for the late-SSE-stream-leak test
/// (v25). The read ignores the idle token, so it is abandoned by the idle bound
/// and completes AFTERWARDS; the SDK must dispose the late-returned stream.</summary>
internal sealed class LateStreamContent : HttpContent
{
    private readonly TimeSpan _delay;
    public readonly TrackedStream Stream = new();
    public LateStreamContent(TimeSpan delay) => _delay = delay;
    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
        Task.Delay(_delay, CancellationToken.None);
    protected override Task<Stream> CreateContentReadStreamAsync() =>
        Task.Delay(_delay, CancellationToken.None).ContinueWith(
            t => { _ = t; return (Stream)Stream; }, TaskScheduler.Default);
    protected override bool TryComputeLength(out long length) { length = -1; return false; }
}

/// <summary>A MemoryStream that records whether it was disposed.</summary>
internal sealed class TrackedStream : MemoryStream
{
    public bool Disposed { get; private set; }
    protected override void Dispose(bool disposing) { Disposed = true; base.Dispose(disposing); }
}

/// <summary>Headers arrive, but the BODY read throws — a socket dropped after the
/// headers. Under ResponseContentRead this surfaces inside the send as an
/// IOException, exercising the chunk retry's transport-failure branch (PBC-13).</summary>
internal sealed class ThrowingContent : HttpContent
{
    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
        Task.FromException(new IOException("connection reset mid-body"));
    protected override bool TryComputeLength(out long length) { length = -1; return false; }
}

internal sealed record RecordedRequest(HttpMethod Method, Uri Uri, string Body, IDictionary<string, string> Headers)
{
    public string Path => Uri.AbsolutePath;
    public string? Origin => Headers.TryGetValue("X-CollieAi-SDK-Origin", out var v) ? v : null;
    public bool HasLastEventId => Headers.ContainsKey("Last-Event-ID");
}

/// <summary>Records requests and returns scripted responses. Optionally gated on a TCS.</summary>
internal sealed class FakeHandler : HttpMessageHandler
{
    public readonly List<RecordedRequest> Requests = new();
    public TaskCompletionSource? Gate;

    private readonly Func<RecordedRequest, int, ResponseSpec> _responder;
    private int _count;

    public FakeHandler(Func<RecordedRequest, int, ResponseSpec> responder) => _responder = responder;

    public int CountWhere(Func<RecordedRequest, bool> predicate) => Requests.Count(predicate);

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        string body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var h in request.Headers)
            headers[h.Key] = string.Join(",", h.Value);
        var record = new RecordedRequest(request.Method, request.RequestUri!, body, headers);
        Requests.Add(record);

        var gate = Gate;
        if (gate is not null)
            // Cancellable so a hung-GET test behaves like a real transport:
            // aborting the request token must abort the in-flight request.
            await gate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        ResponseSpec spec = _responder(record, _count++);
        var resp = new HttpResponseMessage(spec.Status)
        {
            Content = spec.ContentFactory is not null
                ? spec.ContentFactory()
                : new StringContent(spec.Body ?? "", Encoding.UTF8, "application/json"),
        };
        if (spec.RequestId is not null)
            resp.Headers.TryAddWithoutValidation("x-request-id", spec.RequestId);
        if (spec.RetryAfterSeconds is not null)
            resp.Headers.TryAddWithoutValidation("Retry-After", spec.RetryAfterSeconds.Value.ToString());
        if (spec.RetryAfterRaw is not null)
            resp.Headers.TryAddWithoutValidation("Retry-After", spec.RetryAfterRaw);
        if (spec.StreamingProtocol is not null)
            resp.Headers.TryAddWithoutValidation("X-CollieAi-Streaming-Protocol", spec.StreamingProtocol);
        return resp;
    }
}

/// <summary>Virtual clock; optionally advanced by the injected Delay.</summary>
internal sealed class TestClock
{
    private DateTimeOffset _now = DateTimeOffset.UnixEpoch;
    private readonly bool _advanceOnDelay;
    public TestClock(bool advanceOnDelay) => _advanceOnDelay = advanceOnDelay;
    public DateTimeOffset Now => _now;
    public void Advance(TimeSpan d) { if (_advanceOnDelay) _now += d; }
}

internal static class TestClient
{
    public static (CollieClient Client, FakeHandler Handler) Create(
        Func<RecordedRequest, int, ResponseSpec> responder,
        bool advanceClockOnDelay = false,
        Action<CollieClientOptions>? configure = null,
        ILogger? logger = null,
        Func<double>? monotonic = null)
    {
        var handler = new FakeHandler(responder);
        var http = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://test.collieai.io"),
            Timeout = Timeout.InfiniteTimeSpan,
        };
        var options = new CollieClientOptions
        {
            ApiKey = "clai_test",
            BaseUrl = new Uri("https://test.collieai.io"),
            ProjectId = "project_123",
        };
        configure?.Invoke(options);

        var clock = new TestClock(advanceClockOnDelay);
        var hooks = new CollieTestHooks
        {
            Delay = (d, _) => { clock.Advance(d); return Task.CompletedTask; }, // instant; advances virtual clock
            PollDelay = (s, _) => { clock.Advance(TimeSpan.FromSeconds(s)); return Task.CompletedTask; }, // instant pacer sleeps
            Jitter = () => 0.0,                                                  // deterministic backoff
            UtcNow = () => clock.Now,
            // Monotonic seam driven by the SAME virtual clock (v22) — the chunk
            // retry ceiling and the delta batcher now key off Monotonic, so
            // Delay-advanced tests must see it move too. Overridable so a test
            // can simulate time passing DURING a request (v23 late-success).
            Monotonic = monotonic ?? (() => (clock.Now - DateTimeOffset.UnixEpoch).TotalSeconds),
        };
        var client = new CollieClient(options, http, ownsHttp: true, logger: logger, hooks: hooks);
        return (client, handler);
    }
}

/// <summary>Canonical JSON response bodies for the CollieAi wire protocol (plain concatenation — no brace pitfalls).</summary>
internal static class Bodies
{
    public static string JobCreated(string jobId = "job_1") =>
        "{\"job_id\":\"" + jobId + "\",\"status\":\"processing_inbound\"}";

    public static string InboundAllowed() =>
        "{\"status\":\"completed\",\"inbound_result\":{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[]}}";

    public static string InboundBlocked(string message = "blocked by policy") =>
        "{\"status\":\"inbound_blocked\",\"inbound_result\":{\"allowed\":false,\"blocked\":true,\"block_message\":\""
        + message + "\",\"triggered_rules\":[{\"rule_id\":\"r1\",\"rule_name\":\"R1\",\"rule_type\":\"regex\",\"decision\":\"block\",\"monitoring\":false}]}}";

    // Context analysis on the job-status root.
    public static string InboundWithContext(string contextStatus, bool blocked = false, string blockedBy = "none") =>
        "{\"status\":\"completed\",\"inbound_result\":{\"allowed\":true,\"blocked\":false,\"triggered_rules\":[]},"
        + "\"blocked_by\":\"" + blockedBy + "\","
        + "\"context_result\":{\"status\":\"" + contextStatus + "\",\"blocked\":" + (blocked ? "true" : "false")
        + ",\"limit_exceeded\":" + (contextStatus == "degraded" ? "true" : "false")
        + ",\"inference_degraded\":" + (contextStatus == "degraded" ? "true" : "false") + "}}";

    public static string InboundContextBlocked() =>
        "{\"status\":\"inbound_blocked\",\"inbound_result\":{\"allowed\":false,\"blocked\":true,\"block_message\":\"ctx\",\"triggered_rules\":[]},"
        + "\"blocked_by\":\"context\","
        + "\"context_result\":{\"status\":\"blocked\",\"blocked\":true,\"block_message\":\"ctx\","
        + "\"triggering_pointer\":\"/transaction/title\",\"triggering_rule_id\":\"r1\",\"triggering_rule_type\":\"lightweight_model\"}}";

    // Carries the explicit allow the real server always sends (jobs.py
    // _build_filtering_result); the buffered parser requires it (tech-debt #22).
    public static string OutboundCompleted(string filtered) =>
        "{\"status\":\"completed\",\"outbound_result\":{\"allowed\":true,\"blocked\":false,\"filtered_content\":\"" + filtered + "\",\"triggered_rules\":[]}}";

    public static string OutboundBlocked(string message = "output blocked") =>
        "{\"status\":\"outbound_blocked\",\"outbound_result\":{\"blocked\":true,\"block_message\":\"" + message + "\",\"triggered_rules\":[]}}";

    public static string ChunkEmit(int sequence, string content, bool finished = false) =>
        "{\"sequence\":" + sequence + ",\"accepted\":true,\"emits\":[{\"content\":\"" + content
        + "\",\"blocked\":false,\"final\":false,\"triggered_rules\":[]}],\"finished\":" + (finished ? "true" : "false") + "}";

    public static string ChunkBlocked(int sequence) =>
        "{\"sequence\":" + sequence + ",\"accepted\":true,\"emits\":[{\"content\":\"\",\"blocked\":true,\"final\":true,"
        + "\"block_message\":\"Custom rule block message\","
        + "\"triggered_rules\":[{\"rule_id\":\"r2\",\"rule_name\":\"R2\",\"rule_type\":\"regex\",\"decision\":\"block\",\"monitoring\":false}]}],\"finished\":true}";

    public static string ChunkFinal(int sequence) =>
        "{\"sequence\":" + sequence + ",\"accepted\":true,\"emits\":[],\"finished\":true}";

    public static string PreflightStreaming() =>
        "{\"mode\":\"streaming\",\"recommended_client_behavior\":\"stream\",\"project_id\":\"project_123\",\"valid_until\":\"2999-01-01T00:00:00Z\",\"rules\":[]}";

    public static string PreflightBuffered(string reason = "preset_buffered") =>
        "{\"mode\":\"buffered\",\"recommended_client_behavior\":\"buffer_then_show\",\"project_id\":\"project_123\",\"reason\":\""
        + reason + "\",\"reason_detail\":\"policy buffers\",\"valid_until\":\"2999-01-01T00:00:00Z\",\"rules\":[]}";

    public static string PreflightUnsupported(string reason = "plan_not_entitled") =>
        "{\"mode\":\"unsupported\",\"recommended_client_behavior\":\"fail_fast\",\"project_id\":\"project_123\",\"reason\":\""
        + reason + "\",\"reason_detail\":\"not entitled\"}";

    public static string StreamToken(string token = "tok_abc", int expiresIn = 60) =>
        "{\"stream_token\":\"" + token + "\",\"expires_in\":" + expiresIn + "}";
}

/// <summary>Provider stream fakes for ProtectStream/ProtectBuffered tests.</summary>
internal sealed class CountingFactory
{
    public int Calls;
    private readonly string[] _deltas;
    public CountingFactory(params string[] deltas) => _deltas = deltas;

    public Func<CancellationToken, IAsyncEnumerable<string>> Factory => ct =>
    {
        Interlocked.Increment(ref Calls);
        return Emit(_deltas, ct);
    };

    private static async IAsyncEnumerable<string> Emit(string[] deltas, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var d in deltas)
        {
            ct.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return d;
        }
    }
}
