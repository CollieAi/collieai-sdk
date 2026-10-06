// The CollieAi client and its HTTP / retry / idempotency engine.
//
// One HttpClient is reused for keep-alive across all calls. Chunk submission
// goes through PostChunkAsync, which owns the retry + idempotency policy from
// the SDK's retry and batching contract: retry the exact same
// sequence on transient failures with exponential backoff + jitter, bounded by
// an attempt budget and a wall-clock ceiling, mapping every wire error code to
// a typed exception.

using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using CollieAi.Internal;
using Microsoft.Extensions.Logging;

namespace CollieAi;

/// <summary>The top-level CollieAi client (stable public DI contract).</summary>
public interface ICollieClient : IAsyncDisposable
{
    /// <summary>Input-moderation operations (<c>CheckInputAsync</c>).</summary>
    ICollieModerationClient Moderation { get; }
    /// <summary>Customer-owned streaming operations (preflight, protect, sessions, SSE).</summary>
    ICollieStreamingClient Streaming { get; }
}

/// <summary>Test-only seams for deterministic clock/delay/jitter. Internal.</summary>
internal sealed class CollieTestHooks
{
    public Func<DateTimeOffset>? UtcNow { get; init; }
    /// <summary>Monotonic SECONDS (Poll Backoff Contract — the unit is normative).</summary>
    public Func<double>? Monotonic { get; init; }
    public Func<TimeSpan, CancellationToken, Task>? Delay { get; init; }
    /// <summary>Pacer sleeps in raw SECONDS — no TimeSpan quantization, so a
    /// virtual clock stays bit-identical to the reference simulator.</summary>
    public Func<double, CancellationToken, Task>? PollDelay { get; init; }
    public Func<double>? Jitter { get; init; }
}

/// <summary>
/// Async client for the CollieAi API. Construct directly for console/worker apps
/// (implements <see cref="IAsyncDisposable"/>; dispose with <c>await using</c>),
/// or register through <c>AddCollieAi(...)</c> for ASP.NET Core DI.
/// </summary>
public sealed class CollieClient : ICollieClient
{
    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly ILogger? _logger;
    private readonly TimeSpan _timeout;
    private readonly int _maxRetries;
    private readonly TimeSpan _retryCeiling;
    private readonly TimeSpan _baseBackoff;
    private readonly TimeSpan _maxBackoff;
    private readonly Func<double> _jitter;

    // Highest X-CollieAi-Streaming-Protocol version this SDK understands (F9).
    // When the server advertises a newer streaming protocol the SDK warns once
    // per client instead of failing, so a forward rollout never breaks an older
    // SDK. Missing/malformed header values are ignored — older servers omit it.
    internal const int SupportedStreamingProtocol = 1;
    private int _protocolWarned; // 0 = not yet warned; set once via Interlocked.

    // Internal seams used by the sub-clients.
    internal string? ProjectId { get; }
    internal string BaseUrlString { get; }
    internal Func<DateTimeOffset> UtcNow { get; }
    // Monotonic SECONDS for the poll pacer (normative: Stopwatch-based, never
    // wall clock — UtcNow can jump under NTP adjustment).
    internal Func<double> Monotonic { get; }
    internal Func<TimeSpan, CancellationToken, Task> Delay { get; }
    // Pacer sleeps take raw seconds (TimeSpan.FromSeconds rounds to whole
    // milliseconds — harmless in production, but the poll loops' virtual-clock
    // tests must accumulate the exact doubles the simulator does).
    internal Func<double, CancellationToken, Task> PollDelay { get; }
    internal Func<double> Jitter => _jitter;

    /// <summary>Input-moderation operations (<c>CheckInputAsync</c>).</summary>
    public ICollieModerationClient Moderation { get; }
    /// <summary>Customer-owned streaming operations (preflight, protect, sessions, SSE).</summary>
    public ICollieStreamingClient Streaming { get; }

    // Concrete moderation client, so the streaming wrapper can reuse its
    // input-check transport with a wrapper-specific SDK-origin.
    internal ModerationClient ModerationCore { get; }

    /// <summary>Construct a client that owns its <see cref="HttpClient"/>.</summary>
    public CollieClient(CollieClientOptions options)
        : this(options, BuildOwnedHttpClient(options), ownsHttp: true, logger: null, hooks: null)
    {
    }

    internal CollieClient(
        CollieClientOptions options,
        HttpClient http,
        bool ownsHttp,
        ILogger? logger,
        CollieTestHooks? hooks)
    {
        ValidateOptions(options);
        _http = http ?? throw new ArgumentNullException(nameof(http));
        _ownsHttp = ownsHttp;
        _logger = logger;
        ProjectId = options.ProjectId;
        BaseUrlString = (options.BaseUrl?.ToString() ?? "https://app.collieai.io").TrimEnd('/');
        _timeout = options.Timeout;
        _maxRetries = options.MaxRetriesPerChunk;
        _retryCeiling = options.RetryCeilingPerChunk;
        _baseBackoff = options.BaseBackoff;
        _maxBackoff = options.MaxBackoff;

        UtcNow = hooks?.UtcNow ?? (() => DateTimeOffset.UtcNow);
        Monotonic = hooks?.Monotonic
            ?? (() => System.Diagnostics.Stopwatch.GetTimestamp() / (double)System.Diagnostics.Stopwatch.Frequency);
        Delay = hooks?.Delay ?? ((d, c) => Task.Delay(d, c));
        PollDelay = hooks?.PollDelay ?? ((s, c) => Task.Delay(TimeSpan.FromSeconds(s), c));
        _jitter = hooks?.Jitter ?? (() => Random.Shared.NextDouble());

        var moderation = new ModerationClient(this);
        Moderation = moderation;
        ModerationCore = moderation;
        Streaming = new StreamingClient(this);
    }

    private static void ValidateOptions(CollieClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrEmpty(options.ApiKey))
            throw new ArgumentException("CollieClientOptions.ApiKey is required.", nameof(options));
        if (options.BaseUrl is null)
            throw new ArgumentException("CollieClientOptions.BaseUrl is required.", nameof(options));
        // Numeric knobs validated up front (v22): a bad value must fail at
        // construction, not deep inside an operation or with silently odd
        // semantics (e.g. a zero/negative timeout, ceiling, or retry count).
        // The runtime's CancelAfter/timer bound (~24.8 days) — a larger value
        // throws ArgumentOutOfRangeException deep in the first request (v23).
        var timerMax = TimeSpan.FromMilliseconds(int.MaxValue);
        static void Positive(TimeSpan v, string name)
        {
            if (v <= TimeSpan.Zero)
                throw new ArgumentException($"CollieClientOptions.{name} must be > 0.", nameof(options));
        }
        void Bounded(TimeSpan v, string name)
        {
            Positive(v, name);
            if (v > timerMax)
                throw new ArgumentException(
                    $"CollieClientOptions.{name} must be <= {timerMax.TotalDays:0.#} days (the runtime timer bound).", nameof(options));
        }
        Bounded(options.Timeout, nameof(options.Timeout));
        Bounded(options.RetryCeilingPerChunk, nameof(options.RetryCeilingPerChunk));
        Positive(options.BaseBackoff, nameof(options.BaseBackoff));
        Positive(options.MaxBackoff, nameof(options.MaxBackoff));
        if (options.MaxBackoff < options.BaseBackoff)
            throw new ArgumentException(
                "CollieClientOptions.MaxBackoff must be >= BaseBackoff.", nameof(options));
        if (options.MaxRetriesPerChunk < 1)
            throw new ArgumentException(
                "CollieClientOptions.MaxRetriesPerChunk must be >= 1.", nameof(options));
        if (options.MaxConnectionsPerServer < 1)
            throw new ArgumentException(
                "CollieClientOptions.MaxConnectionsPerServer must be >= 1.", nameof(options));
    }

    private static HttpClient BuildOwnedHttpClient(CollieClientOptions o)
    {
        ValidateOptions(o);
        var handler = new SocketsHttpHandler
        {
            MaxConnectionsPerServer = o.MaxConnectionsPerServer,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        };
        var http = new HttpClient(handler, disposeHandler: true)
        {
            BaseAddress = o.BaseUrl,
            // Per-request timeouts are applied by the SDK so a long output stream
            // is never killed by a global client timeout.
            Timeout = Timeout.InfiniteTimeSpan,
        };
        http.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", $"Bearer {o.ApiKey}");
        http.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", Wire.UserAgent);
        return http;
    }

    /// <summary>Disposes the owned <see cref="HttpClient"/> when the client created it.</summary>
    public ValueTask DisposeAsync()
    {
        if (_ownsHttp)
            _http.Dispose();
        return ValueTask.CompletedTask;
    }

    // -- project consistency check -------------------------------------------

    /// <summary>
    /// Validate a per-call project id. The API key scopes the project and job
    /// creation does not send one, so a per-call value is only meaningful as a
    /// consistency check. Reject anything we can't honor rather than ignore it.
    /// </summary>
    internal void CheckProject(string? projectId)
    {
        if (projectId is null)
            return;
        if (ProjectId is null)
            throw new ArgumentException(
                "project_id was passed per-call but the client was constructed without one, " +
                "so it can't be validated — the API key determines the project. Set ProjectId on CollieClientOptions.");
        if (projectId != ProjectId)
            throw new ArgumentException(
                $"project_id '{projectId}' does not match the client's project_id '{ProjectId}'.");
    }

    // -- single (non-chunk) request ------------------------------------------

    internal async Task<(T Body, string? RequestId)> RequestAsync<T>(
        HttpMethod method, string path, object? body, string origin, CancellationToken ct)
        where T : class
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(_timeout);

        HttpResponseMessage resp;
        try
        {
            resp = await SendCoreAsync(method, path, body, origin, HttpCompletionOption.ResponseContentRead, timeoutCts.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw; // genuine caller cancellation — not wrapped
        }
        catch (OperationCanceledException ex)
        {
            throw new CollieConnectionException($"Request to {path} timed out after {_timeout.TotalSeconds:0.#}s", ex);
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException)
        {
            throw new CollieConnectionException($"Request to {path} failed: {ex.Message}", ex);
        }

        using (resp)
        {
            CheckProtocol(resp);
            string content = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            string? requestId = GetRequestId(resp);
            if ((int)resp.StatusCode >= 400)
                throw ApiError((int)resp.StatusCode, content);
            if (string.IsNullOrEmpty(content))
                throw new CollieApiException(
                    "API returned an empty body where a JSON object was expected", (int)resp.StatusCode, "invalid_response");

            T? parsed;
            try
            {
                parsed = JsonSerializer.Deserialize<T>(content, Wire.Json);
            }
            catch (JsonException ex)
            {
                throw new CollieApiException(
                    "API returned a malformed (non-JSON) response", (int)resp.StatusCode, "invalid_response", ex);
            }
            if (parsed is null)
                throw new CollieApiException(
                    "API returned a null JSON body", (int)resp.StatusCode, "invalid_response");
            return (parsed, requestId);
        }
    }

    // -- status-poll transport primitive (Poll Backoff Contract) -------------

    /// <summary>
    /// One status-poll GET. Returns a parsed job for 2xx and <c>Job = null</c>
    /// for 429 — the poll loop owns the 429 pacing decision and needs the
    /// Retry-After delta plus the error content, which the ordinary error path
    /// consumes. Any other ≥400 throws the usual typed error; transport
    /// failures throw <see cref="CollieConnectionException"/> (the client's
    /// configured per-request Timeout applies untouched underneath). The
    /// caller passes its DEADLINE token (linked to the caller token +
    /// CancelAfter(remaining)); cancellation via that token is rethrown RAW so
    /// the loop can classify it structurally (caller vs deadline). The
    /// response is fully read and disposed here — a continued 429 never pins a
    /// pooled connection.
    /// </summary>
    /// <summary>
    /// RFC 9110 delay-seconds, strictly: decimal digits only → seconds. Our
    /// OWN parser on purpose (v9): the framework's typed
    /// <c>Headers.RetryAfter?.Delta</c> silently nulls a VALID-but-large
    /// delta (e.g. 2147483648 s overflows its Int32 backing), which would
    /// take the headerless pressure-floor path and re-poll after 1 s —
    /// violating the server's stated minimum and diverging from Python/Node
    /// (they parse the huge value and the pacer's feasibility check surfaces
    /// the typed 429). Enormous digit strings parse to a large double (or
    /// +Infinity) — same surfacing outcome in all three SDKs.
    /// </summary>
    internal static double? ParseRetryAfterSeconds(HttpResponseMessage resp)
    {
        if (!resp.Headers.TryGetValues("Retry-After", out var values))
            return null;
        string? raw = values.FirstOrDefault();
        if (string.IsNullOrEmpty(raw))
            return null;
        foreach (char c in raw)
        {
            if (c is < '0' or > '9')
                return null; // fractions/exponents/hex/date forms are unusable
        }
        return double.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out double seconds)
            ? seconds
            : double.PositiveInfinity; // digits-only but beyond double parsing — still a huge minimum
    }

    internal async Task<(JobStatusDto? Job, string? RequestId, double? RetryAfterS, string Content)> PollRequestAsync(
        string path, string origin, CancellationToken deadlineCt, CancellationToken callerCt)
    {
        // The transport timeout runs on a STANDALONE timer (not linked to
        // anything), so its fired state deterministically identifies the
        // transport timeout at classification time. A linked CTS checked via
        // IsCancellationRequested after the fact would race: with the default
        // knobs the transport timeout and the first poll deadline are both
        // 30 s and can expire in the same instant.
        using var transportCts = new CancellationTokenSource(_timeout);
        using var sendCts = CancellationTokenSource.CreateLinkedTokenSource(deadlineCt, transportCts.Token);

        // Cancellation priority matrix (normative, v7; provenance-gated in
        // v9): caller > transport > deadline > foreign.
        // The provenance gate runs FIRST: an OCE carrying a token that is
        // none of ours is a FOREIGN transport cancellation — a connection
        // error regardless of what our timers did meanwhile, so a foreign
        // TaskCanceledException cannot impersonate the deadline just because
        // the deadline happens to have fired. (Boundary: once our token HAS
        // fired, HttpClient itself normalizes any handler OCE to carry our
        // token — a foreign cancellation racing an already-fired deadline is
        // attributed to the deadline at this boundary; see the amendment.)
        // Rethrows of ours-attributed OCEs preserve the original stack via
        // ExceptionDispatchInfo.
        [System.Diagnostics.CodeAnalysis.DoesNotReturn]
        void ClassifyCancellation(OperationCanceledException ex, string what)
        {
            bool fromOurTokens = ex.CancellationToken == sendCts.Token
                || ex.CancellationToken == transportCts.Token
                || ex.CancellationToken == deadlineCt
                || ex.CancellationToken == callerCt;
            if (!fromOurTokens)
                throw new CollieConnectionException($"Request to {path} was cancelled by the transport {what}: {ex.Message}", ex);
            if (callerCt.IsCancellationRequested)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex).Throw(); // caller wins
            if (transportCts.IsCancellationRequested)
                throw new CollieConnectionException($"Request to {path} timed out after {_timeout.TotalSeconds:0.#}s", ex);
            if (deadlineCt.IsCancellationRequested)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex).Throw(); // the loop maps it
            // Ours-attributed token yet no source fired (theoretical): a
            // transport oddity, not a deadline claim.
            throw new CollieConnectionException($"Request to {path} was cancelled by the transport {what}: {ex.Message}", ex);
        }

        HttpResponseMessage resp;
        try
        {
            resp = await SendCoreAsync(HttpMethod.Get, path, null, origin, HttpCompletionOption.ResponseContentRead, sendCts.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            ClassifyCancellation(ex, "during send");
            throw; // unreachable
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException)
        {
            throw new CollieConnectionException($"Request to {path} failed: {ex.Message}", ex);
        }

        using (resp)
        {
            CheckProtocol(resp);
            // The body read is a second await under the same tokens: classify
            // its cancellation exactly like the send.
            string content;
            try
            {
                content = await resp.Content.ReadAsStringAsync(sendCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException ex)
            {
                ClassifyCancellation(ex, "reading the response body");
                throw; // unreachable
            }
            catch (Exception ex) when (ex is HttpRequestException or IOException)
            {
                throw new CollieConnectionException($"Request to {path} failed reading the response body: {ex.Message}", ex);
            }
            string? requestId = GetRequestId(resp);
            int status = (int)resp.StatusCode;
            if (status == 429)
            {
                return (null, requestId, ParseRetryAfterSeconds(resp), content);
            }
            if (status >= 400)
                throw ApiError(status, content);
            if (string.IsNullOrEmpty(content))
                throw new CollieApiException(
                    "API returned an empty body where a JSON object was expected", status, "invalid_response");

            JobStatusDto? parsed;
            try
            {
                parsed = JsonSerializer.Deserialize<JobStatusDto>(content, Wire.Json);
            }
            catch (JsonException ex)
            {
                throw new CollieApiException(
                    "API returned a malformed (non-JSON) response", status, "invalid_response", ex);
            }
            if (parsed is null)
                throw new CollieApiException("API returned a null JSON body", status, "invalid_response");
            return (parsed, requestId, null, content);
        }
    }

    // -- chunk submission with retry + idempotency ---------------------------

    private static readonly Dictionary<string, Func<string, int, string?, ChunkException>> FatalChunkCodes = new()
    {
        ["chunk_session_finished"] = (m, s, c) => new ChunkSessionFinishedException(m, s, c),
        ["chunk_sequence_conflict"] = (m, s, c) => new ChunkSequenceConflictException(m, s, c),
        ["chunk_idempotency_conflict"] = (m, s, c) => new ChunkIdempotencyConflictException(m, s, c),
        ["chunk_session_unrecoverable"] = (m, s, c) => new ChunkSessionUnrecoverableException(m, s, c),
        ["chunk_policy_changed"] = (m, s, c) => new ChunkPolicyChangedException(m, s, c),
        ["chunk_streaming_unsupported"] = (m, s, c) => new ChunkStreamingUnsupportedException(m, s, c),
    };

    internal async Task<(ChunkResponseDto Body, string? RequestId)> PostChunkAsync(
        string path, ChunkBody body, string origin, CancellationToken ct)
    {
        int attempts = 0;
        // MONOTONIC clock (v22): a wall-clock start (UtcNow) makes the retry
        // ceiling NTP-sensitive — a backward step could hang or shorten it.
        double startMono = Monotonic();
        double ceilingS = _retryCeiling.TotalSeconds;
        Exception? last = null;

        while (true)
        {
            // Pre-attempt STRICT ceiling (v22): once the budget is spent, no
            // further request is ISSUED (>= not >, checked BEFORE the send). The
            // first attempt (attempts == 0) always runs.
            double elapsedS = Monotonic() - startMono;
            if (attempts >= 1 && elapsedS >= ceilingS)
                throw new ChunkRetryExhaustedException(
                    $"Chunk retry ceiling ({_retryCeiling.TotalSeconds:0.#}s) exceeded", code: "chunk_retry_exhausted", innerException: last);
            attempts++;
            TimeSpan delay = TimeSpan.Zero;

            using (var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                // Bound each attempt by min(request timeout, remaining ceiling)
                // (v22): the ceiling was previously only checked AFTER a request
                // returned, so a single POST could run the full request timeout
                // (e.g. 30 s) well past a 10 s ceiling.
                double attemptS = Math.Max(0.001, Math.Min(_timeout.TotalSeconds, ceilingS - elapsedS));
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(attemptS));
                HttpResponseMessage? resp = null;
                try
                {
                    resp = await SendCoreAsync(HttpMethod.Post, path, body, origin, HttpCompletionOption.ResponseContentRead, timeoutCts.Token)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (OperationCanceledException ex)
                {
                    last = new CollieConnectionException($"Chunk POST to {path} timed out", ex);
                    delay = Backoff(attempts);
                }
                catch (Exception ex) when (ex is HttpRequestException or IOException)
                {
                    last = new CollieConnectionException($"Chunk POST to {path} failed: {ex.Message}", ex);
                    delay = Backoff(attempts);
                }

                if (resp is not null)
                {
                    using (resp)
                    {
                        CheckProtocol(resp);
                        string content = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                        if ((int)resp.StatusCode < 400)
                        {
                            string? requestId = GetRequestId(resp);
                            ChunkResponseDto? parsed;
                            try
                            {
                                parsed = JsonSerializer.Deserialize<ChunkResponseDto>(content, Wire.Json);
                            }
                            catch (JsonException ex)
                            {
                                throw new CollieApiException(
                                    "API returned a malformed chunk response", (int)resp.StatusCode, "invalid_response", ex);
                            }
                            if (parsed is null)
                                throw new CollieApiException(
                                    "API returned a null chunk response", (int)resp.StatusCode, "invalid_response");
                            if (parsed.Sequence is null || parsed.Accepted is null || parsed.Finished is null)
                                throw new CollieApiException(
                                    "API returned a malformed chunk response (missing sequence/accepted/finished)",
                                    (int)resp.StatusCode, "invalid_response");
                            // "Advance only after a confirmed accept": a 200 that doesn't
                            // confirm the exact submitted sequence is not a usable accept.
                            if (parsed.Accepted != true || parsed.Sequence != body.Sequence)
                                throw new CollieApiException(
                                    "API returned an unconfirmed/mismatched chunk response " +
                                    "(accepted must be true and sequence must echo the request)",
                                    (int)resp.StatusCode, "invalid_response");
                            // STRICT total-ceiling acceptance (v23): a 2xx whose
                            // round trip landed at/after the ceiling is rejected,
                            // mirroring the poll strict-acceptance boundary — the
                            // ceiling is a TOTAL wall-clock bound, not merely a
                            // pre-attempt gate (a success was previously accepted
                            // however late it arrived).
                            if (Monotonic() - startMono >= ceilingS)
                                throw new ChunkRetryExhaustedException(
                                    $"Chunk retry ceiling ({_retryCeiling.TotalSeconds:0.#}s) exceeded", code: "chunk_retry_exhausted", innerException: last);
                            return (parsed, requestId);
                        }

                        var decision = ClassifyChunkError(resp, content);
                        if (decision.Fatal)
                            throw decision.Exception;
                        last = decision.Exception;
                        delay = decision.RetryAfter ?? Backoff(attempts);
                    }
                }
            }

            // Reached only on a retryable outcome.
            if (attempts >= _maxRetries)
                throw new ChunkRetryExhaustedException(
                    $"Chunk retry budget exhausted after {attempts} attempts", code: "chunk_retry_exhausted", innerException: last);
            // Post-response STRICT ceiling (v22, >= not >): don't sleep to/past
            // the ceiling and then issue a request AT the boundary.
            if ((Monotonic() - startMono) + delay.TotalSeconds >= ceilingS)
                throw new ChunkRetryExhaustedException(
                    $"Chunk retry ceiling ({_retryCeiling.TotalSeconds:0.#}s) exceeded", code: "chunk_retry_exhausted", innerException: last);

            _logger?.LogDebug(
                "collieai chunk retry: attempt={Attempt} delayMs={DelayMs} seq={Sequence}",
                attempts, delay.TotalMilliseconds, body.Sequence);
            await Delay(delay, ct).ConfigureAwait(false);
        }
    }

    private (bool Fatal, Exception Exception, TimeSpan? RetryAfter) ClassifyChunkError(
        HttpResponseMessage resp, string content)
    {
        var (code, message) = ParseError(content);
        int status = (int)resp.StatusCode;

        if (status == 429)
        {
            TimeSpan? ra = resp.Headers.RetryAfter?.Delta; // numeric (delta) form only; HTTP-date is unsupported
            if (ra is { } d && d >= TimeSpan.Zero && d <= _retryCeiling)
                return (false, new ChunkQuotaExceededException(message ?? "rate limited", status, code), d);
            return (true, new ChunkQuotaExceededException(message ?? "rate limited (no usable Retry-After)", status, code), null);
        }

        if (code is not null && FatalChunkCodes.TryGetValue(code, out var factory))
            return (true, factory(message ?? code, status, code), null);

        if (code == "chunk_concurrent_submit")
            return (false, new ChunkConcurrentSubmitException(message ?? "concurrent submit", status, code), null);

        // 503: a resolver dependency was unreachable — an outage, not a policy
        // shape. Same retry treatment as any 5xx, but typed so the exhaustion
        // cause names the condition. Keyed on the code, like
        // chunk_concurrent_submit above.
        if (code == "chunk_resolution_unavailable")
            return (false, new ChunkResolutionUnavailableException(message ?? "policy resolution unavailable", status, code), null);

        if (status is 502 or 503 or 504)
            return (false, new CollieApiException(message ?? $"HTTP {status}", status, code), null);

        return (true, ApiError(status, content), null);
    }

    // -- SSE ------------------------------------------------------------------

    internal async Task<HttpResponseMessage> OpenSseAsync(
        string path, string? lastEventId, string origin, CancellationToken ct)
    {
        var req = new HttpRequestMessage(HttpMethod.Get, path);
        req.Headers.TryAddWithoutValidation("X-CollieAi-SDK-Origin", origin);
        req.Headers.TryAddWithoutValidation("Accept", "text/event-stream");
        if (lastEventId is not null)
            req.Headers.TryAddWithoutValidation("Last-Event-ID", lastEventId);
        // The response body is read by the caller after we return, so the request
        // message is not disposed here (collected with the response scope).
        var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        CheckProtocol(resp);
        return resp;
    }

    // -- low-level send + helpers --------------------------------------------

    private async Task<HttpResponseMessage> SendCoreAsync(
        HttpMethod method, string path, object? body, string origin, HttpCompletionOption completion, CancellationToken ct)
    {
        using var req = new HttpRequestMessage(method, path);
        req.Headers.TryAddWithoutValidation("X-CollieAi-SDK-Origin", origin);
        if (body is not null)
        {
            string json = JsonSerializer.Serialize(body, body.GetType(), Wire.Json);
            req.Content = new StringContent(json, Encoding.UTF8, "application/json");
        }
        return await _http.SendAsync(req, completion, ct).ConfigureAwait(false);
    }

    internal TimeSpan Backoff(int attempt)
    {
        // Exponential backoff with equal-jitter (attempt is 1-based).
        double rawMs = Math.Min(_maxBackoff.TotalMilliseconds, _baseBackoff.TotalMilliseconds * Math.Pow(2, attempt - 1));
        double jittered = (rawMs * 0.5) + (_jitter() * (rawMs * 0.5));
        return TimeSpan.FromMilliseconds(jittered);
    }

    internal CollieApiException ApiError(int status, string content)
    {
        var (code, message) = ParseError(content);
        return new CollieApiException(message ?? $"HTTP {status}", status, code);
    }

    internal static (string? Code, string? Message) ParseError(string? content)
    {
        if (string.IsNullOrEmpty(content))
            return (null, null);
        try
        {
            using var doc = JsonDocument.Parse(content);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return (null, null);
            if (!doc.RootElement.TryGetProperty("error", out var err) || err.ValueKind != JsonValueKind.Object)
                return (null, null);

            string? code = null;
            if (err.TryGetProperty("type", out var t) && t.ValueKind == JsonValueKind.String)
                code = t.GetString();
            if (code is null && err.TryGetProperty("code", out var c) && c.ValueKind == JsonValueKind.String)
                code = c.GetString();
            string? msg = err.TryGetProperty("message", out var m) && m.ValueKind == JsonValueKind.String
                ? m.GetString()
                : null;
            return (code, msg);
        }
        catch (JsonException)
        {
            return (null, null);
        }
    }

    private static string? GetRequestId(HttpResponseMessage resp) =>
        resp.Headers.TryGetValues("x-request-id", out var vals) ? vals.FirstOrDefault() : null;

    // -- streaming-protocol version check (F9) -------------------------------

    /// <summary>
    /// Warn once per client when the server speaks a newer streaming protocol
    /// than this SDK was built for (<c>X-CollieAi-Streaming-Protocol</c>, F9).
    /// Missing or malformed header values are ignored — older servers omit it.
    /// Thread-safe: the warning fires at most once even under concurrent calls.
    /// Invoked on the JSON, chunk, and SSE response paths.
    /// </summary>
    internal void CheckProtocol(HttpResponseMessage resp)
    {
        if (Volatile.Read(ref _protocolWarned) != 0)
            return;
        if (!resp.Headers.TryGetValues("X-CollieAi-Streaming-Protocol", out var values))
            return;
        if (!int.TryParse(values.FirstOrDefault(), NumberStyles.Integer, CultureInfo.InvariantCulture, out int server))
            return;
        if (server <= SupportedStreamingProtocol)
            return;
        if (Interlocked.Exchange(ref _protocolWarned, 1) != 0)
            return; // another thread already emitted the warning

        string message =
            $"CollieAi server speaks streaming protocol {server}; this CollieAi.Client " +
            $"SDK supports up to {SupportedStreamingProtocol}. Upgrade the CollieAi.Client " +
            "package to avoid contract drift.";
        // Best-effort: a custom logger that throws, or a closed stderr, must never
        // turn an SDK call that already succeeded on the wire into a logging
        // failure. This warning is diagnostics only.
        try
        {
            if (_logger is not null)
                _logger.LogWarning("{Message}", message);
            else
                Console.Error.WriteLine($"CollieAi.Client warning: {message}");
        }
        catch
        {
            // swallowed intentionally
        }
    }
}
