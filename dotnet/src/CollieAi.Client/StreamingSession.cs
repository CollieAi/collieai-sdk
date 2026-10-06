// Low-level customer-owned streaming session.
//
// Wraps the raw chunk-ingestion protocol: creates the job, owns the monotonic
// `sequence` numbers, serializes submits, retries transient failures with the
// SAME sequence (via the client), and exposes typed results. The customer pushes
// upstream-model text and forwards only the returned safe emits.
//
// The low-level session does NOT run input filtering — advanced callers who want
// input safety must call Moderation.CheckInputAsync(...) themselves. The
// higher-level wrappers (ProtectStreamAsync / ProtectBufferedAsync) add the
// bundled input check, batching, and preflight.

using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using CollieAi.Internal;

namespace CollieAi;

/// <summary>A customer-owned streaming session. Dispose with <c>await using</c>.</summary>
public interface IStreamingSession : IAsyncDisposable
{
    /// <summary>Backend job id for this session (null until created).</summary>
    string? JobId { get; }
    /// <summary>Server request id (<c>x-request-id</c>) from job creation.</summary>
    string? RequestId { get; }
    /// <summary>The highest chunk sequence number submitted so far (−1 before the first push).</summary>
    int LastSequence { get; }

    /// <summary>Submit one upstream-model chunk; returns the safe emits for it.</summary>
    Task<ChunkResult> PushAsync(string content, CancellationToken cancellationToken = default);

    /// <summary>Send the terminal chunk exactly once. Idempotent.</summary>
    Task<ChunkResult> FinishAsync(string finishReason = "stop", CancellationToken cancellationToken = default);

    /// <summary>Mint a short-lived, job-scoped browser SSE token (backend-side helper).</summary>
    Task<StreamToken> MintStreamTokenAsync(CancellationToken cancellationToken = default);

    /// <summary>Subscribe to this job's CollieAi SSE stream, yielding safe events.</summary>
    IAsyncEnumerable<CollieStreamEvent> StreamEventsAsync(
        string? lastEventId = null,
        bool autoResume = true,
        int maxReconnects = 5,
        TimeSpan? idleTimeout = null,
        CancellationToken cancellationToken = default);
}

internal sealed class StreamingSession : IStreamingSession
{
    private readonly CollieClient _c;
    private readonly string _input;
    private readonly string? _conversationId;
    private readonly string? _correlationId;
    private readonly string? _inputJobId;
    private readonly string _origin;

    private readonly SemaphoreSlim _lock = new(1, 1);
    private int _nextSequence;
    private bool _finished;
    private bool _closed;
    private readonly object _disposeGate = new();
    private Task? _disposeTask;
    private ChunkResult? _finalResult;
    private ChunkResult? _lastResult;

    public string? JobId { get; private set; }
    public string? RequestId { get; private set; }
    public int LastSequence { get; private set; } = -1;

    internal StreamingSession(
        CollieClient client, string input, string? conversationId, string? correlationId,
        string origin = Wire.OriginSession, string? inputJobId = null)
    {
        _c = client;
        _input = input;
        _conversationId = conversationId;
        _correlationId = correlationId;
        _inputJobId = string.IsNullOrEmpty(inputJobId) ? null : inputJobId;
        _origin = origin;
    }

    // -- lifecycle ------------------------------------------------------------

    internal async Task CreateAsync(CancellationToken ct)
    {
        if (JobId is not null)
            return;
        var body = new JobCreateBody
        {
            MessageInput = _input,
            ConversationId = _conversationId,
            CorrelationId = _correlationId,
            // Server-verified proof the prompt already passed inbound
            // filtering — skips the second pass (duplicate-inbound fix).
            InputJobId = _inputJobId,
        };
        var (created, requestId) = await _c.RequestAsync<JobCreateResponseDto>(
            HttpMethod.Post, "/v1/jobs", body, _origin, ct).ConfigureAwait(false);
        JobId = created.JobId;
        RequestId = requestId;
        if (string.IsNullOrEmpty(JobId))
            throw new CollieApiException("Job creation did not return a job_id", code: "invalid_response");
    }

    public ValueTask DisposeAsync()
    {
        // Idempotent AND fully race-safe (v23): all callers await the SAME
        // dispose task, so a SECOND concurrent DisposeAsync does not report
        // completion until the FIRST has actually finished (v22's
        // Interlocked-return let it complete early while a push was still
        // active). The core acquires the gate — waiting for any in-flight
        // push/finish — before aborting + disposing.
        lock (_disposeGate)
            _disposeTask ??= DisposeCoreAsync();
        return new ValueTask(_disposeTask);
    }

    private async Task DisposeCoreAsync()
    {
        await _lock.WaitAsync().ConfigureAwait(false);
        try
        {
            // Finalize a dangling session (real output streamed, never
            // terminated) so a no-webhook job isn't left to expire; THEN mark
            // unusable. AbortAsync's submit does not take the lock, so calling
            // it while we hold it is safe (no re-entrancy); it early-returns
            // once `_closed` is set, so it must run before we set it.
            await AbortAsync().ConfigureAwait(false);
            _closed = true;
        }
        finally
        {
            _lock.Release();
            _lock.Dispose();
        }
    }

    // -- public API -----------------------------------------------------------

    public async Task<ChunkResult> PushAsync(string content, CancellationToken cancellationToken = default)
    {
        if (_closed)
            throw new CollieException("Session is closed.");
        if (_finished)
            throw new ChunkSessionFinishedException(
                "Session already finished; create a new session to stream again.", code: "chunk_session_finished");

        if (!TryAcquire())
            throw new ConcurrentSessionUseException(
                "Overlapping push detected; chunk submission must be serial per session.");
        try
        {
            return await SubmitAsync(content, isFinal: false, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<ChunkResult> FinishAsync(string finishReason = "stop", CancellationToken cancellationToken = default)
    {
        if (_closed)
            throw new CollieException("Session is closed.");
        if (_finished)
            return CachedTerminal();

        if (!TryAcquire())
            throw new ConcurrentSessionUseException(
                "Overlapping finish detected; chunk submission must be serial per session.");
        try
        {
            if (_finished)
                return CachedTerminal();
            var result = await SubmitAsync(content: "", isFinal: true, cancellationToken, finishReason).ConfigureAwait(false);
            _finalResult = result;
            return result;
        }
        finally
        {
            _lock.Release();
        }
    }

    private ChunkResult CachedTerminal() =>
        _finalResult ?? _lastResult ?? throw new CollieException("Session finished without a result.");

    public async Task<StreamToken> MintStreamTokenAsync(CancellationToken cancellationToken = default)
    {
        if (JobId is null)
            throw new CollieException("MintStreamToken requires a created session (no job_id).");

        var (dto, _) = await _c.RequestAsync<StreamTokenDto>(
            HttpMethod.Post, $"/v1/jobs/{JobId}/stream-token", null, _origin, cancellationToken).ConfigureAwait(false);

        if (string.IsNullOrEmpty(dto.StreamToken))
            throw new CollieApiException("stream-token response missing or invalid stream_token", code: "invalid_response");
        if (dto.ExpiresIn is not int expiresIn || expiresIn <= 0)
            throw new CollieApiException("stream-token response missing or invalid expires_in", code: "invalid_response");

        string url = $"{_c.BaseUrlString}/v1/jobs/{JobId}/stream?stream_token={dto.StreamToken}";
        return new StreamToken { Token = dto.StreamToken, ExpiresIn = expiresIn, Url = url };
    }

    // -- SSE subscription -----------------------------------------------------

    public async IAsyncEnumerable<CollieStreamEvent> StreamEventsAsync(
        string? lastEventId = null,
        bool autoResume = true,
        int maxReconnects = 5,
        TimeSpan? idleTimeout = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (JobId is null)
            throw new CollieException("StreamEvents requires a created session (no job_id).");

        TimeSpan idle = idleTimeout ?? TimeSpan.FromSeconds(60);
        string path = $"/v1/jobs/{JobId}/stream";
        var seenIds = new HashSet<string>(StringComparer.Ordinal);
        string? currentLastId = lastEventId;
        int reconnects = 0;

        while (true)
        {
            string? interruptionReason = null;

            using (var idleCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                idleCts.CancelAfter(idle);

                HttpResponseMessage? resp = null;
                try
                {
                    resp = await _c.OpenSseAsync(path, currentLastId, _origin, idleCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                catch (OperationCanceledException) { interruptionReason = "idle_timeout"; }
                catch (Exception ex) when (ex is HttpRequestException or IOException) { interruptionReason = "disconnect"; }

                if (interruptionReason is null && resp is not null && (int)resp.StatusCode >= 400)
                {
                    // The error-body read is ABSOLUTELY idle-bounded (v23): an
                    // HttpContent that IGNORES cancellation would hang forever on
                    // the token alone, so the read task is raced against the idle
                    // token via WaitAsync. On timeout the abandoned read is
                    // observed and the response disposed (which aborts it).
                    string body;
                    Task<string> readTask = resp.Content.ReadAsStringAsync(cancellationToken);
                    try
                    {
                        body = await readTask.WaitAsync(idleCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        Observe(readTask);
                        resp.Dispose();
                        throw;
                    }
                    catch (Exception ex)
                    {
                        Observe(readTask);
                        resp.Dispose();
                        throw new CollieConnectionException(
                            $"SSE error response body read failed: {ex.Message}", ex);
                    }
                    var err = _c.ApiError((int)resp.StatusCode, body);
                    resp.Dispose();
                    throw err;
                }

                if (interruptionReason is null && resp is not null)
                {
                    bool terminal = false;
                    // The body OPEN is ABSOLUTELY idle-bounded and the response
                    // is released if it fails (v23): raced against the idle token
                    // so a non-cooperative content can't hang the open.
                    Stream? netStream = null;
                    Task<Stream> openTask = resp.Content.ReadAsStreamAsync(cancellationToken);
                    try
                    {
                        netStream = await openTask.WaitAsync(idleCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        ObserveStream(openTask);
                        resp.Dispose();
                        throw;
                    }
                    catch (OperationCanceledException) { ObserveStream(openTask); interruptionReason = "idle_timeout"; }
                    catch (Exception ex) when (ex is HttpRequestException or IOException) { ObserveStream(openTask); interruptionReason = "disconnect"; }
                    catch (Exception ex)
                    {
                        // An UNEXPECTED body-open failure must not leak the
                        // response (v24): the previous code only caught OCE /
                        // HttpRequestException / IOException, so any other
                        // exception escaped before the `netStream is null`
                        // dispose below, leaking the HttpResponseMessage.
                        ObserveStream(openTask);
                        resp.Dispose();
                        throw new CollieConnectionException(
                            $"SSE response body open failed: {ex.Message}", ex);
                    }

                    if (netStream is null)
                    {
                        resp.Dispose();
                    }
                    else
                    {
                    var reader = new StreamReader(netStream, Encoding.UTF8);
                    // Reset the idle window on every received line — including keepalive
                    // comments — so a healthy quiet stream isn't misread as idle_timeout.
                    var frames = Sse.ParseAsync(reader, onActivity: () => idleCts.CancelAfter(idle), ct: idleCts.Token)
                        .GetAsyncEnumerator(idleCts.Token);
                    idleCts.CancelAfter(idle); // reset idle window now that the body is open
                    try
                    {
                        while (true)
                        {
                            bool moved;
                            string? moveReason = null;
                            try
                            {
                                moved = await frames.MoveNextAsync().ConfigureAwait(false);
                                idleCts.CancelAfter(idle); // reset idle timer on activity
                            }
                            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
                            catch (OperationCanceledException) { moved = false; moveReason = "idle_timeout"; }
                            catch (Exception ex) when (ex is HttpRequestException or IOException) { moved = false; moveReason = "disconnect"; }

                            if (moveReason is not null) { interruptionReason = moveReason; break; }
                            if (!moved) { interruptionReason = "disconnect"; break; }

                            SseFrame frame = frames.Current;
                            if (frame.Id is not null)
                                currentLastId = frame.Id;

                            if (frame.EventType == "chunk")
                            {
                                if (frame.Id is not null && !seenIds.Add(frame.Id))
                                    continue; // replayed after resume — don't re-emit

                                ChunkFrame parsed;
                                try { parsed = ParseChunkFrame(frame.Data); }
                                catch (JsonException ex)
                                {
                                    throw new CollieApiException("Malformed SSE chunk frame", code: "invalid_response", innerException: ex);
                                }

                                if (parsed.Blocked)
                                {
                                    yield return new Blocked
                                    {
                                        BlockMessage = parsed.BlockMessage,
                                        TriggeredRules = parsed.TriggeredRules,
                                        JobId = JobId,
                                        ConversationId = _conversationId,
                                        CorrelationId = _correlationId,
                                    };
                                    terminal = true;
                                    break;
                                }
                                if (!string.IsNullOrEmpty(parsed.Text))
                                {
                                    yield return new SafeDelta
                                    {
                                        Text = parsed.Text,
                                        Sequence = parsed.Sequence,
                                        JobId = JobId,
                                        ConversationId = _conversationId,
                                        CorrelationId = _correlationId,
                                    };
                                }
                            }
                            else if (frame.EventType == "end")
                            {
                                string? endReason;
                                try { endReason = ParseEndReason(frame.Data); }
                                catch (JsonException ex)
                                {
                                    throw new CollieApiException("Malformed SSE end frame", code: "invalid_response", innerException: ex);
                                }

                                if (endReason is "final" or "session_finished")
                                {
                                    yield return new Finished
                                    {
                                        FinishReason = endReason,
                                        JobId = JobId,
                                        ConversationId = _conversationId,
                                        CorrelationId = _correlationId,
                                    };
                                    terminal = true;
                                    break;
                                }
                                if (endReason == "blocked") { terminal = true; break; } // Blocked already emitted via chunk
                                if (endReason == "upstream_error")
                                    throw new CollieApiException("Upstream stream error", code: "upstream_error");
                                if (endReason == "session_unrecoverable")
                                    throw new ChunkSessionUnrecoverableException(
                                        "The stream entered an unrecoverable state; create a new job to continue.",
                                        code: "session_unrecoverable");

                                interruptionReason = endReason == "idle_timeout" ? "idle_timeout" : "disconnect";
                                break;
                            }
                            // unknown event types are ignored
                        }
                    }
                    finally
                    {
                        await frames.DisposeAsync().ConfigureAwait(false);
                        reader.Dispose();
                        resp.Dispose();
                    }

                    if (terminal)
                        yield break;
                    }
                }
            }

            // Connection ended without a terminal frame — surface it as resumable.
            yield return new StreamInterrupted
            {
                Reason = interruptionReason ?? "disconnect",
                Resumable = true,
                LastEventId = currentLastId,
                JobId = JobId,
                ConversationId = _conversationId,
                CorrelationId = _correlationId,
            };

            if (!autoResume)
                yield break;
            reconnects++;
            if (reconnects > maxReconnects)
                yield break;
        }
    }

    private readonly record struct ChunkFrame(bool Blocked, string? BlockMessage, string Text, int Sequence, IReadOnlyList<TriggeredRule> TriggeredRules);

    // Throws JsonException on any malformed shape; callers map that to
    // CollieApiException(invalid_response). JsonElement.TryGetProperty throws
    // InvalidOperationException on a non-object root, so the root kind is checked
    // first to keep the failure inside the typed-error contract.
    private static ChunkFrame ParseChunkFrame(string data)
    {
        if (string.IsNullOrEmpty(data))
            return new ChunkFrame(false, null, "", -1, Array.Empty<TriggeredRule>());

        using var doc = JsonDocument.Parse(data);
        JsonElement root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
            throw new JsonException("SSE chunk frame data is not a JSON object");

        bool blocked = root.TryGetProperty("blocked", out var b) && b.ValueKind == JsonValueKind.True;
        // F1: "" and null both mean "no message" on the wire.
        string? blockMessage = root.TryGetProperty("block_message", out var bm)
            && bm.ValueKind == JsonValueKind.String && bm.GetString() is { Length: > 0 } message
            ? message : null;
        string text = root.TryGetProperty("content", out var c) && c.ValueKind == JsonValueKind.String ? (c.GetString() ?? "") : "";

        int sequence = -1;
        if (root.TryGetProperty("sequence", out var s) && s.ValueKind == JsonValueKind.Number && s.TryGetInt32(out int si))
            sequence = si;
        else if (text.Length > 0)
            // A content delta must carry a numeric sequence; otherwise it's malformed.
            throw new JsonException("SSE chunk frame with content is missing a numeric sequence");

        IReadOnlyList<TriggeredRule> rules = Array.Empty<TriggeredRule>();
        if (root.TryGetProperty("triggered_rules", out var tr) && tr.ValueKind != JsonValueKind.Null)
        {
            // Absent or null → no rules; present-but-not-an-array is a contract
            // violation rather than something to silently drop.
            if (tr.ValueKind != JsonValueKind.Array)
                throw new JsonException("SSE chunk frame triggered_rules is present but not an array");
            rules = tr.Deserialize<List<TriggeredRuleDto>>(Wire.Json).ToModels();
        }
        return new ChunkFrame(blocked, blockMessage, text, sequence, rules);
    }

    /// <summary>Non-blocking gate acquire that survives a concurrent dispose
    /// (v23): if the semaphore has already been disposed by DisposeAsync, a
    /// racing push/finish surfaces a clean "Session is closed" rather than a
    /// raw ObjectDisposedException from Wait(0).</summary>
    private bool TryAcquire()
    {
        try
        {
            return _lock.Wait(0);
        }
        catch (ObjectDisposedException)
        {
            throw new CollieException("Session is closed.");
        }
    }

    /// <summary>Fire-and-forget observe an abandoned read task (v23): after a
    /// WaitAsync idle-timeout the underlying read is abandoned and the response
    /// disposed; the eventual fault must be observed so it doesn't surface as an
    /// unobserved TaskException.</summary>
    private static void Observe(Task task)
    {
        _ = task.ContinueWith(
            static t => { _ = t.Exception; },
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>Observe an abandoned body-OPEN task (v25): like <see cref="Observe"/>
    /// it swallows an eventual fault, but if the read LATER completes
    /// successfully the returned stream would otherwise leak (a non-cooperative
    /// content that ignored the idle cancellation and finished after the response
    /// was disposed), so dispose it.</summary>
    private static void ObserveStream(Task<Stream> task)
    {
        _ = task.ContinueWith(
            static t =>
            {
                if (t.IsFaulted) { _ = t.Exception; }
                else if (t.Status == TaskStatus.RanToCompletion) { t.Result?.Dispose(); }
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private static string? ParseEndReason(string data)
    {
        if (string.IsNullOrEmpty(data))
            return null;
        using var doc = JsonDocument.Parse(data);
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            throw new JsonException("SSE end frame data is not a JSON object");
        return doc.RootElement.TryGetProperty("reason", out var r) && r.ValueKind == JsonValueKind.String ? r.GetString() : null;
    }

    // -- internals ------------------------------------------------------------

    private async Task<ChunkResult> SubmitAsync(string content, bool isFinal, CancellationToken ct, string? finishReason = null)
    {
        await CreateAsync(ct).ConfigureAwait(false);
        int sequence = _nextSequence;
        // finish_reason rides the terminal chunk only; empty == unset, like
        // the other opaque metadata fields. Persisted to the audit log.
        var body = new ChunkBody
        {
            Sequence = sequence,
            Content = content,
            IsFinal = isFinal,
            FinishReason = isFinal && !string.IsNullOrEmpty(finishReason) ? finishReason : null,
        };

        ChunkResult result;
        try
        {
            var (dto, requestId) = await _c.PostChunkAsync($"/v1/jobs/{JobId}/chunks", body, _origin, ct).ConfigureAwait(false);
            // Map inside the try so a malformed body discovered during mapping (e.g.
            // a null emits element) also closes the session.
            result = dto.ToModel(JobId, requestId ?? RequestId);
        }
        catch (CollieException)
        {
            // The chunk could not be confirmed (fatal error, retry exhausted, or a
            // malformed body). The job is no longer usable; fail the session. Do
            // NOT advance the sequence — it was never accepted.
            _closed = true;
            throw;
        }
        catch (OperationCanceledException)
        {
            // Caller cancellation: the chunk's fate is UNKNOWN — the server may
            // have already accepted the cancelled request. Reusing this sequence
            // for different content would violate idempotency (the server's
            // cached result for the sequence would differ from the replayed
            // body), so the session is no longer safe to continue (v22).
            _closed = true;
            throw;
        }

        LastSequence = sequence;
        _nextSequence = sequence + 1;
        _lastResult = result;
        if (result.Finished)
            _finished = true;
        return result;
    }

    /// <summary>
    /// Best-effort finalize: if real output was streamed (LastSequence &gt;= 0) but
    /// the stream never reached its own terminal, send the terminal chunk so a
    /// no-webhook job isn't left dangling. A job that never streamed a chunk is
    /// deliberately left to expire rather than faked as completed-empty. No-op if
    /// no job, already finished, or already closed. Swallows errors.
    /// </summary>
    internal async Task AbortAsync()
    {
        if (JobId is null || _finished || _closed || LastSequence < 0)
            return;
        try
        {
            var result = await SubmitAsync(content: "", isFinal: true, CancellationToken.None).ConfigureAwait(false);
            _finalResult = result;
        }
        catch
        {
            // best-effort cleanup — the job expires regardless
        }
    }
}
