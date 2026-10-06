// Streaming client: preflight, the high-level ProtectStream / ProtectBuffered
// wrappers, and the low-level session factory.
//
// The product invariant:
// customers stream only CollieAi-released text, never raw model text. The
// RawStreamFactory is a deferred callable invoked exactly once, only after the
// input check (and preflight, if required) passes — so paid LLM work can't run
// ahead of a block, and raw deltas are never yielded.

using System.Runtime.CompilerServices;
using System.Text;
using CollieAi.Internal;

namespace CollieAi;

/// <summary>Customer-owned streaming + preflight.</summary>
public interface ICollieStreamingClient
{
    /// <summary>Resolve whether the project's policy can stream output, before calling the LLM.</summary>
    Task<StreamingCapability> PreflightAsync(StreamingPreflightRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Stream provider output through CollieAi, yielding only safe events
    /// (<see cref="SafeDelta"/> / <see cref="Blocked"/> / <see cref="InputBlocked"/> /
    /// <see cref="Finished"/>). A policy block is an event, not an exception.
    /// </summary>
    IAsyncEnumerable<CollieStreamEvent> ProtectStreamAsync(ProtectStreamRequest request, CancellationToken cancellationToken = default);

    /// <summary>Buffered fallback: check input, consume the whole response, return the final filtered/blocked result.</summary>
    Task<BufferedResult> ProtectBufferedAsync(ProtectBufferedRequest request, CancellationToken cancellationToken = default);

    /// <summary>Open a low-level session (advanced; manages batching/sequencing yourself).</summary>
    Task<IStreamingSession> CreateSessionAsync(StreamingSessionRequest request, CancellationToken cancellationToken = default);
}

internal sealed class StreamingClient : ICollieStreamingClient
{
    // Force a fresh preflight when less than this much validity remains, so a
    // token-stream UI never starts under a policy that may already have changed.
    private static readonly TimeSpan PreflightMinValidity = TimeSpan.FromSeconds(5);
    private static readonly HashSet<string> BufferedTerminal = new(StringComparer.Ordinal) { "completed", "outbound_blocked" };
    private static readonly HashSet<string> BufferedFailure = new(StringComparer.Ordinal) { "failed", "expired" };
    // Poll knobs are constants (not public API); tests override via
    // PollBufferedAsync's internal parameters.
    private const double PollIntervalS = 0.05;
    private const double PollTimeoutS = 30.0;

    private readonly CollieClient _c;
    private readonly object _cacheLock = new();
    private readonly Dictionary<string, StreamingCapability> _cache = new(StringComparer.Ordinal);

    public StreamingClient(CollieClient c) => _c = c;

    // -- preflight ------------------------------------------------------------

    public Task<StreamingCapability> PreflightAsync(StreamingPreflightRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return ResolveCapabilityAsync(request.ProjectId, TimeSpan.Zero, request.ForceRefresh, cancellationToken);
    }

    private async Task<StreamingCapability> ResolveCapabilityAsync(
        string? projectId, TimeSpan minValidity, bool forceRefresh, CancellationToken ct)
    {
        // Preflight DOES send project_id (the endpoint validates it against the
        // key), so unlike the job helpers we don't reject when the client lacks one.
        string key = projectId ?? _c.ProjectId ?? "";

        if (!forceRefresh)
        {
            lock (_cacheLock)
            {
                if (_cache.TryGetValue(key, out var cached) && cached.ValidUntil is { } validUntil
                    && (validUntil - _c.UtcNow()) > minValidity)
                {
                    return cached;
                }
            }
        }

        var body = new PreflightBody { ProjectId = projectId ?? _c.ProjectId };
        var (dto, _) = await _c.RequestAsync<StreamingCapabilityDto>(
            HttpMethod.Post, "/v1/streaming/preflight", body, Wire.OriginPreflight, ct).ConfigureAwait(false);

        // Strict validation (match the Python/Node SDKs): a malformed body must
        // surface as invalid_response, not be coerced to a fail-closed verdict and
        // cached. `mode` is required-present but its value is left open for
        // forward-compatibility; the recommended behavior must be a known value.
        if (string.IsNullOrEmpty(dto.Mode)
            || string.IsNullOrEmpty(dto.ProjectId)
            || dto.RecommendedClientBehavior is not ("stream" or "buffer_then_show" or "fail_fast"))
        {
            throw new CollieApiException("API returned a malformed preflight response", code: "invalid_response");
        }
        var capability = ToModel(dto);

        // Cache only cacheable results (the backend omits valid_until and sends
        // no-store for transient unsupported verdicts); evict otherwise so a later
        // call can't return a stale verdict.
        lock (_cacheLock)
        {
            if (capability.ValidUntil is not null)
                _cache[key] = capability;
            else
                _cache.Remove(key);
        }
        return capability;
    }

    private static StreamingCapability ToModel(StreamingCapabilityDto dto) => new()
    {
        Mode = dto.Mode ?? "unsupported",
        RecommendedClientBehavior = Mapping.ParseBehavior(dto.RecommendedClientBehavior),
        ProjectId = dto.ProjectId ?? "",
        StreamingMode = dto.StreamingMode,
        Reason = dto.Reason,
        ReasonDetail = dto.ReasonDetail,
        ValidUntil = dto.ValidUntil,
        Rules = dto.Rules.ToModels(), // rejects null elements as invalid_response
    };

    private static PreflightException PreflightError(StreamingCapability cap)
    {
        string detail = cap.ReasonDetail ?? cap.Reason ?? "streaming unavailable";
        return cap.Reason switch
        {
            "project_not_found" => new ProjectNotFoundException(detail),
            "plan_not_entitled" => new PlanNotEntitledException(detail),
            "unknown_rule_type" => new UnknownRuleTypeException(detail),
            "streaming_feature_disabled" => new StreamingFeatureDisabledException(detail),
            "policy_not_streamable" => new PolicyNotStreamableException(detail),
            _ => new PreflightException(detail),
        };
    }

    // -- high-level: protect_stream ------------------------------------------

    public IAsyncEnumerable<CollieStreamEvent> ProtectStreamAsync(ProtectStreamRequest request, CancellationToken cancellationToken = default)
    {
        // All argument validation happens synchronously here, before the iterator
        // is returned, so misuse surfaces immediately rather than on first iteration.
        ArgumentNullException.ThrowIfNull(request);
        _c.CheckProject(request.ProjectId);
        if (request.RawStreamFactory is null)
            throw new ProviderStreamFactoryRequiredException(
                "RawStreamFactory is required and must be a deferred factory — not an already-started stream — " +
                "so the provider cannot run before the input check can block it.");
        // Round-10 review: InputResult with CheckInput=false was silently
        // ignored — no gate reuse, no claim, the duplicate pass survived.
        // A contradiction must be loud, not a footgun.
        if (request.InputResult is not null && !request.CheckInput)
            throw new ArgumentException(
                "InputResult requires CheckInput = true: with CheckInput = false the result would be silently " +
                "ignored (no claim sent, the session re-filters the prompt).",
                nameof(request));
        if (request.InputResult is not null &&
            (request.InputResult.OriginalText != request.Input || !request.InputResult.Allowed))
            throw new ArgumentException(
                "InputResult must correspond to this Input and be allowed (OriginalText must match and Allowed must be true).",
                nameof(request));
        // A precomputed InputResult skips the input gate, so the wrapper can't
        // verify the passed Context matches the Context that produced that result.
        // Reject rather than re-analyze (or silently drop) it.
        if (request.InputResult is not null && (request.Context is not null || request.ContextFormat is not null))
            throw new ArgumentException(
                "Context cannot be combined with a precomputed InputResult: the wrapper can't verify the context " +
                "matches the precomputed result. Omit Context when reusing an already-computed result, or call " +
                "Moderation.CheckInputAsync with the desired context.",
                nameof(request));

        string? conversationId = Metadata.Normalize(request.ConversationId, nameof(request.ConversationId));
        string? correlationId = Metadata.Normalize(request.CorrelationId, nameof(request.CorrelationId));
        return ProtectStreamImpl(request, conversationId, correlationId, cancellationToken);
    }

    private async IAsyncEnumerable<CollieStreamEvent> ProtectStreamImpl(
        ProtectStreamRequest request, string? conversationId, string? correlationId,
        [EnumeratorCancellation] CancellationToken ct)
    {
        BatchingOptions batching = request.Batching ?? new BatchingOptions();

        // 0. require_streaming: refuse to start a token-stream UI under a policy
        //    that buffers (or can't be served), before any provider spend. The
        //    default path does NOT preflight — it streams optimistically and lets
        //    the chunk endpoint be the source of truth (a non-streamable policy
        //    surfaces as ChunkStreamingUnsupportedException on the first push).
        //    This matches the proven Python/Node SDKs; branch explicitly with
        //    PreflightAsync + ProtectBufferedAsync if you have buffered policies.
        if (request.RequireStreaming)
        {
            var capability = await ResolveCapabilityAsync(request.ProjectId, PreflightMinValidity, forceRefresh: false, ct)
                .ConfigureAwait(false);
            if (capability.Mode == "buffered")
                throw new BufferedFallbackRequiredException(
                    capability.ReasonDetail ?? capability.Reason ?? "policy requires buffered response checking");
            if (capability.Mode != "streaming")
                throw PreflightError(capability);
        }

        // 1. Input check (default). Block before any provider spend. Capture the
        //    INPUT-phase context verdict to carry on the success terminal too
        //    (Finished) — monitored/degraded-but-allowed context is observable.
        string? inputBlockedBy = null;
        ContextModerationResult? inputContext = null;
        string? gateJobId = null;
        if (request.CheckInput)
        {
            InputModerationResult result = request.InputResult
                ?? await _c.ModerationCore.RunAsync(request.Input, request.Context, request.ContextFormat, conversationId, correlationId, Wire.OriginProtectStream, ct)
                    .ConfigureAwait(false);
            inputBlockedBy = result.BlockedBy;
            inputContext = result.Context;
            // The gate's job id becomes the session job's input_job_id: the
            // server-verified proof that this exact prompt already passed
            // inbound filtering, so the session job skips its second pass.
            gateJobId = result.JobId;
            // Provider spend is gated on EXPLICIT allow (fail-safe): anything
            // not affirmatively allowed — OR an inconsistent Allowed+Blocked
            // (e.g. a caller-supplied InputResult) — blocks before the factory
            // runs (v23).
            if (!result.Allowed || result.Blocked)
            {
                yield return new InputBlocked
                {
                    BlockMessage = result.BlockMessage,
                    TriggeredRules = result.TriggeredRules,
                    JobId = result.JobId,
                    RequestId = result.RequestId,
                    ConversationId = conversationId,
                    CorrelationId = correlationId,
                    BlockedBy = result.BlockedBy,
                    Context = result.Context,
                };
                yield break;
            }
            if (request.InputResult is null)
                RejectMaskedInput(request.Input, result.FilteredText);
        }

        // 2. Create the Collie session FIRST so an enforceable job exists, then call
        //    the factory (exactly once) inside the protected block so any opened
        //    provider stream is always torn down — even if it starts paid work eagerly.
        var session = new StreamingSession(_c, request.Input, conversationId, correlationId, Wire.OriginProtectStream, inputJobId: gateJobId);
        // The claim-refusal protocol (server contract §10.2). Two 409 codes,
        // two remedies, all resolved BEFORE the provider starts:
        // - input_gate_stale: the policy moved (or the gate aged out) —
        //   re-gate ONCE with the same input and context. A SECOND stale on
        //   the retried create is positive proof of churn and THROWS with
        //   zero provider calls (round 5); only an unverifiable 409 there
        //   (a pin outage beginning mid-turn) downgrades.
        // - input_gate_unverifiable: a policy pin is missing (outage) —
        //   re-gating cannot help, downgrade to a claimless session
        //   immediately: our own gate ran milliseconds ago with the full
        //   context. This is the documented availability carve-out — the
        //   claimless session restores the pre-claim (v1) contract, whose
        //   async second pass can land after provider start; accepted for
        //   the outage case ONLY, where nothing contradicts the fresh
        //   client-side verdict (unlike a proven drift).
        // The ladder is bounded (at most one re-gate, one downgrade) — never
        // a loop. input_gate_claimed and every other code propagate
        // untouched. (The flag dance exists because C# forbids yield inside
        // catch.)
        //
        // The when-clauses require InputResult == null: an EXTERNAL result
        // may have been produced WITH a context this wrapper never saw
        // (Context+InputResult is rejected at the API edge precisely because
        // the pairing is unverifiable) — neither a prompt-only re-gate nor a
        // silent downgrade is honest. The typed error surfaces; the caller
        // re-gates with its own context.
        bool needRegate = false;
        bool needClaimless = false;
        try
        {
            await session.CreateAsync(ct).ConfigureAwait(false);
        }
        catch (CollieApiException e) when (IsClaimRefusal(e) && request.InputResult is null)
        {
            if (e.Code == "input_gate_unverifiable") needClaimless = true;
            else needRegate = true;
        }
        if (needClaimless)
        {
            session = new StreamingSession(_c, request.Input, conversationId, correlationId, Wire.OriginProtectStream);
            await session.CreateAsync(ct).ConfigureAwait(false);
        }
        else if (needRegate)
        {
            InputModerationResult regate = await _c.ModerationCore.RunAsync(
                request.Input, request.Context, request.ContextFormat,
                conversationId, correlationId, Wire.OriginProtectStream, ct).ConfigureAwait(false);
            inputBlockedBy = regate.BlockedBy;
            inputContext = regate.Context;
            if (!regate.Allowed || regate.Blocked)
            {
                // The CURRENT policy blocks this input — the stale 409 did its
                // job: the verdict arrived before any provider spend.
                yield return new InputBlocked
                {
                    BlockMessage = regate.BlockMessage,
                    TriggeredRules = regate.TriggeredRules,
                    JobId = regate.JobId,
                    RequestId = regate.RequestId,
                    ConversationId = conversationId,
                    CorrelationId = correlationId,
                    BlockedBy = regate.BlockedBy,
                    Context = regate.Context,
                };
                yield break;
            }
            RejectMaskedInput(request.Input, regate.FilteredText);
            session = new StreamingSession(_c, request.Input, conversationId, correlationId, Wire.OriginProtectStream, inputJobId: regate.JobId);
            try
            {
                await session.CreateAsync(ct).ConfigureAwait(false);
            }
            catch (CollieApiException e2) when (e2.Code == "input_gate_unverifiable")
            {
                // A second STALE is proven churn — never race the provider
                // against it (round 5); only the unverifiable outage
                // carve-out downgrades. Every other code propagates.
                session = new StreamingSession(_c, request.Input, conversationId, correlationId, Wire.OriginProtectStream);
                await session.CreateAsync(ct).ConfigureAwait(false);
            }
        }

        using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        IAsyncEnumerator<string>? providerEnumerator = null;
        IAsyncEnumerator<string>? batchEnumerator = null;
        try
        {
            // Factory: invoked exactly once, after the gate. Validate it returned a
            // usable stream rather than letting a null escape as a NullReferenceException.
            IAsyncEnumerable<string>? providerStream = request.RawStreamFactory(streamCts.Token);
            if (providerStream is null)
                throw new ProviderStreamFactoryRequiredException(
                    "RawStreamFactory returned null; it must return an async stream of text deltas.");
            providerEnumerator = providerStream.GetAsyncEnumerator(streamCts.Token);
            // Drive the batcher manually (not `await foreach`) so the finally can
            // cancel the provider token BEFORE disposing the batcher — otherwise the
            // batcher's disposal awaits an in-flight provider read and cleanup can
            // hang on a provider still waiting for the next token.
            batchEnumerator = DeltaBatcher
                .BatchAsync(providerEnumerator, batching, _c.Delay, _c.Monotonic, streamCts.Token)
                .GetAsyncEnumerator(streamCts.Token);

            bool blocked = false;
            bool serverFinished = false;
            while (await batchEnumerator.MoveNextAsync().ConfigureAwait(false))
            {
                ChunkResult result = await session.PushAsync(batchEnumerator.Current, ct).ConfigureAwait(false);
                foreach (CollieStreamEvent ev in EmitsToEvents(result, session, conversationId, correlationId))
                {
                    yield return ev;
                    if (ev is Blocked)
                        blocked = true;
                }
                if (blocked)
                    break;
                if (result.Finished)
                {
                    // The backend finalized the session on a push (no block) —
                    // don't re-process emits via Finish(), but STILL close with
                    // a terminal Finished below (the finished-early path). v22:
                    // the old code set terminated=true here and skipped the
                    // Finished event entirely, ending the stream with a bare
                    // SafeDelta and violating the every-stream-ends-terminal
                    // invariant (Python already had this path).
                    serverFinished = true;
                    break;
                }
            }

            if (!blocked && !serverFinished)
            {
                ChunkResult final = await session.FinishAsync("stop", ct).ConfigureAwait(false);
                foreach (CollieStreamEvent ev in EmitsToEvents(final, session, conversationId, correlationId))
                {
                    yield return ev;
                    if (ev is Blocked)
                        blocked = true;
                }
            }

            // Invariant: every stream ends with a terminal event — Blocked
            // above, InputBlocked before the provider opened, or Finished here
            // (including the finished-early path).
            if (!blocked)
                yield return new Finished
                {
                    FinishReason = "stop",
                    JobId = session.JobId,
                    RequestId = session.RequestId,
                    ConversationId = conversationId,
                    CorrelationId = correlationId,
                    BlockedBy = inputBlockedBy,
                    Context = inputContext,
                };
        }
        finally
        {
            // Cancel FIRST so an in-flight provider read unblocks before disposal
            // awaits it, then dispose batcher -> provider -> session in order.
            streamCts.Cancel();
            if (batchEnumerator is not null)
                await batchEnumerator.DisposeAsync().ConfigureAwait(false);
            if (providerEnumerator is not null)
                await providerEnumerator.DisposeAsync().ConfigureAwait(false);
            await session.DisposeAsync().ConfigureAwait(false); // abort dangling + close
        }
    }

    private static IEnumerable<CollieStreamEvent> EmitsToEvents(
        ChunkResult result, StreamingSession session, string? conversationId, string? correlationId)
    {
        // Raw provider text is never surfaced — only CollieAi-released emit text.
        foreach (SafeEmit emit in result.Emits)
        {
            if (emit.Blocked)
            {
                yield return new Blocked
                {
                    BlockMessage = emit.BlockMessage,
                    TriggeredRules = emit.TriggeredRules,
                    JobId = session.JobId,
                    RequestId = result.RequestId,
                    ConversationId = conversationId,
                    CorrelationId = correlationId,
                };
                yield break; // a block is terminal
            }
            if (!string.IsNullOrEmpty(emit.Text))
            {
                yield return new SafeDelta
                {
                    Text = emit.Text,
                    Sequence = result.Sequence,
                    JobId = session.JobId,
                    RequestId = result.RequestId,
                    ConversationId = conversationId,
                    CorrelationId = correlationId,
                };
            }
        }
    }

    // -- high-level: protect_buffered ----------------------------------------

    public async Task<BufferedResult> ProtectBufferedAsync(ProtectBufferedRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        _c.CheckProject(request.ProjectId);
        if (request.RawStreamFactory is null)
            throw new ProviderStreamFactoryRequiredException(
                "RawStreamFactory is required and must be a deferred factory — not an already-started stream.");
        if (request.InputResult is not null && !request.CheckInput)
            throw new ArgumentException(
                "InputResult requires CheckInput = true: with CheckInput = false the result would be silently ignored.",
                nameof(request));
        if (request.InputResult is not null &&
            (request.InputResult.OriginalText != request.Input || !request.InputResult.Allowed))
            throw new ArgumentException("InputResult must correspond to this Input and be allowed.", nameof(request));
        if (request.InputResult is not null && (request.Context is not null || request.ContextFormat is not null))
            throw new ArgumentException(
                "Context cannot be combined with a precomputed InputResult: the wrapper can't verify the context " +
                "matches the precomputed result. Omit Context when reusing an already-computed result, or call " +
                "Moderation.CheckInputAsync with the desired context.",
                nameof(request));

        string? conversationId = Metadata.Normalize(request.ConversationId, nameof(request.ConversationId));
        string? correlationId = Metadata.Normalize(request.CorrelationId, nameof(request.CorrelationId));

        // 1. Input gate. Capture the INPUT-phase context verdict to surface on the
        //    result whether or not the input blocked (a monitored/degraded context
        //    still allows the request).
        string? inputBlockedBy = null;
        ContextModerationResult? inputContext = null;
        if (request.CheckInput)
        {
            InputModerationResult result = request.InputResult
                ?? await _c.ModerationCore.RunAsync(request.Input, request.Context, request.ContextFormat, conversationId, correlationId, Wire.OriginProtectBuffered, cancellationToken)
                    .ConfigureAwait(false);
            inputBlockedBy = result.BlockedBy;
            inputContext = result.Context;
            // Provider spend is gated on EXPLICIT allow (fail-safe); an
            // inconsistent Allowed+Blocked also blocks (v23).
            if (!result.Allowed || result.Blocked)
                return new BufferedResult
                {
                    Blocked = true,
                    InputBlocked = true,
                    BlockMessage = result.BlockMessage,
                    TriggeredRules = result.TriggeredRules,
                    JobId = result.JobId,
                    RequestId = result.RequestId,
                    BlockedBy = result.BlockedBy,
                    Context = result.Context,
                };
            if (request.InputResult is null)
                RejectMaskedInput(request.Input, result.FilteredText);
        }

        // 2. Consume the entire provider response (factory runs once, after the gate).
        //    The factory call and GetAsyncEnumerator are INSIDE the try (v22): if
        //    the factory starts eager paid work and then throws, the finally must
        //    still cancel the provider token — otherwise generation keeps running.
        using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var sb = new StringBuilder();
        IAsyncEnumerator<string>? enumerator = null;
        try
        {
            IAsyncEnumerable<string>? providerStream = request.RawStreamFactory(streamCts.Token);
            if (providerStream is null)
                throw new ProviderStreamFactoryRequiredException(
                    "RawStreamFactory returned null; it must return an async stream of text deltas.");
            enumerator = providerStream.GetAsyncEnumerator(streamCts.Token);
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                string delta = enumerator.Current;
                if (!string.IsNullOrEmpty(delta))
                    sb.Append(delta);
            }
        }
        finally
        {
            streamCts.Cancel();
            if (enumerator is not null)
                await enumerator.DisposeAsync().ConfigureAwait(false);
        }

        // 3. Submit the full response for outbound filtering (no webhook), poll.
        var body = new JobCreateBody { MessageOutput = sb.ToString(), ConversationId = conversationId, CorrelationId = correlationId };
        var (created, requestId) = await _c.RequestAsync<JobCreateResponseDto>(
            HttpMethod.Post, "/v1/jobs", body, Wire.OriginProtectBuffered, cancellationToken).ConfigureAwait(false);
        string? jobId = created.JobId;
        if (string.IsNullOrEmpty(jobId))
            throw new CollieApiException("Job creation did not return a job_id", code: "invalid_response");

        JobStatusDto job = await PollBufferedAsync(jobId, Wire.OriginProtectBuffered, cancellationToken).ConfigureAwait(false);
        // Carry the input-phase context verdict onto the (output) result too.
        return ToBufferedResult(job, jobId, requestId) with { BlockedBy = inputBlockedBy, Context = inputContext };
    }

    internal async Task<JobStatusDto> PollBufferedAsync(
        string jobId, string origin, CancellationToken ct,
        double pollIntervalS = PollIntervalS, double timeoutS = PollTimeoutS)
    {
        // Pacing per the Poll Backoff Contract (see Internal/PollPacer and the
        // moderation loop — same structure, this flow's typed errors).
        PollPacer.Validate(pollIntervalS, timeoutS);
        var pacer = new PollPacer(pollIntervalS, timeoutS, _c.Monotonic, _c.Jitter);
        CollieApiException TimeoutError() =>
            new($"Polling job {jobId} timed out after {timeoutS:0.###}s", code: "poll_timeout");
        while (true)
        {
            double remaining = pacer.Remaining();
            if (remaining <= 0)
                throw TimeoutError();
            using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            // Ceil to whole ms — FromSeconds rounds to the nearest ms and
            // could cancel the GET before the deadline (see moderation loop).
            deadlineCts.CancelAfter(TimeSpan.FromMilliseconds(Math.Ceiling(remaining * 1000)));
            JobStatusDto? job;
            double? retryAfter;
            string errorContent;
            try
            {
                (job, _, retryAfter, errorContent) = await _c.PollRequestAsync(
                    $"/v1/jobs/{jobId}", origin, deadlineCts.Token, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw; // genuine caller cancellation — never rewrapped
            }
            catch (OperationCanceledException)
            {
                // Only deadline-caused OCEs reach here (see the primitive's
                // priority matrix).
                throw TimeoutError();
            }
            catch (CollieApiException) when (pacer.Remaining() <= 0)
            {
                // Strict acceptance for late-landing typed HTTP errors
                // (connection errors propagate untouched).
                throw TimeoutError();
            }
            // Strict acceptance boundary: a response landing at/after the
            // deadline is rejected, never accepted late.
            if (pacer.Remaining() <= 0)
                throw TimeoutError();
            if (job is null)
            {
                // 429 — pace, continue, or surface the typed 429.
                double? sleep = pacer.SleepFor429(retryAfter);
                if (sleep is null)
                    throw _c.ApiError(429, errorContent);
                await _c.PollDelay(sleep.Value, ct).ConfigureAwait(false);
                continue;
            }
            string? status = job.Status;
            if (status is not null && BufferedTerminal.Contains(status))
                return job;
            if (status is not null && BufferedFailure.Contains(status))
                throw new CollieApiException($"Job {jobId} ended in terminal state '{status}'", code: "job_failed");
            await _c.PollDelay(pacer.NextSleep(job.SuggestedPollMs), ct).ConfigureAwait(false);
        }
    }

    private static bool IsClaimRefusal(CollieApiException e) =>
        e.Code is "input_gate_stale" or "input_gate_unverifiable";

    // Fail closed when the wrapper's OWN input gate masked the prompt (round
    // 5): the provider factory closes over the ORIGINAL text, so proceeding
    // would stream unmasked content to the model. `!=` (not null/empty
    // checks): a full wipe to "" is a real mask verdict. The InputResult
    // path is exempt — the caller demonstrably holds FilteredText.
    private static void RejectMaskedInput(string original, string? filtered)
    {
        if (filtered is not null && filtered != original)
        {
            throw new MaskedInputException(
                "the input policy MASKED this prompt; the provider factory " +
                "would stream the ORIGINAL (unmasked) text to the model. " +
                "Call Moderation.CheckInputAsync yourself, build the " +
                "provider stream over result.FilteredText, and pass " +
                "InputResult = result to the wrapper.");
        }
    }

    // The v24 fail-safe verdict table applied to outbound_result, mirroring
    // ModerationClient's output resolution line for line (tech-debt #22 closed
    // the buffered gap): a terminal job without its result object is malformed
    // (typed error, never an implicit allow); NOT blocked requires an EXPLICIT
    // Allowed == true and no hard-block signal; ambiguity — an empty object,
    // an absent Allowed — resolves to blocked. BufferedResult keeps its
    // blocked-only surface; Blocked is the strict complement of resolved allow.
    private static BufferedResult ToBufferedResult(JobStatusDto job, string jobId, string? requestId)
    {
        var outbound = job.OutboundResult
            ?? throw new CollieApiException(
                "API returned a malformed buffered result (missing outbound_result)", code: "invalid_response");
        bool statusBlocked = job.Status == "outbound_blocked";
        bool hardBlocked = statusBlocked || outbound.Blocked || outbound.Allowed == false;
        bool allowed = outbound.Allowed == true && !hardBlocked;
        return new BufferedResult
        {
            Blocked = !allowed,
            BlockMessage = outbound.BlockMessage,
            FilteredText = outbound.FilteredContent,
            InputBlocked = false,
            TriggeredRules = outbound.TriggeredRules.ToModels(),
            JobId = jobId,
            RequestId = requestId,
        };
    }

    // -- low-level session factory -------------------------------------------

    public async Task<IStreamingSession> CreateSessionAsync(StreamingSessionRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        _c.CheckProject(request.ProjectId);
        string? conversationId = Metadata.Normalize(request.ConversationId, nameof(request.ConversationId));
        string? correlationId = Metadata.Normalize(request.CorrelationId, nameof(request.CorrelationId));
        var session = new StreamingSession(_c, request.Input, conversationId, correlationId, Wire.OriginSession, inputJobId: request.InputJobId);
        await session.CreateAsync(cancellationToken).ConfigureAwait(false);
        return session;
    }
}
