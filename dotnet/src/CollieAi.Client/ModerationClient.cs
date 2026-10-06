// Input and output moderation — the standalone safety checks.
//
// CheckInputAsync lets a customer check a user prompt before calling their LLM,
// without implementing webhooks. MVP transport: create an inbound_only job (no
// webhook) and poll it to a terminal inbound state.
//
// CheckOutputAsync is the sibling for assistant text produced OUTSIDE a
// ProtectStream/ProtectBuffered wrapper (proactive notifications,
// escalations): a message_output-only job polled to a terminal outbound
// state, evaluated by OUTPUT-direction rules.

using System.Text.Json;
using CollieAi.Internal;

namespace CollieAi;

/// <summary>Standalone input and output moderation.</summary>
public interface ICollieModerationClient
{
    /// <summary>
    /// Check <see cref="InputModerationRequest.Prompt"/> against the project's
    /// input rules. A policy block is a normal result (<c>Blocked = true</c>), not
    /// an exception. Throws <see cref="ModerationException"/> if the job dies
    /// (failed/expired) or polling times out.
    /// </summary>
    Task<InputModerationResult> CheckInputAsync(InputModerationRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Check <see cref="OutputModerationRequest.Response"/> (assistant/LLM
    /// output) against the project's OUTPUT rules — the standalone check for
    /// text produced outside a <c>Protect*</c> wrapper: proactive
    /// notifications, escalation messages, any side channel. Routing such text
    /// through <see cref="CheckInputAsync"/> evaluates it with INBOUND rules:
    /// output-safety and masking rules silently never run, and injection
    /// detectors false-block assistant-style imperatives.
    /// A policy block is a normal result (<c>Blocked = true</c>).
    /// <see cref="OutputModerationResult.FilteredText"/> carries the MASKED
    /// output — send it, not the original. Throws
    /// <see cref="ModerationException"/> if the job dies (failed/expired) or
    /// polling times out.
    /// </summary>
    Task<OutputModerationResult> CheckOutputAsync(OutputModerationRequest request, CancellationToken cancellationToken = default);
}

internal sealed class ModerationClient : ICollieModerationClient
{
    private static readonly HashSet<string> TerminalVerdict = new(StringComparer.Ordinal) { "completed", "inbound_blocked" };
    private static readonly HashSet<string> TerminalFailure = new(StringComparer.Ordinal) { "failed", "expired" };
    // The outbound sets — identical to ProtectBuffered's, because a
    // message_output-only job is the same job shape.
    private static readonly HashSet<string> TerminalOutputVerdict = new(StringComparer.Ordinal) { "completed", "outbound_blocked" };
    private static readonly HashSet<string> TerminalOutputFailure = new(StringComparer.Ordinal) { "failed", "expired" };
    // Poll knobs are constants (not public API); tests override via RunAsync's
    // internal parameters.
    private const double PollIntervalS = 0.05;
    private const double PollTimeoutS = 30.0;

    private readonly CollieClient _c;

    public ModerationClient(CollieClient c) => _c = c;

    public Task<InputModerationResult> CheckInputAsync(InputModerationRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        _c.CheckProject(request.ProjectId);
        string? conversationId = Metadata.Normalize(request.ConversationId, nameof(request.ConversationId));
        string? correlationId = Metadata.Normalize(request.CorrelationId, nameof(request.CorrelationId));
        return RunAsync(request.Prompt, request.Context, request.ContextFormat, conversationId, correlationId, Wire.OriginModerateInput, cancellationToken,
            timeoutS: request.TimeoutS ?? PollTimeoutS);
    }

    public Task<OutputModerationResult> CheckOutputAsync(OutputModerationRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        _c.CheckProject(request.ProjectId);
        string? conversationId = Metadata.Normalize(request.ConversationId, nameof(request.ConversationId));
        string? correlationId = Metadata.Normalize(request.CorrelationId, nameof(request.CorrelationId));
        return RunOutputAsync(request.Response, conversationId, correlationId, cancellationToken,
            timeoutS: request.TimeoutS ?? PollTimeoutS);
    }

    /// <summary>
    /// The output-check transport: a <c>message_output</c>-only job (the exact
    /// ProtectBuffered step-3 shape) polled to a terminal outbound state. Same
    /// loop structure as <see cref="RunAsync"/>, this flow's typed errors —
    /// died/timed out is a <see cref="ModerationException"/> on BOTH moderation
    /// methods, so one catch covers the pair.
    /// </summary>
    internal async Task<OutputModerationResult> RunOutputAsync(
        string response, string? conversationId, string? correlationId, CancellationToken ct,
        double pollIntervalS = PollIntervalS, double timeoutS = PollTimeoutS)
    {
        // Knob validation BEFORE the job is created (fail fast, no side effects).
        PollPacer.Validate(pollIntervalS, timeoutS);
        var body = new JobCreateBody
        {
            MessageOutput = response,
            ConversationId = conversationId,
            CorrelationId = correlationId,
        };
        var (created, requestId) = await _c.RequestAsync<JobCreateResponseDto>(
            HttpMethod.Post, "/v1/jobs", body, Wire.OriginModerateOutput, ct).ConfigureAwait(false);
        string? jobId = created.JobId;
        if (string.IsNullOrEmpty(jobId))
            throw new CollieApiException("Job creation did not return a job_id", code: "invalid_response");

        var pacer = new PollPacer(pollIntervalS, timeoutS, _c.Monotonic, _c.Jitter);
        ModerationException TimeoutError() =>
            new($"CheckOutput timed out after {timeoutS:0.###}s waiting for job {jobId}");
        while (true)
        {
            double remaining = pacer.Remaining();
            if (remaining <= 0)
                throw TimeoutError();
            using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            // Ceil to whole ms — FromSeconds rounds to the nearest ms and could
            // cancel the GET before the deadline (see the input loop).
            deadlineCts.CancelAfter(TimeSpan.FromMilliseconds(Math.Ceiling(remaining * 1000)));
            JobStatusDto? job;
            string? pollRequestId;
            double? retryAfter;
            string errorContent;
            try
            {
                (job, pollRequestId, retryAfter, errorContent) = await _c.PollRequestAsync(
                    $"/v1/jobs/{jobId}", Wire.OriginModerateOutput, deadlineCts.Token, ct).ConfigureAwait(false);
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
            // Strict acceptance boundary (v5): a response landing at/after the
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
            if (status is not null && TerminalOutputVerdict.Contains(status))
                return ToOutputResult(response, jobId, requestId ?? pollRequestId, job);
            if (status is not null && TerminalOutputFailure.Contains(status))
                throw new ModerationException($"Job {jobId} ended in terminal state '{status}' before an output verdict");
            await _c.PollDelay(pacer.NextSleep(job.SuggestedPollMs), ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Shared input-check transport. <paramref name="origin"/> attributes the
    /// requests to the public method that triggered them (moderate.input directly,
    /// or protect_stream / protect_buffered via a wrapper). Metadata is assumed
    /// pre-normalized by the caller.
    /// </summary>
    internal async Task<InputModerationResult> RunAsync(
        string prompt, object? context, string? contextFormat,
        string? conversationId, string? correlationId, string origin, CancellationToken ct,
        double pollIntervalS = PollIntervalS, double timeoutS = PollTimeoutS)
    {
        // Knob validation BEFORE the job is created (fail fast, no side
        // effects). The knobs are internal constants today, so this guards
        // future public exposure and test overrides.
        PollPacer.Validate(pollIntervalS, timeoutS);
        // Context-analysis input surface.
        // Pre-serialize to a JsonElement with default options so customer keys
        // are preserved VERBATIM: the body serializer's SnakeCaseLower
        // DictionaryKeyPolicy must not rewrite context keys (e.g. titleNew),
        // which the Slice-1 JSON-Pointer attribution depends on. A JsonElement
        // is written raw, bypassing the key policy. null stays omitted (= unset;
        // policy default applies). Reject non-finite numbers / non-serializable
        // values here as a deterministic ArgumentException so behavior matches
        // the other SDKs (no silent NaN -> null).
        object? contextPayload = null;
        if (context is not null)
        {
            try
            {
                contextPayload = JsonSerializer.SerializeToElement(context);
            }
            catch (Exception ex)
            {
                throw new ArgumentException(
                    "Context must be strict-JSON-compatible (no NaN/Infinity or non-serializable values).",
                    nameof(context), ex);
            }
        }

        var body = new JobCreateBody
        {
            MessageInput = prompt,
            InboundOnly = true,
            ConversationId = conversationId,
            CorrelationId = correlationId,
            Context = contextPayload,
            ContextFormat = contextFormat,
        };
        var (created, requestId) = await _c.RequestAsync<JobCreateResponseDto>(
            HttpMethod.Post, "/v1/jobs", body, origin, ct).ConfigureAwait(false);
        string? jobId = created.JobId;
        if (string.IsNullOrEmpty(jobId))
            throw new CollieApiException("Job creation did not return a job_id", code: "invalid_response");

        // Pacing per the Poll Backoff Contract (see Internal/PollPacer). The
        // budget starts AFTER job creation, matching the prior deadline
        // semantics. Flow-specific errors stay here; the pacer owns timing.
        var pacer = new PollPacer(pollIntervalS, timeoutS, _c.Monotonic, _c.Jitter);
        ModerationException TimeoutError() =>
            new($"CheckInput timed out after {timeoutS:0.###}s waiting for job {jobId}");
        while (true)
        {
            double remaining = pacer.Remaining();
            if (remaining <= 0)
                throw TimeoutError();
            // Cooperative wall-clock bound on the in-flight GET (PBC-08: a suppressing transport can outrun it; PBC-07 no-late-result is the hard guarantee) (v4): a linked
            // CTS cancelled at the remaining budget. Taxonomy is structural:
            // caller cancellation propagates (checked FIRST — the deadline
            // token links it); only our own bound firing maps to this flow's
            // timeout; transport failures stay CollieConnectionException
            // regardless of the clock.
            using var deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            // Ceil to whole milliseconds: TimeSpan.FromSeconds rounds to the
            // NEAREST ms, so a sub-0.5 ms remainder would become zero and
            // cancel the GET before the deadline. Never undercut the budget —
            // a late-landing response is cut by the post-GET check instead.
            deadlineCts.CancelAfter(TimeSpan.FromMilliseconds(Math.Ceiling(remaining * 1000)));
            JobStatusDto? job;
            string? pollRequestId;
            double? retryAfter;
            string errorContent;
            try
            {
                (job, pollRequestId, retryAfter, errorContent) = await _c.PollRequestAsync(
                    $"/v1/jobs/{jobId}", origin, deadlineCts.Token, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw; // genuine caller cancellation — never rewrapped
            }
            catch (OperationCanceledException)
            {
                // Only deadline-caused OCEs reach here: the primitive's
                // priority matrix converts transport/foreign cancellations to
                // connection errors and rethrows caller cancellations raw.
                throw TimeoutError();
            }
            catch (CollieApiException) when (pacer.Remaining() <= 0)
            {
                // Strict acceptance applies to EVERY late-landing HTTP
                // response: a typed HTTP error arriving at/after the deadline
                // is this flow's timeout too (connection errors propagate).
                throw TimeoutError();
            }
            // Strict acceptance boundary (v5): every bound can be outrun — a
            // response landing at/after the deadline is rejected, never
            // accepted late.
            if (pacer.Remaining() <= 0)
                throw TimeoutError();
            if (job is null)
            {
                // 429 — pace, continue, or surface the typed 429. The
                // response was fully read+disposed by the primitive.
                double? sleep = pacer.SleepFor429(retryAfter);
                if (sleep is null)
                    throw _c.ApiError(429, errorContent);
                await _c.PollDelay(sleep.Value, ct).ConfigureAwait(false);
                continue;
            }
            string? status = job.Status;
            if (status is not null && TerminalVerdict.Contains(status))
                return ToResult(prompt, jobId, requestId ?? pollRequestId, job);
            if (status is not null && TerminalFailure.Contains(status))
                throw new ModerationException($"Job {jobId} ended in terminal state '{status}' before an input verdict");
            await _c.PollDelay(pacer.NextSleep(job.SuggestedPollMs), ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The v24 fail-safe verdict table applied to <c>outbound_result</c>
    /// (sdk-plan § Output Moderation): a terminal job without its result
    /// object is malformed (typed error, never an implicit allow); ALLOWED
    /// requires an EXPLICIT <c>allowed == true</c> and no hard-block signal;
    /// ambiguity resolves to blocked.
    /// </summary>
    private static OutputModerationResult ToOutputResult(string response, string jobId, string? requestId, JobStatusDto job)
    {
        var outbound = job.OutboundResult
            ?? throw new CollieApiException(
                "API returned a malformed moderation result (missing outbound_result)", code: "invalid_response");
        bool statusBlocked = job.Status == "outbound_blocked";
        bool hardBlocked = statusBlocked || outbound.Blocked || outbound.Allowed == false;
        bool allowed = outbound.Allowed == true && !hardBlocked;
        return new OutputModerationResult
        {
            Allowed = allowed,
            Blocked = !allowed,
            BlockMessage = outbound.BlockMessage,
            OriginalText = response,
            FilteredText = outbound.FilteredContent,
            TriggeredRules = outbound.TriggeredRules.ToModels(),
            JobId = jobId,
            RequestId = requestId,
        };
    }

    private static InputModerationResult ToResult(string prompt, string jobId, string? requestId, JobStatusDto job)
    {
        // A terminal job must carry its inbound_result; a missing one is malformed,
        // not an implicit "allowed".
        var inbound = job.InboundResult
            ?? throw new CollieApiException(
                "API returned a malformed moderation result (missing inbound_result)", code: "invalid_response");

        // FAIL-SAFE verdict resolution (a wrapper must never fail-open on an
        // ambiguous/inconsistent response). BLOCKED if ANY of: the terminal
        // status says so (authoritative even when inbound_result omits
        // "blocked"), inbound.blocked==true, inbound.allowed==false, OR a
        // context_result block (v23 — a context block gates provider spend too;
        // it lives on the job-status root, not inside inbound_result).
        // ALLOWED requires an EXPLICIT allowed==true AND not blocked — an
        // ABSENT allowed no longer defaults to allow (v23: `?? !blocked` let a
        // completed job with an empty inbound_result fail-open).
        bool statusBlocked = job.Status == "inbound_blocked";
        bool contextBlocked = job.ContextResult?.Blocked == true;
        bool hardBlocked = statusBlocked || inbound.Blocked || inbound.Allowed == false || contextBlocked;
        bool allowed = inbound.Allowed == true && !hardBlocked;
        // Blocked is the strict complement of allowed (v23): an ambiguous or
        // malformed verdict — not affirmatively allowed AND not affirmatively
        // blocked — surfaces as Blocked=true so a caller gating on Blocked
        // (not just !Allowed) also stays fail-safe.
        bool blocked = !allowed;
        // Context analysis (§4.1, §5.5): context_result + blocked_by live on the
        // job-status root, not inside inbound_result. Null on a job without it.
        ContextModerationResult? context = job.ContextResult is { } c
            ? new ContextModerationResult
            {
                Status = c.Status ?? "not_provided",
                Blocked = c.Blocked,
                BlockMessage = c.BlockMessage,
                TriggeringPointer = c.TriggeringPointer,
                TriggeringRuleId = c.TriggeringRuleId,
                TriggeringRuleType = c.TriggeringRuleType,
                ParseDegraded = c.ParseDegraded,
                LimitExceeded = c.LimitExceeded,
                InferenceDegraded = c.InferenceDegraded,
            }
            : null;
        return new InputModerationResult
        {
            Allowed = allowed,
            Blocked = blocked,
            // Surface the context-block reason when the inbound result carries no
            // message of its own (v24): a context block (blocked_by=context with an
            // allowed inbound_result) otherwise leaves BlockMessage null and a
            // caller showing input.BlockMessage displays nothing.
            BlockMessage = inbound.BlockMessage ?? (contextBlocked ? job.ContextResult?.BlockMessage : null),
            OriginalText = prompt,
            FilteredText = inbound.FilteredContent,
            TriggeredRules = inbound.TriggeredRules.ToModels(),
            JobId = jobId,
            RequestId = requestId,
            BlockedBy = job.BlockedBy,
            Context = context,
        };
    }
}
