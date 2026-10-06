// Request records for the public API surface. Records with init-only
// properties; required where the call is meaningless without the value.

namespace CollieAi;

/// <summary>Request for <c>collie.Moderation.CheckInputAsync(...)</c>.</summary>
public sealed record InputModerationRequest
{
    /// <summary>The user prompt to check before calling your LLM.</summary>
    public required string Prompt { get; init; }

    /// <summary>Validated against the client's project (the API key scopes the project); not sent in the body.</summary>
    public string? ProjectId { get; init; }

    /// <summary>Optional: groups turns into one conversation for log grouping.</summary>
    public string? ConversationId { get; init; }
    /// <summary>Optional: pins one turn for trace grouping.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Structured data (object/array) or raw string analyzed alongside the prompt.
    /// A structured value is sent as JSON. Inert
    /// until context analysis is enabled server-side.
    /// </summary>
    public object? Context { get; init; }
    /// <summary>Parsing hint for a raw-string context ("auto"/"json"/"text"); ignored for structured context. Omitted = policy default.</summary>
    public string? ContextFormat { get; init; }

    /// <summary>
    /// Optional wall-clock budget in seconds for POLLING the moderation job to
    /// a verdict. Bounds the poll phase only — it starts
    /// AFTER the job-create POST, which is bounded separately by
    /// <see cref="CollieClientOptions.Timeout"/> — so the worst-case call is
    /// roughly the sum of the two. On expiry the call throws
    /// <see cref="ModerationException"/> (the same died-without-verdict error a
    /// fail-open wrapper already classifies). Null = the SDK default (30 s).
    /// Validated before the job is created; invalid values throw
    /// <see cref="ArgumentOutOfRangeException"/> with no side effects.
    /// </summary>
    public double? TimeoutS { get; init; }
}

/// <summary>
/// Request for <c>collie.Moderation.CheckOutputAsync(...)</c>.
/// Deliberately has NO <c>Context</c>/<c>ContextFormat</c>: context is an
/// input surface (with context analysis enabled server-side, an
/// outbound-only job carrying one is rejected with 400); context-aware
/// output filtering is <c>ProtectBufferedAsync</c> /
/// <c>ProtectStreamAsync</c>, where the input pass carries it.
/// </summary>
public sealed record OutputModerationRequest
{
    /// <summary>The assistant/LLM output to check against OUTPUT rules.</summary>
    public required string Response { get; init; }

    /// <summary>Validated against the client's project (the API key scopes the project); not sent in the body.</summary>
    public string? ProjectId { get; init; }

    /// <summary>Optional: groups turns into one conversation for log grouping.</summary>
    public string? ConversationId { get; init; }
    /// <summary>Optional: pins one turn for trace grouping.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Optional wall-clock budget in seconds for POLLING the moderation job to
    /// a verdict — same contract as
    /// <see cref="InputModerationRequest.TimeoutS"/>. Null = the SDK default
    /// (30 s).
    /// </summary>
    public double? TimeoutS { get; init; }
}

/// <summary>Request for <c>collie.Streaming.PreflightAsync(...)</c>.</summary>
public sealed record StreamingPreflightRequest
{
    /// <summary>Defaults to the project the API key is scoped to. If provided, must match the key's project.</summary>
    public string? ProjectId { get; init; }

    /// <summary>Bypass the per-project preflight cache and force a fresh fetch.</summary>
    public bool ForceRefresh { get; init; }
}

/// <summary>Coalescing thresholds for high-level streaming. Flush on any threshold (or provider end).</summary>
public sealed record BatchingOptions
{
    /// <summary>Flush a batch after this much wall-clock time. Default 50 ms.</summary>
    public TimeSpan FlushInterval { get; init; } = TimeSpan.FromMilliseconds(50);
    /// <summary>Flush after this many provider deltas accumulate. Default 16.</summary>
    public int MaxProviderDeltas { get; init; } = 16;
    /// <summary>Flush after the buffered text reaches this many characters. Default 2048.</summary>
    public int MaxCharacters { get; init; } = 2048;
}

/// <summary>Request for <c>collie.Streaming.ProtectStreamAsync(...)</c>.</summary>
public sealed record ProtectStreamRequest
{
    /// <summary>The prompt to input-filter when <see cref="CheckInput"/> is set.
    /// On the streaming path it is also submitted as the job's message input, so
    /// output/chunk rules see it as prompt context. (The buffered fallback —
    /// see <see cref="ProtectBufferedRequest.Input"/> — does NOT carry it.)</summary>
    public required string Input { get; init; }

    /// <summary>
    /// Zero-arg-ish factory returning the provider's text-delta stream. Receives a
    /// <see cref="CancellationToken"/> that the SDK cancels on teardown (early
    /// exit, block, or abandonment) — forward it to your provider so paid LLM work
    /// stops promptly. Called <b>exactly once</b>, only after the input check (and
    /// preflight, if required) passes; CollieAi chunk retries never re-invoke it.
    /// </summary>
    public required Func<CancellationToken, IAsyncEnumerable<string>> RawStreamFactory { get; init; }

    /// <summary>Validated against the client's project; optional.</summary>
    public string? ProjectId { get; init; }
    /// <summary>Optional: groups turns into one conversation for log grouping.</summary>
    public string? ConversationId { get; init; }
    /// <summary>Optional: pins one turn for trace grouping.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Structured data or raw string analyzed alongside the input prompt. Forwarded to the input gate.</summary>
    public object? Context { get; init; }
    /// <summary>Parsing hint for a raw-string context ("auto"/"json"/"text"); ignored for structured context.</summary>
    public string? ContextFormat { get; init; }

    /// <summary>Run the input check before consuming provider output. Default true.</summary>
    public bool CheckInput { get; init; } = true;

    /// <summary>
    /// If true, preflight is consulted first: a buffered policy throws
    /// <see cref="BufferedFallbackRequiredException"/> and an unservable policy
    /// throws a <see cref="PreflightException"/> — both before the provider runs.
    /// </summary>
    public bool RequireStreaming { get; init; }

    /// <summary>Optional batching overrides. Null uses the defaults (50 ms / 16 deltas / 2048 chars).</summary>
    public BatchingOptions? Batching { get; init; }

    /// <summary>
    /// A previously obtained input result to avoid a duplicate input check. Its
    /// <see cref="InputModerationResult.OriginalText"/> must equal <see cref="Input"/>
    /// and <see cref="InputModerationResult.Allowed"/> must be true. Requires
    /// <see cref="CheckInput"/> = true (the default) — combining it with
    /// <c>CheckInput = false</c> is a contradiction and throws (2.1; it was
    /// silently ignored before, which also silently dropped the claim).
    /// The wrapper forwards the result's <c>JobId</c> as the single-use
    /// session claim; on the claim's 409s there is deliberately NO automatic
    /// remedy on this path — the result may carry a context the wrapper
    /// never saw, so the typed errors surface to your code. The remedies
    /// differ per code: <c>input_gate_stale</c> — re-gate (with your own
    /// context) and retry with the fresh id; <c>input_gate_unverifiable</c>
    /// — retry without the claim (re-gating will not help);
    /// <c>input_gate_claimed</c> — the gate was already consumed, do not
    /// retry this turn. The
    /// wrapper's masked-input fail-closed is also exempt here: you
    /// demonstrably hold <see cref="InputModerationResult.FilteredText"/>
    /// and own what the factory streams.
    /// </summary>
    public InputModerationResult? InputResult { get; init; }
}

/// <summary>Request for <c>collie.Streaming.ProtectBufferedAsync(...)</c>.</summary>
public sealed record ProtectBufferedRequest
{
    /// <summary>The prompt to input-filter (blocked before any provider spend).
    /// NOTE: the buffered fallback submits only the model OUTPUT for output
    /// filtering, so output rules do NOT currently see this prompt as
    /// conversation context — carrying the prompt onto the outbound job is a
    /// planned wire-contract change. Do not rely on output rules reading the
    /// input here.</summary>
    public required string Input { get; init; }
    /// <summary>
    /// Factory returning the provider's text-delta stream. Receives a
    /// <see cref="CancellationToken"/> the SDK cancels on teardown; called exactly
    /// once, only after the input check passes.
    /// </summary>
    public required Func<CancellationToken, IAsyncEnumerable<string>> RawStreamFactory { get; init; }
    /// <summary>Validated against the client's project; optional.</summary>
    public string? ProjectId { get; init; }
    /// <summary>Optional: groups turns into one conversation for log grouping.</summary>
    public string? ConversationId { get; init; }
    /// <summary>Optional: pins one turn for trace grouping.</summary>
    public string? CorrelationId { get; init; }
    /// <summary>Structured data or raw string analyzed alongside the input prompt. Forwarded to the input gate.</summary>
    public object? Context { get; init; }
    /// <summary>Parsing hint for a raw-string context ("auto"/"json"/"text"); ignored for structured context.</summary>
    public string? ContextFormat { get; init; }
    /// <summary>Run the input check before consuming provider output. Default true.</summary>
    public bool CheckInput { get; init; } = true;
    /// <summary>A previously obtained input result to skip the duplicate
    /// input check (its <see cref="InputModerationResult.OriginalText"/> must
    /// equal <see cref="Input"/> and <see cref="InputModerationResult.Allowed"/>
    /// must be true; requires <see cref="CheckInput"/> = true — combining it
    /// with <c>CheckInput = false</c> throws, same as on
    /// <see cref="ProtectStreamRequest.InputResult"/>). Unlike the streaming
    /// path this is a TRUSTED-CLIENT reuse, not the claim protocol: the
    /// buffered path opens no streaming session and sends no
    /// <c>input_job_id</c>, so the server neither verifies nor consumes the
    /// result — no <c>input_gate_*</c> 409s can occur here, and
    /// freshness/single-use are the caller's responsibility.</summary>
    public InputModerationResult? InputResult { get; init; }
}

/// <summary>Request for <c>collie.Streaming.CreateSessionAsync(...)</c> (advanced, low-level).</summary>
public sealed record StreamingSessionRequest
{
    /// <summary>Prompt context output rules may use; sent as the job's <c>message_input</c>.</summary>
    public required string Input { get; init; }

    /// <summary>Validated against the client's project; optional.</summary>
    public string? ProjectId { get; init; }
    /// <summary>Optional: groups turns into one conversation for log grouping.</summary>
    public string? ConversationId { get; init; }
    /// <summary>Optional: pins one turn for trace grouping.</summary>
    public string? CorrelationId { get; init; }
    /// <summary>Optional: job id of a <c>Moderation.CheckInputAsync</c> call that
    /// already gated exactly this <see cref="Input"/>. The server verifies the
    /// reference (same project, identical text, completed and not blocked) and
    /// skips re-filtering the prompt on the session job — one inbound pass per
    /// turn. An invalid reference is a request-time error; older servers ignore
    /// the field and filter as before. Manual sessions do NOT auto-retry the
    /// claim protocol: a <c>CollieApiException</c> with <c>Code ==
    /// "input_gate_stale"</c> means re-gate (same context) and open a new
    /// session with the fresh id; <c>"input_gate_unverifiable"</c> means retry
    /// WITHOUT the reference (re-gating will not help);
    /// <c>"input_gate_claimed"</c> means the gate was already consumed.
    /// <c>ProtectStreamAsync</c> handles this ladder automatically, but ONLY
    /// when it runs its own gate — with an external <c>InputResult</c> the
    /// typed errors surface to your code by design (the wrapper cannot
    /// re-check a context it never saw).</summary>
    public string? InputJobId { get; init; }
}
