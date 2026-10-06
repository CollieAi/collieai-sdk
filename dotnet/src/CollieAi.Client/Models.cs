// Public, immutable result models for the CollieAi SDK.
//
// Wire payloads are snake_case; these records are PascalCase. Mapping from the
// wire DTOs (Internal/Wire.cs) lives in the clients, so these stay free of
// serialization attributes.

using System.Text.Json;

namespace CollieAi;

/// <summary>Recommended client UX, resolved by streaming preflight.</summary>
public enum RecommendedClientBehavior
{
    /// <summary>The policy can stream; render a token-stream UI.</summary>
    Stream,

    /// <summary>The policy must buffer; show a "checking response…" UI.</summary>
    BufferThenShow,

    /// <summary>Streaming is unavailable; fail fast and surface the reason.</summary>
    FailFast,
}

/// <summary>A rule that fired during filtering.</summary>
public sealed record TriggeredRule
{
    /// <summary>Server-assigned rule id.</summary>
    public string RuleId { get; init; } = "";
    /// <summary>Human-readable rule name.</summary>
    public string RuleName { get; init; } = "";
    /// <summary>Rule type (e.g. <c>regex</c>, <c>llm</c>).</summary>
    public string RuleType { get; init; } = "";
    /// <summary>The rule's decision (e.g. <c>block</c>, <c>mask</c>, <c>flag</c>).</summary>
    public string Decision { get; init; } = "";
    /// <summary>True when the rule ran in monitor-only mode (recorded but did not enforce).</summary>
    public bool Monitoring { get; init; }

    /// <summary>Opaque, rule-type-specific match metadata, when the server provides it.</summary>
    public IReadOnlyDictionary<string, JsonElement>? MatchInfo { get; init; }
}

/// <summary>Result of <c>collie.Moderation.CheckInputAsync(...)</c>.</summary>
public sealed record InputModerationResult
{
    /// <summary>True when the input passed all inbound rules.</summary>
    public required bool Allowed { get; init; }
    /// <summary>True when an inbound rule blocked the input.</summary>
    public required bool Blocked { get; init; }
    /// <summary>Customer-facing message when blocked; otherwise null.</summary>
    public string? BlockMessage { get; init; }
    /// <summary>The prompt that was checked.</summary>
    public required string OriginalText { get; init; }
    /// <summary>The input after masking, when the policy masks rather than blocks; otherwise null.</summary>
    public string? FilteredText { get; init; }
    /// <summary>Rules that fired during the input check.</summary>
    public IReadOnlyList<TriggeredRule> TriggeredRules { get; init; } = Array.Empty<TriggeredRule>();
    /// <summary>Backend job id for this moderation call.</summary>
    public string? JobId { get; init; }
    /// <summary>Server request id (<c>x-request-id</c>) for support/tracing.</summary>
    public string? RequestId { get; init; }
    /// <summary>Which surface blocked: "prompt", "context", or "none". Null on a job without context analysis.</summary>
    public string? BlockedBy { get; init; }
    /// <summary>The context-analysis verdict; null on a job without context analysis.</summary>
    public ContextModerationResult? Context { get; init; }
}

/// <summary>
/// Result of <c>collie.Moderation.CheckOutputAsync(...)</c>.
/// No <c>Context</c>/<c>BlockedBy</c>: context is an input surface and
/// output moderation has a single surface, so there is nothing to attribute
/// between.
/// </summary>
public sealed record OutputModerationResult
{
    /// <summary>True when the output passed all outbound rules.</summary>
    public required bool Allowed { get; init; }
    /// <summary>True when an outbound rule blocked the output.</summary>
    public required bool Blocked { get; init; }
    /// <summary>Customer-facing message when blocked; otherwise null.</summary>
    public string? BlockMessage { get; init; }
    /// <summary>The output that was checked.</summary>
    public required string OriginalText { get; init; }
    /// <summary>
    /// The MASKED output when masking rules fired. Send this, not the
    /// original — a caller that ignores it silently defeats outbound masking
    /// (PII redaction etc.).
    /// </summary>
    public string? FilteredText { get; init; }
    /// <summary>Rules that fired during the output check.</summary>
    public IReadOnlyList<TriggeredRule> TriggeredRules { get; init; } = Array.Empty<TriggeredRule>();
    /// <summary>Backend job id for this moderation call.</summary>
    public string? JobId { get; init; }
    /// <summary>Server request id (<c>x-request-id</c>) for support/tracing.</summary>
    public string? RequestId { get; init; }
}

/// <summary>
/// Context-analysis verdict, present when the
/// policy analyzes context alongside the prompt.
/// </summary>
public sealed record ContextModerationResult
{
    /// <summary>Closed status enum: not_provided / disabled / not_run / clean / monitored / blocked / degraded.</summary>
    public required string Status { get; init; }
    /// <summary>True when context analysis blocked the request.</summary>
    public bool Blocked { get; init; }
    /// <summary>Customer-facing message when the context blocked; otherwise null.</summary>
    public string? BlockMessage { get; init; }
    /// <summary>RFC 6901 JSON Pointer of the triggering leaf (a path, never a value).</summary>
    public string? TriggeringPointer { get; init; }
    /// <summary>Id of the rule that triggered the context finding.</summary>
    public string? TriggeringRuleId { get; init; }
    /// <summary>Type of the rule that triggered the context finding.</summary>
    public string? TriggeringRuleType { get; init; }
    /// <summary>Context was malformed JSON and fell back to text analysis.</summary>
    public bool ParseDegraded { get; init; }
    /// <summary>A worker cap was hit, so coverage was incomplete.</summary>
    public bool LimitExceeded { get; init; }
    /// <summary>An ML/LLM detector failed open, so coverage was incomplete.</summary>
    public bool InferenceDegraded { get; init; }
}

/// <summary>
/// One CollieAi-released emit for a submitted chunk. <see cref="Text"/> is the
/// safe content the customer may forward to the end user.
/// </summary>
public sealed record SafeEmit
{
    /// <summary>Safe content the customer may forward to the end user.</summary>
    public string Text { get; init; } = "";
    /// <summary>True when this emit represents a block rather than safe content.</summary>
    public bool Blocked { get; init; }
    /// <summary>Customer-facing message for blocked emits (the blocking rule's
    /// configured message or the backend default). Null on non-block emits.</summary>
    public string? BlockMessage { get; init; }
    /// <summary>True on the terminal emit for the stream.</summary>
    public bool Final { get; init; }
    /// <summary>Rules that fired for this emit.</summary>
    public IReadOnlyList<TriggeredRule> TriggeredRules { get; init; } = Array.Empty<TriggeredRule>();
}

/// <summary>Response from one <c>session.PushAsync(...)</c> / <c>session.FinishAsync(...)</c>.</summary>
public sealed record ChunkResult
{
    /// <summary>Zero-based sequence number this result corresponds to.</summary>
    public int Sequence { get; init; }
    /// <summary>True when the server accepted the submitted chunk.</summary>
    public bool Accepted { get; init; }
    /// <summary>CollieAi-released emits produced by this chunk.</summary>
    public IReadOnlyList<SafeEmit> Emits { get; init; } = Array.Empty<SafeEmit>();
    /// <summary>True once the stream has reached its terminal state.</summary>
    public bool Finished { get; init; }

    // Populated by the SDK from the session / response headers, not the body.
    /// <summary>Backend job id for the session this chunk belongs to.</summary>
    public string? JobId { get; init; }
    /// <summary>Server request id (<c>x-request-id</c>) for this chunk submission.</summary>
    public string? RequestId { get; init; }
}

/// <summary>
/// Result of <c>protect_buffered(...)</c> — the policy was checked over the full
/// response rather than streamed.
/// </summary>
public sealed record BufferedResult
{
    /// <summary>True when the response (or input) was blocked by policy.</summary>
    public required bool Blocked { get; init; }
    /// <summary>Customer-facing message when blocked; otherwise null.</summary>
    public string? BlockMessage { get; init; }
    /// <summary>The released text after filtering/masking; null when blocked.</summary>
    public string? FilteredText { get; init; }

    /// <summary>True iff the block happened on the input (before generation), not the output.</summary>
    public bool InputBlocked { get; init; }

    /// <summary>Rules that fired during filtering.</summary>
    public IReadOnlyList<TriggeredRule> TriggeredRules { get; init; } = Array.Empty<TriggeredRule>();
    /// <summary>Backend job id for this buffered call.</summary>
    public string? JobId { get; init; }
    /// <summary>Server request id (<c>x-request-id</c>) for support/tracing.</summary>
    public string? RequestId { get; init; }
    /// <summary>Which surface blocked: "prompt", "context", or "none". Null on a job without context analysis.</summary>
    public string? BlockedBy { get; init; }
    /// <summary>The context-analysis verdict (pointer/rule/degraded), populated on an input block.</summary>
    public ContextModerationResult? Context { get; init; }
}

/// <summary>Per-rule streaming support, for showing <em>which</em> rule forces buffering.</summary>
public sealed record RuleCapability
{
    /// <summary>Server-assigned rule id.</summary>
    public string RuleId { get; init; } = "";
    /// <summary>Human-readable rule name.</summary>
    public string RuleName { get; init; } = "";
    /// <summary>Rule type (e.g. <c>regex</c>, <c>llm</c>).</summary>
    public string RuleType { get; init; } = "";
    /// <summary>The rule's decision (e.g. <c>block</c>, <c>mask</c>, <c>flag</c>).</summary>
    public string Decision { get; init; } = "";
    /// <summary>True when the rule runs in monitor-only mode.</summary>
    public bool Monitoring { get; init; }
    /// <summary>True when this rule can run on a token stream.</summary>
    public bool StreamingSupported { get; init; }
    /// <summary>Why this rule forces buffering when it can't stream; otherwise null.</summary>
    public string? FallbackReason { get; init; }
    /// <summary>
    /// Which of four roles the rule plays in a streaming request:
    /// <c>enforce_streaming</c>, <c>enforce_postflight</c>, <c>stream_observed</c>,
    /// <c>postflight_observed</c>. <see cref="StreamingSupported"/> cannot express
    /// this — it is false for EVERY monitor rule, so a stream-capable observer and
    /// a full-context one look identical without this field.
    /// <para>
    /// Left as a string rather than an enum so a fifth server-side role does not
    /// break older SDKs. Compare against the four names and treat anything else
    /// as unknown.
    /// </para>
    /// <para>
    /// Three limits inherited from the server. It is per-RULE and ignores
    /// policy-level gates, so a rule can report <c>stream_observed</c> while the
    /// request still buffers — read <see cref="StreamingCapability.Mode"/> for
    /// whether the request streams at all. It reflects the project's
    /// <c>StreamingMode</c>, so a project set to buffered sees full-context roles
    /// throughout. And it describes the rule's role ON THE SERVER THAT
    /// ANSWERED: servers with monitor streaming deployed run monitor rules as
    /// observers and the request streams; an older server still buffers any
    /// monitor policy and answers <c>Mode="buffered"</c>,
    /// <c>Reason="monitor_mode"</c> until upgraded. Either way,
    /// <see cref="StreamingCapability.Mode"/> is the delivery verdict — never
    /// this field.
    /// </para>
    /// Null when the rule cannot be planned at all (unregistered type, or a
    /// handler that raises), since such a rule has no role.
    /// </summary>
    public string? ExecutionRole { get; init; }
}

/// <summary>Result of <c>streaming.PreflightAsync(...)</c>.</summary>
public sealed record StreamingCapability
{
    /// <summary>"streaming", "buffered", or "unsupported". Left as a string so a
    /// new server mode does not break older SDKs.</summary>
    public required string Mode { get; init; }

    /// <summary>Recommended client UX for this policy.</summary>
    public required RecommendedClientBehavior RecommendedClientBehavior { get; init; }
    /// <summary>The project the capability was resolved for.</summary>
    public required string ProjectId { get; init; }

    /// <summary>The project's configured streaming preset (auto/buffered/incremental).</summary>
    public string? StreamingMode { get; init; }

    /// <summary>Stable machine reason code; null when <see cref="Mode"/> is "streaming".</summary>
    public string? Reason { get; init; }

    /// <summary>Human-readable explanation of <see cref="Reason"/>.</summary>
    public string? ReasonDetail { get; init; }

    /// <summary>The SDK may cache this result until this time; null if not cacheable.</summary>
    public DateTimeOffset? ValidUntil { get; init; }

    /// <summary>Per-rule streaming-support detail (which rules force buffering).</summary>
    public IReadOnlyList<RuleCapability> Rules { get; init; } = Array.Empty<RuleCapability>();
}

/// <summary>
/// A minted browser SSE stream token (<c>session.MintStreamTokenAsync()</c>).
///
/// Hand only <see cref="Token"/> (or the ready-to-use <see cref="Url"/>) to the
/// browser — keep your API key server-side. Re-mint before <see cref="ExpiresIn"/>
/// seconds elapse.
/// </summary>
public sealed record StreamToken
{
    /// <summary>The opaque stream token; hand only this (or <see cref="Url"/>) to the browser.</summary>
    public required string Token { get; init; }
    /// <summary>Seconds until the token expires; re-mint before then.</summary>
    public required int ExpiresIn { get; init; }

    /// <summary>Ready-to-use browser SSE URL: {base}/v1/jobs/{id}/stream?stream_token=…</summary>
    public required string Url { get; init; }
}
