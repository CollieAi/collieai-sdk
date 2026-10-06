// High-level streaming events. ProtectStreamAsync / StreamEventsAsync yield
// these; callers discriminate with C# pattern matching (`switch (ev) { case
// SafeDelta d: … }`). Policy decisions are events, not exceptions — only
// infrastructure failures throw.

namespace CollieAi;

/// <summary>Base type for streaming events. Carries logging context.</summary>
public abstract record CollieStreamEvent
{
    /// <summary>Backend job id this event belongs to.</summary>
    public string? JobId { get; init; }
    /// <summary>Server request id (<c>x-request-id</c>) for support/tracing.</summary>
    public string? RequestId { get; init; }
    /// <summary>Conversation id supplied on the originating request, if any.</summary>
    public string? ConversationId { get; init; }
    /// <summary>Correlation id supplied on the originating request, if any.</summary>
    public string? CorrelationId { get; init; }
}

/// <summary>A CollieAi-released text delta — safe to forward to the end user.</summary>
public sealed record SafeDelta : CollieStreamEvent
{
    /// <summary>The safe text to forward to the end user.</summary>
    public required string Text { get; init; }
    /// <summary>Zero-based sequence number of the chunk that produced this delta.</summary>
    public required int Sequence { get; init; }
}

/// <summary>A rule blocked the output mid-stream. Terminal.</summary>
public sealed record Blocked : CollieStreamEvent
{
    /// <summary>Customer-facing block message (configured rule message or backend default).</summary>
    public string? BlockMessage { get; init; }
    /// <summary>Rules that fired to cause the block.</summary>
    public IReadOnlyList<TriggeredRule> TriggeredRules { get; init; } = Array.Empty<TriggeredRule>();
}

/// <summary>The input was blocked before any provider output was consumed. Terminal.</summary>
public sealed record InputBlocked : CollieStreamEvent
{
    /// <summary>Customer-facing block message (configured rule message or backend default).</summary>
    public string? BlockMessage { get; init; }
    /// <summary>Rules that fired to block the input.</summary>
    public IReadOnlyList<TriggeredRule> TriggeredRules { get; init; } = Array.Empty<TriggeredRule>();
    /// <summary>Which surface blocked: "prompt", "context", or "none". Null on a job without context analysis.</summary>
    public string? BlockedBy { get; init; }
    /// <summary>The context-analysis verdict (pointer/rule/degraded), when present.</summary>
    public ContextModerationResult? Context { get; init; }
}

/// <summary>The stream finished normally. Terminal.</summary>
public sealed record Finished : CollieStreamEvent
{
    /// <summary>Optional provider-supplied finish reason, when known.</summary>
    public string? FinishReason { get; init; }
    /// <summary>Which surface blocked: "prompt", "context", or "none". Null on a job without context analysis.</summary>
    public string? BlockedBy { get; init; }
    /// <summary>The INPUT-phase context verdict, carried on the success terminal too
    /// so a monitored/degraded-but-allowed context is observable. Null when no context analysis ran.</summary>
    public ContextModerationResult? Context { get; init; }
}

/// <summary>
/// The policy cannot safely stream; the caller should switch to a buffered UX.
/// Part of the event vocabulary; <c>ProtectStreamAsync(RequireStreaming=true)</c>
/// throws <see cref="BufferedFallbackRequiredException"/> rather than yielding
/// this, so it is not produced by the current flows.
/// </summary>
public sealed record BufferedFallback : CollieStreamEvent
{
    /// <summary>Stable machine reason code for why streaming is unavailable.</summary>
    public required string Reason { get; init; }
    /// <summary>Recommended client UX; defaults to <c>buffer_then_show</c>.</summary>
    public string RecommendedClientBehavior { get; init; } = "buffer_then_show";
}

/// <summary>
/// An SSE subscription was interrupted (idle timeout or disconnect) but can be
/// resumed from <see cref="LastEventId"/>. With auto-resume the SDK reconnects
/// after emitting this; otherwise iteration stops here and the caller may resume
/// by passing <see cref="LastEventId"/> back to <c>StreamEventsAsync</c>.
/// </summary>
public sealed record StreamInterrupted : CollieStreamEvent
{
    /// <summary>"idle_timeout" or "disconnect".</summary>
    public required string Reason { get; init; }
    /// <summary>True when the subscription can be resumed from <see cref="LastEventId"/>.</summary>
    public bool Resumable { get; init; } = true;
    /// <summary>The last SSE event id seen, to resume from on reconnect.</summary>
    public string? LastEventId { get; init; }
}
