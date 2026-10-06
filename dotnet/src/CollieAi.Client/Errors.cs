// Typed exception hierarchy for the CollieAi .NET SDK.
//
// Mirrors the SDK's error-handling contract (and the proven Python
// SDK's errors.py). Applications catch these instead of parsing error strings;
// each maps to a raw API error code or a client-side precondition. The full
// tree is defined here — not just the codes the MVP emits — so the public
// surface stays stable across slices.
//
// Cancellation follows .NET conventions: OperationCanceledException is NOT
// wrapped as a CollieException. Callers that handle both should catch
// OperationCanceledException before CollieException.

namespace CollieAi;

/// <summary>Base for every error raised by the SDK.</summary>
public class CollieException : Exception
{
    /// <summary>Initializes a new instance with a message and optional inner exception.</summary>
    public CollieException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// An unexpected or unmapped HTTP error from the API (including a malformed
/// response body). Carries the HTTP status and the wire error code/message when
/// present.
/// </summary>
public sealed class CollieApiException : CollieException
{
    /// <summary>HTTP status code, when derived from a response.</summary>
    public int? StatusCode { get; }
    /// <summary>Wire error code (<c>error.type</c>/<c>error.code</c>), when present.</summary>
    public string? Code { get; }

    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public CollieApiException(
        string message,
        int? statusCode = null,
        string? code = null,
        Exception? innerException = null)
        : base(message, innerException)
    {
        StatusCode = statusCode;
        Code = code;
    }
}

/// <summary>
/// A transport-level failure (timeout, connection refused/reset, DNS, …) before
/// a usable HTTP response was received. Wraps the underlying exception as
/// <see cref="Exception.InnerException"/>.
/// </summary>
public sealed class CollieConnectionException : CollieException
{
    /// <summary>Initializes a new instance wrapping the underlying transport failure.</summary>
    public CollieConnectionException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}

// --- Preflight ---------------------------------------------------------------

/// <summary>Base for streaming-preflight failures.</summary>
public class PreflightException : CollieException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public PreflightException(string message) : base(message) { }
}

/// <summary>The API key cannot access the project, or the project id is invalid.</summary>
public sealed class ProjectNotFoundException : PreflightException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public ProjectNotFoundException(string message) : base(message) { }
}

/// <summary>The global streaming feature flag is disabled on the backend.</summary>
public sealed class StreamingFeatureDisabledException : PreflightException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public StreamingFeatureDisabledException(string message) : base(message) { }
}

/// <summary>The project/account plan does not include customer-owned streaming.</summary>
public sealed class PlanNotEntitledException : PreflightException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public PlanNotEntitledException(string message) : base(message) { }
}

/// <summary>The policy contains an enabled rule whose handler is not registered.</summary>
public sealed class UnknownRuleTypeException : PreflightException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public UnknownRuleTypeException(string message) : base(message) { }
}

/// <summary>The policy has no safe streaming or buffered path for the direction.</summary>
public sealed class PolicyNotStreamableException : PreflightException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public PolicyNotStreamableException(string message) : base(message) { }
}

// --- High-level wrapper preconditions ----------------------------------------

/// <summary>
/// Raised when the caller required true streaming (<c>RequireStreaming = true</c>)
/// but the policy must buffer. Raised before the provider stream is invoked.
/// </summary>
public sealed class BufferedFallbackRequiredException : CollieException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public BufferedFallbackRequiredException(string message) : base(message) { }
}

/// <summary>
/// Raised when high-level streaming is invoked without a usable
/// <c>RawStreamFactory</c> while input checking is enabled — a deferred factory
/// is required so the provider cannot run before the input check can block it.
/// </summary>
public sealed class ProviderStreamFactoryRequiredException : CollieException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public ProviderStreamFactoryRequiredException(string message) : base(message) { }
}

/// <summary>The input policy MASKED this prompt, and the provider factory
/// cannot receive the masked text — proceeding would send the ORIGINAL prompt
/// to the model. Remedy: call <c>Moderation.CheckInputAsync</c> yourself,
/// build the stream over <c>result.FilteredText</c> (it can legitimately be
/// <c>""</c> — a full wipe), and pass <c>InputResult = result</c> — that path
/// is exempt because the caller demonstrably holds the filtered text.</summary>
public sealed class MaskedInputException : CollieException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public MaskedInputException(string message) : base(message) { }
}

// --- Chunk submission --------------------------------------------------------

/// <summary>
/// Base for chunk-submission errors. Carries the wire code + HTTP status when
/// derived from an API response.
/// </summary>
public class ChunkException : CollieException
{
    /// <summary>HTTP status code, when derived from a response.</summary>
    public int? StatusCode { get; }
    /// <summary>Wire error code (<c>error.type</c>/<c>error.code</c>), when present.</summary>
    public string? Code { get; }

    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkException(
        string message,
        int? statusCode = null,
        string? code = null,
        Exception? innerException = null)
        : base(message, innerException)
    {
        StatusCode = statusCode;
        Code = code;
    }
}

/// <summary>The per-chunk retry budget/ceiling was exhausted on a retryable error.</summary>
public sealed class ChunkRetryExhaustedException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkRetryExhaustedException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

/// <summary>429 with no usable Retry-After (missing, negative, or beyond the ceiling).</summary>
public sealed class ChunkQuotaExceededException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkQuotaExceededException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

/// <summary>409 chunk_policy_changed — fatal; create a new job for the new policy.</summary>
public sealed class ChunkPolicyChangedException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkPolicyChangedException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

/// <summary>409 chunk_session_unrecoverable — fatal; the stream can't be repaired.</summary>
public sealed class ChunkSessionUnrecoverableException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkSessionUnrecoverableException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

/// <summary>409 chunk_session_finished — the session is already terminal.</summary>
public sealed class ChunkSessionFinishedException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkSessionFinishedException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

/// <summary>409 chunk_idempotency_conflict — same sequence replayed with different content.</summary>
public sealed class ChunkIdempotencyConflictException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkIdempotencyConflictException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

/// <summary>409 chunk_sequence_conflict — a sequence gap (the SDK owns sequence numbers).</summary>
public sealed class ChunkSequenceConflictException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkSequenceConflictException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

/// <summary>409 chunk_concurrent_submit — a server-side in-flight collision (retryable).</summary>
public sealed class ChunkConcurrentSubmitException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkConcurrentSubmitException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

/// <summary>400 chunk_streaming_unsupported — the policy can't be served by the streaming engine.</summary>
public sealed class ChunkStreamingUnsupportedException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkStreamingUnsupportedException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

/// <summary>
/// 503 chunk_resolution_unavailable — the server could not RESOLVE the policy
/// at all (a resolver dependency was unreachable), as opposed to the policy
/// being unable to stream. Retryable: the SDK retries the same sequence, and
/// when the outage outlasts the retry budget this is the
/// <see cref="Exception.InnerException"/> of the
/// <see cref="ChunkRetryExhaustedException"/> it throws.
/// </summary>
public sealed class ChunkResolutionUnavailableException : ChunkException
{
    /// <summary>Initializes a new instance with the wire status/code and optional inner exception.</summary>
    public ChunkResolutionUnavailableException(
        string message, int? statusCode = null, string? code = null, Exception? innerException = null)
        : base(message, statusCode, code, innerException) { }
}

// --- Session / moderation ----------------------------------------------------

/// <summary>
/// Overlapping <c>PushAsync</c>/<c>FinishAsync</c> calls were attempted on one
/// session. Chunk submission must be serial per session.
/// </summary>
public sealed class ConcurrentSessionUseException : CollieException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public ConcurrentSessionUseException(string message) : base(message) { }
}

/// <summary>
/// <c>CheckInputAsync</c> ended in an unexpected terminal state (failed/expired)
/// or timed out before producing a verdict. (A policy block is NOT an error — it
/// is a normal <see cref="InputModerationResult"/> with <c>Blocked = true</c>.)
/// </summary>
public sealed class ModerationException : CollieException
{
    /// <summary>Initializes a new instance with a message.</summary>
    public ModerationException(string message) : base(message) { }
}
