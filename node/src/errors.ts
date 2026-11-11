/**
 * Typed error hierarchy for the CollieAi SDK — mirrors the Python SDK so the
 * two stay aligned. Catch these instead of parsing strings.
 */
export class CollieError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** Unexpected/unmapped HTTP error or malformed response body. */
export class CollieApiError extends CollieError {
  readonly statusCode?: number;
  readonly code?: string;
  constructor(message: string, opts: { statusCode?: number; code?: string } = {}) {
    super(message);
    this.statusCode = opts.statusCode;
    this.code = opts.code;
  }
}

/** Transport-level failure before a usable HTTP response. */
export class CollieConnectionError extends CollieError {}

// --- Preflight ---------------------------------------------------------------
export class PreflightError extends CollieError {}
export class ProjectNotFound extends PreflightError {}
export class StreamingFeatureDisabled extends PreflightError {}
export class PlanNotEntitled extends PreflightError {}
export class UnknownRuleType extends PreflightError {}
export class PolicyNotStreamable extends PreflightError {}

// --- High-level wrapper preconditions ---------------------------------------
export class BufferedFallbackRequired extends CollieError {}
export class ProviderStreamFactoryRequired extends CollieError {}

/** The input policy MASKED this prompt, and the provider factory cannot
 * receive the masked text — proceeding would send the ORIGINAL prompt to the
 * model. Remedy: call `moderate.input` yourself, build the stream over
 * `result.filteredText` (it can legitimately be `""`), and pass
 * `inputResult: result` — that path is exempt because the caller
 * demonstrably holds the filtered text. */
export class MaskedInputError extends CollieError {}

// --- Chunk submission --------------------------------------------------------
export class ChunkError extends CollieError {
  readonly statusCode?: number;
  readonly code?: string;
  constructor(message: string, opts: { statusCode?: number; code?: string } = {}) {
    super(message);
    this.statusCode = opts.statusCode;
    this.code = opts.code;
  }
}
export class ChunkRetryExhausted extends ChunkError {}
export class ChunkQuotaExceeded extends ChunkError {}
export class ChunkPolicyChanged extends ChunkError {}
export class ChunkSessionUnrecoverable extends ChunkError {}
export class ChunkSessionFinished extends ChunkError {}
export class ChunkIdempotencyConflict extends ChunkError {}
export class ChunkSequenceConflict extends ChunkError {}
export class ChunkConcurrentSubmit extends ChunkError {}
export class ChunkStreamingUnsupported extends ChunkError {}
/**
 * 503 `chunk_resolution_unavailable` — the server could not RESOLVE the
 * policy at all (a resolver dependency was unreachable), as opposed to the
 * policy being unable to stream. Retryable: the SDK retries the same
 * sequence, and when the outage outlasts the retry budget this is the
 * `cause` of the `ChunkRetryExhausted` it throws.
 */
export class ChunkResolutionUnavailable extends ChunkError {}

/** Overlapping push()/finish() on one session — submission must be serial. */
export class ConcurrentSessionUseError extends CollieError {}

/** moderate.input ended failed/expired or timed out without a verdict. */
export class ModerationError extends CollieError {}

/** moderate.input / moderate.output gave up waiting: the SDK's own poll
 * budget (`timeoutS`) ran out before a verdict. A `ModerationError`, so
 * existing `instanceof ModerationError` checks keep catching it. `jobId`
 * names the job that may still finish server-side. */
export class ModerationTimeout extends ModerationError {
  readonly jobId?: string;
  constructor(message: string, opts: { jobId?: string } = {}) {
    super(message);
    this.jobId = opts.jobId;
  }
}

/** File analysis is not available for this request: the deployment has it
 * off (HTTP 422, code `file_analysis_unavailable`), or a job with
 * attachments finished its input check without a `file_result` (code
 * `file_result_missing` — a server that does not analyze files). */
export class FileAnalysisUnavailable extends CollieApiError {}
