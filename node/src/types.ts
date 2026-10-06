/**
 * Public types for the CollieAi SDK. Wire fields are snake_case; the SDK maps
 * them to camelCase (the mapping lives in client.ts / streaming.ts).
 */

export interface TriggeredRule {
  ruleId: string;
  ruleName: string;
  ruleType: string;
  decision: string;
  monitoring: boolean;
  matchInfo?: Record<string, unknown> | null;
}

/** Closed status enum for context analysis.
 * Widened to string so a newer server status doesn't break an older SDK. */
export type ContextStatus =
  | "not_provided"
  | "disabled"
  | "not_run"
  | "clean"
  | "monitored"
  | "blocked"
  | "degraded";

export interface ContextModerationResult {
  status: ContextStatus | string;
  blocked: boolean;
  blockMessage?: string | null;
  /** RFC 6901 JSON Pointer of the triggering leaf (a path, never a value). */
  triggeringPointer?: string | null;
  triggeringRuleId?: string | null;
  triggeringRuleType?: string | null;
  parseDegraded: boolean;
  limitExceeded: boolean;
  inferenceDegraded: boolean;
}

export interface InputModerationResult {
  allowed: boolean;
  blocked: boolean;
  blockMessage?: string | null;
  originalText: string;
  filteredText?: string | null;
  triggeredRules: TriggeredRule[];
  jobId?: string | null;
  requestId?: string | null;
  /** Context analysis. blockedBy ∈
   * {"prompt","context","none"}; context is null on a job without analysis. */
  blockedBy?: string | null;
  context?: ContextModerationResult | null;
}

/** Result of `collie.moderate.output(...)`. No
 * `context`/`blockedBy`: context is an input surface (the server rejects an
 * outbound-only job carrying one) and output moderation has a single
 * surface, so there is nothing to attribute between. */
export interface OutputModerationResult {
  allowed: boolean;
  blocked: boolean;
  blockMessage?: string | null;
  originalText: string;
  /** The MASKED output when masking rules fired. Send this, not the
   * original — a caller that ignores it silently defeats outbound masking
   * (PII redaction etc.). */
  filteredText?: string | null;
  triggeredRules: TriggeredRule[];
  jobId?: string | null;
  requestId?: string | null;
}

/** One CollieAi-released emit for a submitted chunk. */
export interface SafeEmit {
  text: string;
  blocked: boolean;
  /** Customer-facing message for blocked emits (the blocking rule's
   * configured message or the backend default). Null on non-block emits. */
  blockMessage: string | null;
  final: boolean;
  triggeredRules: Array<Record<string, unknown>>;
}

/** Response from one session.push() / session.finish(). */
export interface ChunkResult {
  sequence: number;
  accepted: boolean;
  emits: SafeEmit[];
  finished: boolean;
  jobId?: string | null;
  requestId?: string | null;
}

// --- High-level streaming events (discriminated on `type`) -------------------
interface BaseEvent {
  jobId?: string | null;
  requestId?: string | null;
  conversationId?: string | null;
  correlationId?: string | null;
}
export interface SafeDelta extends BaseEvent {
  type: "delta";
  text: string;
  sequence: number;
}
export interface Blocked extends BaseEvent {
  type: "blocked";
  blockMessage?: string | null;
  triggeredRules: TriggeredRule[];
}
export interface InputBlocked extends BaseEvent {
  type: "input_blocked";
  blockMessage?: string | null;
  triggeredRules: TriggeredRule[];
  /** Context analysis: blockedBy ∈
   * {"prompt","context","none"} + the typed context verdict, so a wrapper
   * caller can tell a context block from a prompt block and show the pointer. */
  blockedBy?: string | null;
  context?: ContextModerationResult | null;
}
export interface Finished extends BaseEvent {
  type: "finished";
  finishReason?: string | null;
  /** The INPUT-phase context verdict, carried on
   * the success terminal too so a monitored/degraded-but-allowed context is
   * observable, not only on a block. Null when no context analysis ran. */
  blockedBy?: string | null;
  context?: ContextModerationResult | null;
}
export interface BufferedFallback {
  type: "buffered_fallback";
  reason: string;
  recommendedClientBehavior: "buffer_then_show";
}
export interface StreamInterrupted extends BaseEvent {
  type: "interrupted";
  reason: string; // "idle_timeout" | "disconnect"
  resumable: boolean;
  lastEventId?: string | null;
}
export type StreamEvent =
  | SafeDelta
  | Blocked
  | InputBlocked
  | Finished
  | BufferedFallback
  | StreamInterrupted;

export interface BufferedResult {
  blocked: boolean;
  blockMessage?: string | null;
  filteredText?: string | null;
  inputBlocked: boolean;
  triggeredRules: TriggeredRule[];
  jobId?: string | null;
  requestId?: string | null;
  /** Context analysis — populated on an input
   * block so a buffered caller sees a context block + the pointer/rule detail. */
  blockedBy?: string | null;
  context?: ContextModerationResult | null;
}

/**
 * Which of four roles a rule plays in a streaming request.
 *
 * The `(string & {})` arm is deliberate: it keeps autocomplete for the four
 * known names while still accepting a fifth the server may add later. This SDK
 * CONSUMES the value, so a closed union would turn a server-side addition into
 * a breaking change for clients that never touch the new role.
 */
export type ExecutionRole =
  | "enforce_streaming"
  | "enforce_postflight"
  | "stream_observed"
  | "postflight_observed"
  // eslint-disable-next-line @typescript-eslint/ban-types
  | (string & {});

export interface RuleCapability {
  ruleId: string;
  ruleName: string;
  ruleType: string;
  decision: string;
  monitoring: boolean;
  streamingSupported: boolean;
  fallbackReason?: string | null;
  /**
   * The rule's role in a streaming request. `streamingSupported` cannot
   * express this — it is false for EVERY monitor rule, so a stream-capable
   * observer and a full-context one look identical without this field.
   *
   * Three limits inherited from the server. It is per-RULE and ignores
   * policy-level gates, so a rule can report `stream_observed` while the
   * request still buffers — read `StreamingCapability.mode` for whether the
   * request streams at all. It reflects the project's `streamingMode`, so
   * a project set to `buffered` sees full-context roles throughout. And it
   * describes the rule's role ON THE SERVER THAT ANSWERED: servers with
   * monitor streaming deployed run monitor rules as observers and the
   * request streams; an older server still buffers any monitor policy and
   * answers `mode: "buffered"`, `reason: "monitor_mode"` until upgraded.
   * Either way, `mode` is the delivery verdict — never this field.
   *
   * `null` when the rule cannot be planned at all (unregistered type, or a
   * handler that raises), since such a rule has no role.
   */
  executionRole?: ExecutionRole | null;
}

export interface StreamingCapability {
  mode: "streaming" | "buffered" | "unsupported";
  recommendedClientBehavior: "stream" | "buffer_then_show" | "fail_fast";
  projectId: string;
  streamingMode?: string | null;
  reason?: string | null;
  reasonDetail?: string | null;
  /** SDK may cache until this time (epoch ms); null if not cacheable. */
  validUntil?: number | null;
  rules: RuleCapability[];
}

export interface StreamToken {
  token: string;
  expiresIn: number;
  /** Ready-to-use browser SSE URL: {base}/v1/jobs/{id}/stream?stream_token=... */
  url: string;
}

/** A factory returning the provider's text-delta stream. It receives an
 * `AbortSignal` that is aborted when the SDK tears down the stream (early exit,
 * block, or the caller abandoning the result) — forward it to your provider so
 * paid LLM work stops promptly and cleanup can't stall on an in-flight read.
 * Pass it in the provider's request options (NOT the JSON body), e.g.
 * `client.chat.completions.create({ ...body, stream: true }, { signal })`.
 * Factories that ignore the argument still work. */
export type RawStreamFactory = (signal?: AbortSignal) => AsyncIterable<string>;
