/**
 * Streaming: preflight, low-level session, protectStream/protectBuffered, SSE
 * subscription. Mirrors the Python streaming module.
 */
import type { CollieClient } from "./client.js";
import {
  BufferedFallbackRequired,
  ChunkSessionFinished,
  ChunkSessionUnrecoverable,
  CollieApiError,
  CollieError,
  ConcurrentSessionUseError,
  MaskedInputError,
  PlanNotEntitled,
  PolicyNotStreamable,
  PreflightError,
  ProjectNotFound,
  ProviderStreamFactoryRequired,
  StreamingFeatureDisabled,
  UnknownRuleType,
} from "./errors.js";
import { toTriggeredRule } from "./_map.js";
import { PollPacer } from "./pacing.js";
import type {
  Blocked,
  BufferedResult,
  ChunkResult,
  ContextModerationResult,
  InputModerationResult,
  RawStreamFactory,
  RuleCapability,
  SafeDelta,
  StreamEvent,
  StreamingCapability,
  StreamToken,
  TriggeredRule,
} from "./types.js";

const SESSION_ORIGIN = "streaming.session";
const PREFLIGHT_ORIGIN = "streaming.preflight";
const PROTECT_STREAM_ORIGIN = "protect_stream";
const PROTECT_BUFFERED_ORIGIN = "protect_buffered";
const PREFLIGHT_MIN_VALIDITY_S = 5;
const DEFAULT_FLUSH_INTERVAL_S = 0.05;
const DEFAULT_MAX_DELTAS = 16;
const DEFAULT_MAX_CHARS = 2048;
const BUFFERED_TERMINAL = new Set(["completed", "outbound_blocked"]);
const BUFFERED_FAILURE = new Set(["failed", "expired"]);

const PREFLIGHT_ERRORS: Record<string, new (m?: string) => PreflightError> = {
  project_not_found: ProjectNotFound,
  plan_not_entitled: PlanNotEntitled,
  unknown_rule_type: UnknownRuleType,
  streaming_feature_disabled: StreamingFeatureDisabled,
  policy_not_streamable: PolicyNotStreamable,
};

export interface ProtectStreamOptions {
  input: string;
  rawStreamFactory: RawStreamFactory;
  projectId?: string;
  conversationId?: string;
  correlationId?: string;
  /** Structured data or raw string analyzed alongside the input prompt.
   * Forwarded to the input gate. */
  context?: unknown;
  contextFormat?: "auto" | "json" | "text";
  checkInput?: boolean;
  /** A previously obtained input result to avoid a duplicate input check
   * (its `originalText` must equal `input` and `allowed` must be true).
   * Requires `checkInput` unset/true — combining it with
   * `checkInput: false` is a contradiction and throws (2.1; it was
   * silently ignored before, which also silently dropped the claim). The
   * wrapper forwards the result's `jobId` as the single-use session
   * claim; on the claim's 409s (`input_gate_stale` /
   * `input_gate_unverifiable` / `input_gate_claimed`) there is
   * deliberately NO automatic remedy on this path — the result may carry
   * a context the wrapper never saw, so the typed errors surface to your
   * code. The masked-input fail-closed is also exempt here: you
   * demonstrably hold `filteredText` and own what the factory streams. */
  inputResult?: InputModerationResult;
  requireStreaming?: boolean;
  flushIntervalS?: number;
  maxDeltas?: number;
  maxChars?: number;
}

export type ProtectBufferedOptions = Omit<
  ProtectStreamOptions,
  "requireStreaming" | "flushIntervalS" | "maxDeltas" | "maxChars" | "inputResult"
> & {
  timeoutS?: number;
  /** A previously obtained input result to skip the duplicate input check
   * (its `originalText` must equal `input` and `allowed` must be true;
   * requires `checkInput` unset/true — combining with `checkInput: false`
   * throws, same as on `protectStream`). Unlike the streaming path this
   * is a TRUSTED-CLIENT reuse, not the claim protocol: the buffered path
   * opens no streaming session and sends no `input_job_id`, so the server
   * neither verifies nor consumes the result — no `input_gate_*` 409s can
   * occur here, and freshness/single-use are the caller's
   * responsibility. */
  inputResult?: InputModerationResult;
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function resolveProviderIterator(provider: unknown): AsyncIterator<string> {
  const obj = provider as Record<symbol | string, unknown> | null;
  if (obj != null && typeof (obj as { then?: unknown }).then === "function") {
    throw new ProviderStreamFactoryRequired(
      "rawStreamFactory must return an async iterable, not a Promise — pass `() => myStream()`.",
    );
  }
  const aiter = obj?.[Symbol.asyncIterator] as undefined | (() => AsyncIterator<string>);
  if (typeof aiter !== "function") {
    throw new ProviderStreamFactoryRequired(
      `rawStreamFactory must return an async iterable; got ${provider == null ? "null" : typeof provider}.`,
    );
  }
  const it = aiter.call(obj);
  if (!it || typeof it.next !== "function") {
    throw new ProviderStreamFactoryRequired(
      "rawStreamFactory's async iterator has no next().",
    );
  }
  return it;
}

async function closeProvider(provider: unknown): Promise<void> {
  const obj = provider as Record<string, unknown> | null;
  if (!obj) return;
  // Honor whichever teardown method the provider object exposes. SDK stream
  // objects variously use return() (async generators), or close()/aclose()
  // (provider stream wrappers) for releasing the underlying connection.
  for (const name of ["return", "aclose", "close"] as const) {
    if (typeof obj[name] === "function") {
      try {
        await (obj[name] as () => unknown)();
      } catch {
        /* best-effort */
      }
      return;
    }
  }
  const aiter = (obj as { [Symbol.asyncIterator]?: () => AsyncIterator<string> })[Symbol.asyncIterator];
  if (typeof aiter === "function") {
    const it = aiter.call(obj);
    if (it && typeof it.return === "function") {
      try {
        await it.return();
      } catch {
        /* best-effort */
      }
    }
  }
}

async function closeIterator(it: AsyncIterator<string>): Promise<void> {
  if (typeof it.return === "function") {
    try {
      await it.return();
    } catch {
      /* best-effort */
    }
  }
}

const FLUSH = Symbol("flush");

async function* batchDeltas(
  iterator: AsyncIterator<string>,
  opts: { flushIntervalS: number; maxDeltas: number; maxChars: number },
): AsyncGenerator<string> {
  let buf: string[] = [];
  let chars = 0;
  let deltas = 0;
  let windowStart = 0;
  let pending: Promise<IteratorResult<string>> | null = null;
  try {
    while (true) {
      if (!pending) pending = iterator.next();
      let result: IteratorResult<string> | typeof FLUSH;
      if (buf.length) {
        const remaining = opts.flushIntervalS * 1000 - (Date.now() - windowStart);
        if (remaining <= 0) {
          yield buf.join("");
          buf = [];
          chars = 0;
          deltas = 0;
          continue;
        }
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timeout = new Promise<typeof FLUSH>((res) => {
          timer = setTimeout(() => res(FLUSH), remaining);
        });
        result = await Promise.race([pending, timeout]);
        if (timer) clearTimeout(timer);
        if (result === FLUSH) {
          yield buf.join("");
          buf = [];
          chars = 0;
          deltas = 0;
          continue;
        }
      } else {
        result = await pending;
      }
      pending = null;
      const r = result as IteratorResult<string>;
      if (r.done) break;
      const delta = r.value;
      if (delta) {
        if (!buf.length) windowStart = Date.now();
        buf.push(delta);
        chars += delta.length;
        deltas += 1;
        if (deltas >= opts.maxDeltas || chars >= opts.maxChars) {
          yield buf.join("");
          buf = [];
          chars = 0;
          deltas = 0;
        }
      }
    }
    if (buf.length) yield buf.join("");
  } finally {
    await closeIterator(iterator);
  }
}

async function* iterSse(
  response: Response,
  onActivity?: () => void,
): AsyncGenerator<[string | null, string, string]> {
  if (!response.body) return;
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let eventId: string | null = null;
  let eventType: string | null = null;
  let dataLines: string[] = [];
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      onActivity?.(); // bytes arrived — reset the idle timer
      buffer += decoder.decode(value, { stream: true });
      let idx: number;
      while ((idx = buffer.indexOf("\n")) >= 0) {
        let line = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 1);
        if (line.endsWith("\r")) line = line.slice(0, -1);
        if (line === "") {
          if (eventType !== null || dataLines.length) {
            yield [eventId, eventType ?? "message", dataLines.join("\n")];
          }
          eventId = null;
          eventType = null;
          dataLines = [];
          continue;
        }
        if (line.startsWith(":")) continue; // comment / keepalive
        const ci = line.indexOf(":");
        let field: string;
        let value: string;
        if (ci === -1) {
          field = line;
          value = "";
        } else {
          field = line.slice(0, ci);
          value = line.slice(ci + 1);
          if (value.startsWith(" ")) value = value.slice(1);
        }
        if (field === "event") eventType = value;
        else if (field === "data") dataLines.push(value);
        else if (field === "id") eventId = value;
      }
    }
    if (eventType !== null || dataLines.length) {
      yield [eventId, eventType ?? "message", dataLines.join("\n")];
    }
  } finally {
    // cancel() (not just releaseLock) closes the underlying body so an early
    // caller exit doesn't leave the HTTP connection open.
    try {
      await reader.cancel();
    } catch {
      /* best-effort */
    }
  }
}

/** Re-encode a stream event as an SSE frame for relaying to a browser. */
/** blocked_by + the context verdict (snake_case for the wire) for an SSE relay
 * frame, so a browser sees the context block/pointer/rule + degraded markers.
 * Keys omitted when absent. */
function contextSsePayload(event: {
  blockedBy?: string | null;
  context?: ContextModerationResult | null;
}): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  if (event.blockedBy != null) out.blocked_by = event.blockedBy;
  if (event.context != null) {
    const c = event.context;
    out.context = {
      status: c.status,
      blocked: c.blocked,
      block_message: c.blockMessage ?? null,
      triggering_pointer: c.triggeringPointer ?? null,
      triggering_rule_id: c.triggeringRuleId ?? null,
      triggering_rule_type: c.triggeringRuleType ?? null,
      parse_degraded: c.parseDegraded,
      limit_exceeded: c.limitExceeded,
      inference_degraded: c.inferenceDegraded,
    };
  }
  return out;
}

export function toSse(event: StreamEvent): string {
  let name: string;
  let payload: Record<string, unknown>;
  switch (event.type) {
    case "delta":
      name = "delta";
      payload = { text: event.text, sequence: event.sequence };
      break;
    case "blocked":
      name = "blocked";
      payload = { block_message: event.blockMessage ?? null };
      break;
    case "input_blocked":
      name = "input_blocked";
      payload = { block_message: event.blockMessage ?? null, ...contextSsePayload(event) };
      break;
    case "finished":
      name = "finished";
      payload = { finish_reason: event.finishReason ?? null, ...contextSsePayload(event) };
      break;
    case "interrupted":
      name = "interrupted";
      payload = { reason: event.reason, resumable: event.resumable };
      break;
    case "buffered_fallback":
      name = "buffered_fallback";
      payload = {
        reason: event.reason,
        recommended_client_behavior: event.recommendedClientBehavior,
      };
      break;
    default:
      name = (event as { type: string }).type ?? "message";
      payload = {};
  }
  return `event: ${name}\ndata: ${JSON.stringify(payload)}\n\n`;
}

/** Fail closed when the wrapper's OWN input gate masked the prompt (round
 * 5): the provider factory closes over the ORIGINAL text, so proceeding
 * would stream unmasked content to the model. `!==` (not truthiness): a
 * full wipe to `""` is a real mask verdict. The `inputResult` path is
 * exempt — the caller demonstrably holds `filteredText`. */
function rejectMaskedInput(original: string, filtered: string | null): void {
  if (filtered !== null && filtered !== original) {
    throw new MaskedInputError(
      "the input policy MASKED this prompt; the provider factory would " +
        "stream the ORIGINAL (unmasked) text to the model. Call " +
        "moderate.input yourself, build the provider stream over " +
        "result.filteredText, and pass inputResult: result to the wrapper.",
    );
  }
}

function preflightError(cap: StreamingCapability): PreflightError {
  const Ctor = PREFLIGHT_ERRORS[cap.reason ?? ""] ?? PreflightError;
  return new Ctor(cap.reasonDetail ?? cap.reason ?? "streaming unavailable");
}

function toRuleCapability(r: Record<string, unknown>): RuleCapability {
  if (r === null || typeof r !== "object" || Array.isArray(r)) {
    throw new TypeError("rule capability must be an object");
  }
  return {
    ruleId: (r.rule_id as string) ?? "",
    ruleName: (r.rule_name as string) ?? "",
    ruleType: (r.rule_type as string) ?? "",
    decision: (r.decision as string) ?? "",
    monitoring: Boolean(r.monitoring),
    streamingSupported: Boolean(r.streaming_supported),
    fallbackReason: (r.fallback_reason as string | null) ?? null,
    executionRole: (r.execution_role as string | null) ?? null,
  };
}

function toCapability(d: Record<string, unknown>): StreamingCapability {
  // Validate the contract fields like Pydantic would (mode, behavior, project_id
  // are required Literals/str; rules are objects) — never cache a verdict built
  // from a bad shape. Callers wrap a throw here as invalid_response.
  const mode = d.mode;
  if (mode !== "streaming" && mode !== "buffered" && mode !== "unsupported") {
    throw new TypeError("invalid capability 'mode'");
  }
  const behavior = d.recommended_client_behavior;
  if (behavior !== "stream" && behavior !== "buffer_then_show" && behavior !== "fail_fast") {
    throw new TypeError("invalid capability 'recommended_client_behavior'");
  }
  if (typeof d.project_id !== "string") {
    throw new TypeError("capability missing string 'project_id'");
  }
  const rawRules = d.rules ?? [];
  if (!Array.isArray(rawRules)) throw new TypeError("capability 'rules' must be a list");
  // A present-but-unparsable valid_until is a malformed response, not "no expiry".
  let validUntil: number | null = null;
  if (d.valid_until != null) {
    const parsed = typeof d.valid_until === "string" ? Date.parse(d.valid_until) : NaN;
    if (Number.isNaN(parsed)) throw new TypeError("capability has unparsable 'valid_until'");
    validUntil = parsed;
  }
  return {
    mode,
    recommendedClientBehavior: behavior,
    projectId: d.project_id,
    streamingMode: (d.streaming_mode as string | null) ?? null,
    reason: (d.reason as string | null) ?? null,
    reasonDetail: (d.reason_detail as string | null) ?? null,
    validUntil,
    rules: (rawRules as Array<Record<string, unknown>>).map(toRuleCapability),
  };
}

function toChunkResult(d: Record<string, unknown>): ChunkResult {
  // Validate required fields like Pydantic would — never coerce a missing
  // sequence to NaN and then advance the SDK's sequence counter on it. Callers
  // wrap a throw here as invalid_response.
  if (typeof d.sequence !== "number" || !Number.isFinite(d.sequence)) {
    throw new TypeError("chunk response missing numeric 'sequence'");
  }
  if (typeof d.accepted !== "boolean" || typeof d.finished !== "boolean") {
    throw new TypeError("chunk response missing boolean 'accepted'/'finished'");
  }
  const rawEmits = d.emits ?? [];
  if (!Array.isArray(rawEmits)) throw new TypeError("chunk response 'emits' must be a list");
  const emits = (rawEmits as Array<Record<string, unknown>>).map((e) => {
    if (e === null || typeof e !== "object" || Array.isArray(e)) {
      throw new TypeError("chunk emit must be an object");
    }
    const tr = e.triggered_rules ?? [];
    if (!Array.isArray(tr)) throw new TypeError("emit 'triggered_rules' must be a list");
    for (const r of tr) {
      if (r === null || typeof r !== "object" || Array.isArray(r)) {
        throw new TypeError("triggered rule must be an object");
      }
    }
    return {
      text: String(e.content ?? ""),
      blocked: Boolean(e.blocked),
      // "" and null both mean "no message" on the wire.
      blockMessage: e.block_message ? String(e.block_message) : null,
      final: Boolean(e.final),
      triggeredRules: tr as Array<Record<string, unknown>>,
    };
  });
  return {
    sequence: d.sequence,
    accepted: d.accepted,
    finished: d.finished,
    emits,
  };
}

function* emitsToEvents(
  result: ChunkResult,
  session: StreamingSession,
  ctx: { conversationId: string | null; correlationId: string | null },
): Generator<SafeDelta | Blocked> {
  for (const emit of result.emits) {
    if (emit.blocked) {
      yield {
        type: "blocked",
        blockMessage: emit.blockMessage,
        triggeredRules: emit.triggeredRules.map(toTriggeredRule),
        jobId: session.jobId,
        requestId: result.requestId ?? null,
        ...ctx,
      };
      break;
    }
    if (emit.text) {
      yield {
        type: "delta",
        text: emit.text,
        sequence: result.sequence,
        jobId: session.jobId,
        requestId: result.requestId ?? null,
        ...ctx,
      };
    }
  }
}

// ---------------------------------------------------------------------------
// StreamingClient
// ---------------------------------------------------------------------------
export class StreamingClient {
  private readonly cache = new Map<string, StreamingCapability>();

  constructor(private readonly c: CollieClient) {}

  async preflight(opts: { projectId?: string; forceRefresh?: boolean } = {}): Promise<StreamingCapability> {
    return this.resolveCapability(opts.projectId, {
      minValidityS: 0,
      forceRefresh: opts.forceRefresh ?? false,
    });
  }

  async resolveCapability(
    projectId: string | undefined,
    opts: { minValidityS: number; forceRefresh: boolean },
  ): Promise<StreamingCapability> {
    const pid = projectId ?? this.c.projectId;
    const key = pid ?? "";
    if (!opts.forceRefresh) {
      const cached = this.cache.get(key);
      if (cached && cached.validUntil != null) {
        const remaining = (cached.validUntil - Date.now()) / 1000;
        if (remaining > opts.minValidityS) return cached;
      }
    }
    const body: Record<string, unknown> = {};
    if (pid != null) body.project_id = pid;
    const { data, response } = await this.c._requestJson("POST", "/v1/streaming/preflight", {
      sdkOrigin: PREFLIGHT_ORIGIN,
      json: body,
    });
    let cap: StreamingCapability;
    try {
      cap = toCapability(data);
    } catch {
      throw new CollieApiError("API returned a malformed preflight response", {
        statusCode: response.status,
        code: "invalid_response",
      });
    }
    if (cap.validUntil != null) this.cache.set(key, cap);
    else this.cache.delete(key);
    return cap;
  }

  session(opts: {
    input: string;
    projectId?: string;
    conversationId?: string;
    correlationId?: string;
    /** Job id of a moderate.input call that already gated exactly this
     * input. The server verifies the reference (same project, identical
     * text, completed and not blocked) and skips re-filtering the prompt
     * on the session job — one inbound pass per turn. An invalid
     * reference is a request-time error; older servers ignore the field
     * and filter as before. Manual sessions do NOT auto-retry the claim
     * protocol: a CollieApiError with code `input_gate_stale` means
     * re-gate (same context) and open a new session with the fresh id;
     * `input_gate_unverifiable` means retry WITHOUT the reference
     * (re-gating will not help); `input_gate_claimed` means the gate was
     * already consumed. `protectStream` handles this ladder
     * automatically, but ONLY when it runs its own gate — with an
     * external `inputResult` the typed errors surface to your code by
     * design (the wrapper cannot re-check a context it never saw). */
    inputJobId?: string;
  }): StreamingSession {
    this.c._checkProject(opts.projectId);
    return new StreamingSession(this.c, {
      input: opts.input,
      conversationId: opts.conversationId,
      correlationId: opts.correlationId,
      sdkOrigin: SESSION_ORIGIN,
      inputJobId: opts.inputJobId,
    });
  }

  protectStream(opts: ProtectStreamOptions): AsyncGenerator<StreamEvent> {
    this.c._checkProject(opts.projectId);
    if (typeof opts.rawStreamFactory !== "function") {
      throw new ProviderStreamFactoryRequired(
        "rawStreamFactory must be a zero-arg function returning an async iterable.",
      );
    }
    if (opts.inputResult && opts.checkInput === false) {
      // Round-10 review: this combination was silently ignored — no gate
      // reuse, no claim, the duplicate pass survived. Loud, not a footgun.
      throw new TypeError(
        "inputResult requires checkInput to be true: with checkInput: false " +
          "the result would be silently ignored (no claim sent, the session " +
          "re-filters the prompt).",
      );
    }
    if (
      opts.inputResult &&
      (opts.inputResult.originalText !== opts.input || !opts.inputResult.allowed)
    ) {
      throw new TypeError(
        "inputResult must correspond to this input and be allowed.",
      );
    }
    // A precomputed inputResult skips the input gate, so the wrapper can't
    // verify the passed context matches the context that produced that result.
    // Reject rather than re-analyze (or silently drop) it.
    if (opts.inputResult && (opts.context != null || opts.contextFormat != null)) {
      throw new TypeError(
        "context cannot be combined with a precomputed inputResult: the wrapper " +
          "can't verify the context matches the precomputed result. Omit context " +
          "when reusing an already-computed result, or call moderate.input with " +
          "the desired context.",
      );
    }
    return this.protectStreamImpl(opts);
  }

  private async *protectStreamImpl(opts: ProtectStreamOptions): AsyncGenerator<StreamEvent> {
    const ctx = {
      conversationId: opts.conversationId ?? null,
      correlationId: opts.correlationId ?? null,
    };

    if (opts.requireStreaming) {
      const cap = await this.resolveCapability(opts.projectId, {
        minValidityS: PREFLIGHT_MIN_VALIDITY_S,
        forceRefresh: false,
      });
      if (cap.mode === "buffered") {
        throw new BufferedFallbackRequired(
          cap.reasonDetail ?? cap.reason ?? "policy requires buffered response checking",
        );
      }
      if (cap.mode !== "streaming") throw preflightError(cap);
    }

    // The INPUT-phase context verdict, carried to the success terminal
    // (Finished) too so monitored/degraded-but-allowed context is observable.
    let inputBlockedBy: string | null = null;
    let inputContext: ContextModerationResult | null = null;
    let gateJobId: string | undefined;
    if (opts.checkInput ?? true) {
      const result =
        opts.inputResult ??
        (await this.c.moderate.run({
          prompt: opts.input,
          context: opts.context,
          contextFormat: opts.contextFormat,
          conversationId: opts.conversationId,
          correlationId: opts.correlationId,
          sdkOrigin: PROTECT_STREAM_ORIGIN,
        }));
      inputBlockedBy = result.blockedBy ?? null;
      inputContext = result.context ?? null;
      // The gate's job id becomes the session job's input_job_id: the
      // server-verified proof that this exact prompt already passed inbound
      // filtering, so the session job skips its second pass.
      gateJobId = result.jobId ?? undefined;
      if (!result.allowed || result.blocked) {
        // fail-safe gate (v24): an ambiguous verdict resolves to
        // allowed=false/blocked=true, but check both for parity with .NET.
        yield {
          type: "input_blocked",
          blockMessage: result.blockMessage ?? null,
          triggeredRules: result.triggeredRules,
          jobId: result.jobId ?? null,
          requestId: result.requestId ?? null,
          blockedBy: result.blockedBy ?? null,
          context: result.context ?? null,
          ...ctx,
        };
        return;
      }
      if (!opts.inputResult) rejectMaskedInput(opts.input, result.filteredText ?? null);
    }

    let session = new StreamingSession(this.c, {
      input: opts.input,
      conversationId: opts.conversationId,
      correlationId: opts.correlationId,
      sdkOrigin: PROTECT_STREAM_ORIGIN,
      inputJobId: gateJobId,
    });
    try {
      await session.open();
    } catch (e) {
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
      // a loop. input_gate_claimed and every other code propagate untouched.
      const isClaimRefusal = (err: unknown): err is CollieApiError =>
        err instanceof CollieApiError &&
        (err.code === "input_gate_stale" || err.code === "input_gate_unverifiable");
      if (!isClaimRefusal(e)) throw e;
      // An EXTERNAL inputResult may have been produced WITH a context this
      // wrapper never saw (context+inputResult is rejected at the API edge
      // precisely because the pairing is unverifiable) — neither a
      // prompt-only re-gate nor a silent downgrade is an honest remedy.
      // Surface the typed error; the caller re-gates with its own context.
      if (opts.inputResult) throw e;
      const claimlessSession = () =>
        new StreamingSession(this.c, {
          input: opts.input,
          conversationId: opts.conversationId,
          correlationId: opts.correlationId,
          sdkOrigin: PROTECT_STREAM_ORIGIN,
        });
      if (e.code === "input_gate_unverifiable") {
        session = claimlessSession();
        await session.open();
      } else {
        const regate = await this.c.moderate.run({
          prompt: opts.input,
          context: opts.context,
          contextFormat: opts.contextFormat,
          conversationId: opts.conversationId,
          correlationId: opts.correlationId,
          sdkOrigin: PROTECT_STREAM_ORIGIN,
        });
        inputBlockedBy = regate.blockedBy ?? null;
        inputContext = regate.context ?? null;
        if (!regate.allowed || regate.blocked) {
          // The CURRENT policy blocks this input — the stale 409 did its
          // job: the verdict arrived before any provider spend.
          yield {
            type: "input_blocked",
            blockMessage: regate.blockMessage ?? null,
            triggeredRules: regate.triggeredRules,
            jobId: regate.jobId ?? null,
            requestId: regate.requestId ?? null,
            blockedBy: regate.blockedBy ?? null,
            context: regate.context ?? null,
            ...ctx,
          };
          return;
        }
        rejectMaskedInput(opts.input, regate.filteredText ?? null);
        session = new StreamingSession(this.c, {
          input: opts.input,
          conversationId: opts.conversationId,
          correlationId: opts.correlationId,
          sdkOrigin: PROTECT_STREAM_ORIGIN,
          inputJobId: regate.jobId ?? undefined,
        });
        try {
          await session.open();
        } catch (e2) {
          // A second STALE is proven churn — never race the provider
          // against it (round 5); only the unverifiable outage carve-out
          // downgrades. Every other code propagates.
          if (!(e2 instanceof CollieApiError) || e2.code !== "input_gate_unverifiable") throw e2;
          session = claimlessSession();
          await session.open();
        }
      }
    }

    const controller = new AbortController();
    let provider: unknown;
    let iterator: AsyncIterator<string> | null = null;
    let batcher: AsyncGenerator<string> | null = null;
    try {
      provider = opts.rawStreamFactory(controller.signal);
      iterator = resolveProviderIterator(provider);
      batcher = batchDeltas(iterator, {
        flushIntervalS: opts.flushIntervalS ?? DEFAULT_FLUSH_INTERVAL_S,
        maxDeltas: opts.maxDeltas ?? DEFAULT_MAX_DELTAS,
        maxChars: opts.maxChars ?? DEFAULT_MAX_CHARS,
      });
      // Distinguish "terminated by a block" from "server finished the session
      // on a push" (v24 — parity with Python's finished_early path): a block is
      // its own terminal event, but a server-initiated finish must STILL close
      // with a terminal `finished` event rather than ending on a bare delta.
      let blocked = false;
      let finishedEarly = false;
      for await (const batch of batcher) {
        const result = await session.push(batch);
        for (const ev of emitsToEvents(result, session, ctx)) {
          yield ev;
          if (ev.type === "blocked") blocked = true;
        }
        if (blocked) break;
        if (result.finished) {
          // Backend finalized the session on a push without a block — don't
          // re-process emits via finish(), but still emit the terminal event.
          finishedEarly = true;
          break;
        }
      }
      if (!blocked && !finishedEarly) {
        const final = await session.finish();
        for (const ev of emitsToEvents(final, session, ctx)) {
          yield ev;
          if (ev.type === "blocked") blocked = true;
        }
      }
      if (!blocked) {
        // Invariant: every stream ends with a terminal event — Blocked above,
        // InputBlocked before the provider opened, or Finished here (including
        // the finished-early path).
        yield {
          type: "finished",
          finishReason: "stop",
          jobId: session.jobId,
          requestId: session.requestId,
          blockedBy: inputBlockedBy,
          context: inputContext,
          ...ctx,
        };
      }
    } finally {
      // Signal the provider to stop FIRST, so any in-flight read inside the
      // batcher is cancelled before we await teardown — otherwise return() on an
      // iterator blocked in a pending next() could stall cleanup.
      controller.abort();
      if (batcher) {
        try {
          await batcher.return(undefined); // closes the resolved iterator
        } catch {
          /* best-effort */
        }
      }
      // Also close the original provider object when it's distinct from the
      // iterator (its [asyncIterator]() returned a separate object), or when
      // validation failed before an iterator was resolved.
      if (provider !== undefined && provider !== iterator) {
        await closeProvider(provider);
      }
      // Finalize the job iff real output was streamed but it never reached its
      // own terminal (e.g. upstream raised mid-stream). A factory failure before
      // any chunk is a no-op here (see abort()) — left to expire, not faked.
      await session.abort();
      await session.close();
    }
  }

  async protectBuffered(opts: ProtectBufferedOptions): Promise<BufferedResult> {
    // Knob validation BEFORE any side effect — the provider factory must not
    // run and no job may be created on invalid knobs (_pollJob's own
    // validation would fire only after both). 0.05 is _pollJob's
    // pollIntervalS default (not exposed here). Default applies to
    // `undefined` only — an explicit null must fail here.
    PollPacer.validate(0.05, opts.timeoutS === undefined ? 30 : opts.timeoutS);
    this.c._checkProject(opts.projectId);
    if (typeof opts.rawStreamFactory !== "function") {
      throw new ProviderStreamFactoryRequired(
        "rawStreamFactory must be a zero-arg function returning an async iterable.",
      );
    }
    if (opts.inputResult && opts.checkInput === false) {
      throw new TypeError(
        "inputResult requires checkInput to be true: with checkInput: false " +
          "the result would be silently ignored.",
      );
    }
    if (
      opts.inputResult &&
      (opts.inputResult.originalText !== opts.input || !opts.inputResult.allowed)
    ) {
      throw new TypeError("inputResult must correspond to this input and be allowed.");
    }
    if (opts.inputResult && (opts.context != null || opts.contextFormat != null)) {
      throw new TypeError(
        "context cannot be combined with a precomputed inputResult: the wrapper " +
          "can't verify the context matches the precomputed result. Omit context " +
          "when reusing an already-computed result, or call moderate.input with " +
          "the desired context.",
      );
    }

    // The INPUT-phase context verdict, surfaced on the result whether or not
    // the input blocked (a monitored/degraded context still allows the request).
    let inputBlockedBy: string | null = null;
    let inputContext: ContextModerationResult | null = null;
    if (opts.checkInput ?? true) {
      const result =
        opts.inputResult ??
        (await this.c.moderate.run({
          prompt: opts.input,
          context: opts.context,
          contextFormat: opts.contextFormat,
          conversationId: opts.conversationId,
          correlationId: opts.correlationId,
          sdkOrigin: PROTECT_BUFFERED_ORIGIN,
        }));
      inputBlockedBy = result.blockedBy ?? null;
      inputContext = result.context ?? null;
      if (!result.allowed || result.blocked) {
        // fail-safe gate (v24): an ambiguous verdict resolves to
        // allowed=false/blocked=true, but check both for parity with .NET.
        return {
          blocked: true,
          inputBlocked: true,
          blockMessage: result.blockMessage ?? null,
          filteredText: null,
          triggeredRules: result.triggeredRules,
          jobId: result.jobId ?? null,
          requestId: result.requestId ?? null,
          blockedBy: result.blockedBy ?? null,
          context: result.context ?? null,
        };
      }
      if (!opts.inputResult) rejectMaskedInput(opts.input, result.filteredText ?? null);
    }

    const controller = new AbortController();
    const provider = opts.rawStreamFactory(controller.signal);
    let iterator: AsyncIterator<string>;
    try {
      iterator = resolveProviderIterator(provider);
    } catch (e) {
      controller.abort();
      await closeProvider(provider);
      throw e;
    }
    const parts: string[] = [];
    try {
      while (true) {
        const r = await iterator.next();
        if (r.done) break;
        if (r.value) parts.push(r.value);
      }
    } finally {
      controller.abort();
      await closeIterator(iterator);
      if ((provider as unknown) !== (iterator as unknown)) await closeProvider(provider);
    }
    const fullText = parts.join("");

    const body: Record<string, unknown> = { message_output: fullText };
    if (opts.conversationId) body.conversation_id = opts.conversationId;
    if (opts.correlationId) body.correlation_id = opts.correlationId;
    const { data, response } = await this.c._requestJson("POST", "/v1/jobs", {
      sdkOrigin: PROTECT_BUFFERED_ORIGIN,
      json: body,
    });
    const jobId = data.job_id as string | undefined;
    if (!jobId) {
      throw new CollieApiError("Job creation did not return a job_id", {
        code: "invalid_response",
      });
    }
    const requestId = this.c._requestId(response);
    const job = await this.c._pollJob(jobId, {
      sdkOrigin: PROTECT_BUFFERED_ORIGIN,
      terminal: BUFFERED_TERMINAL,
      failure: BUFFERED_FAILURE,
      timeoutS: opts.timeoutS,
    });
    // Carry the input-phase context verdict onto the (output) result too.
    return { ...toBufferedResult(job, jobId, requestId), blockedBy: inputBlockedBy, context: inputContext };
  }
}

/** The v24 fail-safe verdict table applied to `outbound_result`, mirroring
 * `toOutputResult` line for line (tech-debt #22 closed the buffered gap): a
 * terminal job without its result object is malformed (typed error, never an
 * implicit allow); NOT blocked requires an EXPLICIT `allowed === true` and no
 * hard-block signal; ambiguity — an empty object, an absent `allowed` —
 * resolves to blocked. `BufferedResult` keeps its blocked-only surface;
 * `blocked` is the strict complement of resolved allow. */
function toBufferedResult(
  job: Record<string, unknown>,
  jobId: string,
  requestId: string | null,
): BufferedResult {
  try {
    const ob = job.outbound_result;
    if (!ob || typeof ob !== "object" || Array.isArray(ob)) throw new Error("missing outbound_result");
    const o = ob as Record<string, unknown>;
    const statusBlocked = job.status === "outbound_blocked";
    const outboundAllowed = o.allowed;
    const hardBlocked = statusBlocked || Boolean(o.blocked) || outboundAllowed === false;
    const allowed = outboundAllowed === true && !hardBlocked;
    return {
      blocked: !allowed,
      blockMessage: (o.block_message as string | null) ?? null,
      filteredText: (o.filtered_content as string | null) ?? null,
      inputBlocked: false,
      triggeredRules: ((o.triggered_rules as Array<Record<string, unknown>>) ?? []).map(toTriggeredRule),
      jobId,
      requestId,
    };
  } catch (e) {
    if (e instanceof CollieError) throw e;
    throw new CollieApiError("API returned a malformed buffered result", {
      code: "invalid_response",
    });
  }
}

// ---------------------------------------------------------------------------
// StreamingSession
// ---------------------------------------------------------------------------
export class StreamingSession {
  jobId: string | null = null;
  requestId: string | null = null;
  lastSequence = -1;

  private nextSequence = 0;
  private finished = false;
  private closed = false;
  private finalResult: ChunkResult | null = null;
  private lastResult: ChunkResult | null = null;
  private inFlight = false;

  constructor(
    private readonly c: CollieClient,
    private readonly opts: {
      input: string;
      conversationId?: string;
      correlationId?: string;
      sdkOrigin?: string;
      /** Job id of the moderate.input gate that already filtered this
       * input — the server verifies it and skips the second inbound pass. */
      inputJobId?: string;
    },
  ) {}

  private get sdkOrigin(): string {
    return this.opts.sdkOrigin ?? SESSION_ORIGIN;
  }

  async open(): Promise<this> {
    await this.createJob();
    return this;
  }

  private async createJob(): Promise<void> {
    if (this.jobId) return;
    const body: Record<string, unknown> = { message_input: this.opts.input };
    if (this.opts.conversationId) body.conversation_id = this.opts.conversationId;
    if (this.opts.correlationId) body.correlation_id = this.opts.correlationId;
    if (this.opts.inputJobId) body.input_job_id = this.opts.inputJobId;
    const { data, response } = await this.c._requestJson("POST", "/v1/jobs", {
      sdkOrigin: this.sdkOrigin,
      json: body,
    });
    this.jobId = (data.job_id as string) ?? null;
    this.requestId = this.c._requestId(response);
    if (!this.jobId) {
      throw new CollieApiError("Job creation did not return a job_id", {
        code: "invalid_response",
      });
    }
  }

  async push(content: string): Promise<ChunkResult> {
    if (this.closed) throw new CollieError("Session is closed");
    if (this.finished) {
      throw new ChunkSessionFinished(
        "Session already finished; create a new session to stream again.",
        { code: "chunk_session_finished" },
      );
    }
    if (this.inFlight) {
      throw new ConcurrentSessionUseError(
        "Overlapping push() detected; chunk submission must be serial per session.",
      );
    }
    this.inFlight = true;
    try {
      return await this.submit(content, false);
    } finally {
      this.inFlight = false;
    }
  }

  async finish(finishReason = "stop"): Promise<ChunkResult> {
    if (this.closed) throw new CollieError("Session is closed");
    if (this.finished) return (this.finalResult ?? this.lastResult) as ChunkResult;
    if (this.inFlight) {
      throw new ConcurrentSessionUseError(
        "Overlapping finish() detected; chunk submission must be serial per session.",
      );
    }
    this.inFlight = true;
    try {
      if (this.finished) return (this.finalResult ?? this.lastResult) as ChunkResult;
      const result = await this.submit("", true, finishReason);
      this.finalResult = result;
      return result;
    } finally {
      this.inFlight = false;
    }
  }

  async close(): Promise<void> {
    this.closed = true;
  }

  /** Best-effort: finalize the backend job if real output was streamed but the
   * stream didn't reach its own terminal (e.g. the upstream model threw
   * mid-stream, or the caller abandoned protectStream) — so a no-webhook job
   * isn't left dangling until expiry.
   *
   * Only finalizes when lastSequence >= 0 (at least one chunk accepted). A job
   * that never streamed a chunk (factory threw / upstream setup failed) is NOT
   * finalized: is_final is the protocol's only terminal and would mark the job
   * "completed with empty output", masking the failure in the audit log — such
   * a job is left to expire instead. (A true cancel/upstream-error terminal
   * would need a backend protocol addition.) No-op if no job, already finished,
   * or closed by a chunk error. Swallows errors. */
  async abort(): Promise<void> {
    if (this.jobId === null || this.finished || this.closed || this.lastSequence < 0) return;
    try {
      this.finalResult = await this.submit("", true);
    } catch {
      /* best-effort cleanup; the job expires regardless */
    }
  }

  private async submit(
    content: string,
    isFinal: boolean,
    finishReason?: string,
  ): Promise<ChunkResult> {
    await this.createJob();
    const sequence = this.nextSequence;
    const body: Record<string, unknown> = { sequence, content, is_final: isFinal };
    // Only on the terminal chunk, and omit empty ("" == unset). Persisted to
    // the audit log (request_logs.finish_reason).
    if (isFinal && finishReason) body.finish_reason = finishReason;
    let response: Response;
    let data: Record<string, unknown>;
    try {
      // _postChunk now reads the 2xx body INSIDE its retry loop (v21) — a
      // hung/aborted body read is bounded and retried, and its taxonomy is
      // decided there.
      ({ response, data } = await this.c._postChunk(
        `/v1/jobs/${this.jobId}/chunks`,
        body,
        this.sdkOrigin,
      ));
    } catch (e) {
      if (e instanceof CollieError) this.closed = true;
      throw e;
    }
    let result: ChunkResult;
    try {
      result = toChunkResult(data);
    } catch (e) {
      this.closed = true;
      if (e instanceof CollieError) throw e;
      throw new CollieApiError("API returned a malformed chunk response", {
        statusCode: response.status,
        code: "invalid_response",
      });
    }
    result.jobId = this.jobId;
    result.requestId = this.c._requestId(response) ?? this.requestId;
    // A 2xx body must confirm THIS chunk: accepted, echoing the submitted
    // sequence. Anything else (accepted=false, or a stale/cached response for
    // a different sequence) is a contract violation — advancing on it could
    // desync the sequence or replay another chunk's safe text to the end
    // user. Fail the session instead. (Mirrors the .NET SDK's check.)
    if (!result.accepted || result.sequence !== sequence) {
      this.closed = true;
      throw new CollieApiError(
        `API returned an unconfirmed chunk response (accepted=${result.accepted}, ` +
          `sequence=${result.sequence}, expected sequence=${sequence})`,
        { statusCode: response.status, code: "invalid_response" },
      );
    }
    // Advance only after a confirmed accept (fresh or idempotent replay).
    this.lastSequence = sequence;
    this.nextSequence = sequence + 1;
    this.lastResult = result;
    if (result.finished) this.finished = true;
    return result;
  }

  async mintStreamToken(): Promise<StreamToken> {
    if (!this.jobId) {
      throw new CollieError("mintStreamToken requires a created session (no jobId).");
    }
    const { data } = await this.c._requestJson(
      "POST",
      `/v1/jobs/${this.jobId}/stream-token`,
      { sdkOrigin: this.sdkOrigin },
    );
    const token = data.stream_token;
    const expiresIn = data.expires_in;
    if (typeof token !== "string" || !token) {
      throw new CollieApiError("stream-token response missing or invalid stream_token", {
        code: "invalid_response",
      });
    }
    if (typeof expiresIn !== "number" || !Number.isInteger(expiresIn) || expiresIn <= 0) {
      throw new CollieApiError("stream-token response missing or invalid expires_in", {
        code: "invalid_response",
      });
    }
    const base = this.c.baseUrl.replace(/\/+$/, "");
    return {
      token,
      expiresIn,
      url: `${base}/v1/jobs/${this.jobId}/stream?stream_token=${token}`,
    };
  }

  async *streamEvents(
    opts: {
      lastEventId?: string;
      autoResume?: boolean;
      maxReconnects?: number;
      idleTimeoutS?: number;
    } = {},
  ): AsyncGenerator<StreamEvent> {
    if (!this.jobId) {
      throw new CollieError("streamEvents requires a created session (no jobId).");
    }
    const autoResume = opts.autoResume ?? true;
    const maxReconnects = opts.maxReconnects ?? 5;
    // Client-side idle timeout: if no SSE bytes arrive for this long, abort the
    // connection and surface StreamInterrupted(reason="idle_timeout"). 0 disables
    // it (rely on the server's own idle end frame). Mirrors the Python default.
    const idleTimeoutS = opts.idleTimeoutS ?? 60;
    const ctx = {
      conversationId: this.opts.conversationId ?? null,
      correlationId: this.opts.correlationId ?? null,
    };
    const seen = new Set<string>();
    let currentLastId = opts.lastEventId ?? null;
    let reconnects = 0;

    while (true) {
      let reason = "disconnect";
      let terminal = false;
      const controller = new AbortController();
      let idleTimer: ReturnType<typeof setTimeout> | undefined;
      let idleFired = false;
      const armIdle = () => {
        if (idleTimeoutS <= 0) return;
        if (idleTimer) clearTimeout(idleTimer);
        idleTimer = setTimeout(() => {
          idleFired = true;
          controller.abort();
        }, idleTimeoutS * 1000);
      };
      try {
        const headers = this.c._headers(this.sdkOrigin, false);
        if (currentLastId != null) headers["last-event-id"] = currentLastId;
        armIdle();
        const response = await this.c._rawFetch(
          this.c._url(`/v1/jobs/${this.jobId}/stream`),
          { method: "GET", headers, signal: controller.signal },
        );
        // The SSE response is part of the streaming protocol surface too —
        // a mixed-fleet rollout can serve it from a newer pod than create.
        this.c._checkProtocol(response);
        if (!response.ok) throw await this.c._apiError(response);

        for await (const [evId, evType, dataStr] of iterSse(response, armIdle)) {
          if (evId != null) currentLastId = evId;
          if (evType === "chunk") {
            if (evId != null && seen.has(evId)) continue;
            if (evId != null) seen.add(evId);
            let isBlocked: boolean;
            let blockMessage: string | null;
            let triggered: TriggeredRule[];
            let text: string;
            let seq: number;
            try {
              const payload = dataStr ? JSON.parse(dataStr) : {};
              isBlocked = Boolean(payload.blocked);
              blockMessage = payload.block_message ? String(payload.block_message) : null;
              triggered = ((payload.triggered_rules as Array<Record<string, unknown>>) ?? []).map(toTriggeredRule);
              text = String(payload.content ?? "");
              const s = Number(payload.sequence ?? -1);
              seq = Number.isFinite(s) ? s : -1;
            } catch {
              throw new CollieApiError("Malformed SSE chunk frame", { code: "invalid_response" });
            }
            if (isBlocked) {
              yield { type: "blocked", blockMessage, triggeredRules: triggered, jobId: this.jobId, ...ctx };
              terminal = true;
              break;
            }
            if (text) {
              yield { type: "delta", text, sequence: seq, jobId: this.jobId, ...ctx };
            }
          } else if (evType === "end") {
            let endReason: string | undefined;
            try {
              endReason = dataStr ? (JSON.parse(dataStr).reason as string) : undefined;
            } catch {
              throw new CollieApiError("Malformed SSE end frame", { code: "invalid_response" });
            }
            if (endReason === "final" || endReason === "session_finished") {
              yield { type: "finished", finishReason: endReason, jobId: this.jobId, ...ctx };
              terminal = true;
              break;
            }
            if (endReason === "blocked") {
              terminal = true;
              break;
            }
            if (endReason === "upstream_error") {
              throw new CollieApiError("Upstream stream error", { code: "upstream_error" });
            }
            if (endReason === "session_unrecoverable") {
              throw new ChunkSessionUnrecoverable(
                "The stream entered an unrecoverable state; create a new job.",
                { code: "session_unrecoverable" },
              );
            }
            reason = endReason === "idle_timeout" ? "idle_timeout" : "disconnect";
            break;
          }
        }
      } catch (e) {
        // CollieApiError / ChunkSessionUnrecoverable are fatal — propagate.
        if (e instanceof CollieApiError || e instanceof ChunkSessionUnrecoverable) throw e;
        // Our own idle-timeout abort vs. a transport-level disconnect. Both are
        // resumable, but report the distinct reason.
        reason = idleFired ? "idle_timeout" : "disconnect";
      } finally {
        if (idleTimer) clearTimeout(idleTimer);
        // Cancel the HTTP body — on a caller early-exit (generator return) this
        // runs during unwinding, so the fetch connection is closed promptly
        // rather than left open until the server gives up.
        controller.abort();
      }

      if (terminal) return;
      yield {
        type: "interrupted",
        reason,
        resumable: true,
        lastEventId: currentLastId,
        jobId: this.jobId,
        ...ctx,
      };
      if (!autoResume) return;
      reconnects += 1;
      if (reconnects > maxReconnects) return;
    }
  }
}
