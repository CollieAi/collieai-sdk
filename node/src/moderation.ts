/** Input and output moderation — the standalone safety checks.
 *
 * `moderate.output(...)` is the sibling of `moderate.input(...)` for
 * assistant text produced OUTSIDE a `protectStream`/`protectBuffered`
 * wrapper (proactive notifications, escalations): a
 * `message_output`-only job polled to a terminal outbound state, evaluated
 * by OUTPUT-direction rules. */
import type { CollieClient } from "./client.js";
import { CollieApiError, CollieError, ModerationError } from "./errors.js";
import { deadlineSignal, isDeadlineAbort, PollPacer } from "./pacing.js";
import { toContextResult, toTriggeredRule } from "./_map.js";
import type { InputModerationResult, OutputModerationResult } from "./types.js";

const SDK_ORIGIN = "moderate.input";
const SDK_ORIGIN_OUTPUT = "moderate.output";
// The outbound sets — identical to protectBuffered's, because a
// message_output-only job is the same job shape.
const TERMINAL_OUTPUT_VERDICT = new Set(["completed", "outbound_blocked"]);
const TERMINAL_OUTPUT_FAILURE = new Set(["failed", "expired"]);

/** Reject context that wouldn't serialize to faithful, deterministic JSON, so
 * behavior matches the other SDKs and Slice-1 JSON-Pointer attribution is
 * stable. `JSON.stringify` silently corrupts many values — NaN/Infinity -> null,
 * Map/Set/RegExp -> {}, Date -> a string, class instances -> own enumerable
 * props only, symbol keys / non-enumerable props dropped. So only **plain** JSON
 * data is allowed: primitives, arrays, and plain records (prototype
 * `Object.prototype` or `null`, no symbol keys, all-enumerable). Throws a
 * `TypeError` with the offending path. */
function assertJsonSafeContext(value: unknown, path = "context"): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError(
        `${path} contains a non-finite number (NaN/Infinity), which is not valid JSON`,
      );
    }
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((v, i) => assertJsonSafeContext(v, `${path}[${i}]`));
    return;
  }
  if (typeof value === "object") {
    const proto = Object.getPrototypeOf(value);
    if (proto !== Object.prototype && proto !== null) {
      throw new TypeError(
        `${path} must be a plain object, not ${Object.prototype.toString.call(value)} ` +
          `(Map/Set/Date/RegExp/class instances aren't faithfully JSON-serializable)`,
      );
    }
    if (Object.getOwnPropertySymbols(value).length > 0) {
      throw new TypeError(`${path} has symbol keys, which JSON.stringify silently drops`);
    }
    if (Object.getOwnPropertyNames(value).length !== Object.keys(value).length) {
      throw new TypeError(
        `${path} has non-enumerable properties, which JSON.stringify silently drops`,
      );
    }
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      assertJsonSafeContext(v, `${path}.${k}`);
    }
    return;
  }
  // bigint, function, symbol, undefined
  throw new TypeError(`${path} contains a non-JSON value (${typeof value})`);
}

export interface ModerateInputOptions {
  prompt: string;
  /** Structured data (object/array) or raw string analyzed alongside the
   * prompt. A structured value travels as JSON.
   * Inert until context analysis is enabled server-side. */
  context?: unknown;
  /** Parsing hint for a raw-string context; ignored for structured context.
   * Omitted = policy default. */
  contextFormat?: "auto" | "json" | "text";
  projectId?: string;
  conversationId?: string;
  correlationId?: string;
  pollIntervalS?: number;
  timeoutS?: number;
}

export interface ModerateOutputOptions {
  /** The assistant/LLM output to check. No `context`: context is an input
   * surface (with context analysis enabled server-side, an outbound-only
   * job carrying one is rejected with 400); context-aware output filtering
   * is `protectBuffered`/`protectStream`. */
  response: string;
  projectId?: string;
  conversationId?: string;
  correlationId?: string;
  pollIntervalS?: number;
  timeoutS?: number;
}

interface RunOptions {
  prompt: string;
  context?: unknown;
  contextFormat?: string;
  conversationId?: string;
  correlationId?: string;
  sdkOrigin: string;
  pollIntervalS?: number;
  timeoutS?: number;
}

export class ModerationClient {
  constructor(private readonly c: CollieClient) {}

  /** Check `prompt` against the project's input rules. A policy block is a
   * normal result (`blocked === true`), not an error. */
  async input(opts: ModerateInputOptions): Promise<InputModerationResult> {
    this.c._checkProject(opts.projectId);
    return this.run({
      prompt: opts.prompt,
      context: opts.context,
      contextFormat: opts.contextFormat,
      conversationId: opts.conversationId,
      correlationId: opts.correlationId,
      sdkOrigin: SDK_ORIGIN,
      pollIntervalS: opts.pollIntervalS,
      timeoutS: opts.timeoutS,
    });
  }

  /** Check `response` (assistant/LLM output) against the project's OUTPUT
   * rules — the standalone check for text produced outside a `protect*`
   * wrapper: proactive notifications, escalation messages, any side channel.
   * Routing such text through `moderate.input`
   * evaluates it with INBOUND rules: output-safety and masking rules
   * silently never run, and injection detectors false-block assistant-style
   * imperatives.
   *
   * A policy block is a normal result (`blocked === true`). `filteredText`
   * carries the MASKED output — send it, not the original. Throws
   * `ModerationError` if the job dies (failed/expired) or polling times
   * out. */
  async output(opts: ModerateOutputOptions): Promise<OutputModerationResult> {
    this.c._checkProject(opts.projectId);
    // Knob validation BEFORE the job is created (fail fast, no side
    // effects). Defaults apply to `undefined` ONLY (see run()).
    const pollIntervalS = opts.pollIntervalS === undefined ? 0.05 : opts.pollIntervalS;
    const timeoutS = opts.timeoutS === undefined ? 30 : opts.timeoutS;
    PollPacer.validate(pollIntervalS, timeoutS);
    // Omit empty strings, not just null: at the SDK boundary "" == unset.
    const body: Record<string, unknown> = { message_output: opts.response };
    if (opts.conversationId) body.conversation_id = opts.conversationId;
    if (opts.correlationId) body.correlation_id = opts.correlationId;

    const { data, response } = await this.c._requestJson("POST", "/v1/jobs", {
      sdkOrigin: SDK_ORIGIN_OUTPUT,
      json: body,
    });
    const jobId = data.job_id as string | undefined;
    const requestId = this.c._requestId(response);
    if (!jobId) {
      throw new CollieApiError("Job creation did not return a job_id", {
        code: "invalid_response",
      });
    }

    let job: Record<string, unknown>;
    try {
      job = await this.c._pollJob(jobId, {
        sdkOrigin: SDK_ORIGIN_OUTPUT,
        terminal: TERMINAL_OUTPUT_VERDICT,
        failure: TERMINAL_OUTPUT_FAILURE,
        timeoutS,
        pollIntervalS,
      });
    } catch (e) {
      // Keep moderate.input's error taxonomy: a job that died or a poll
      // that timed out is a ModerationError on BOTH moderation methods, so
      // one catch covers the pair. Everything else stays typed as-is.
      if (e instanceof CollieApiError && e.code === "poll_timeout") {
        throw new ModerationError(
          `moderate.output timed out after ${timeoutS}s waiting for job ${jobId}`,
        );
      }
      if (e instanceof CollieApiError && e.code === "job_failed") {
        // Keep the actual terminal status in the message (input parity):
        // _pollJob's text is "ended in terminal state 'X'".
        throw new ModerationError(`${e.message} before an output verdict`);
      }
      throw e;
    }
    return toOutputModerationResult(opts.response, jobId, requestId, job);
  }

  /** Shared input-check transport. `sdkOrigin` attributes the requests to the
   * public method that triggered them (moderate.input / protect_stream / ...). */
  async run(opts: RunOptions): Promise<InputModerationResult> {
    // Knob validation BEFORE the job is created (fail fast, no side effects).
    // Defaults apply to `undefined` ONLY: an explicit null must fail here,
    // not silently become the default.
    const pollIntervalS = opts.pollIntervalS === undefined ? 0.05 : opts.pollIntervalS;
    const timeoutS = opts.timeoutS === undefined ? 30 : opts.timeoutS;
    PollPacer.validate(pollIntervalS, timeoutS);
    const body: Record<string, unknown> = {
      message_input: opts.prompt,
      inbound_only: true,
    };
    // Context-analysis input surface. A
    // structured value serializes as JSON in the body; `!= null` (not
    // truthiness) so an explicit empty object/array/string is still sent.
    // context_format is sent only when set, so omitted = policy default.
    if (opts.context != null) {
      assertJsonSafeContext(opts.context);
      body.context = opts.context;
    }
    if (opts.contextFormat) body.context_format = opts.contextFormat;
    if (opts.conversationId) body.conversation_id = opts.conversationId;
    if (opts.correlationId) body.correlation_id = opts.correlationId;

    const { data, response } = await this.c._requestJson("POST", "/v1/jobs", {
      sdkOrigin: opts.sdkOrigin,
      json: body,
    });
    const jobId = data.job_id as string | undefined;
    const requestId = this.c._requestId(response);
    if (!jobId) {
      throw new CollieApiError("Job creation did not return a job_id", {
        code: "invalid_response",
      });
    }

    // Pacing per the Poll Backoff Contract (see pacing.PollPacer). The
    // budget starts AFTER job creation, matching the prior deadline
    // semantics. Flow-specific errors stay here; the pacer owns timing.
    const pacer = new PollPacer({
      intervalS: pollIntervalS,
      timeoutS,
      monotonic: this.c._monotonic,
      rand: this.c._rand,
    });
    const timeoutError = () =>
      new ModerationError(
        `moderate.input timed out after ${timeoutS}s waiting for job ${jobId}`,
      );
    while (true) {
      const remaining = pacer.remaining();
      if (remaining <= 0) throw timeoutError();
      // Cooperative wall-clock bound on the in-flight GET (PBC-08: a suppressing transport can outrun it; PBC-07 no-late-result is the hard guarantee) (v4 — see
      // client._pollJob): only this signal firing maps to this flow's
      // timeout; transport failures stay CollieConnectionError.
      const deadline = deadlineSignal(remaining);
      let result: Awaited<ReturnType<typeof this.c._pollRequest>>;
      try {
        result = await this.c._pollRequest(`/v1/jobs/${jobId}`, {
          sdkOrigin: opts.sdkOrigin,
          signal: deadline,
        });
      } catch (e) {
        if (isDeadlineAbort(e, deadline)) throw timeoutError();
        // Late-landing typed HTTP errors are rejected by the strict
        // boundary too (see client._pollJob).
        if (e instanceof CollieApiError && pacer.remaining() <= 0) {
          throw timeoutError();
        }
        throw e;
      }
      // Strict acceptance boundary (v5 — see client._pollJob): a response
      // landing at/after the deadline is rejected, never accepted late.
      if (pacer.remaining() <= 0) {
        if (result.kind === "rate_limited") this.c._discardBody(result.response);
        throw timeoutError();
      }
      if (result.kind === "rate_limited") {
        // Shared 429 handling: pace (continue) or surface the typed error
        // (throws), classifying the REAL body-read failure + re-checking the
        // deadline afterwards (v27).
        await this.c._handle429(result, pacer, deadline, timeoutError);
        continue;
      }
      const jresp = result.response;
      const jdata = result.data;
      const status = jdata.status as string;
      if (status === "completed" || status === "inbound_blocked") {
        return toModerationResult(
          opts.prompt,
          jobId,
          requestId ?? this.c._requestId(jresp),
          jdata,
        );
      }
      if (status === "failed" || status === "expired") {
        throw new ModerationError(
          `Job ${jobId} ended in terminal state '${status}' before an input verdict`,
        );
      }
      await this.c._sleep(pacer.nextSleep(jdata.suggested_poll_ms));
    }
  }
}

/** The v24 fail-safe verdict table applied to `outbound_result` (sdk-plan
 * § Output Moderation): a terminal job without its result object is
 * malformed (typed error, never an implicit allow); ALLOWED requires an
 * EXPLICIT `allowed === true` and no hard-block signal; ambiguity resolves
 * to blocked. */
function toOutputModerationResult(
  response: string,
  jobId: string,
  requestId: string | null,
  jdata: Record<string, unknown>,
): OutputModerationResult {
  try {
    const outbound = jdata.outbound_result;
    if (!outbound || typeof outbound !== "object" || Array.isArray(outbound)) {
      throw new Error("missing or non-object outbound_result");
    }
    const ob = outbound as Record<string, unknown>;
    const outboundBlocked = Boolean(ob.blocked);
    const rules = (ob.triggered_rules as Array<Record<string, unknown>>) ?? [];
    const statusBlocked = jdata.status === "outbound_blocked";
    const outboundAllowed = ob.allowed;
    const hardBlocked =
      statusBlocked || outboundBlocked || outboundAllowed === false;
    const allowed = outboundAllowed === true && !hardBlocked;
    return {
      allowed,
      blocked: !allowed,
      blockMessage: (ob.block_message as string | null) ?? null,
      originalText: response,
      filteredText: (ob.filtered_content as string | null) ?? null,
      triggeredRules: rules.map(toTriggeredRule),
      jobId,
      requestId,
    };
  } catch (e) {
    if (e instanceof CollieError) throw e;
    throw new CollieApiError("API returned a malformed moderation result", {
      code: "invalid_response",
    });
  }
}

function toModerationResult(
  prompt: string,
  jobId: string,
  requestId: string | null,
  jdata: Record<string, unknown>,
): InputModerationResult {
  try {
    const inbound = jdata.inbound_result;
    if (!inbound || typeof inbound !== "object" || Array.isArray(inbound)) {
      throw new Error("missing or non-object inbound_result");
    }
    const ib = inbound as Record<string, unknown>;
    const inboundBlocked = Boolean(ib.blocked);
    const rules = (ib.triggered_rules as Array<Record<string, unknown>>) ?? [];
    // Context analysis (§4.1, §5.5): context_result + blocked_by live on the
    // job-status root, not inside inbound_result.
    const context = toContextResult(jdata.context_result);
    // FAIL-SAFE verdict resolution (v24 — cross-SDK parity with the .NET v23
    // fix). HARD-BLOCKED if ANY of: the terminal status says so, inbound.blocked,
    // inbound.allowed === false, OR a context_result block. ALLOWED requires an
    // EXPLICIT allowed === true AND not hard-blocked — an ABSENT allowed no
    // longer defaults to allow (the old `ib.allowed ?? !blocked` let a completed
    // job with an empty inbound_result fail-open). `blocked` is the strict
    // complement so an ambiguous verdict surfaces as blocked=true.
    const statusBlocked = jdata.status === "inbound_blocked";
    const contextBlocked = context?.blocked === true;
    const inboundAllowed = ib.allowed;
    const hardBlocked =
      statusBlocked || inboundBlocked || inboundAllowed === false || contextBlocked;
    const allowed = inboundAllowed === true && !hardBlocked;
    // Surface the context-block reason when inbound carries no message of its
    // own (finding 6 parity).
    const blockMessage =
      (ib.block_message as string | null) ??
      (contextBlocked ? (context?.blockMessage ?? null) : null);
    return {
      allowed,
      blocked: !allowed,
      blockMessage,
      originalText: prompt,
      filteredText: (ib.filtered_content as string | null) ?? null,
      triggeredRules: rules.map(toTriggeredRule),
      jobId,
      requestId,
      blockedBy: (jdata.blocked_by as string | null) ?? null,
      context,
    };
  } catch (e) {
    if (e instanceof CollieError) throw e;
    throw new CollieApiError("API returned a malformed moderation result", {
      code: "invalid_response",
    });
  }
}
