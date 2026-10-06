/**
 * The CollieAi client and its fetch-based HTTP + retry engine. Mirrors the
 * Python AsyncCollie. No runtime dependencies — uses global `fetch`, injectable
 * for tests.
 */
import {
  ChunkConcurrentSubmit,
  ChunkError,
  ChunkIdempotencyConflict,
  ChunkPolicyChanged,
  ChunkQuotaExceeded,
  ChunkResolutionUnavailable,
  ChunkRetryExhausted,
  ChunkSequenceConflict,
  ChunkSessionFinished,
  ChunkSessionUnrecoverable,
  ChunkStreamingUnsupported,
  CollieApiError,
  CollieConnectionError,
} from "./errors.js";
import { ModerationClient } from "./moderation.js";
import { deadlineSignal, isDeadlineAbort, MAX_TIMEOUT_S, PollPacer } from "./pacing.js";
import { StreamingClient } from "./streaming.js";

// Keep in sync with package.json — this value is the User-Agent the backend
// sees, and the release gate verifies it on the wire against the release.
export const VERSION = "2.1.0";
const DEFAULT_BASE_URL = "https://app.collieai.io";

type ChunkErrCtor = new (
  message: string,
  opts?: { statusCode?: number; code?: string },
) => ChunkError;

const FATAL_CHUNK_CODES: Record<string, ChunkErrCtor> = {
  chunk_session_finished: ChunkSessionFinished,
  chunk_sequence_conflict: ChunkSequenceConflict,
  chunk_idempotency_conflict: ChunkIdempotencyConflict,
  chunk_session_unrecoverable: ChunkSessionUnrecoverable,
  chunk_policy_changed: ChunkPolicyChanged,
  chunk_streaming_unsupported: ChunkStreamingUnsupported,
};

export interface CollieClientOptions {
  apiKey: string;
  baseUrl?: string;
  projectId?: string;
  /** Injectable fetch (defaults to global fetch). Used for testing. */
  fetch?: typeof fetch;
  /**
   * SDK-owned per-request timeout (seconds) for NON-STREAMING requests —
   * job creation, status polls, and each chunk-submit attempt. `fetch` has
   * no built-in timeout, so without this a hung connection (or a hung
   * response-body read) would wait forever; the retry/poll budgets are only
   * checked AFTER a request returns and cannot bound one that never does.
   * Applied as an `AbortSignal` composed with the poll deadline and the
   * chunk retry ceiling (whichever is nearest). Does NOT bound the long-lived
   * output SSE stream, which has its own idle handling. Default 30 s.
   */
  requestTimeoutS?: number;
  maxRetries?: number;
  retryMaxPerChunkS?: number;
  baseBackoffS?: number;
  maxBackoffS?: number;
  /** Test seams. */
  sleep?: (seconds: number) => Promise<void>;
  monotonic?: () => number;
  rand?: () => number;
  debugLogger?: { debug: (message: string, meta?: unknown) => void };
}

interface ChunkErrorDecision {
  fatal: boolean;
  error: Error;
  retryAfter?: number;
}

const defaultSleep = (s: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, s * 1000));

// Highest X-CollieAi-Streaming-Protocol version this SDK understands (F9).
// The backend stamps its version on streaming-surface responses; if the
// server is newer, the SDK warns once per client instead of failing.
const SUPPORTED_STREAMING_PROTOCOL = 1;

/** The outcome of a status-poll GET. A discriminated union so the 429 case
 * (body unread; a `surface` closure to read+classify it) and the 2xx case
 * (parsed `data`) can't be confused — no `data: null` + missing closure. */
type PollResult =
  | { kind: "ok"; response: Response; data: Record<string, unknown> }
  | { kind: "rate_limited"; response: Response; surface: () => Promise<Error> };

export class CollieClient {
  readonly apiKey: string;
  readonly baseUrl: string;
  readonly projectId?: string;
  readonly moderate: ModerationClient;
  readonly streaming: StreamingClient;

  private readonly _fetch: typeof fetch;
  private readonly _requestTimeoutS: number;
  private readonly _maxRetries: number;
  private readonly _retryCeilingS: number;
  private readonly _baseBackoff: number;
  private readonly _maxBackoff: number;
  readonly _sleep: (seconds: number) => Promise<void>;
  readonly _monotonic: () => number;
  readonly _rand: () => number;
  private readonly _debug?: { debug: (message: string, meta?: unknown) => void };
  private protocolWarned = false;

  constructor(options: CollieClientOptions) {
    if (!options.apiKey) throw new Error("apiKey is required");
    this.apiKey = options.apiKey;
    this.baseUrl = options.baseUrl ?? DEFAULT_BASE_URL;
    this.projectId = options.projectId;
    this._fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    // Validate BEFORE any HTTP side effect (v21): a bad value must not
    // silently become a 1 ms timer (0/negative) or throw ERR_OUT_OF_RANGE
    // deep inside the first request (NaN/Infinity).
    const requestTimeoutS = options.requestTimeoutS ?? 30.0;
    if (
      typeof requestTimeoutS !== "number" ||
      !Number.isFinite(requestTimeoutS) ||
      requestTimeoutS <= 0 ||
      requestTimeoutS > MAX_TIMEOUT_S
    ) {
      throw new RangeError(
        `requestTimeoutS must be a finite number in (0, ${MAX_TIMEOUT_S}]s, ` +
          `got ${String(requestTimeoutS)}`,
      );
    }
    this._requestTimeoutS = requestTimeoutS;
    this._maxRetries = options.maxRetries ?? 3;
    this._retryCeilingS = options.retryMaxPerChunkS ?? 10.0;
    this._baseBackoff = options.baseBackoffS ?? 0.25;
    this._maxBackoff = options.maxBackoffS ?? 4.0;
    // Numeric knobs validated up front (v23): a bad value must fail at
    // construction, not deep inside a chunk submit or with odd semantics.
    // Positive, finite, AND within the 2^31-1 ms timer bound (v24): a large but
    // otherwise "valid" backoff/ceiling would overflow setTimeout's 32-bit
    // delay and Node silently clamps it to 1 ms — turning a long backoff into a
    // tight retry loop that hammers the server.
    const bounded = (v: number): boolean =>
      typeof v === "number" && Number.isFinite(v) && v > 0 && v <= MAX_TIMEOUT_S;
    if (!Number.isInteger(this._maxRetries) || this._maxRetries < 1)
      throw new RangeError(`maxRetries must be an integer >= 1, got ${String(this._maxRetries)}`);
    if (!bounded(this._retryCeilingS))
      throw new RangeError(
        `retryMaxPerChunkS must be a finite number in (0, ${MAX_TIMEOUT_S}]s, got ${String(this._retryCeilingS)}`,
      );
    if (!bounded(this._baseBackoff))
      throw new RangeError(
        `baseBackoffS must be a finite number in (0, ${MAX_TIMEOUT_S}]s, got ${String(this._baseBackoff)}`,
      );
    if (!bounded(this._maxBackoff) || this._maxBackoff < this._baseBackoff)
      throw new RangeError(
        `maxBackoffS must be a finite number in (0, ${MAX_TIMEOUT_S}]s and >= baseBackoffS ` +
          `(${this._baseBackoff}), got ${String(this._maxBackoff)}`,
      );
    this._sleep = options.sleep ?? defaultSleep;
    // SECONDS from a monotonic source (normative for the Poll Backoff
    // Contract) — performance.now() is monotonic; Date.now() is not.
    this._monotonic = options.monotonic ?? (() => performance.now() / 1000);
    this._rand = options.rand ?? Math.random;
    this._debug = options.debugLogger;
    this.moderate = new ModerationClient(this);
    this.streaming = new StreamingClient(this);
  }

  /** No-op; provided for API parity (fetch has no pooled client to close). */
  async close(): Promise<void> {}

  /** Raw fetch passthrough (used by the SSE consumer for a streaming GET). */
  _rawFetch(url: string, init: RequestInit): Promise<Response> {
    return this._fetch(url, init);
  }

  // -- internal helpers (used by sub-clients) --------------------------------
  _checkProject(projectId?: string | null): void {
    if (projectId == null) return;
    if (this.projectId == null) {
      throw new Error(
        "projectId was passed per-call but the client was constructed without " +
          "one, so it can't be validated or applied — the API key determines " +
          "the project. Set projectId on the client.",
      );
    }
    if (projectId !== this.projectId) {
      throw new Error(
        `projectId=${projectId} does not match the client's projectId=` +
          `${this.projectId}; they must agree (or omit the argument).`,
      );
    }
  }

  _url(path: string, params?: Record<string, string>): string {
    const base = this.baseUrl.replace(/\/+$/, "");
    let url = base + path;
    if (params) {
      const qs = new URLSearchParams(params).toString();
      if (qs) url += `?${qs}`;
    }
    return url;
  }

  _headers(sdkOrigin: string, json: boolean): Record<string, string> {
    const h: Record<string, string> = {
      authorization: `Bearer ${this.apiKey}`,
      "user-agent": `collieai-node/${VERSION}`,
      "x-collieai-sdk-origin": sdkOrigin,
    };
    if (json) h["content-type"] = "application/json";
    return h;
  }

  _requestId(response: Response): string | null {
    return response.headers.get("x-request-id") ?? null;
  }

  /** Warn once per client when the server speaks a newer streaming protocol
   * than this SDK was built for (X-CollieAi-Streaming-Protocol, F9). Missing
   * or malformed header values are ignored — older servers don't send it.
   * Internal (used by the session's SSE consumer too). */
  _checkProtocol(response: Response): void {
    if (this.protocolWarned) return;
    const raw = response.headers.get("x-collieai-streaming-protocol");
    if (raw === null) return;
    const server = Number(raw);
    if (!Number.isInteger(server)) return;
    if (server > SUPPORTED_STREAMING_PROTOCOL) {
      this.protocolWarned = true;
      console.warn(
        `CollieAi server speaks streaming protocol ${server}; this @collieai/sdk ` +
          `supports up to ${SUPPORTED_STREAMING_PROTOCOL}. Upgrade the SDK to ` +
          `avoid contract drift.`,
      );
    }
  }

  /** Parse an ALREADY-READ error body into {code, message}. Pure — it does no
   * body read, so unlike the old `_parseError` it cannot swallow an abort or a
   * transport drop and mislabel it as the HTTP status (v24). */
  private _parseErrorText(text: string): { code?: string; message?: string } {
    let body: unknown;
    try {
      body = JSON.parse(text);
    } catch {
      return {};
    }
    if (body && typeof body === "object") {
      const err = (body as Record<string, unknown>).error;
      if (err && typeof err === "object") {
        const e = err as Record<string, unknown>;
        return {
          code: (e.type as string) ?? (e.code as string),
          message: e.message as string,
        };
      }
    }
    return {};
  }

  private _apiErrorFromText(text: string, status: number): CollieApiError {
    const { code, message } = this._parseErrorText(text);
    return new CollieApiError(message ?? `HTTP ${status}`, {
      statusCode: status,
      code,
    });
  }

  /** Build a typed error from a non-2xx response, reading the error body. A
   * FAILED read — a hung body the request signal aborts, or a socket dropped
   * after the headers — is a CONNECTION error, NOT the HTTP status the headers
   * advertised (v24). Used by surfaces without their own signal handling (the
   * poll loops surfacing a typed 429 whose body `_pollRequest` left unread);
   * `_pollRequest`/`_requestJson` classify structurally in-line instead. */
  async _apiError(response: Response): Promise<Error> {
    let text: string;
    try {
      text = await response.text();
    } catch (e) {
      return new CollieConnectionError(
        `Failed reading the ${response.status} error body: ${String(e)}`,
      );
    }
    return this._apiErrorFromText(text, response.status);
  }

  /** Parse a (2xx) body or throw a typed CollieApiError. Reads the body once. */
  async jsonOrError(response: Response): Promise<unknown> {
    let text: string;
    try {
      text = await response.text();
    } catch {
      throw new CollieApiError("Failed to read response body", {
        statusCode: response.status,
        code: "invalid_response",
      });
    }
    if (!text) {
      throw new CollieApiError(
        "API returned an empty body where a JSON object was expected",
        { statusCode: response.status, code: "invalid_response" },
      );
    }
    try {
      return JSON.parse(text);
    } catch {
      throw new CollieApiError("API returned a malformed (non-JSON) response", {
        statusCode: response.status,
        code: "invalid_response",
      });
    }
  }

  /** SDK-owned per-request abort signal (v20): `fetch` has no built-in
   * timeout, so every non-streaming request carries one. `capS` (when given)
   * caps it at the remaining chunk retry ceiling. `requestTimeoutS` is
   * constructor-validated positive, and callers only pass a positive `capS`
   * (the ceiling is checked strictly before an attempt), so the delay is
   * always >= 1 ms; the whole round trip INCLUDING the body read is governed
   * by it (an AbortSignal.timeout keeps firing across both fetch phases). */
  _requestSignal(capS?: number): AbortSignal {
    const s = capS === undefined ? this._requestTimeoutS : Math.min(this._requestTimeoutS, capS);
    return AbortSignal.timeout(Math.max(1, Math.ceil(s * 1000)));
  }

  /** Compose two abort signals into one that fires (with the firing
   * signal's `reason`) when EITHER does — so the poll GET can carry both its
   * deadline bound and the request timeout and still be classified by which
   * fired. Uses `AbortSignal.any` when available (Node 20.3+), else a manual
   * controller (Node 18). */
  private _composeSignals(a: AbortSignal | undefined, b: AbortSignal): AbortSignal {
    if (!a) return b;
    const anyFn = (AbortSignal as unknown as {
      any?: (signals: AbortSignal[]) => AbortSignal;
    }).any;
    if (typeof anyFn === "function") return anyFn([a, b]);
    const ctrl = new AbortController();
    if (a.aborted) ctrl.abort(a.reason);
    else if (b.aborted) ctrl.abort(b.reason);
    else {
      a.addEventListener("abort", () => ctrl.abort(a.reason), { once: true });
      b.addEventListener("abort", () => ctrl.abort(b.reason), { once: true });
    }
    return ctrl.signal;
  }

  /** Read a 2xx body as a JSON object. An abort (the request timeout, or a
   * transport drop) is a CONNECTION error — NOT `invalid_response`, which is
   * reserved for a body that arrived but was empty/malformed/non-object
   * (v21). `signal.aborted` distinguishes the two. */
  async _readJsonBody(response: Response, signal?: AbortSignal): Promise<Record<string, unknown>> {
    let text: string;
    try {
      text = await response.text();
    } catch (e) {
      if (signal?.aborted) {
        throw new CollieConnectionError(
          `Request timed out reading the response body: ${String(e)}`,
        );
      }
      throw new CollieConnectionError(`Failed reading the response body: ${String(e)}`);
    }
    if (!text) {
      throw new CollieApiError(
        "API returned an empty body where a JSON object was expected",
        { statusCode: response.status, code: "invalid_response" },
      );
    }
    let data: unknown;
    try {
      data = JSON.parse(text);
    } catch {
      throw new CollieApiError("API returned a malformed (non-JSON) response", {
        statusCode: response.status,
        code: "invalid_response",
      });
    }
    if (data === null || typeof data !== "object" || Array.isArray(data)) {
      throw new CollieApiError("API returned a non-object JSON body", {
        statusCode: response.status,
        code: "invalid_response",
      });
    }
    return data as Record<string, unknown>;
  }

  async _requestJson(
    method: string,
    path: string,
    opts: { sdkOrigin: string; json?: unknown; params?: Record<string, string> },
  ): Promise<{ data: Record<string, unknown>; response: Response }> {
    let response: Response;
    const signal = this._requestSignal();
    try {
      response = await this._fetch(this._url(path, opts.params), {
        method,
        headers: this._headers(opts.sdkOrigin, opts.json !== undefined),
        body: opts.json !== undefined ? JSON.stringify(opts.json) : undefined,
        signal,
      });
    } catch (e) {
      throw new CollieConnectionError(`Request to ${path} failed: ${String(e)}`);
    }
    this._checkProtocol(response);
    if (!response.ok) {
      // Read the error body DIRECTLY under the request signal so a failed read
      // is classified, not swallowed by `_apiError`/`_parseError` and mislabeled
      // as the HTTP status (v24). A signal-aborted read is a request timeout; a
      // non-abort read failure is a socket dropped after the headers — both are
      // connection errors, never the advertised status.
      let errText: string;
      try {
        errText = await response.text();
      } catch (e) {
        if (signal.aborted)
          throw new CollieConnectionError(
            `Request to ${path} timed out reading the error body: ${String(e)}`,
          );
        throw new CollieConnectionError(
          `Request to ${path} failed reading the error body: ${String(e)}`,
        );
      }
      throw this._apiErrorFromText(errText, response.status);
    }
    // Body read under the SAME signal (v21): an abort here is a connection
    // error, not `invalid_response` — `_readJsonBody` classifies via
    // `signal.aborted`.
    const data = await this._readJsonBody(response, signal);
    return { data, response };
  }

  /** Status-poll transport primitive (Poll Backoff Contract).
   *
   * Returns a `PollResult` discriminated union: `{ kind: "ok", data }` for 2xx,
   * or `{ kind: "rate_limited", surface }` for 429 (body left unread; the poll
   * loop owns the 429 pacing decision and needs the Retry-After header, and
   * `surface()` reads+classifies the body only if the loop decides to surface
   * the typed error). Any other >= 400 throws the usual typed error; transport
   * failures throw CollieConnectionError.
   *
   * The GET (headers AND body) is bounded by TWO composed signals (v21): the
   * loop's wall-clock DEADLINE (`opts.signal`) and the SDK-owned per-request
   * timeout. Taxonomy is classified from the REAL aborting error via
   * `classifyAbort`: a DEADLINE abort is rethrown UNTOUCHED (the loop maps it to
   * poll-timeout); a request-timeout abort or any other transport failure is a
   * CONNECTION error. Precedence is transport > deadline (PBC-09b): a transport
   * failure is a connection error even when the deadline also fired — the
   * classification keys off which error actually occurred, NOT which timers
   * happen to be aborted. */
  async _pollRequest(
    path: string,
    opts: { sdkOrigin: string; signal?: AbortSignal },
  ): Promise<PollResult> {
    const deadline = opts.signal;
    const reqTimeout = this._requestSignal();
    const composed = this._composeSignals(deadline, reqTimeout);
    // Taxonomy is STRUCTURAL (v5): only the DEADLINE signal's own reason (by
    // identity or cause) rethrows untouched → the loop maps it to
    // poll-timeout. A request-timeout abort — or a foreign abort that merely
    // arrives after the deadline fired — is a CONNECTION error. When the
    // deadline fires, the composed signal carries ITS reason, so a genuine
    // deadline-caused abort still matches isDeadlineAbort.
    const isReqTimeoutAbort = (e: unknown): boolean =>
      // Guard on `reqTimeout.aborted` FIRST (v24): a non-aborted signal has
      // `reason === undefined`, so an ordinary Error with `cause === undefined`
      // would otherwise false-match `undefined === undefined` and be mislabeled
      // a request timeout.
      reqTimeout.aborted &&
      (e === reqTimeout.reason || (e instanceof Error && e.cause === reqTimeout.reason));
    const classifyAbort = (e: unknown, phase: string): Error => {
      if (deadline && isDeadlineAbort(e, deadline)) return e as Error;
      const label = isReqTimeoutAbort(e) ? " timed out (requestTimeoutS)" : " failed";
      return new CollieConnectionError(`Request to ${path}${label}${phase}: ${String(e)}`);
    };
    let response: Response;
    try {
      response = await this._fetch(this._url(path), {
        method: "GET",
        headers: this._headers(opts.sdkOrigin, false),
        signal: composed,
      });
    } catch (e) {
      throw classifyAbort(e, "");
    }
    this._checkProtocol(response);
    // 429: body deliberately left unread. The loop reads it only if it decides
    // to surface the typed error, via `surface429` — which classifies the REAL
    // failure under the composed signal (like the non-2xx path below) instead of
    // reconstructing the taxonomy from timer flags (v27: the flag reconstruction
    // mislabeled an independent transport reset as a poll-timeout because the
    // deadline happened to have fired).
    if (response.status === 429) {
      const surface = async (): Promise<Error> => {
        let text: string;
        try {
          text = await response.text();
        } catch (e) {
          throw classifyAbort(e, " reading the surfaced 429 body");
        }
        return this._apiErrorFromText(text, response.status);
      };
      return { kind: "rate_limited", response, surface };
    }
    if (!response.ok) {
      // Read the error body under the SAME composed signal and classify the
      // REAL failure (v24). The old path called `_apiError`, whose read swallows
      // the abort, then classified a SYNTHETIC error — losing the deadline's
      // identity (so a deadline abort was mislabeled a request timeout) and
      // treating a socket-drop as the HTTP status. Passing the actual caught
      // error to `classifyAbort` restores: deadline abort → rethrown to
      // poll-timeout, request-timeout abort or transport drop → connection error.
      let errText: string;
      try {
        errText = await response.text();
      } catch (e) {
        throw classifyAbort(e, " reading the error body");
      }
      throw this._apiErrorFromText(errText, response.status);
    }
    // The body read is a second await governed by the same composed signal:
    // classify its abort the same way. Any non-abort failure here is a
    // TRANSPORT failure (a socket dropped after the headers), not a malformed
    // response (v5).
    let text: string;
    try {
      text = await response.text();
    } catch (e) {
      throw classifyAbort(e, " reading the response body");
    }
    if (!text) {
      throw new CollieApiError(
        "API returned an empty body where a JSON object was expected",
        { statusCode: response.status, code: "invalid_response" },
      );
    }
    let data: unknown;
    try {
      data = JSON.parse(text);
    } catch {
      throw new CollieApiError("API returned a malformed (non-JSON) response", {
        statusCode: response.status,
        code: "invalid_response",
      });
    }
    if (data === null || typeof data !== "object" || Array.isArray(data)) {
      throw new CollieApiError("API returned a non-object JSON body", {
        statusCode: response.status,
        code: "invalid_response",
      });
    }
    return { kind: "ok", response, data: data as Record<string, unknown> };
  }

  /** Shared 429 handling for both poll loops: pace (sleep + return, caller
   * continues) or SURFACE the typed error (throws). On surface it reads the body
   * via the result's `surface` closure — classifying the REAL error (deadline →
   * poll-timeout via `timeoutError`; transport failure → connection error,
   * transport > deadline) — and re-checks the budget AFTER the read (a body that
   * crossed the deadline is a late 429 → poll-timeout, PBC-07). */
  async _handle429(
    result: { response: Response; surface: () => Promise<Error> },
    pacer: PollPacer,
    deadline: AbortSignal | undefined,
    timeoutError: () => Error,
  ): Promise<void> {
    const sleepS = pacer.sleepFor429(this._parseRetryAfter(result.response));
    if (sleepS === null) {
      let err: Error;
      try {
        err = await result.surface();
      } catch (e) {
        if (deadline && isDeadlineAbort(e, deadline)) throw timeoutError();
        throw e;
      }
      if (pacer.remaining() <= 0) throw timeoutError();
      throw err;
    }
    this._discardBody(result.response);
    await this._sleep(sleepS);
  }

  /** Release a response body we won't read. Undici keeps the underlying
   * connection busy until the body is consumed or cancelled — a continued
   * 429 that just sleeps would pin a pool connection per attempt.
   * Fire-and-forget on purpose: cancel() may return a slow (or never
   * settling) promise, and cleanup must never stall the poll loop or eat
   * into the budget. The stream is marked disturbed synchronously. */
  _discardBody(response: Response): void {
    try {
      response.body?.cancel().catch(() => {});
    } catch {
      /* best-effort */
    }
  }

  /** Poll GET /v1/jobs/{jobId} until a terminal status, returning the job
   * body. Throws CollieApiError on a failure status or on timeout.
   *
   * Pacing per the Poll Backoff Contract (see pacing.PollPacer): a literal
   * warm phase, a jittered doubling ramp, `suggested_poll_ms` server
   * pacing, a respectful 429 contract, and a true wall-clock budget. */
  async _pollJob(
    jobId: string,
    opts: {
      sdkOrigin: string;
      terminal: Set<string>;
      failure: Set<string>;
      timeoutS?: number;
      pollIntervalS?: number;
    },
  ): Promise<Record<string, unknown>> {
    // Defaults apply to `undefined` ONLY: an explicit null is a caller bug
    // and must fail validation, not silently become the default.
    const timeoutS = opts.timeoutS === undefined ? 30 : opts.timeoutS;
    const pacer = new PollPacer({
      intervalS: opts.pollIntervalS === undefined ? 0.05 : opts.pollIntervalS,
      timeoutS,
      monotonic: this._monotonic,
      rand: this._rand,
    });
    const timeoutError = () =>
      new CollieApiError(`Polling job ${jobId} timed out after ${timeoutS}s`, {
        code: "poll_timeout",
      });
    while (true) {
      const remaining = pacer.remaining();
      if (remaining <= 0) throw timeoutError();
      // The poll budget is absolute wall clock: the in-flight GET is
      // bounded by cancellation at the deadline (transport timeouts are
      // per-phase and cannot cap total request time; the runtime's own
      // transport behavior applies untouched underneath). Taxonomy is
      // structural: only this signal firing is the flow's poll-timeout;
      // transport failures stay CollieConnectionError regardless of the
      // clock.
      const deadline = deadlineSignal(remaining);
      let result: PollResult;
      try {
        result = await this._pollRequest(`/v1/jobs/${jobId}`, {
          sdkOrigin: opts.sdkOrigin,
          signal: deadline,
        });
      } catch (e) {
        if (isDeadlineAbort(e, deadline)) throw timeoutError();
        // Strict acceptance applies to EVERY late-landing HTTP response: a
        // typed HTTP error (4xx/5xx, malformed body) arriving at/after the
        // deadline is this flow's poll-timeout too. Connection errors
        // propagate untouched — transport taxonomy is clock-independent.
        if (e instanceof CollieApiError && pacer.remaining() <= 0) {
          throw timeoutError();
        }
        throw e;
      }
      // Strict acceptance boundary (v5): every bound can be outrun — the
      // integer-ceiled timer fires late, the event loop lags, an injected
      // fetch may ignore the signal. A response landing at/after the
      // deadline is rejected, never accepted late.
      if (pacer.remaining() <= 0) {
        if (result.kind === "rate_limited") this._discardBody(result.response);
        throw timeoutError();
      }
      if (result.kind === "rate_limited") {
        await this._handle429(result, pacer, deadline, timeoutError);
        continue; // paced; _handle429 threw if it surfaced the typed 429
      }
      const data = result.data;
      const status = data.status as string;
      if (opts.terminal.has(status)) return data;
      if (opts.failure.has(status)) {
        throw new CollieApiError(`Job ${jobId} ended in terminal state '${status}'`, {
          code: "job_failed",
        });
      }
      await this._sleep(pacer.nextSleep(data.suggested_poll_ms));
    }
  }

  private _backoff(attempt: number): number {
    const raw = Math.min(this._maxBackoff, this._baseBackoff * 2 ** (attempt - 1));
    return raw * 0.5 + Math.random() * raw * 0.5;
  }

  /** RFC 9110 delay-seconds: a non-negative decimal integer (1*DIGIT),
   * strictly. `Number()` would admit "1.5", "1e2", "0x10" and "" (as 0) and
   * diverge from the other SDKs; the HTTP-date form and anything malformed
   * are unusable and take the headerless (pressure-floor) path. */
  _parseRetryAfter(response: Response): number | null {
    const v = response.headers.get("retry-after");
    if (v === null) return null;
    if (!/^\d+$/.test(v)) return null;
    return Number(v);
  }

  /** Classify a non-2xx chunk error from its ALREADY-READ body text plus the
   * response (for the Retry-After header) (v25 — pure over the body, so a
   * body-read failure is handled by the caller as a retryable transport error
   * rather than swallowed here and mislabeled fatal). */
  private _classifyChunkErrorFromText(text: string, response: Response): ChunkErrorDecision {
    const { code, message } = this._parseErrorText(text);
    const status = response.status;
    if (status === 429) {
      const retryAfter = this._parseRetryAfter(response);
      if (retryAfter !== null && retryAfter <= this._retryCeilingS) {
        return {
          fatal: false,
          error: new ChunkQuotaExceeded(message ?? "rate limited", { statusCode: 429, code }),
          retryAfter,
        };
      }
      return {
        fatal: true,
        error: new ChunkQuotaExceeded(message ?? "rate limited (no usable Retry-After)", {
          statusCode: 429,
          code,
        }),
      };
    }
    const Ctor = code ? FATAL_CHUNK_CODES[code] : undefined;
    if (Ctor) {
      return { fatal: true, error: new Ctor(message ?? code ?? "chunk error", { statusCode: status, code }) };
    }
    if (code === "chunk_concurrent_submit") {
      return {
        fatal: false,
        error: new ChunkConcurrentSubmit(message ?? "concurrent submit", { statusCode: status, code }),
      };
    }
    if (code === "chunk_resolution_unavailable") {
      // 503: a resolver dependency was unreachable — an outage, not a policy
      // shape. Same retry treatment as any 5xx, but typed so the exhaustion
      // cause names the condition. Keyed on the code, like
      // `chunk_concurrent_submit` above.
      return {
        fatal: false,
        error: new ChunkResolutionUnavailable(message ?? "policy resolution unavailable", { statusCode: status, code }),
      };
    }
    if (status === 502 || status === 503 || status === 504) {
      return { fatal: false, error: new CollieApiError(message ?? `HTTP ${status}`, { statusCode: status, code }) };
    }
    return { fatal: true, error: new CollieApiError(message ?? `HTTP ${status}`, { statusCode: status, code }) };
  }

  /** POST a chunk with retry+idempotency. Reads and returns the first 2xx
   * body — the read happens INSIDE the retry loop under the same signal
   * (v21), so a hung or aborted body read is bounded and RETRIED, not thrown
   * after return. Sends the same body each attempt so the server's
   * idempotent replay returns the cached result. */
  async _postChunk(
    path: string,
    body: unknown,
    sdkOrigin: string,
  ): Promise<{ response: Response; data: Record<string, unknown> }> {
    let attempts = 0;
    const start = this._monotonic();
    let lastError: unknown = null;
    // The last classified error rides as `cause` (python: `from last_error`,
    // dotnet: innerException) so a typed condition — e.g.
    // ChunkResolutionUnavailable — stays visible after exhaustion.
    const exhausted = (msg: string): ChunkRetryExhausted => {
      const err = new ChunkRetryExhausted(msg, { code: "chunk_retry_exhausted" });
      if (lastError != null) (err as Error & { cause?: unknown }).cause = lastError;
      return err;
    };
    while (true) {
      // Pre-attempt STRICT ceiling check (v21): once the budget is spent, no
      // further request is ISSUED (>= not >, and checked BEFORE the fetch).
      // The first attempt (attempts === 0) always runs.
      if (attempts >= 1 && this._monotonic() - start >= this._retryCeilingS) {
        throw exhausted(`Chunk retry ceiling (${this._retryCeilingS}s) exceeded`);
      }
      attempts++;
      let response: Response | null = null;
      let delay = 0;
      try {
        // Each attempt is bounded by min(request timeout, remaining retry
        // ceiling), covering the body read below.
        const remainingCeilingS = this._retryCeilingS - (this._monotonic() - start);
        response = await this._fetch(this._url(path), {
          method: "POST",
          headers: this._headers(sdkOrigin, true),
          body: JSON.stringify(body),
          signal: this._requestSignal(remainingCeilingS),
        });
      } catch (e) {
        lastError = e;
        delay = this._backoff(attempts);
      }
      if (response) {
        this._checkProtocol(response);
        if (response.ok) {
          try {
            const data = await this._readJsonBody(response);
            // STRICT total-ceiling acceptance (v23): a 2xx whose round trip
            // landed at/after the ceiling is rejected, mirroring the poll
            // strict-acceptance boundary — the ceiling is a TOTAL wall-clock
            // bound, not merely a pre-attempt gate.
            if (this._monotonic() - start >= this._retryCeilingS)
              throw exhausted(`Chunk retry ceiling (${this._retryCeilingS}s) exceeded`);
            return { response, data };
          } catch (e) {
            if (e instanceof ChunkRetryExhausted) throw e;
            // A body that ARRIVED but is empty/malformed/non-object is a
            // fatal contract violation (the server spoke, just wrong). A
            // connection error (abort/transport drop mid-body) is retryable
            // within the ceiling.
            if (e instanceof CollieApiError) throw e;
            lastError = e;
            delay = this._backoff(attempts);
          }
        } else {
          // Read the error body DIRECTLY under the per-attempt signal (v25). A
          // read failure — a socket dropped after the headers, or the signal
          // aborting mid-body — means the response never fully arrived, so it
          // is a RETRYABLE transport failure within the ceiling, NOT a fatal
          // HTTP status. The old path read it inside `_classifyChunkError` via
          // `_parseError`, which swallowed the failure and mislabeled the
          // incomplete 4xx/5xx as fatal (so a 400 + reset body did not retry).
          let errText: string | null = null;
          try {
            errText = await response.text();
          } catch (e) {
            lastError = e;
            delay = this._backoff(attempts);
          }
          if (errText !== null) {
            const decision = this._classifyChunkErrorFromText(errText, response);
            if (decision.fatal) throw decision.error;
            lastError = decision.error;
            delay = decision.retryAfter ?? this._backoff(attempts);
          }
        }
      }
      if (attempts >= this._maxRetries) {
        throw exhausted(`Chunk retry budget exhausted after ${attempts} attempts`);
      }
      // Post-response STRICT ceiling check (v21): if the planned sleep would
      // reach or pass the ceiling, stop now rather than sleeping and then
      // issuing a request AT the boundary (>= not >).
      if (this._monotonic() - start + delay >= this._retryCeilingS) {
        throw exhausted(`Chunk retry ceiling (${this._retryCeilingS}s) exceeded`);
      }
      this._debug?.debug("collieai chunk retry", { attempt: attempts, delayS: delay });
      await this._sleep(delay);
    }
  }
}
