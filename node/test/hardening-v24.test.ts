/**
 * Twentieth-review follow-ups (v24): the verdict fail-safe and terminal-event
 * guarantees the .NET SDK got in v22/v23 brought to Node parity, the transport
 * taxonomy gaps the reviewer confirmed (deadline vs request-timeout vs socket
 * drop vs hung 429 when reading a non-2xx body), the setTimeout-overflow bound
 * on the backoff knobs, and the SHARED cross-SDK verdict canon.
 */
import { readFileSync } from "node:fs";
import { expect, test } from "vitest";

import { CollieConnectionError, ModerationError } from "../src/errors.js";
import { MAX_TIMEOUT_S } from "../src/pacing.js";
import type { StreamEvent } from "../src/types.js";
import { bodyOf, collect, json, makeClient, pathOf, type Handler } from "./helpers.js";

const futureIso = () => new Date(Date.now() + 60_000).toISOString();

// ---- finding 1 (+6): shared cross-SDK verdict-resolution canon -------------

const VERDICT = JSON.parse(
  readFileSync(new URL("../../conformance/verdict_cases.json", import.meta.url), "utf8"),
) as {
  cases: Array<{
    name: string;
    job: Record<string, unknown>;
    expect: { allowed: boolean; blocked: boolean; block_message?: string };
  }>;
};

function verdictHandler(job: Record<string, unknown>): Handler {
  return (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs")
      return json(202, { job_id: "job_v", status: "processing_inbound" });
    return json(200, { ...job, job_id: "job_v" });
  };
}

for (const c of VERDICT.cases) {
  test(`verdict canon matches the shared contract: ${c.name}`, async () => {
    const collie = makeClient(verdictHandler(c.job));
    const result = await collie.moderate.input({ prompt: "x" });
    expect(result.allowed).toBe(c.expect.allowed);
    expect(result.blocked).toBe(c.expect.blocked);
    if (c.expect.block_message !== undefined)
      expect(result.blockMessage).toBe(c.expect.block_message);
  });
}

// ---- finding 4: a server-initiated finish still emits a terminal event -----

function factoryOf(deltas: string[]) {
  return () =>
    (async function* () {
      for (const d of deltas) yield d;
    })();
}

test("a server-initiated finish on a normal push still emits a terminal finished event", async () => {
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/streaming/preflight")
      return json(200, {
        mode: "streaming",
        recommended_client_behavior: "stream",
        project_id: "proj_1",
        valid_until: futureIso(),
        rules: [],
      });
    if (method === "POST" && path === "/v1/jobs")
      return json(202, { job_id: bodyOf(init).inbound_only ? "job_mod" : "job_stream" });
    if (method === "GET" && path === "/v1/jobs/job_mod")
      return json(200, {
        status: "completed",
        inbound_result: { allowed: true, blocked: false, triggered_rules: [] },
      });
    if (path === "/v1/jobs/job_stream/chunks") {
      const b = bodyOf(init);
      // The backend finalizes the session on the FIRST (non-final) push, with a
      // safe emit and no block — the old code set `terminated` and skipped the
      // finished event, ending on a bare delta.
      return json(200, {
        sequence: b.sequence,
        accepted: true,
        emits: [{ content: b.content ?? "", blocked: false, final: false, triggered_rules: [] }],
        finished: true,
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const events: StreamEvent[] = await collect(
    collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["hello"]) }),
  );
  expect(events.some((e) => e.type === "finished")).toBe(true);
  expect(events.at(-1)?.type).toBe("finished");
});

// ---- finding 3: non-2xx body-read taxonomy ---------------------------------

/** Headers arrive with `status`, then the body hangs until the request signal
 * aborts and errors with its reason — so `response.text()` rejects with the
 * abort (deadline OR request timeout, whichever fired). */
function bodyHangs(status: number, init: RequestInit, headers: Record<string, string> = {}): Response {
  const sig = init.signal!;
  const stream = new ReadableStream({
    start(controller) {
      if (sig.aborted) return controller.error(sig.reason);
      sig.addEventListener("abort", () => controller.error(sig.reason), { once: true });
    },
  });
  return new Response(stream, { status, headers: { "content-type": "application/json", ...headers } });
}

/** Headers arrive with `status`, then the body stream errors IMMEDIATELY with a
 * non-abort transport error (a socket dropped after the headers). */
function bodyResets(status: number): Response {
  const stream = new ReadableStream({
    start(controller) {
      controller.error(new Error("ECONNRESET"));
    },
  });
  return new Response(stream, { status, headers: { "content-type": "application/json" } });
}

test("3a: a DEADLINE abort during a non-2xx body read is a poll timeout, not a connection error", async () => {
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs")
      return json(202, { job_id: "job_v", status: "processing_inbound" });
    return bodyHangs(400, init); // the 400 error body hangs until the deadline aborts
  };
  // Deadline (timeoutS) << requestTimeoutS, so the deadline is the binding abort.
  const collie = makeClient(handler, { requestTimeoutS: 30 });
  await expect(collie.moderate.input({ prompt: "hi", timeoutS: 0.05 })).rejects.toBeInstanceOf(
    ModerationError,
  );
});

test("3b: a socket reset reading a non-2xx body is a connection error, not the HTTP status", async () => {
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs")
      return json(202, { job_id: "job_v", status: "processing_inbound" });
    return bodyResets(503); // 503 headers, then the socket drops mid-body
  };
  const collie = makeClient(handler, { requestTimeoutS: 30 });
  const err = await collie.moderate.input({ prompt: "hi", timeoutS: 5 }).catch((e) => e);
  expect(err).toBeInstanceOf(CollieConnectionError);
});

test("3c: a hung surfaced-429 body is a connection error, not a bare 429", async () => {
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs")
      return json(202, { job_id: "job_v", status: "processing_inbound" });
    // An INFEASIBLE Retry-After forces the surface path (sleepFor429 -> null);
    // the body then hangs until the request timeout aborts the unread body.
    return bodyHangs(429, init, { "retry-after": "3600" });
  };
  // Small requestTimeoutS so the lingering signal aborts the unread 429 body.
  const collie = makeClient(handler, { requestTimeoutS: 0.05 });
  const err = await collie.moderate.input({ prompt: "hi", timeoutS: 5 }).catch((e) => e);
  expect(err).toBeInstanceOf(CollieConnectionError);
});

// ---- finding 5: backoff knobs bounded at the 32-bit timer limit ------------

test.each<[string, Partial<import("../src/client.js").CollieClientOptions>]>([
  ["maxBackoffS overflows the timer", { maxBackoffS: MAX_TIMEOUT_S + 1 }],
  ["baseBackoffS overflows the timer", { baseBackoffS: MAX_TIMEOUT_S + 1, maxBackoffS: MAX_TIMEOUT_S + 2 }],
  ["retryMaxPerChunkS overflows the timer", { retryMaxPerChunkS: MAX_TIMEOUT_S + 1 }],
])("an oversized %s is rejected at construction (would clamp setTimeout to 1ms)", (_n, opts) => {
  expect(() => makeClient((_u, _i) => json(200, {}), opts)).toThrow(RangeError);
});
