/**
 * Twenty-first-review follow-ups (v25): a DEADLINE abort while reading the
 * surfaced-429 body is a poll-timeout, not a connection error (both timer
 * orders, both poll loops); a chunk error-body read failure is a RETRYABLE
 * transport error, not a fatal HTTP status; and the shared verdict canon driven
 * end-to-end through protectStream.
 */
import { readFileSync } from "node:fs";
import { expect, test } from "vitest";

import {
  ChunkRetryExhausted,
  CollieApiError,
  CollieConnectionError,
  ModerationError,
} from "../src/errors.js";
import type { StreamEvent } from "../src/types.js";
import { bodyOf, collect, json, makeClient, pathOf, type Handler } from "./helpers.js";

const futureIso = () => new Date(Date.now() + 60_000).toISOString();

/** Headers arrive, then the body hangs until the request signal aborts. */
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

/** Headers arrive with `status`, then the body errors immediately (socket drop). */
function bodyResets(status: number): Response {
  const stream = new ReadableStream({
    start(controller) {
      controller.error(new Error("ECONNRESET"));
    },
  });
  return new Response(stream, { status, headers: { "content-type": "application/json" } });
}

// ---- finding 2: surfaced-429 body-read abort taxonomy ----------------------

function hung429Handler(): Handler {
  return (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs")
      return json(202, { job_id: "job_v", status: "processing_inbound" });
    // An INFEASIBLE Retry-After forces the surface path; the body then hangs
    // until whichever composed signal (deadline vs request timeout) fires.
    return bodyHangs(429, init, { "retry-after": "3600" });
  };
}

// Each loop reports a poll-timeout with its own error type: moderation.input
// throws ModerationError, the generic _pollJob throws CollieApiError(poll_timeout).
const isPollTimeout = {
  moderation: (e: unknown) => e instanceof ModerationError,
  pollJob: (e: unknown) => e instanceof CollieApiError && (e as CollieApiError).code === "poll_timeout",
};

const pollLoops: Array<[string, (c: ReturnType<typeof makeClient>, timeoutS: number) => Promise<unknown>, (e: unknown) => boolean]> = [
  ["moderation loop", (c, timeoutS) => c.moderate.input({ prompt: "hi", timeoutS }), isPollTimeout.moderation],
  ["poll-job loop", (c, timeoutS) =>
    c._pollJob("job_v", {
      sdkOrigin: "test",
      timeoutS,
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
    }), isPollTimeout.pollJob],
];

for (const [loopName, run, isTimeout] of pollLoops) {
  test(`surfaced-429: a DEADLINE abort is a poll-timeout via ${loopName} (v25)`, async () => {
    // deadline (50 ms) << requestTimeoutS (30 s): the deadline is the binding
    // abort while reading the hung 429 body -> poll-timeout, NOT a connection
    // error.
    const collie = makeClient(hung429Handler(), { requestTimeoutS: 30 });
    const err = await run(collie, 0.05).catch((e) => e);
    expect(isTimeout(err)).toBe(true);
    expect(err).not.toBeInstanceOf(CollieConnectionError);
  });

  test(`surfaced-429: a REQUEST-TIMEOUT abort is a connection error via ${loopName} (v25)`, async () => {
    // requestTimeoutS (50 ms) << deadline (5 s): the request timeout fires ->
    // connection error, not a poll-timeout.
    const collie = makeClient(hung429Handler(), { requestTimeoutS: 0.05 });
    await expect(run(collie, 5)).rejects.toBeInstanceOf(CollieConnectionError);
  });
}

// ---- finding 3: a chunk error-body read failure is retried -----------------

test("a chunk 400 whose error body resets mid-read is retried, not fatal (v25)", async () => {
  let posts = 0;
  const collie = makeClient((url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url).endsWith("/chunks")) {
      posts++;
      return bodyResets(400); // 400 headers, then the socket drops mid-body
    }
    return json(404, { error: { message: "nf", type: "x" } });
  }, { maxRetries: 2 });

  // The response never fully arrived, so it is a retryable transport failure;
  // after maxRetries attempts the ceiling/budget is exhausted.
  await expect(
    collie._postChunk("/v1/jobs/j/chunks", { sequence: 0, content: "x", is_final: false }, "test"),
  ).rejects.toBeInstanceOf(ChunkRetryExhausted);
  expect(posts).toBe(2); // retried, not one-and-done
});

// ---- finding 5: the verdict canon gates the provider END-TO-END ------------

const VERDICT = JSON.parse(
  readFileSync(new URL("../../conformance/verdict_cases.json", import.meta.url), "utf8"),
) as {
  cases: Array<{ name: string; job: Record<string, unknown>; expect: { provider_runs: boolean } }>;
};

function canonStreamHandler(job: Record<string, unknown>): Handler {
  return (url, init) => {
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
    if (method === "GET" && path === "/v1/jobs/job_mod") return json(200, { ...job, job_id: "job_mod" });
    if (path === "/v1/jobs/job_stream/chunks") {
      const b = bodyOf(init);
      const isFinal = Boolean(b.is_final);
      const content = (b.content as string) ?? "";
      const emits = content || isFinal ? [{ content, blocked: false, final: isFinal, triggered_rules: [] }] : [];
      return json(200, { sequence: b.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

for (const c of VERDICT.cases) {
  test(`verdict canon gates the provider end-to-end: ${c.name}`, async () => {
    const collie = makeClient(canonStreamHandler(c.job));
    let calls = 0;
    const factory = () =>
      (async function* () {
        calls += 1;
        yield "hello";
      })();
    const events: StreamEvent[] = await collect(
      collie.streaming.protectStream({ input: "x", rawStreamFactory: factory }),
    );
    if (c.expect.provider_runs) {
      expect(calls).toBe(1);
      expect(events.some((e) => e.type === "input_blocked")).toBe(false);
    } else {
      expect(calls).toBe(0);
      expect(events[0]?.type).toBe("input_blocked");
    }
  });
}
