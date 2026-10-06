/**
 * Twenty-third-review follow-ups (v27): the surfaced-429 body read must classify
 * the REAL error (not reconstruct from timer flags), and must re-check the
 * deadline AFTER the read (a slow-but-correct body that crosses the deadline is a
 * poll-timeout, not a typed 429).
 */
import { expect, test } from "vitest";

import { CollieApiError, CollieConnectionError, ModerationError } from "../src/errors.js";
import { json, makeClient, pathOf, type Handler } from "./helpers.js";

const isPollTimeout = {
  moderation: (e: unknown) => e instanceof ModerationError,
  pollJob: (e: unknown) => e instanceof CollieApiError && (e as CollieApiError).code === "poll_timeout",
};
const loops: Array<[string, (c: ReturnType<typeof makeClient>, timeoutS: number, mono?: () => number) => Promise<unknown>, (e: unknown) => boolean]> = [
  ["moderation loop", (c, timeoutS) => c.moderate.input({ prompt: "hi", timeoutS }), isPollTimeout.moderation],
  ["poll-job loop", (c, timeoutS) =>
    c._pollJob("job_v", { sdkOrigin: "test", timeoutS, terminal: new Set(["completed"]), failure: new Set(["failed"]) }),
    isPollTimeout.pollJob],
];

// ---- finding 1: an INDEPENDENT transport reset of the 429 body -------------

function body429ResetsAfter(ms: number): Handler {
  return (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs")
      return json(202, { job_id: "job_v", status: "processing_inbound" });
    // The body errors on its OWN timer (a socket drop), NOT via the abort signal.
    const stream = new ReadableStream({
      pull(controller) {
        setTimeout(() => controller.error(new Error("ECONNRESET")), ms);
      },
    });
    return new Response(stream, {
      status: 429,
      headers: { "content-type": "application/json", "retry-after": "3600" },
    });
  };
}

for (const [loopName, run] of loops) {
  test(`surfaced-429: an independent transport reset is a connection error via ${loopName} (v27, PBC-09b)`, async () => {
    // deadline 10 ms (fires first), request timeout 50 ms, body resets at 40 ms.
    // The reset is a TRANSPORT failure -> connection error, even though the
    // deadline has fired (transport > deadline).
    const collie = makeClient(body429ResetsAfter(40), { requestTimeoutS: 0.05 });
    const err = await run(collie, 0.01).catch((e) => e);
    expect(err).toBeInstanceOf(CollieConnectionError);
  });
}

// ---- finding 2: a slow-but-correct 429 body that crosses the deadline ------

function body429SlowOk(bumpTo: number, monoRef: { v: number }): Handler {
  return (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs")
      return json(202, { job_id: "job_v", status: "processing_inbound" });
    const payload = JSON.stringify({ error: { message: "rate limited", type: "rate_limited" } });
    // highWaterMark 0 defers `pull` until the body is actually READ (not eagerly
    // at construction), so the clock jump lands DURING response.text() — after
    // the loop's pre-429 remaining() check, exercising the missing post-read one.
    const stream = new ReadableStream(
      {
        pull(controller) {
          monoRef.v = bumpTo; // the budget is spent DURING the body read
          controller.enqueue(new TextEncoder().encode(payload));
          controller.close();
        },
      },
      { highWaterMark: 0 },
    );
    return new Response(stream, {
      status: 429,
      headers: { "content-type": "application/json", "retry-after": "3600" },
    });
  };
}

for (const [loopName, run, isTimeout] of loops) {
  test(`surfaced-429: a body that crosses the deadline is a poll-timeout via ${loopName} (v27, PBC-07)`, async () => {
    // timeout 1 s; the monotonic clock jumps 0 -> 2 s DURING response.text().
    const monoRef = { v: 0 };
    const collie = makeClient(body429SlowOk(2.0, monoRef), { monotonic: () => monoRef.v });
    const err = await run(collie, 1.0).catch((e) => e);
    expect(isTimeout(err)).toBe(true);
    // Not surfaced as the rate-limit 429 (the poll-job timeout is itself a
    // CollieApiError with code poll_timeout, so assert on the code).
    if (err instanceof CollieApiError) expect(err.code).not.toBe("rate_limited");
  });
}
