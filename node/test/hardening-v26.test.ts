/**
 * Twenty-second-review follow-up (v26): PBC-09 precedence is transport > deadline.
 * On a surfaced-429 whose body errors LATE (after both the request timeout AND
 * the deadline have fired), the earlier-firing REQUEST TIMEOUT must win — the v25
 * `deadline.aborted` check alone wrongly returned a poll-timeout.
 */
import { expect, test } from "vitest";

import { CollieClient } from "../src/client.js";
import { CollieApiError, CollieConnectionError, ModerationError } from "../src/errors.js";
import { json, makeClient, pathOf, type Handler } from "./helpers.js";

test("[PBC-15a] default time seam is monotonic (performance.now, not Date.now)", () => {
  const c = new CollieClient({ apiKey: "clai_test", baseUrl: "http://test" });
  const a = c._monotonic();
  expect(c._monotonic()).toBeGreaterThanOrEqual(a);
  // Date.now()/1000 would be ~1.7e9; performance.now()/1000 is process-uptime
  // seconds (well under 1e6), so this fails if the seam regresses to Date.now.
  expect(c._monotonic()).toBeLessThan(1e6);
});

/** A 429 whose HEADERS arrive at once but whose BODY errors on its OWN timer at
 * `ms`, ignoring the abort signal (a non-compliant transport) — so the body
 * failure lands AFTER both the request timeout and the deadline have fired. */
function body429ErrorsAfter(ms: number): Handler {
  return (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs")
      return json(202, { job_id: "job_v", status: "processing_inbound" });
    const stream = new ReadableStream({
      start(controller) {
        setTimeout(() => controller.error(new Error("late body error")), ms);
      },
    });
    return new Response(stream, {
      status: 429,
      headers: { "content-type": "application/json", "retry-after": "3600" },
    });
  };
}

const isConnErr = (e: unknown) => e instanceof CollieConnectionError;

const loops: Array<[string, (c: ReturnType<typeof makeClient>) => Promise<unknown>]> = [
  ["moderation loop", (c) => c.moderate.input({ prompt: "hi", timeoutS: 0.05 })],
  ["poll-job loop", (c) =>
    c._pollJob("job_v", {
      sdkOrigin: "test",
      timeoutS: 0.05,
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
    })],
];

for (const [loopName, run] of loops) {
  test(`surfaced-429: request-timeout (earlier) beats the deadline via ${loopName} (v26, PBC-09)`, async () => {
    // requestTimeoutS 10 ms < deadline 50 ms; the body errors at 80 ms. The
    // request timeout fired FIRST, so per transport > deadline it is a
    // CONNECTION error, not a poll-timeout.
    const collie = makeClient(body429ErrorsAfter(80), { requestTimeoutS: 0.01 });
    const err = await run(collie).catch((e) => e);
    expect(isConnErr(err)).toBe(true);
    expect(err).not.toBeInstanceOf(ModerationError);
    expect(err).not.toBeInstanceOf(CollieApiError); // not a bare 429 either
  });
}
