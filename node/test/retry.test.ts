import { test, expect } from "vitest";
import {
  ChunkPolicyChanged,
  ChunkQuotaExceeded,
  ChunkResolutionUnavailable,
  ChunkRetryExhausted,
  ChunkSessionUnrecoverable,
  ChunkStreamingUnsupported,
} from "../src/errors.js";
import { makeClient, json, errorJson, bodyOf, pathOf, type Handler } from "./helpers.js";

/** Chunk handler that fails the first `failTimes` chunk submits, then echoes. */
function flaky(failTimes: number, failure: () => Response | never): {
  handler: Handler;
  seqs: number[];
} {
  const seqs: number[] = [];
  let calls = 0;
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
    if (path.endsWith("/chunks")) {
      calls += 1;
      const b = bodyOf(init);
      seqs.push(b.sequence as number);
      if (calls <= failTimes) return failure();
      return json(200, {
        sequence: b.sequence,
        accepted: true,
        emits: [{ content: b.content, blocked: false, final: false, triggered_rules: [] }],
        finished: false,
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  return { handler, seqs };
}

async function openSession(handler: Handler) {
  const collie = makeClient(handler);
  const s = collie.streaming.session({ input: "p" });
  await s.open();
  return s;
}

test("network timeout retries the same sequence", async () => {
  const { handler, seqs } = flaky(1, () => {
    throw new Error("ECONNRESET");
  });
  const s = await openSession(handler);
  const r = await s.push("hello");
  expect(seqs).toEqual([0, 0]);
  expect(r.emits[0]?.text).toBe("hello");
});

test("504 chunk_filter_timeout retries then succeeds", async () => {
  const { handler, seqs } = flaky(1, () => errorJson(504, "chunk_filter_timeout"));
  const s = await openSession(handler);
  const r = await s.push("hi");
  expect(seqs).toEqual([0, 0]);
  expect(r.accepted).toBe(true);
});

test("retry budget exhausted", async () => {
  const { handler } = flaky(99, () => errorJson(504, "chunk_filter_timeout"));
  const collie = makeClient(handler, { maxRetries: 3 });
  const s = collie.streaming.session({ input: "p" });
  await s.open();
  await expect(s.push("hi")).rejects.toBeInstanceOf(ChunkRetryExhausted);
});

test("503 chunk_resolution_unavailable retries then succeeds", async () => {
  const { handler, seqs } = flaky(1, () => errorJson(503, "chunk_resolution_unavailable"));
  const s = await openSession(handler);
  const r = await s.push("hi");
  expect(seqs).toEqual([0, 0]);
  expect(r.accepted).toBe(true);
});

test("resolution-unavailable exhaustion names the condition via cause", async () => {
  const { handler } = flaky(99, () => errorJson(503, "chunk_resolution_unavailable"));
  const collie = makeClient(handler, { maxRetries: 3 });
  const s = collie.streaming.session({ input: "p" });
  await s.open();
  const err: unknown = await s.push("hi").catch((e: unknown) => e);
  expect(err).toBeInstanceOf(ChunkRetryExhausted);
  const cause = (err as Error).cause;
  expect(cause).toBeInstanceOf(ChunkResolutionUnavailable);
  expect((cause as ChunkResolutionUnavailable).code).toBe("chunk_resolution_unavailable");
  expect((cause as ChunkResolutionUnavailable).statusCode).toBe(503);
});

test("429 Retry-After:0 retries immediately", async () => {
  const { handler, seqs } = flaky(1, () => errorJson(429, "rate_limited", "slow", { "retry-after": "0" }));
  const s = await openSession(handler);
  const r = await s.push("hi");
  expect(r.accepted).toBe(true);
  expect(seqs.length).toBe(2);
});

test("429 without Retry-After raises quota", async () => {
  const { handler } = flaky(99, () => errorJson(429, "rate_limited"));
  const s = await openSession(handler);
  await expect(s.push("hi")).rejects.toBeInstanceOf(ChunkQuotaExceeded);
});

test("fatal codes map to typed errors and fail the session", async () => {
  const cases: Array<[number, string, new (...a: never[]) => Error]> = [
    [409, "chunk_policy_changed", ChunkPolicyChanged],
    [409, "chunk_session_unrecoverable", ChunkSessionUnrecoverable],
    [400, "chunk_streaming_unsupported", ChunkStreamingUnsupported],
  ];
  for (const [status, code, Cls] of cases) {
    let calls = 0;
    const handler: Handler = (url, init) => {
      const path = pathOf(url);
      if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
      if (path.endsWith("/chunks")) {
        calls += 1;
        return errorJson(status, code, `${code}!`);
      }
      return json(404, { error: { message: "nf", type: "x" } });
    };
    const s = await openSession(handler);
    await expect(s.push("x")).rejects.toBeInstanceOf(Cls);
    expect(calls).toBe(1); // fatal: no retry
  }
});
