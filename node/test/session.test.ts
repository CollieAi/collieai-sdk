import { test, expect } from "vitest";
import { ChunkSessionFinished, ConcurrentSessionUseError } from "../src/errors.js";
import { makeClient, json, bodyOf, pathOf, type Handler } from "./helpers.js";

function recordingEcho(): { handler: Handler; chunks: Record<string, unknown>[] } {
  const chunks: Record<string, unknown>[] = [];
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      return json(202, { job_id: bodyOf(init).inbound_only ? "job_mod" : "job_stream" });
    }
    if (path === "/v1/jobs/job_stream/chunks") {
      const b = bodyOf(init);
      chunks.push(b);
      const isFinal = Boolean(b.is_final);
      const content = (b.content as string) ?? "";
      const emits =
        content || isFinal ? [{ content, blocked: false, final: isFinal, triggered_rules: [] }] : [];
      return json(200, { sequence: b.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  return { handler, chunks };
}

test("push assigns monotonic sequences and finish sends is_final", async () => {
  const { handler, chunks } = recordingEcho();
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  const r0 = await session.push("alpha");
  const r1 = await session.push("beta");
  const rf = await session.finish();
  expect(chunks.map((c) => c.sequence)).toEqual([0, 1, 2]);
  expect(r0.sequence).toBe(0);
  expect(r1.sequence).toBe(1);
  expect(r0.emits[0]?.text).toBe("alpha");
  expect(chunks[2]?.is_final).toBe(true);
  expect(chunks[2]?.content).toBe("");
  // F5: the terminal chunk carries finish_reason; non-final pushes never do.
  expect(chunks[2]?.finish_reason).toBe("stop");
  expect(chunks.slice(0, 2).every((c) => !("finish_reason" in c))).toBe(true);
  expect(rf.finished).toBe(true);
  expect(session.lastSequence).toBe(2);
});

test("custom finish_reason is transmitted on the terminal chunk", async () => {
  const { handler, chunks } = recordingEcho();
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  await session.push("alpha");
  await session.finish("length");
  expect(chunks[chunks.length - 1]?.finish_reason).toBe("length");
});

test("session sends input_job_id when provided and omits it otherwise", async () => {
  const createdJobs: Array<Record<string, unknown>> = [];
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs") {
      createdJobs.push(bodyOf(init));
      return json(202, { job_id: "job_stream" });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  await collie.streaming.session({ input: "p", inputJobId: "job_gate" }).open();
  await collie.streaming.session({ input: "p" }).open();
  expect(createdJobs[0]?.input_job_id).toBe("job_gate");
  expect(createdJobs[0]?.message_input).toBe("p");
  expect("input_job_id" in createdJobs[1]!).toBe(false);
});

function unconfirmedBackend(
  chunkBody: (b: Record<string, unknown>) => Record<string, unknown>,
): Handler {
  return (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") {
      return json(202, { job_id: "job_stream" });
    }
    if (path === "/v1/jobs/job_stream/chunks") return json(200, chunkBody(bodyOf(init)));
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

test("accepted=false response fails the session without advancing", async () => {
  const collie = makeClient(
    unconfirmedBackend((b) => ({ sequence: b.sequence, accepted: false, emits: [], finished: false })),
  );
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  await expect(session.push("x")).rejects.toMatchObject({ code: "invalid_response" });
  expect(session.lastSequence).toBe(-1); // never advanced
  await expect(session.push("y")).rejects.toThrow("Session is closed");
});

test("wrong sequence echo fails the session (stale/cached response)", async () => {
  const collie = makeClient(
    unconfirmedBackend((b) => ({
      sequence: (b.sequence as number) + 7,
      accepted: true,
      emits: [{ content: "stale", blocked: false, final: false, triggered_rules: [] }],
      finished: false,
    })),
  );
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  await expect(session.push("x")).rejects.toMatchObject({ code: "invalid_response" });
  expect(session.lastSequence).toBe(-1); // the stale emit was never surfaced
});

test("no webhook_url in job creation; chunks attributed to streaming.session", async () => {
  const origins: (string | undefined)[] = [];
  const { handler } = recordingEcho();
  const wrapped: Handler = (url, init) => {
    origins.push((init.headers as Record<string, string>)["x-collieai-sdk-origin"]);
    return handler(url, init);
  };
  const collie = makeClient(wrapped);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  await session.push("x");
  expect(new Set(origins)).toEqual(new Set(["streaming.session"]));
});

test("finish is idempotent", async () => {
  const { handler, chunks } = recordingEcho();
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  await session.push("x");
  const first = await session.finish();
  const second = await session.finish();
  expect(second).toBe(first);
  expect(chunks.filter((c) => c.is_final).length).toBe(1);
});

test("block emit is terminal", async () => {
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
    if (path.endsWith("/chunks")) {
      const b = bodyOf(init);
      return json(200, {
        sequence: b.sequence,
        accepted: true,
        emits: [{ content: "", blocked: true, final: true, triggered_rules: [] }],
        finished: true,
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  const r = await session.push("bad");
  expect(r.finished).toBe(true);
  expect(r.emits[0]?.blocked).toBe(true);
  await expect(session.push("more")).rejects.toBeInstanceOf(ChunkSessionFinished);
});

test("concurrent push throws", async () => {
  const { handler } = recordingEcho();
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  const p1 = session.push("a");
  await expect(session.push("b")).rejects.toBeInstanceOf(ConcurrentSessionUseError);
  await p1;
});

test("missing job_id on create is invalid_response", async () => {
  const collie = makeClient((url, init) => json(202, { status: "processing_inbound" }));
  const session = collie.streaming.session({ input: "p" });
  await expect(session.open()).rejects.toMatchObject({ code: "invalid_response" });
});
