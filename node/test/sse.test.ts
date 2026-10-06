import { test, expect } from "vitest";
import { ChunkSessionUnrecoverable } from "../src/errors.js";
import type { SafeDelta, StreamEvent } from "../src/types.js";
import { toSse } from "../src/streaming.js";
import { makeClient, json, sse, pathOf, collect, type Handler } from "./helpers.js";

const chunkFrame = (id: string, content: string, blocked = false, blockMessage: string | null = null) =>
  `id: ${id}\nevent: chunk\ndata: ${JSON.stringify({ sequence: 0, content, blocked, block_message: blockMessage, final: false, triggered_rules: [] })}\n\n`;
const endFrame = (reason: string) => `event: end\ndata: ${JSON.stringify({ reason })}\n\n`;

async function openSession(sseHandler: (init: RequestInit) => Response) {
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
    if ((init.method ?? "GET") === "GET" && path === "/v1/jobs/job_stream/stream") return sseHandler(init);
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  return session;
}

const texts = (events: StreamEvent[]) =>
  events.filter((e): e is SafeDelta => e.type === "delta").map((e) => e.text);

test("yields deltas and finished, ignoring keepalive", async () => {
  const session = await openSession(() =>
    sse(chunkFrame("0-0", "hello"), ": keepalive\n\n", chunkFrame("1-0", " world"), endFrame("final")),
  );
  const events = await collect(session.streamEvents());
  expect(texts(events)).toEqual(["hello", " world"]);
  expect(events[events.length - 1]?.type).toBe("finished");
});

test("blocked is terminal and carries block_message (F1)", async () => {
  const session = await openSession(() =>
    sse(chunkFrame("0-0", "", true, "Custom rule block message"), endFrame("blocked")),
  );
  const events = await collect(session.streamEvents());
  const last = events[events.length - 1];
  expect(last?.type).toBe("blocked");
  expect(last && "blockMessage" in last ? last.blockMessage : null).toBe(
    "Custom rule block message",
  );
});

test("upstream_error raises CollieApiError", async () => {
  const session = await openSession(() => sse(chunkFrame("0-0", "partial"), endFrame("upstream_error")));
  await expect(collect(session.streamEvents())).rejects.toMatchObject({ code: "upstream_error" });
});

test("session_unrecoverable is fatal, not resumed", async () => {
  let calls = 0;
  const session = await openSession(() => {
    calls += 1;
    return sse(chunkFrame("0-0", "x"), endFrame("session_unrecoverable"));
  });
  await expect(collect(session.streamEvents())).rejects.toBeInstanceOf(ChunkSessionUnrecoverable);
  expect(calls).toBe(1);
});

test("idle_timeout is a typed resumable event", async () => {
  const session = await openSession(() => sse(chunkFrame("0-0", "hello"), endFrame("idle_timeout")));
  const events = await collect(session.streamEvents({ autoResume: false }));
  const last = events[events.length - 1];
  expect(last?.type).toBe("interrupted");
  expect(last && "reason" in last ? last.reason : null).toBe("idle_timeout");
});

test("client-side idle timeout surfaces idle_timeout", async () => {
  // SSE response that never sends bytes and never ends; honors the abort signal.
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
    if (path === "/v1/jobs/job_stream/stream") {
      const signal = (init as RequestInit).signal ?? undefined;
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          signal?.addEventListener("abort", () => controller.error(new DOMException("aborted", "AbortError")));
        },
      });
      return new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  const events = await collect(session.streamEvents({ idleTimeoutS: 0.05, autoResume: false }));
  expect(events).toHaveLength(1);
  const last = events[0];
  expect(last?.type).toBe("interrupted");
  expect(last && "reason" in last ? last.reason : null).toBe("idle_timeout");
});

test("caller early exit cancels the HTTP stream", async () => {
  let cancelled = false;
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
    if (path === "/v1/jobs/job_stream/stream") {
      const signal = (init as RequestInit).signal ?? undefined;
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(chunkFrame("0-0", "hello")));
          // stays open (no terminal) until cancelled
          signal?.addEventListener("abort", () => {
            try {
              controller.error(new DOMException("aborted", "AbortError"));
            } catch {
              /* already closed */
            }
          });
        },
        cancel() {
          cancelled = true;
        },
      });
      return new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  for await (const ev of session.streamEvents({ idleTimeoutS: 0 })) {
    if (ev.type === "delta") break; // relay stops after the first delta
  }
  expect(cancelled).toBe(true); // body was cancelled, not left open
});

test("reconnect resumes without duplicate", async () => {
  let calls = 0;
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
    if (path === "/v1/jobs/job_stream/stream") {
      calls += 1;
      if (calls === 1) return sse(chunkFrame("0-0", "hello")); // drops, no terminal
      expect((init.headers as Record<string, string>)["last-event-id"]).toBe("0-0");
      return sse(chunkFrame("0-0", "hello"), chunkFrame("1-0", " world"), endFrame("final"));
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  const events = await collect(session.streamEvents());
  expect(texts(events)).toEqual(["hello", " world"]);
  expect(events.some((e) => e.type === "interrupted")).toBe(true);
  expect(events[events.length - 1]?.type).toBe("finished");
  expect(calls).toBe(2);
});

test("malformed chunk frame is invalid_response", async () => {
  const session = await openSession(() => new Response("id: 0-0\nevent: chunk\ndata: not-json\n\n", { status: 200 }));
  await expect(collect(session.streamEvents())).rejects.toMatchObject({ code: "invalid_response" });
});

test("404 on stream raises CollieApiError", async () => {
  const session = await openSession(() => json(404, { error: { message: "gone", type: "not_found" } }));
  await expect(collect(session.streamEvents())).rejects.toMatchObject({ statusCode: 404 });
});

test("mintStreamToken returns token + browser url", async () => {
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
    if (path === "/v1/jobs/job_stream/stream-token") return json(200, { stream_token: "tok_abc", expires_in: 60 });
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  const st = await session.mintStreamToken();
  expect(st.token).toBe("tok_abc");
  expect(st.expiresIn).toBe(60);
  expect(st.url).toBe("http://test/v1/jobs/job_stream/stream?stream_token=tok_abc");
});

test("mintStreamToken rejects invalid expires_in", async () => {
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
    if (path === "/v1/jobs/job_stream/stream-token") return json(200, { stream_token: "tok", expires_in: 0.5 });
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  await expect(session.mintStreamToken()).rejects.toMatchObject({ code: "invalid_response" });
});

test("toSse formats frames", () => {
  expect(toSse({ type: "delta", text: "hi", sequence: 2 })).toBe(
    'event: delta\ndata: {"text":"hi","sequence":2}\n\n',
  );
  expect(toSse({ type: "interrupted", reason: "disconnect", resumable: true })).toContain("event: interrupted");
});

test("toSse serializes buffered_fallback", () => {
  const frame = toSse({
    type: "buffered_fallback",
    reason: "preset_buffered",
    recommendedClientBehavior: "buffer_then_show",
  });
  expect(frame).toContain("event: buffered_fallback");
  expect(frame).toContain("preset_buffered");
  expect(frame).toContain("buffer_then_show");
});

test("toSse input_blocked carries the context verdict (snake_case)", () => {
  const frame = toSse({
    type: "input_blocked",
    blockMessage: "ctx",
    triggeredRules: [],
    blockedBy: "context",
    context: {
      status: "blocked", blocked: true,
      triggeringPointer: "/transaction/title", triggeringRuleType: "lightweight_model",
      parseDegraded: false, limitExceeded: false, inferenceDegraded: false,
    },
  });
  expect(frame).toContain("event: input_blocked");
  const data = JSON.parse(frame.split("data: ")[1]);
  expect(data.blocked_by).toBe("context");
  expect(data.context.status).toBe("blocked");
  expect(data.context.triggering_pointer).toBe("/transaction/title");
});

test("toSse finished carries the context verdict", () => {
  const frame = toSse({
    type: "finished",
    finishReason: "stop",
    blockedBy: "none",
    context: {
      status: "degraded", blocked: false,
      parseDegraded: false, limitExceeded: false, inferenceDegraded: true,
    },
  });
  const data = JSON.parse(frame.split("data: ")[1]);
  expect(data.context.status).toBe("degraded");
  expect(data.context.inference_degraded).toBe(true);
});

test("toSse finished without context omits the keys", () => {
  const data = JSON.parse(toSse({ type: "finished", finishReason: "stop" }).split("data: ")[1]);
  expect("context" in data).toBe(false);
  expect("blocked_by" in data).toBe(false);
});
