// F9: X-CollieAi-Streaming-Protocol negotiation — warn once per client when
// the server is newer; same/older/missing/malformed stay silent.
import { afterEach, expect, test, vi } from "vitest";
import { makeClient, json, bodyOf, pathOf, type Handler } from "./helpers.js";

function jobBackend(protocolHeader?: string): Handler {
  const headers = protocolHeader === undefined ? {} : { "x-collieai-streaming-protocol": protocolHeader };
  return (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") {
      return json(202, { job_id: "job_x" }, headers);
    }
    if (path === "/v1/jobs/job_x/chunks") {
      const b = bodyOf(init);
      return json(
        200,
        { sequence: b.sequence, accepted: true, emits: [], finished: Boolean(b.is_final) },
        headers,
      );
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

afterEach(() => {
  vi.restoreAllMocks();
});

test("newer server protocol warns exactly once per client", async () => {
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const collie = makeClient(jobBackend("2"));
  const session = collie.streaming.session({ input: "p" });
  await session.open(); // job create carries the header
  await session.push("a"); // would warn again without the latch
  await session.push("b");

  const protocolWarnings = warn.mock.calls.filter((c) =>
    String(c[0]).includes("streaming protocol 2"),
  );
  expect(protocolWarnings).toHaveLength(1);
  expect(String(protocolWarnings[0][0])).toContain("supports up to 1");
});

test.each([undefined, "1", "0", "garbage", "1.5"])(
  "header %s stays silent",
  async (header) => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const collie = makeClient(jobBackend(header));
    const session = collie.streaming.session({ input: "p" });
    await session.open();
    await session.push("a");
    expect(warn).not.toHaveBeenCalled();
  },
);

test("SSE response header also warns (mixed-fleet rollout)", async () => {
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") {
      return json(202, { job_id: "job_x" }); // old pod: no header
    }
    if (path === "/v1/jobs/job_x/stream") {
      return new Response('event: end\ndata: {"reason": "final"}\n\n', {
        status: 200,
        headers: {
          "content-type": "text/event-stream",
          "x-collieai-streaming-protocol": "2", // newer pod
        },
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  for await (const _event of session.streamEvents()) {
    void _event; // drain to terminal
  }
  expect(warn.mock.calls.some((c) => String(c[0]).includes("streaming protocol 2"))).toBe(true);
});
