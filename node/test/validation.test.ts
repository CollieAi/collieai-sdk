import { test, expect } from "vitest";
import { makeClient, json, pathOf, type Handler } from "./helpers.js";

/** Response-shape validation: malformed-but-2xx bodies must raise
 * CollieApiError(code="invalid_response"), matching Python/Pydantic — never be
 * silently coerced (NaN sequence, allowed:true from a list, blank rules). */

test("chunk result missing sequence is invalid_response (no sequence advance)", async () => {
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_stream" });
    if (path === "/v1/jobs/job_stream/chunks") return json(200, {}); // {} -> would be sequence: NaN
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const session = collie.streaming.session({ input: "p" });
  await session.open();
  await expect(session.push("hi")).rejects.toMatchObject({ code: "invalid_response" });
});

test("array inbound_result is invalid_response (not allowed:true)", async () => {
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_mod" });
    if (path === "/v1/jobs/job_mod") return json(200, { status: "completed", inbound_result: [] });
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  await expect(collie.moderate.input({ prompt: "hi" })).rejects.toMatchObject({ code: "invalid_response" });
});

test("primitive triggered rule is invalid_response (not a blank rule)", async () => {
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_mod" });
    if (path === "/v1/jobs/job_mod") {
      return json(200, {
        status: "inbound_blocked",
        inbound_result: { allowed: false, blocked: true, triggered_rules: ["oops"] },
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  await expect(collie.moderate.input({ prompt: "hi" })).rejects.toMatchObject({ code: "invalid_response" });
});

test("array outbound_result is invalid_response", async () => {
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") return json(202, { job_id: "job_buf" });
    if (path === "/v1/jobs/job_buf") return json(200, { status: "completed", outbound_result: [] });
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  await expect(
    collie.streaming.protectBuffered({
      input: "hi",
      checkInput: false,
      rawStreamFactory: () =>
        (async function* () {
          yield "x";
        })(),
    }),
  ).rejects.toMatchObject({ code: "invalid_response" });
});
