import { test, expect } from "vitest";
import { CollieApiError, ModerationError } from "../src/errors.js";
import { makeClient, json, bodyOf, pathOf, type Handler } from "./helpers.js";

function modHandler(opts: {
  statuses: string[];
  inboundResult?: unknown;
  capture?: { body?: Record<string, unknown>; origin?: string | null };
  contextResult?: unknown;
  blockedBy?: string;
}): Handler {
  let gets = 0;
  return (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      if (opts.capture) {
        opts.capture.body = bodyOf(init);
        opts.capture.origin = (init.headers as Record<string, string>)["x-collieai-sdk-origin"];
      }
      return json(202, { job_id: "job_m" });
    }
    if (method === "GET" && path === "/v1/jobs/job_m") {
      const idx = Math.min(gets, opts.statuses.length - 1);
      gets += 1;
      const status = opts.statuses[idx];
      const payload: Record<string, unknown> = { job_id: "job_m", status };
      if (status === "completed" || status === "inbound_blocked") {
        payload.inbound_result = opts.inboundResult ?? {};
        if (opts.contextResult !== undefined) payload.context_result = opts.contextResult;
        if (opts.blockedBy !== undefined) payload.blocked_by = opts.blockedBy;
      }
      return json(200, payload);
    }
    return json(404, { error: { message: "nf", type: "not_found" } });
  };
}

test("input allowed", async () => {
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, filtered_content: "hi", triggered_rules: [] },
    }),
  );
  const r = await collie.moderate.input({ prompt: "hi" });
  expect(r.allowed).toBe(true);
  expect(r.blocked).toBe(false);
  expect(r.originalText).toBe("hi");
  expect(r.filteredText).toBe("hi");
  expect(r.jobId).toBe("job_m");
});

test("input blocked is a normal result", async () => {
  const collie = makeClient(
    modHandler({
      statuses: ["inbound_blocked"],
      inboundResult: {
        allowed: false,
        blocked: true,
        block_message: "PII",
        triggered_rules: [{ rule_id: "r1", rule_name: "PII", rule_type: "regex", decision: "block" }],
      },
    }),
  );
  const r = await collie.moderate.input({ prompt: "ssn 123" });
  expect(r.blocked).toBe(true);
  expect(r.blockMessage).toBe("PII");
  expect(r.triggeredRules[0]?.ruleName).toBe("PII");
});

test("input sends structured context as json body", async () => {
  const capture: { body?: Record<string, unknown> } = {};
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
      capture,
    }),
  );
  await collie.moderate.input({
    prompt: "summarize my last payment",
    context: { transaction: { title: "hi" } },
    contextFormat: "json",
  });
  expect(capture.body?.message_input).toBe("summarize my last payment");
  expect(capture.body?.context).toEqual({ transaction: { title: "hi" } });
  expect(capture.body?.context_format).toBe("json");
});

test("input sends string context with format", async () => {
  const capture: { body?: Record<string, unknown> } = {};
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
      capture,
    }),
  );
  await collie.moderate.input({ prompt: "hi", context: "raw blob", contextFormat: "text" });
  expect(capture.body?.context).toBe("raw blob");
  expect(capture.body?.context_format).toBe("text");
});

test("input omits context when absent", async () => {
  const capture: { body?: Record<string, unknown> } = {};
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
      capture,
    }),
  );
  await collie.moderate.input({ prompt: "hi" });
  expect(capture.body && "context" in capture.body).toBe(false);
  expect(capture.body && "context_format" in capture.body).toBe(false);
});

test("input rejects non-finite context (cross-SDK parity)", async () => {
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
    }),
  );
  await expect(
    collie.moderate.input({ prompt: "hi", context: { amount: NaN } }),
  ).rejects.toBeInstanceOf(TypeError);
});

test("input rejects non-plain-object context (Map/Date) that JSON.stringify mangles", async () => {
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
    }),
  );
  // Map serializes to {} — silent data loss.
  await expect(
    collie.moderate.input({ prompt: "hi", context: new Map([["a", 1]]) }),
  ).rejects.toBeInstanceOf(TypeError);
  // Date nested as a value -> converted to a string under the hood.
  await expect(
    collie.moderate.input({ prompt: "hi", context: { when: new Date() } }),
  ).rejects.toBeInstanceOf(TypeError);
});

test("input rejects symbol-keyed context", async () => {
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
    }),
  );
  const obj: Record<string | symbol, unknown> = { a: 1 };
  obj[Symbol("s")] = 2;
  await expect(
    collie.moderate.input({ prompt: "hi", context: obj }),
  ).rejects.toBeInstanceOf(TypeError);
});

test("polls until terminal", async () => {
  const handler = modHandler({
    statuses: ["processing_inbound", "processing_inbound", "completed"],
    inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
  });
  const collie = makeClient(handler);
  const r = await collie.moderate.input({ prompt: "hi", pollIntervalS: 0.01 });
  expect(r.allowed).toBe(true);
});

test("failed job raises ModerationError", async () => {
  const collie = makeClient(modHandler({ statuses: ["failed"] }));
  await expect(collie.moderate.input({ prompt: "hi" })).rejects.toBeInstanceOf(ModerationError);
});

test("missing inbound_result is invalid_response", async () => {
  const collie = makeClient((url, init) => {
    if ((init.method ?? "GET") === "POST") return json(202, { job_id: "job_m" });
    return json(200, { status: "completed" }); // no inbound_result
  });
  await expect(collie.moderate.input({ prompt: "hi" })).rejects.toMatchObject({ code: "invalid_response" });
});

test("sends metadata + origin, no webhook_url", async () => {
  const capture: { body?: Record<string, unknown>; origin?: string | null } = {};
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
      capture,
    }),
  );
  await collie.moderate.input({ prompt: "hi", conversationId: "c1", correlationId: "t1" });
  expect(capture.body?.message_input).toBe("hi");
  expect(capture.body?.inbound_only).toBe(true);
  expect(capture.body?.conversation_id).toBe("c1");
  expect(capture.body?.correlation_id).toBe("t1");
  expect("webhook_url" in (capture.body ?? {})).toBe(false);
  expect(capture.origin).toBe("moderate.input");
});

test("per-call projectId without client project throws", async () => {
  const collie = makeClient(modHandler({ statuses: ["completed"] }), { projectId: undefined });
  await expect(collie.moderate.input({ prompt: "hi", projectId: "p" })).rejects.toBeInstanceOf(Error);
});

test("connection error is wrapped", async () => {
  const collie = makeClient(() => {
    throw new Error("boom");
  });
  await expect(collie.moderate.input({ prompt: "hi" })).rejects.toMatchObject({
    name: "CollieConnectionError",
  });
});

// ---------------------------------------------------------------------------
// Context analysis typed result
// ---------------------------------------------------------------------------

const CONTEXT_STATUSES = [
  "not_provided", "disabled", "not_run", "clean", "monitored", "blocked", "degraded",
] as const;

for (const status of CONTEXT_STATUSES) {
  test(`context status parsed: ${status}`, async () => {
    const collie = makeClient(
      modHandler({
        statuses: ["completed"],
        inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
        contextResult: { status, blocked: status === "blocked" },
        blockedBy: status === "blocked" ? "context" : "none",
      }),
    );
    const r = await collie.moderate.input({ prompt: "hi", context: { a: "b" } });
    expect(r.context?.status).toBe(status);
    expect(r.blockedBy).toBe(status === "blocked" ? "context" : "none");
  });
}

test("context block detail mapped to camelCase", async () => {
  const collie = makeClient(
    modHandler({
      statuses: ["inbound_blocked"],
      inboundResult: { allowed: false, blocked: true, block_message: "ctx", triggered_rules: [] },
      contextResult: {
        status: "blocked", blocked: true, block_message: "ctx",
        triggering_pointer: "/transaction/title", triggering_rule_id: "r1",
        triggering_rule_type: "lightweight_model",
      },
      blockedBy: "context",
    }),
  );
  const r = await collie.moderate.input({ prompt: "summarize", context: { transaction: { title: "x" } } });
  expect(r.blocked).toBe(true);
  expect(r.blockedBy).toBe("context");
  expect(r.context?.status).toBe("blocked");
  expect(r.context?.triggeringPointer).toBe("/transaction/title");
  expect(r.context?.triggeringRuleType).toBe("lightweight_model");
});

test("context degraded markers mapped", async () => {
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
      contextResult: { status: "degraded", limit_exceeded: true, inference_degraded: true },
      blockedBy: "none",
    }),
  );
  const r = await collie.moderate.input({ prompt: "hi", context: { a: "b" } });
  expect(r.context?.status).toBe("degraded");
  expect(r.context?.limitExceeded).toBe(true);
  expect(r.context?.inferenceDegraded).toBe(true);
});

test("no context_result -> context null", async () => {
  const collie = makeClient(
    modHandler({
      statuses: ["completed"],
      inboundResult: { allowed: true, blocked: false, triggered_rules: [] },
    }),
  );
  const r = await collie.moderate.input({ prompt: "hi" });
  expect(r.context == null).toBe(true);
  expect(r.blockedBy == null).toBe(true);
});

// --- moderate.output --------------------------------------------------------

function outHandler(opts: {
  statuses: string[];
  outboundResult?: unknown;
  capture?: { body?: Record<string, unknown>; origin?: string | null };
  createJson?: Record<string, unknown>;
}): Handler {
  let gets = 0;
  return (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      if (opts.capture) {
        opts.capture.body = bodyOf(init);
        opts.capture.origin = (init.headers as Record<string, string>)["x-collieai-sdk-origin"];
      }
      return json(202, opts.createJson ?? { job_id: "job_o" });
    }
    if (method === "GET" && path === "/v1/jobs/job_o") {
      const idx = Math.min(gets, opts.statuses.length - 1);
      gets += 1;
      const status = opts.statuses[idx];
      const payload: Record<string, unknown> = { job_id: "job_o", status };
      if (
        (status === "completed" || status === "outbound_blocked") &&
        opts.outboundResult !== undefined
      ) {
        payload.outbound_result = opts.outboundResult;
      }
      return json(200, payload);
    }
    return json(404, { error: { message: "nf", type: "not_found" } });
  };
}

test("output allowed carries filteredText (the text to send)", async () => {
  const collie = makeClient(
    outHandler({
      statuses: ["completed"],
      outboundResult: {
        allowed: true,
        blocked: false,
        filtered_content: "your card is [MASKED]",
        triggered_rules: [
          { rule_id: "r9", rule_name: "pii-mask", rule_type: "regex", decision: "mask" },
        ],
      },
    }),
  );
  const r = await collie.moderate.output({ response: "your card is 4111-1111" });
  expect(r.allowed).toBe(true);
  expect(r.blocked).toBe(false);
  expect(r.originalText).toBe("your card is 4111-1111");
  expect(r.filteredText).toBe("your card is [MASKED]");
  expect(r.triggeredRules[0].ruleName).toBe("pii-mask");
  expect(r.jobId).toBe("job_o");
});

test("output blocked is a normal result", async () => {
  const collie = makeClient(
    outHandler({
      statuses: ["outbound_blocked"],
      outboundResult: {
        allowed: false,
        blocked: true,
        block_message: "Response blocked",
        triggered_rules: [],
      },
    }),
  );
  const r = await collie.moderate.output({ response: "the FORBIDDEN text" });
  expect(r.blocked).toBe(true);
  expect(r.allowed).toBe(false);
  expect(r.blockMessage).toBe("Response blocked");
});

// The v24 fail-safe table (sdk-plan § Output Moderation), row by row.
for (const [name, status, outboundResult, expectAllowed] of [
  ["explicit allowed", "completed", { allowed: true, blocked: false }, true],
  ["empty result object is ambiguous", "completed", {}, false],
  ["absent allowed with blocked=false is ambiguous", "completed", { blocked: false }, false],
  ["explicit allowed=false", "completed", { allowed: false }, false],
  [
    "blocked status beats a contradicting body",
    "outbound_blocked",
    { allowed: true, blocked: false },
    false,
  ],
  ["truthy-but-not-true allowed", "completed", { allowed: 1, blocked: false }, false],
] as Array<[string, string, Record<string, unknown>, boolean]>) {
  test(`output verdict fail-safe: ${name}`, async () => {
    const collie = makeClient(outHandler({ statuses: [status], outboundResult }));
    const r = await collie.moderate.output({ response: "text" });
    expect(r.allowed).toBe(expectAllowed);
    expect(r.blocked).toBe(!expectAllowed);
  });
}

test("output terminal job without outbound_result is a typed error", async () => {
  const collie = makeClient(outHandler({ statuses: ["completed"] }));
  await expect(collie.moderate.output({ response: "text" })).rejects.toMatchObject({
    code: "invalid_response",
  });
});

test("output malformed triggered_rules is a typed error", async () => {
  const collie = makeClient(
    outHandler({
      statuses: ["completed"],
      outboundResult: { allowed: true, blocked: false, triggered_rules: ["not-a-rule"] },
    }),
  );
  await expect(collie.moderate.output({ response: "text" })).rejects.toMatchObject({
    code: "invalid_response",
  });
});

for (const dead of ["failed", "expired"]) {
  test(`output dead job (${dead}) raises ModerationError`, async () => {
    const collie = makeClient(outHandler({ statuses: [dead] }));
    await expect(collie.moderate.output({ response: "text" })).rejects.toBeInstanceOf(
      ModerationError,
    );
  });
}

test("output sends message_output only, with the new origin", async () => {
  const capture: { body?: Record<string, unknown>; origin?: string | null } = {};
  const collie = makeClient(
    outHandler({
      statuses: ["completed"],
      outboundResult: { allowed: true, blocked: false, triggered_rules: [] },
      capture,
    }),
  );
  await collie.moderate.output({
    response: "hello",
    conversationId: "conv1",
    correlationId: "turn1",
  });
  expect(capture.body).toMatchObject({
    message_output: "hello",
    conversation_id: "conv1",
    correlation_id: "turn1",
  });
  expect(capture.body).not.toHaveProperty("message_input");
  expect(capture.body).not.toHaveProperty("inbound_only");
  expect(capture.body).not.toHaveProperty("context");
  expect(capture.body).not.toHaveProperty("webhook_url");
  expect(capture.origin).toBe("moderate.output");
});

test("output omits empty conversation/correlation ids", async () => {
  const capture: { body?: Record<string, unknown>; origin?: string | null } = {};
  const collie = makeClient(
    outHandler({
      statuses: ["completed"],
      outboundResult: { allowed: true, blocked: false, triggered_rules: [] },
      capture,
    }),
  );
  await collie.moderate.output({ response: "hello", conversationId: "", correlationId: "" });
  expect(capture.body).not.toHaveProperty("conversation_id");
  expect(capture.body).not.toHaveProperty("correlation_id");
});

test("output projectId mismatch throws before any request", async () => {
  const capture: { body?: Record<string, unknown>; origin?: string | null } = {};
  const collie = makeClient(outHandler({ statuses: ["completed"], capture }));
  await expect(
    collie.moderate.output({ response: "hello", projectId: "proj_other" }),
  ).rejects.toBeInstanceOf(Error);
  expect(capture.body).toBeUndefined();
});

test("output create without job_id is a typed error", async () => {
  const collie = makeClient(
    outHandler({ statuses: ["completed"], createJson: { status: "accepted" } }),
  );
  await expect(collie.moderate.output({ response: "hello" })).rejects.toMatchObject({
    code: "invalid_response",
  });
});

test("output invalid knobs fail before any request", async () => {
  const capture: { body?: Record<string, unknown>; origin?: string | null } = {};
  const collie = makeClient(outHandler({ statuses: ["completed"], capture }));
  await expect(
    collie.moderate.output({ response: "hello", timeoutS: -1 }),
  ).rejects.toBeInstanceOf(Error);
  expect(capture.body).toBeUndefined();
});
