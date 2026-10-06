import { test, expect } from "vitest";
import { makeClient, json, pathOf, type Handler } from "./helpers.js";

const futureIso = (s = 60) => new Date(Date.now() + s * 1000).toISOString();
const pastIso = () => new Date(Date.now() - 1000).toISOString();

function preflightHandler(opts: {
  mode?: string;
  behavior?: string;
  reason?: string | null;
  validUntil?: string | null;
  count?: { n: number };
  rules?: unknown[];
}): Handler {
  return (url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/streaming/preflight") {
      if (opts.count) opts.count.n += 1;
      return json(200, {
        mode: opts.mode ?? "streaming",
        recommended_client_behavior: opts.behavior ?? "stream",
        project_id: "proj_1",
        streaming_mode: "auto",
        reason: opts.reason ?? null,
        reason_detail: opts.reason ?? null,
        valid_until: opts.validUntil ?? null,
        rules: opts.rules ?? [],
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

test("streaming verdict", async () => {
  const collie = makeClient(preflightHandler({ mode: "streaming", behavior: "stream", validUntil: futureIso() }));
  const cap = await collie.streaming.preflight();
  expect(cap.mode).toBe("streaming");
  expect(cap.recommendedClientBehavior).toBe("stream");
});

test("buffered verdict with reason", async () => {
  const collie = makeClient(
    preflightHandler({ mode: "buffered", behavior: "buffer_then_show", reason: "rule_requires_full_context", validUntil: futureIso() }),
  );
  const cap = await collie.streaming.preflight();
  expect(cap.mode).toBe("buffered");
  expect(cap.reason).toBe("rule_requires_full_context");
});

test("unsupported verdict (no validUntil)", async () => {
  const collie = makeClient(preflightHandler({ mode: "unsupported", behavior: "fail_fast", reason: "project_not_found" }));
  const cap = await collie.streaming.preflight();
  expect(cap.mode).toBe("unsupported");
  expect(cap.validUntil).toBeNull();
});

test("rule breakdown maps to camelCase", async () => {
  const collie = makeClient(
    preflightHandler({
      validUntil: futureIso(),
      rules: [{ rule_id: "r1", rule_name: "Block", rule_type: "regex", decision: "block", monitoring: false, streaming_supported: true }],
    }),
  );
  const cap = await collie.streaming.preflight();
  expect(cap.rules[0]?.streamingSupported).toBe(true);
  expect(cap.rules[0]?.ruleName).toBe("Block");
});

test("caches within validity", async () => {
  const count = { n: 0 };
  const collie = makeClient(preflightHandler({ validUntil: futureIso(60), count }));
  await collie.streaming.preflight();
  await collie.streaming.preflight();
  expect(count.n).toBe(1);
});

test("refetches when expired", async () => {
  const count = { n: 0 };
  const collie = makeClient(preflightHandler({ validUntil: pastIso(), count }));
  await collie.streaming.preflight();
  await collie.streaming.preflight();
  expect(count.n).toBe(2);
});

test("forceRefresh bypasses cache", async () => {
  const count = { n: 0 };
  const collie = makeClient(preflightHandler({ validUntil: futureIso(60), count }));
  await collie.streaming.preflight();
  await collie.streaming.preflight({ forceRefresh: true });
  expect(count.n).toBe(2);
});

test("malformed preflight is invalid_response", async () => {
  const collie = makeClient((url, init) =>
    pathOf(url) === "/v1/streaming/preflight" ? json(200, { mode: "weird" }) : json(404, {}),
  );
  await expect(collie.streaming.preflight()).rejects.toMatchObject({ code: "invalid_response" });
});

test("invalid recommended_client_behavior is invalid_response", async () => {
  const collie = makeClient((url) =>
    pathOf(url) === "/v1/streaming/preflight"
      ? json(200, { mode: "streaming", recommended_client_behavior: "nope", project_id: "proj_1", valid_until: futureIso(), rules: [] })
      : json(404, {}),
  );
  await expect(collie.streaming.preflight()).rejects.toMatchObject({ code: "invalid_response" });
});

test("non-string project_id is invalid_response", async () => {
  const collie = makeClient((url) =>
    pathOf(url) === "/v1/streaming/preflight"
      ? json(200, { mode: "streaming", recommended_client_behavior: "stream", project_id: 123, valid_until: futureIso(), rules: [] })
      : json(404, {}),
  );
  await expect(collie.streaming.preflight()).rejects.toMatchObject({ code: "invalid_response" });
});

test("primitive rules entry is invalid_response", async () => {
  const collie = makeClient((url) =>
    pathOf(url) === "/v1/streaming/preflight"
      ? json(200, { mode: "streaming", recommended_client_behavior: "stream", project_id: "proj_1", valid_until: futureIso(), rules: ["oops"] })
      : json(404, {}),
  );
  await expect(collie.streaming.preflight()).rejects.toMatchObject({ code: "invalid_response" });
});

test("unparsable valid_until is invalid_response", async () => {
  const collie = makeClient((url) =>
    pathOf(url) === "/v1/streaming/preflight"
      ? json(200, { mode: "streaming", recommended_client_behavior: "stream", project_id: "proj_1", valid_until: "not-a-date", rules: [] })
      : json(404, {}),
  );
  await expect(collie.streaming.preflight()).rejects.toMatchObject({ code: "invalid_response" });
});

// ---------------------------------------------------------------------------
// Forward compatibility of the preflight wire shape
// ---------------------------------------------------------------------------
// The server adds fields to this response without an SDK release —
// `execution_role` on `rules[]` shipped 2026-07-26 and was modelled here a day
// later. Tolerance here is `toRuleCapability`, a hand-written ALLOWLIST mapper
// that reads only known keys; in the Python SDK it is pydantic's
// `extra="ignore"` and in .NET it is System.Text.Json's default Skip. Three
// different mechanisms, none of them previously pinned, so a zod migration
// here would break only this one and nothing would notice.
//
// Modelling `execution_role` MOVED the frontier rather than closing it: the
// unknown thing is now a fifth ROLE. That is why `ExecutionRole` keeps its
// `(string & {})` arm instead of being a closed union, and why the
// unknown-VALUE case is pinned below next to the unknown-field one.

test("an unknown rule field does not break deserialization", async () => {
  const collie = makeClient(preflightHandler({
    rules: [{
      rule_id: "r1", rule_name: "n", rule_type: "regex",
      decision: "mask", monitoring: true,
      streaming_supported: false, fallback_reason: "monitor_mode",
      execution_role: "stream_observed",          // live server-side today
      some_future_field: { nested: [1, 2, 3] },
    }],
  }));

  const cap = await collie.streaming.preflight();

  expect(cap.rules).toHaveLength(1);
  expect(cap.rules[0].ruleId).toBe("r1");
  expect(cap.rules[0].fallbackReason).toBe("monitor_mode");
  expect(cap.rules[0].executionRole).toBe("stream_observed");
});

test("a role this SDK has never heard of still deserializes", async () => {
  // `ExecutionRole` keeps an open `(string & {})` arm. A fifth server-side role
  // must reach the caller verbatim so it can be handled as unknown.
  const collie = makeClient(preflightHandler({
    rules: [{
      rule_id: "r1", rule_name: "n", rule_type: "regex",
      decision: "mask", monitoring: true,
      streaming_supported: false, fallback_reason: null,
      execution_role: "enforce_midflight_v2",
    }],
  }));

  const cap = await collie.streaming.preflight();

  expect(cap.rules[0].executionRole).toBe("enforce_midflight_v2");
});

test("a rule from an old server has no role either", async () => {
  // Round 3, T-04. The ABSENT-field case — an old server that has never
  // heard of execution_role — had no assertion anywhere: dropping the
  // `?? null` in toRuleCapability yields `undefined`, tsc still passes
  // (the field is optional), and callers doing `role === null` silently
  // stop matching. Absence and explicit null must be the same answer.
  const collie = makeClient(preflightHandler({
    rules: [{
      rule_id: "r1", rule_name: "n", rule_type: "regex",
      decision: "mask", monitoring: true,
      streaming_supported: false, fallback_reason: "monitor_mode",
      // no execution_role key at all
    }],
  }));

  const cap = await collie.streaming.preflight();

  expect(cap.rules[0].executionRole).toBeNull();
});

test("an unplannable rule reports no role", async () => {
  // Null is a real answer, not a missing one: the server sends it for a rule it
  // could not plan at all. The allowlist mapper must not turn it into "".
  const collie = makeClient(preflightHandler({
    rules: [{
      rule_id: "r1", rule_name: "n", rule_type: "who_knows",
      decision: "mask", monitoring: false,
      streaming_supported: false, fallback_reason: "unknown_rule_type",
      execution_role: null,
    }],
  }));

  const cap = await collie.streaming.preflight();

  expect(cap.rules[0].executionRole).toBeNull();
});

test("an unknown top-level field does not break it either", async () => {
  const collie = makeClient((url, init) => {
    if ((init.method ?? "GET") === "POST" && pathOf(url) === "/v1/streaming/preflight") {
      return json(200, {
        mode: "streaming", recommended_client_behavior: "stream",
        project_id: "proj_1", streaming_mode: "auto",
        reason: null, reason_detail: null, valid_until: null,
        rules: [],
        observer_summary: { stream_observed: 2 },
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  });

  expect((await collie.streaming.preflight()).mode).toBe("streaming");
});
