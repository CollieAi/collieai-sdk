import { test, expect } from "vitest";
import {
  BufferedFallbackRequired,
  CollieApiError,
  MaskedInputError,
  PlanNotEntitled,
  ProviderStreamFactoryRequired,
} from "../src/errors.js";
import type { InputBlocked, SafeDelta, StreamEvent } from "../src/types.js";
import { makeClient, json, errorJson, bodyOf, pathOf, collect, type Handler } from "./helpers.js";

const futureIso = () => new Date(Date.now() + 60_000).toISOString();

interface PsOpts {
  inputBlocked?: boolean;
  maskTo?: string;
  blockOutput?: boolean;
  chunkFail?: () => Response;
  chunkFailTimes?: number;
  capability?: Record<string, unknown>;
  countPreflight?: { n: number };
  contextResult?: unknown;
  blockedBy?: string;
  /** Records every POST /v1/jobs body for assertions. */
  createdJobs?: Array<Record<string, unknown>>;
}

function psBackend(opts: PsOpts = {}): Handler {
  let calls = 0;
  return (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/streaming/preflight") {
      if (opts.countPreflight) opts.countPreflight.n += 1;
      return json(
        200,
        opts.capability ?? {
          mode: "streaming",
          recommended_client_behavior: "stream",
          project_id: "proj_1",
          valid_until: futureIso(),
          rules: [],
        },
      );
    }
    if (method === "POST" && path === "/v1/jobs") {
      const body = bodyOf(init);
      opts.createdJobs?.push(body);
      return json(202, { job_id: body.inbound_only ? "job_mod" : "job_stream" });
    }
    if (method === "GET" && path === "/v1/jobs/job_mod") {
      const payload: Record<string, unknown> = {
        status: opts.inputBlocked ? "inbound_blocked" : "completed",
        inbound_result: {
          allowed: !opts.inputBlocked,
          blocked: !!opts.inputBlocked,
          block_message: opts.inputBlocked ? "input blocked" : null,
          triggered_rules: [],
        },
      };
      if (opts.contextResult !== undefined) payload.context_result = opts.contextResult;
      if (opts.blockedBy !== undefined) payload.blocked_by = opts.blockedBy;
      return json(200, payload);
    }
    if (path === "/v1/jobs/job_stream/chunks") {
      calls += 1;
      const b = bodyOf(init);
      if (opts.chunkFail && calls <= (opts.chunkFailTimes ?? 0)) return opts.chunkFail();
      const content = (b.content as string) ?? "";
      const isFinal = Boolean(b.is_final);
      if (opts.blockOutput && content) {
        return json(200, {
          sequence: b.sequence,
          accepted: true,
          emits: [{ content: "", blocked: true, final: true,
                    block_message: "Custom rule block message", triggered_rules: [] }],
          finished: true,
        });
      }
      const out = opts.maskTo != null && content ? opts.maskTo : content;
      const emits = content || isFinal ? [{ content: out, blocked: false, final: isFinal, triggered_rules: [] }] : [];
      return json(200, { sequence: b.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

function factoryOf(deltas: string[], state?: { calls: number }) {
  return () =>
    (async function* () {
      if (state) state.calls += 1;
      for (const d of deltas) yield d;
    })();
}

const texts = (events: StreamEvent[]) =>
  events.filter((e): e is SafeDelta => e.type === "delta").map((e) => e.text);

test("input block skips the factory and yields input_blocked", async () => {
  const state = { calls: 0 };
  const collie = makeClient(psBackend({ inputBlocked: true }));
  const events = await collect(
    collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["x"], state) }),
  );
  expect(state.calls).toBe(0);
  expect(events).toHaveLength(1);
  expect(events[0]?.type).toBe("input_blocked");
});

test("success invokes factory and yields safe deltas", async () => {
  const state = { calls: 0 };
  const collie = makeClient(psBackend());
  const events = await collect(
    collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["hello world"], state) }),
  );
  expect(state.calls).toBe(1);
  expect(texts(events).join("")).toBe("hello world");
  expect(events[events.length - 1]?.type).toBe("finished");
});

test("raw provider chunks are never yielded (masking)", async () => {
  const collie = makeClient(psBackend({ maskTo: "SAFE" }));
  const events = await collect(
    collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["RAWTEXT"]) }),
  );
  expect(texts(events)).toEqual(["SAFE"]);
});

test("timeout retry does not duplicate deltas", async () => {
  const collie = makeClient(psBackend({ chunkFail: () => errorJson(504, "chunk_filter_timeout"), chunkFailTimes: 1 }));
  const events = await collect(
    collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["hello"]) }),
  );
  expect(texts(events)).toEqual(["hello"]);
});

test("factory called exactly once across retries", async () => {
  const state = { calls: 0 };
  const collie = makeClient(psBackend({ chunkFail: () => errorJson(503, "chunk_persistence_unavailable"), chunkFailTimes: 1 }));
  await collect(collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["a", "b"], state) }));
  expect(state.calls).toBe(1);
});

test("blocked output yields a terminal blocked event", async () => {
  const collie = makeClient(psBackend({ blockOutput: true }));
  const events = await collect(
    collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["secret"]) }),
  );
  const last = events[events.length - 1];
  expect(last?.type).toBe("blocked");
  // F1 parity with Python: the rule's configured message reaches the event.
  expect(last && "blockMessage" in last ? last.blockMessage : null).toBe(
    "Custom rule block message",
  );
  expect(events.some((e) => e.type === "finished")).toBe(false);
});

test("non-function factory throws ProviderStreamFactoryRequired synchronously", () => {
  const collie = makeClient(psBackend());
  expect(() =>
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: (async function* () {})() as never,
    }),
  ).toThrow(ProviderStreamFactoryRequired);
});

test("factory returning a non-async-iterable raises", async () => {
  const collie = makeClient(psBackend());
  await expect(
    collect(
      collie.streaming.protectStream({
        input: "hi",
        checkInput: false,
        rawStreamFactory: (() => ["not async"]) as never,
      }),
    ),
  ).rejects.toBeInstanceOf(ProviderStreamFactoryRequired);
});

test("session create carries the gate's job id as input_job_id", async () => {
  // The duplicate-inbound contract: the session job proves its
  // prompt was gated instead of paying a second inbound pass. message_input
  // stays — the chunk path's guard prompt context reads it.
  const createdJobs: Array<Record<string, unknown>> = [];
  const collie = makeClient(psBackend({ createdJobs }));
  await collect(
    collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["ok"]) }),
  );
  const sessionBodies = createdJobs.filter((b) => !b.inbound_only);
  expect(sessionBodies).toHaveLength(1);
  expect(sessionBodies[0]?.input_job_id).toBe("job_mod");
  expect(sessionBodies[0]?.message_input).toBe("hi");
});

test("no gate reference without an input check", async () => {
  // checkInput=false ran no gate, so there is no proof to send.
  const createdJobs: Array<Record<string, unknown>> = [];
  const collie = makeClient(psBackend({ createdJobs }));
  await collect(
    collie.streaming.protectStream({
      input: "hi",
      checkInput: false,
      rawStreamFactory: factoryOf(["ok"]),
    }),
  );
  const sessionBodies = createdJobs.filter((b) => !b.inbound_only);
  expect(sessionBodies).toHaveLength(1);
  expect("input_job_id" in sessionBodies[0]!).toBe(false);
});

test("precomputed inputResult supplies its own job id", async () => {
  const createdJobs: Array<Record<string, unknown>> = [];
  const collie = makeClient(psBackend({ createdJobs }));
  await collect(
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["ok"]),
      inputResult: {
        allowed: true, blocked: false, originalText: "hi",
        triggeredRules: [], jobId: "job_pre",
      },
    }),
  );
  const sessionBodies = createdJobs.filter((b) => !b.inbound_only);
  expect(sessionBodies[0]?.input_job_id).toBe("job_pre");
});

test("precomputed inputResult without a job id omits the reference", async () => {
  const createdJobs: Array<Record<string, unknown>> = [];
  const collie = makeClient(psBackend({ createdJobs }));
  await collect(
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["ok"]),
      inputResult: { allowed: true, blocked: false, originalText: "hi", triggeredRules: [] },
    }),
  );
  const sessionBodies = createdJobs.filter((b) => !b.inbound_only);
  expect("input_job_id" in sessionBodies[0]!).toBe(false);
});

// ---------------------------------------------------------------------------
// The 409 input_gate_stale protocol (server claim contract §10.2)
// ---------------------------------------------------------------------------
function staleBackend(opts: {
  staleTimes: number;
  blockOnRegate?: boolean;
  code?: string;
  createdJobs: Array<Record<string, unknown>>;
  counters: { modCalls: number; staleServed: number };
}): Handler {
  const code = opts.code ?? "input_gate_stale";
  return (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const body = bodyOf(init);
      opts.createdJobs.push(body);
      if (body.inbound_only) {
        opts.counters.modCalls += 1;
        return json(202, { job_id: `job_mod${opts.counters.modCalls}` });
      }
      if (body.input_job_id && opts.counters.staleServed < opts.staleTimes) {
        opts.counters.staleServed += 1;
        return json(409, { error: { message: "claim refused", type: code, code } });
      }
      return json(202, { job_id: "job_stream" });
    }
    if (method === "GET" && path.startsWith("/v1/jobs/job_mod")) {
      const blocked = Boolean(opts.blockOnRegate) && path.endsWith("job_mod2");
      return json(200, {
        status: blocked ? "inbound_blocked" : "completed",
        inbound_result: {
          allowed: !blocked, blocked,
          block_message: blocked ? "blocked by the CURRENT policy" : null,
          triggered_rules: [],
        },
      });
    }
    if (path === "/v1/jobs/job_stream/chunks") {
      const b = bodyOf(init);
      const isFinal = Boolean(b.is_final);
      const content = (b.content as string) ?? "";
      const emits = content || isFinal
        ? [{ content, blocked: false, final: isFinal, triggered_rules: [] }]
        : [];
      return json(200, { sequence: b.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

test("stale gate re-gates once before the provider", async () => {
  const createdJobs: Array<Record<string, unknown>> = [];
  const counters = { modCalls: 0, staleServed: 0 };
  const collie = makeClient(staleBackend({ staleTimes: 1, createdJobs, counters }));
  const factoryState = { calls: 0 };

  const events = await collect(
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["ok"], factoryState),
    }),
  );

  expect(counters.modCalls).toBe(2); // original gate + exactly one re-gate
  const sessionBodies = createdJobs.filter((b) => !b.inbound_only);
  expect(sessionBodies.map((b) => b.input_job_id)).toEqual(["job_mod1", "job_mod2"]);
  expect(texts(events).join("")).toBe("ok");
  expect(factoryState.calls).toBe(1);
});

test("a second stale raises with zero provider calls", async () => {
  // Round 5: a stale on the RETRIED create is POSITIVE PROOF of policy
  // churn — the round-4 claimless downgrade would have raced the provider
  // against a proven drift. Typed error, provider never started.
  const createdJobs: Array<Record<string, unknown>> = [];
  const counters = { modCalls: 0, staleServed: 0 };
  const collie = makeClient(staleBackend({ staleTimes: 2, createdJobs, counters }));
  const factoryState = { calls: 0 };

  await expect(
    collect(collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["never"], factoryState),
    })),
  ).rejects.toMatchObject({ code: "input_gate_stale" });

  expect(counters.modCalls).toBe(2); // exactly one re-gate, no loop
  expect(factoryState.calls).toBe(0); // provider NEVER started
});

test("stale then unverifiable downgrades to claimless (outage carve-out)", async () => {
  const codes = ["input_gate_stale", "input_gate_unverifiable"];
  let served = 0;
  const createdJobs: Array<Record<string, unknown>> = [];
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const body = bodyOf(init);
      createdJobs.push(body);
      if (body.inbound_only) {
        return json(202, { job_id: `job_mod${createdJobs.filter((b) => b.inbound_only).length}` });
      }
      if (body.input_job_id && served < codes.length) {
        const code = codes[served++];
        return json(409, { error: { message: "refused", type: code, code } });
      }
      return json(202, { job_id: "job_stream" });
    }
    if (method === "GET" && path.startsWith("/v1/jobs/job_mod")) {
      return json(200, {
        status: "completed",
        inbound_result: { allowed: true, blocked: false, triggered_rules: [] },
      });
    }
    if (path === "/v1/jobs/job_stream/chunks") {
      const b = bodyOf(init);
      const isFinal = Boolean(b.is_final);
      const content = (b.content as string) ?? "";
      const emits = content || isFinal
        ? [{ content, blocked: false, final: isFinal, triggered_rules: [] }]
        : [];
      return json(200, { sequence: b.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const events = await collect(
    collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["ok"]) }),
  );
  const sessionBodies = createdJobs.filter((b) => !b.inbound_only);
  expect(sessionBodies.map((b) => b.input_job_id ?? null)).toEqual([
    "job_mod1", "job_mod2", null,
  ]);
  expect(texts(events).join("")).toBe("ok");
});

function maskingBackend(filteredText: string, createdJobs: Array<Record<string, unknown>>): Handler {
  return (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const body = bodyOf(init);
      createdJobs.push(body);
      return json(202, { job_id: body.inbound_only ? "job_mod" : "job_stream" });
    }
    if (method === "GET" && path === "/v1/jobs/job_mod") {
      return json(200, {
        status: "completed",
        inbound_result: {
          allowed: true, blocked: false,
          filtered_content: filteredText, triggered_rules: [],
        },
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

for (const filtered of ["my [MASKED] prompt", ""]) {
  test(`masked input fails closed before the provider (filtered=${JSON.stringify(filtered)})`, async () => {
    // Round 5: the wrapper's own gate MASKED the prompt but the factory
    // closes over the ORIGINAL — fail closed, zero provider calls, no
    // session created. The "" row is the full wipe.
    const createdJobs: Array<Record<string, unknown>> = [];
    const collie = makeClient(maskingBackend(filtered, createdJobs));
    const factoryState = { calls: 0 };

    await expect(
      collect(collie.streaming.protectStream({
        input: "my secret prompt",
        rawStreamFactory: factoryOf(["never"], factoryState),
      })),
    ).rejects.toBeInstanceOf(MaskedInputError);

    expect(factoryState.calls).toBe(0);
    expect(createdJobs.filter((b) => !b.inbound_only)).toHaveLength(0);
  });
}

test("masked input fails closed in protectBuffered too", async () => {
  const createdJobs: Array<Record<string, unknown>> = [];
  const collie = makeClient(maskingBackend("my [MASKED] prompt", createdJobs));
  const factoryState = { calls: 0 };
  await expect(
    collie.streaming.protectBuffered({
      input: "my secret prompt",
      rawStreamFactory: factoryOf(["never"], factoryState),
    }),
  ).rejects.toBeInstanceOf(MaskedInputError);
  expect(factoryState.calls).toBe(0);
});

for (const filtered of ["my [MASKED] prompt", ""]) {
  test(`re-gate mask fails closed before the provider (filtered=${JSON.stringify(filtered)})`, async () => {
    // Round 6: the mask check must hold on the LADDER's re-gate too.
    let modCalls = 0;
    let staleServed = 0;
    const handler: Handler = (url, init) => {
      const path = pathOf(url);
      const method = init.method ?? "GET";
      if (method === "POST" && path === "/v1/jobs") {
        const body = bodyOf(init);
        if (body.inbound_only) {
          modCalls += 1;
          return json(202, { job_id: `job_mod${modCalls}` });
        }
        if (body.input_job_id && staleServed < 1) {
          staleServed += 1;
          return json(409, { error: {
            message: "stale", type: "input_gate_stale", code: "input_gate_stale",
          }});
        }
        return json(202, { job_id: "job_stream" });
      }
      if (method === "GET" && path.startsWith("/v1/jobs/job_mod")) {
        const masked = path.endsWith("job_mod2");
        const inbound: Record<string, unknown> = {
          allowed: true, blocked: false, triggered_rules: [],
        };
        if (masked) inbound.filtered_content = filtered;
        return json(200, { status: "completed", inbound_result: inbound });
      }
      return json(404, { error: { message: "nf", type: "x" } });
    };
    const collie = makeClient(handler);
    const factoryState = { calls: 0 };

    await expect(
      collect(collie.streaming.protectStream({
        input: "my secret prompt",
        rawStreamFactory: factoryOf(["never"], factoryState),
      })),
    ).rejects.toBeInstanceOf(MaskedInputError);

    expect(modCalls).toBe(2);
    expect(factoryState.calls).toBe(0);
  });

  test(`the masking recipe streams the filtered prompt (filtered=${JSON.stringify(filtered)})`, async () => {
    // The POSITIVE half of the recipe: gate manually, build the factory
    // over filteredText (null-safe — "" is a legitimate full wipe), pass
    // inputResult. The captured prompt PROVES the model gets the
    // filtered text.
    const collie = makeClient(psBackend({}));
    const original = "my secret prompt";
    const promptForModel = filtered ?? original;
    const usedPrompts: string[] = [];

    const factory = () =>
      (async function* () {
        usedPrompts.push(promptForModel);
        yield "ok";
      })();

    const events = await collect(
      collie.streaming.protectStream({
        input: original,
        rawStreamFactory: factory,
        inputResult: {
          allowed: true, blocked: false, originalText: original,
          filteredText: filtered, triggeredRules: [], jobId: "job_pre",
        },
      }),
    );

    expect(texts(events).join("")).toBe("ok");
    expect(usedPrompts).toEqual([filtered]); // the model saw the FILTERED prompt
    expect(usedPrompts).not.toContain(original);
  });
}

test("the inputResult path is exempt from the mask fail-closed", async () => {
  // The caller demonstrably holds filteredText and owns the factory.
  const collie = makeClient(psBackend({}));
  const events = await collect(
    collie.streaming.protectStream({
      input: "my secret prompt",
      rawStreamFactory: factoryOf(["ok"]),
      inputResult: {
        allowed: true, blocked: false, originalText: "my secret prompt",
        filteredText: "my [MASKED] prompt", triggeredRules: [], jobId: "job_pre",
      },
    }),
  );
  expect(texts(events).join("")).toBe("ok");
});

test("unverifiable downgrades immediately without a re-gate", async () => {
  // Round 4: a pin outage makes re-gating pointless (a fresh gate is
  // just as unpinned) — go straight to the claimless session.
  const createdJobs: Array<Record<string, unknown>> = [];
  const counters = { modCalls: 0, staleServed: 0 };
  const collie = makeClient(staleBackend({
    staleTimes: 1, code: "input_gate_unverifiable", createdJobs, counters,
  }));
  const factoryState = { calls: 0 };

  const events = await collect(
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["ok"], factoryState),
    }),
  );

  expect(counters.modCalls).toBe(1); // the original gate only — NO re-gate
  const sessionBodies = createdJobs.filter((b) => !b.inbound_only);
  expect(sessionBodies.map((b) => b.input_job_id ?? null)).toEqual(["job_mod1", null]);
  expect(texts(events).join("")).toBe("ok");
  expect(factoryState.calls).toBe(1);
});

test("a re-gate block yields input_blocked without provider spend", async () => {
  const createdJobs: Array<Record<string, unknown>> = [];
  const counters = { modCalls: 0, staleServed: 0 };
  const collie = makeClient(
    staleBackend({ staleTimes: 1, blockOnRegate: true, createdJobs, counters }),
  );
  const factoryState = { calls: 0 };

  const events = await collect(
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["never"], factoryState),
    }),
  );

  expect(events).toHaveLength(1);
  const blocked = events[0] as InputBlocked;
  expect(blocked.type).toBe("input_blocked");
  expect(blocked.blockMessage).toBe("blocked by the CURRENT policy");
  expect(factoryState.calls).toBe(0);
});

for (const code of ["input_gate_stale", "input_gate_unverifiable"]) {
  test(`${code} with an external inputResult raises — no auto remedy`, async () => {
    // Rounds 3–4: an external inputResult may carry a verdict produced
    // WITH a context this wrapper never saw; neither a prompt-only
    // re-gate nor a silent claimless downgrade is honest. The caller
    // re-gates with its own context.
    const createdJobs: Array<Record<string, unknown>> = [];
    const counters = { modCalls: 0, staleServed: 0 };
    const collie = makeClient(staleBackend({ staleTimes: 1, code, createdJobs, counters }));
    const factoryState = { calls: 0 };

    await expect(
      collect(collie.streaming.protectStream({
        input: "hi",
        rawStreamFactory: factoryOf(["never"], factoryState),
        inputResult: {
          allowed: true, blocked: false, originalText: "hi",
          triggeredRules: [], jobId: "job_pre",
        },
      })),
    ).rejects.toMatchObject({ code });

    expect(counters.modCalls).toBe(0); // NO automatic re-gate
    expect(factoryState.calls).toBe(0); // provider never started
  });
}

test("input_gate_claimed propagates untouched", async () => {
  // The counters pin "untouched": adding the code to the refusal ladder
  // would run a billed re-gate + a second create before the same
  // exception, and only the counts catch that (the post-round-10 workflow
  // review found the code-only assert survived exactly that mutation).
  const counts = { gateCreates: 0, sessionCreates: 0 };
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      if (bodyOf(init).inbound_only) {
        counts.gateCreates += 1;
        return json(202, { job_id: "job_mod" });
      }
      counts.sessionCreates += 1;
      return json(409, { error: {
        message: "already claimed", type: "input_gate_claimed", code: "input_gate_claimed",
      }});
    }
    if (method === "GET" && path === "/v1/jobs/job_mod") {
      return json(200, {
        status: "completed",
        inbound_result: { allowed: true, blocked: false, triggered_rules: [] },
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  const factoryState = { calls: 0 };
  await expect(
    collect(collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["never"], factoryState),
    })),
  ).rejects.toMatchObject({ code: "input_gate_claimed" });
  expect(counts.gateCreates).toBe(1); // the wrapper's own gate — no re-gate
  expect(counts.sessionCreates).toBe(1); // no second create attempt
  expect(factoryState.calls).toBe(0); // provider never started
});

test("inputResult mismatch throws synchronously", () => {
  const collie = makeClient(psBackend());
  expect(() =>
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["x"]),
      inputResult: { allowed: true, blocked: false, originalText: "DIFFERENT", triggeredRules: [] },
    }),
  ).toThrow(TypeError);
});

test("inputResult with checkInput: false is rejected before any side effect in both wrappers", async () => {
  // Round 10: this combination used to be silently ignored (in
  // protectStream the claim was dropped and the session re-filtered; in
  // protectBuffered the input pass was simply skipped) — exactly the shape
  // a checkInput: false integration would produce by accident. Now loud,
  // with zero provider calls and zero jobs — the result carries a REAL
  // jobId (the reported shape: a claim that would have been silently lost).
  const createdJobs: Array<Record<string, unknown>> = [];
  const collie = makeClient(psBackend({ createdJobs }));
  const state = { calls: 0 };
  const ir = {
    allowed: true, blocked: false, originalText: "hi",
    triggeredRules: [], jobId: "job_gate",
  };
  expect(() =>
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["x"], state),
      inputResult: ir,
      checkInput: false,
    }),
  ).toThrow(/checkInput to be true/);
  await expect(
    collie.streaming.protectBuffered({
      input: "hi",
      rawStreamFactory: factoryOf(["x"], state),
      inputResult: ir,
      checkInput: false,
    }),
  ).rejects.toThrow(/checkInput to be true/);
  expect(state.calls).toBe(0);
  expect(createdJobs).toEqual([]);
});

test("protectBuffered external result is a trusted-client reuse — no inbound job, no claim", async () => {
  // The positive wire pin of the buffered trusted-client contract (round
  // 10): an external allowed result — carrying a REAL jobId — skips the
  // input pass entirely and claims nothing. Provider runs once, the ONLY
  // job created is the message_output one, and input_job_id is sent
  // nowhere (the absence that lets the server neither verify nor consume
  // the result).
  const createdJobs: Array<Record<string, unknown>> = [];
  const handler: Handler = (url, init) => {
    const method = init.method ?? "GET";
    const path = pathOf(url);
    if (method === "POST" && path === "/v1/jobs") {
      createdJobs.push(bodyOf(init));
      return json(202, { job_id: "job_buf", status: "processing_outbound" });
    }
    if (method === "GET" && path === "/v1/jobs/job_buf") {
      return json(200, {
        job_id: "job_buf",
        status: "completed",
        outbound_result: { allowed: true, blocked: false, filtered_content: "ok", triggered_rules: [] },
      });
    }
    return json(404, { error: { message: "nf", type: "not_found" } });
  };
  const collie = makeClient(handler);
  const state = { calls: 0 };
  const result = await collie.streaming.protectBuffered({
    input: "hi",
    rawStreamFactory: factoryOf(["ok"], state),
    inputResult: {
      allowed: true, blocked: false, originalText: "hi",
      triggeredRules: [], jobId: "job_gate",
    },
  });
  expect(result.blocked).toBe(false);
  expect(state.calls).toBe(1);
  expect(createdJobs).toHaveLength(1); // no inbound moderation job
  expect(createdJobs[0].message_output).toBe("ok");
  expect(createdJobs[0].inbound_only).toBeUndefined();
  expect(JSON.stringify(createdJobs)).not.toContain("input_job_id");
});

test("protectStream forwards context to the input gate", async () => {
  let captured: Record<string, unknown> | undefined;
  const handler: Handler = (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const b = bodyOf(init);
      if (b.inbound_only) {
        captured = b;
        return json(202, { job_id: "job_mod" });
      }
      return json(202, { job_id: "job_stream" });
    }
    if (method === "GET" && path === "/v1/jobs/job_mod") {
      return json(200, {
        status: "completed",
        inbound_result: { allowed: true, blocked: false, triggered_rules: [] },
      });
    }
    if (path === "/v1/jobs/job_stream/chunks") {
      const b = bodyOf(init);
      const content = (b.content as string) ?? "";
      const isFinal = Boolean(b.is_final);
      const emits =
        content || isFinal ? [{ content, blocked: false, final: isFinal, triggered_rules: [] }] : [];
      return json(200, { sequence: b.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
  const collie = makeClient(handler);
  await collect(
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["ok"]),
      context: { transaction: { title: "x" } },
      contextFormat: "json",
    }),
  );
  expect(captured?.context).toEqual({ transaction: { title: "x" } });
  expect(captured?.context_format).toBe("json");
});

test("protectStream rejects context combined with inputResult", () => {
  const collie = makeClient(psBackend());
  expect(() =>
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["x"]),
      inputResult: { allowed: true, blocked: false, originalText: "hi", triggeredRules: [] },
      context: { a: "b" },
    }),
  ).toThrow(TypeError);
});

test("protectBuffered rejects context combined with inputResult", async () => {
  const collie = makeClient(psBackend());
  await expect(
    collie.streaming.protectBuffered({
      input: "hi",
      rawStreamFactory: factoryOf(["x"]),
      inputResult: { allowed: true, blocked: false, originalText: "hi", triggeredRules: [] },
      context: { a: "b" },
    }),
  ).rejects.toBeInstanceOf(TypeError);
});

test("requireStreaming + buffered policy raises BufferedFallbackRequired before factory", async () => {
  const state = { calls: 0 };
  const collie = makeClient(
    psBackend({
      capability: {
        mode: "buffered",
        recommended_client_behavior: "buffer_then_show",
        project_id: "proj_1",
        reason: "preset_buffered",
        reason_detail: "buffered",
        valid_until: futureIso(),
        rules: [],
      },
    }),
  );
  await expect(
    collect(
      collie.streaming.protectStream({
        input: "hi",
        rawStreamFactory: factoryOf(["x"], state),
        requireStreaming: true,
      }),
    ),
  ).rejects.toBeInstanceOf(BufferedFallbackRequired);
  expect(state.calls).toBe(0);
});

test("requireStreaming + unsupported raises a PreflightError", async () => {
  const collie = makeClient(
    psBackend({
      capability: {
        mode: "unsupported",
        recommended_client_behavior: "fail_fast",
        project_id: "proj_1",
        reason: "plan_not_entitled",
        reason_detail: "no entitlement",
        valid_until: null,
        rules: [],
      },
    }),
  );
  await expect(
    collect(
      collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["x"]), requireStreaming: true }),
    ),
  ).rejects.toBeInstanceOf(PlanNotEntitled);
});

function chunkRecordingBackend(chunks: Record<string, unknown>[]): Handler {
  return (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      return json(202, { job_id: bodyOf(init).inbound_only ? "job_mod" : "job_stream" });
    }
    if (path === "/v1/jobs/job_mod") {
      return json(200, { status: "completed", inbound_result: { allowed: true, blocked: false, triggered_rules: [] } });
    }
    if (path === "/v1/jobs/job_stream/chunks") {
      const b = bodyOf(init);
      chunks.push(b);
      const content = (b.content as string) ?? "";
      const isFinal = Boolean(b.is_final);
      const emits = content || isFinal ? [{ content, blocked: false, final: isFinal, triggered_rules: [] }] : [];
      return json(200, { sequence: b.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

test("factory failure does not fake-finalize the job", async () => {
  // A job that never streamed a chunk must NOT get a spurious empty is_final
  // (which would read as "completed empty" in the audit log). Left to expire.
  const chunks: Record<string, unknown>[] = [];
  const collie = makeClient(chunkRecordingBackend(chunks));
  const factory = () => {
    throw new Error("provider open failed");
  };
  await expect(
    collect(collie.streaming.protectStream({ input: "hi", rawStreamFactory: factory })),
  ).rejects.toThrow("provider open failed");
  expect(chunks).toHaveLength(0);
});

test("mid-stream provider error finalizes after an accepted chunk", async () => {
  const chunks: Record<string, unknown>[] = [];
  const collie = makeClient(chunkRecordingBackend(chunks));
  const factory = () =>
    (async function* () {
      yield "hello";
      throw new Error("upstream blew up");
    })();
  await expect(
    collect(collie.streaming.protectStream({ input: "hi", rawStreamFactory: factory, maxDeltas: 1 })),
  ).rejects.toThrow("upstream blew up");
  expect(chunks.some((c) => !c.is_final)).toBe(true); // the "hello" chunk landed
  expect(chunks.some((c) => c.is_final)).toBe(true); // then it was finalized
});

/** A provider whose [asyncIterator]() returns a SEPARATE object, and which
 * exposes aclose() — so the iterator and the provider object must BOTH be torn
 * down (the Python parity case). */
function distinctProvider(deltas: string[]) {
  const state = { providerClosed: false, iteratorClosed: false };
  const provider = {
    [Symbol.asyncIterator]() {
      let i = 0;
      return {
        async next() {
          return i < deltas.length ? { value: deltas[i++]!, done: false } : { value: undefined, done: true };
        },
        async return() {
          state.iteratorClosed = true;
          return { value: undefined, done: true };
        },
      };
    },
    async aclose() {
      state.providerClosed = true;
    },
  };
  return { provider, state };
}

test("protectStream closes a distinct provider object and its iterator", async () => {
  const { provider, state } = distinctProvider(["hello"]);
  const collie = makeClient(psBackend());
  await collect(collie.streaming.protectStream({ input: "hi", rawStreamFactory: () => provider }));
  expect(state.iteratorClosed).toBe(true);
  expect(state.providerClosed).toBe(true);
});

test("protectStream aborts the provider signal on teardown", async () => {
  let aborted = false;
  const factory = (signal?: AbortSignal) => {
    signal?.addEventListener("abort", () => {
      aborted = true;
    });
    return (async function* () {
      yield "hello";
    })();
  };
  const collie = makeClient(psBackend());
  await collect(collie.streaming.protectStream({ input: "hi", rawStreamFactory: factory }));
  expect(aborted).toBe(true);
});

function bufferedBackend(opts: { contextResult?: unknown; blockedBy?: string } = {}): Handler {
  return (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      return json(202, { job_id: bodyOf(init).inbound_only ? "job_mod" : "job_buf" });
    }
    if (path === "/v1/jobs/job_mod") {
      const payload: Record<string, unknown> = { status: "completed", inbound_result: { allowed: true, blocked: false, triggered_rules: [] } };
      if (opts.contextResult !== undefined) payload.context_result = opts.contextResult;
      if (opts.blockedBy !== undefined) payload.blocked_by = opts.blockedBy;
      return json(200, payload);
    }
    if (path === "/v1/jobs/job_buf") {
      // The real server always sends BOTH allowed and blocked (jobs.py
      // _build_filtering_result); the buffered parser now requires the
      // explicit allow (tech-debt #22).
      return json(200, { status: "completed", outbound_result: { allowed: true, blocked: false, filtered_content: "hello", triggered_rules: [] } });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

// The v24 fail-safe table on the buffered path (tech-debt #22): NOT blocked
// requires an EXPLICIT allowed === true and no hard-block signal; ambiguity
// resolves to blocked — same rows as moderate.output's table, mirrored per-SDK.
const bufferedVerdictRows: Array<{
  name: string;
  status: string;
  outbound: Record<string, unknown>;
  expectBlocked: boolean;
}> = [
  // The #22 fix itself: an EMPTY outbound_result used to fail-OPEN.
  { name: "empty object", status: "completed", outbound: {}, expectBlocked: true },
  { name: "absent allowed", status: "completed", outbound: { blocked: false }, expectBlocked: true },
  { name: "explicit deny", status: "completed", outbound: { allowed: false }, expectBlocked: true },
  { name: "explicit allow", status: "completed", outbound: { allowed: true, blocked: false }, expectBlocked: false },
  { name: "inconsistent", status: "completed", outbound: { allowed: true, blocked: true }, expectBlocked: true },
  { name: "status wins", status: "outbound_blocked", outbound: { allowed: true, blocked: false }, expectBlocked: true },
  { name: "non-bool allowed", status: "completed", outbound: { allowed: "yes", blocked: false }, expectBlocked: true },
];

for (const row of bufferedVerdictRows) {
  test(`buffered verdict table is fail-safe: ${row.name}`, async () => {
    const handler: Handler = (url, init) => {
      const path = pathOf(url);
      if ((init.method ?? "GET") === "POST" && path === "/v1/jobs") {
        return json(202, { job_id: "job_buf" });
      }
      if (path === "/v1/jobs/job_buf") {
        return json(200, { status: row.status, outbound_result: row.outbound });
      }
      return json(404, { error: { message: "nf", type: "x" } });
    };
    const collie = makeClient(handler);
    const result = await collie.streaming.protectBuffered({
      input: "hi",
      checkInput: false,
      rawStreamFactory: factoryOf(["text"]),
    });
    expect(result.blocked).toBe(row.expectBlocked);
  });
}

test("protectBuffered closes a distinct provider object and its iterator", async () => {
  const { provider, state } = distinctProvider(["hello"]);
  const collie = makeClient(bufferedBackend());
  const result = await collie.streaming.protectBuffered({ input: "hi", rawStreamFactory: () => provider });
  expect(result.filteredText).toBe("hello");
  expect(state.iteratorClosed).toBe(true);
  expect(state.providerClosed).toBe(true);
});

test("default (no requireStreaming) does not preflight", async () => {
  const countPreflight = { n: 0 };
  const collie = makeClient(psBackend({ countPreflight }));
  await collect(collie.streaming.protectStream({ input: "hi", rawStreamFactory: factoryOf(["x"]) }));
  expect(countPreflight.n).toBe(0);
});

// ---------------------------------------------------------------------------
// Context verdict carried on the wrapper input-block paths (Slice 1f review)
// ---------------------------------------------------------------------------

function ctxInputBlockHandler(): Handler {
  return (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const b = bodyOf(init);
      return json(202, { job_id: b.inbound_only ? "job_mod" : "job_stream" });
    }
    if (method === "GET" && path === "/v1/jobs/job_mod") {
      return json(200, {
        status: "inbound_blocked",
        inbound_result: { allowed: false, blocked: true, block_message: "ctx", triggered_rules: [] },
        blocked_by: "context",
        context_result: {
          status: "blocked", blocked: true,
          triggering_pointer: "/transaction/title", triggering_rule_type: "lightweight_model",
        },
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  };
}

test("protectStream input block carries the context verdict", async () => {
  const collie = makeClient(ctxInputBlockHandler());
  const events = await collect(
    collie.streaming.protectStream({
      input: "hi", rawStreamFactory: factoryOf(["x"]),
      context: { transaction: { title: "y" } },
    }),
  );
  expect(events.length).toBe(1);
  const ev = events[0] as InputBlocked;
  expect(ev.type).toBe("input_blocked");
  expect(ev.blockedBy).toBe("context");
  expect(ev.context?.status).toBe("blocked");
  expect(ev.context?.triggeringPointer).toBe("/transaction/title");
  expect(ev.context?.triggeringRuleType).toBe("lightweight_model");
});

test("protectBuffered input block carries the context verdict", async () => {
  const collie = makeClient(ctxInputBlockHandler());
  const result = await collie.streaming.protectBuffered({
    input: "hi", rawStreamFactory: factoryOf(["x"]), context: { a: "b" },
  });
  expect(result.inputBlocked).toBe(true);
  expect(result.blockedBy).toBe("context");
  expect(result.context?.status).toBe("blocked");
  expect(result.context?.triggeringPointer).toBe("/transaction/title");
});

test("protectStream finished carries the input context verdict", async () => {
  // monitored/degraded-but-allowed context is observable on the success
  // terminal (Finished), not only on a block (Slice 1f review).
  const collie = makeClient(
    psBackend({ contextResult: { status: "monitored", blocked: false }, blockedBy: "none" }),
  );
  const events = await collect(
    collie.streaming.protectStream({
      input: "hi", rawStreamFactory: factoryOf(["hello"]), context: { a: "b" },
    }),
  );
  const finished = events[events.length - 1];
  expect(finished.type).toBe("finished");
  expect((finished as { blockedBy?: string }).blockedBy).toBe("none");
  expect((finished as { context?: { status: string } }).context?.status).toBe("monitored");
});

test("protectBuffered success carries the input context verdict", async () => {
  const collie = makeClient(
    bufferedBackend({ contextResult: { status: "degraded", inference_degraded: true }, blockedBy: "none" }),
  );
  const result = await collie.streaming.protectBuffered({
    input: "hi", rawStreamFactory: factoryOf(["x"]), context: { a: "b" },
  });
  expect(result.blocked).toBe(false);
  expect(result.context?.status).toBe("degraded");
  expect(result.context?.inferenceDegraded).toBe(true);
});

// ---------------------------------------------------------------------------
// Mutation-kill tests (#26 streaming target) — each exists because a
// specific surviving mutant proved the class unpinned (the maintainers'
// mutation-testing register, "Node streaming").
// ---------------------------------------------------------------------------
test("a not-allowed external inputResult is rejected at the API edge", async () => {
  // The explicit-allow gate's `||` is defense-in-depth behind TWO guards
  // (parser normalization + this edge TypeError) — same chain as the
  // Python twin. This pins the edge guard; without it the `||`→`&&`
  // mutant (L661) would be a REACHABLE hole, not an equivalent.
  const createdJobs: Array<Record<string, unknown>> = [];
  const collie = makeClient(psBackend({ createdJobs }));
  const factoryState = { calls: 0 };

  // The guard fires SYNCHRONOUSLY at call time — before the generator,
  // before any wire traffic (same contract as the factory validation).
  expect(() =>
    collie.streaming.protectStream({
      input: "hi",
      rawStreamFactory: factoryOf(["never"], factoryState),
      inputResult: { allowed: false, blocked: false, originalText: "hi", triggeredRules: [] },
    }),
  ).toThrow(/must correspond to this input and be allowed/);

  expect(factoryState.calls).toBe(0);
  expect(createdJobs).toEqual([]);
});

test("a filteredText echoing the ORIGINAL is not a mask", async () => {
  // rejectMaskedInput's condition is `filtered !== null && filtered !==
  // original` — the always-true mutant survived because no test covered
  // a server that ECHOES the unmasked prompt back as filtered_content.
  const collie = makeClient((url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const body = bodyOf(init);
      return json(202, { job_id: body.inbound_only ? "job_mod" : "job_stream" });
    }
    if (method === "GET" && path === "/v1/jobs/job_mod") {
      return json(200, {
        status: "completed",
        inbound_result: {
          allowed: true, blocked: false,
          filtered_content: "hi",  // == the original: NOT a mask
          triggered_rules: [],
        },
      });
    }
    if (path === "/v1/jobs/job_stream/chunks") {
      const b = bodyOf(init);
      const isFinal = Boolean(b.is_final);
      const content = (b.content as string) ?? "";
      const emits = content || isFinal
        ? [{ content, blocked: false, final: isFinal, triggered_rules: [] }]
        : [];
      return json(200, { sequence: b.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  });

  const events = await collect(collie.streaming.protectStream({
    input: "hi",
    rawStreamFactory: factoryOf(["ok"]),
  }));
  expect(texts(events).join("")).toBe("ok"); // streamed, no MaskedInputError
});

test("the claimless downgrade's create body carries the original input and ids", async () => {
  // Survivors L717/727 replaced the claimless session's args with `{}`
  // and every flow assert stayed green — the fake server never checked
  // the BODY. The claimless create must byte-carry the original input
  // and the conversation/correlation ids.
  const createdJobs: Array<Record<string, unknown>> = [];
  const counters = { modCalls: 0, staleServed: 0 };
  const collie = makeClient(staleBackend({
    staleTimes: 1, code: "input_gate_unverifiable", createdJobs, counters,
  }));

  const events = await collect(collie.streaming.protectStream({
    input: "the original prompt",
    conversationId: "conv-7",
    correlationId: "corr-7",
    rawStreamFactory: factoryOf(["ok"]),
  }));

  expect(texts(events).join("")).toBe("ok");
  const sessionBodies = createdJobs.filter((b) => !b.inbound_only);
  // claimed attempt first, then the claimless retry WITHOUT the reference
  expect(sessionBodies.length).toBe(2);
  const claimless = sessionBodies[1];
  expect(claimless.input_job_id).toBeUndefined();
  expect(claimless.message_input).toBe("the original prompt");
  expect(claimless.conversation_id).toBe("conv-7");
  expect(claimless.correlation_id).toBe("corr-7");
});

test("a regate block's InputBlocked event carries the verdict fields", async () => {
  // Survivors L744-747 nulled jobId/requestId/blockedBy/context on the
  // regate-blocked event — flow asserts alone can't see field loss.
  const createdJobs: Array<Record<string, unknown>> = [];
  const counters = { modCalls: 0, staleServed: 0 };
  const collie = makeClient((url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const body = bodyOf(init);
      createdJobs.push(body);
      if (body.inbound_only) {
        counters.modCalls += 1;
        return json(202, { job_id: `job_mod${counters.modCalls}` });
      }
      if (body.input_job_id && counters.staleServed < 1) {
        counters.staleServed += 1;
        return json(409, { error: { message: "stale", type: "input_gate_stale", code: "input_gate_stale" } });
      }
      return json(202, { job_id: "job_stream" });
    }
    if (method === "GET" && path.startsWith("/v1/jobs/job_mod")) {
      if (path.endsWith("job_mod2")) {
        return json(200, {
          job_id: "job_mod2",
          status: "inbound_blocked",
          inbound_result: {
            allowed: false, blocked: true,
            block_message: "ctx block", triggered_rules: [],
          },
          blocked_by: "context",
          context_result: { status: "blocked", blocked: true, block_message: "ctx block", triggering_pointer: "/page" },
        });
      }
      return json(200, {
        status: "completed",
        inbound_result: { allowed: true, blocked: false, triggered_rules: [] },
      });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  });

  const events = await collect(collie.streaming.protectStream({
    input: "hi",
    rawStreamFactory: factoryOf(["never"]),
  }));

  expect(events.length).toBe(1);
  const blocked = events[0] as InputBlocked;
  expect(blocked.type).toBe("input_blocked");
  expect(blocked.blockMessage).toBe("ctx block");
  expect(blocked.jobId).toBe("job_mod2");
  expect(blocked.blockedBy).toBe("context");
  expect(blocked.context?.status).toBe("blocked");
});

test("Finished carries the regate's context verdict on a successful turn", async () => {
  // The observability half (survivors L735/736): a stale ladder that
  // re-gates and then SUCCEEDS must surface the re-gate's blockedBy and
  // context on the Finished terminal.
  const counters = { modCalls: 0, staleServed: 0 };
  const collie = makeClient((url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const body = bodyOf(init);
      if (body.inbound_only) {
        counters.modCalls += 1;
        return json(202, { job_id: `job_mod${counters.modCalls}` });
      }
      if (body.input_job_id && counters.staleServed < 1) {
        counters.staleServed += 1;
        return json(409, { error: { message: "stale", type: "input_gate_stale", code: "input_gate_stale" } });
      }
      return json(202, { job_id: "job_stream" });
    }
    if (method === "GET" && path.startsWith("/v1/jobs/job_mod")) {
      const payload: Record<string, unknown> = {
        status: "completed",
        inbound_result: { allowed: true, blocked: false, triggered_rules: [] },
      };
      if (path.endsWith("job_mod2")) {
        payload.blocked_by = "none";
        payload.context_result = { status: "monitored", blocked: false };
      }
      return json(200, payload);
    }
    if (path === "/v1/jobs/job_stream/chunks") {
      const b = bodyOf(init);
      const isFinal = Boolean(b.is_final);
      const content = (b.content as string) ?? "";
      const emits = content || isFinal
        ? [{ content, blocked: false, final: isFinal, triggered_rules: [] }]
        : [];
      return json(200, { sequence: b.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  });

  const events = await collect(collie.streaming.protectStream({
    input: "hi",
    rawStreamFactory: factoryOf(["ok"]),
  }));

  const finished = events[events.length - 1] as Extract<StreamEvent, { type: "finished" }>;
  expect(finished.type).toBe("finished");
  expect(counters.modCalls).toBe(2);
  expect(finished.blockedBy).toBe("none");
  expect(finished.context?.status).toBe("monitored");
});

test("the regate carries the ORIGINAL prompt and context in its body", async () => {
  // Survivor L727 emptied the regate moderate.run args to `{}` — the
  // fake never checked the re-check's BODY. A prompt-only (or empty)
  // re-gate under-checks: the context must ride the re-gate
  // byte-for-byte (round-7 lesson, checkpoint class).
  const createdJobs: Array<Record<string, unknown>> = [];
  const counters = { modCalls: 0, staleServed: 0 };
  const collie = makeClient(staleBackend({ staleTimes: 1, createdJobs, counters }));

  await collect(collie.streaming.protectStream({
    input: "the original prompt",
    context: { page: 1 },
    contextFormat: "json",
    rawStreamFactory: factoryOf(["ok"]),
  }));

  const gates = createdJobs.filter((b) => b.inbound_only);
  expect(gates.length).toBe(2);
  const regate = gates[1];
  expect(regate.message_input).toBe("the original prompt");
  expect(regate.context).toEqual({ page: 1 });
  expect(regate.context_format).toBe("json");
});

test("the primary gate's InputBlocked event carries the verdict fields", async () => {
  // Survivors L666/668/669: `?? null` → `&& null` on the PRIMARY block
  // event's blockMessage/jobId/requestId — the regate twin was pinned,
  // the primary path was not.
  const collie = makeClient((url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const body = bodyOf(init);
      if (body.inbound_only) {
        return json(202, { job_id: "job_mod" }, { "x-request-id": "rid-9" });
      }
      return json(202, { job_id: "job_stream" });
    }
    if (method === "GET" && path === "/v1/jobs/job_mod") {
      return json(200, {
        job_id: "job_mod",
        status: "inbound_blocked",
        inbound_result: {
          allowed: false, blocked: true,
          block_message: "nope", triggered_rules: [],
        },
      }, { "x-request-id": "rid-9" });
    }
    return json(404, { error: { message: "nf", type: "x" } });
  });

  const events = await collect(collie.streaming.protectStream({
    input: "hi",
    rawStreamFactory: factoryOf(["never"]),
  }));

  const blocked = events[0] as InputBlocked;
  expect(blocked.type).toBe("input_blocked");
  expect(blocked.blockMessage).toBe("nope");
  expect(blocked.jobId).toBe("job_mod");
  expect(blocked.requestId).toBe("rid-9");
});
