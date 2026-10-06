/**
 * Flow-level poll-backoff tests (Poll Backoff Contract, amendment v27): both
 * Node poll loops (moderate.input inline loop + CollieClient._pollJob) are
 * wired to the pacer — observed poll counts EQUAL the reference simulator's
 * output for identical parameters (wiring + math in one assertion), plus the
 * latency gate, hint flows, 429 flows, wall-clock stop, and the error
 * taxonomy. Mirrors sdk/python/tests/test_poll_backoff.py.
 */
import { readFileSync } from "node:fs";
import { expect, test } from "vitest";

import {
  ChunkRetryExhausted,
  CollieApiError,
  CollieConnectionError,
  ModerationError,
} from "../src/errors.js";
import { errorJson, json, makeClient, pathOf, type Handler } from "./helpers.js";

/**
 * Canonical expected poll counts from the reference simulator — an
 * INDEPENDENT implementation of the contract — loaded from the GENERATED
 * `sdk/conformance/poll_cases.json` (single source; a Python test guards the
 * file against the live simulator; regenerate with
 * `python poll_sim.py --json > poll_cases.json`). Parameters: interval
 * 0.05 s, rtt 0.01 s, timeout 30 s.
 */
const CANON = JSON.parse(
  readFileSync(new URL("../../conformance/poll_cases.json", import.meta.url), "utf8"),
) as {
  cases: Array<{
    name: string;
    polls: number;
    timed_out: boolean;
    rtt_s?: number;
    hint_schedule?: Array<[number, number]>;
    rtt_schedule?: Array<[number, number]>;
  }>;
};

function canonCase(name: string) {
  const found = CANON.cases.find((c) => c.name === name);
  if (!found) throw new Error(`missing canonical case ${name} in poll_cases.json`);
  return found;
}

function canonPolls(name: string): number {
  return canonCase(name).polls;
}

const SIM = {
  expectedPolls: {
    0.3: canonPolls("job_0.3s_draw_0.5"),
    8: canonPolls("job_8s_draw_0.5"),
    11: canonPolls("job_11s_draw_0.5"),
  } as Record<number, number>,
  worstPolls8s: canonPolls("job_8s_draw_0.0"),
  timeoutPolls: canonPolls("job_999s_draw_0.5"),
  hintedPolls8s: canonPolls("job_8s_draw_0.5_hint_5000"),
};

const RTT = 0.01;

/** Virtual time: the injected sleep advances it; handlers add RTT. */
class VClock {
  now = 0;
  monotonic = (): number => this.now;
  sleep = async (s: number): Promise<void> => {
    this.now += s;
  };
}

/** Job that becomes terminal when the GET *response* lands at/after `jobS` —
 * the same counting model as the simulator. */
function timedJobHandler(
  clock: VClock,
  jobS: number,
  opts: {
    jobId?: string;
    // A function models a DYNAMIC hint schedule (e.g. the backend age
    // floor), evaluated at the response time — same convention as the
    // simulator's callable hint_ms.
    hintMs?: number | ((t: number) => number | undefined);
    // A function models a VARIABLE round trip (rtt_schedule), evaluated at
    // the 1-based poll index — same convention as the simulator's callable
    // rtt_s.
    rtt?: number | ((pollIndex: number) => number);
  } = {},
): { handler: Handler; state: { gets: number } } {
  const jobId = opts.jobId ?? "job_t";
  const rtt = opts.rtt ?? RTT;
  const state = { gets: 0 };
  const handler: Handler = (url, init) => {
    const method = init.method ?? "GET";
    const path = pathOf(url);
    if (method === "POST" && path === "/v1/jobs") {
      return json(202, { job_id: jobId, status: "processing_inbound" });
    }
    if (method === "GET" && path === `/v1/jobs/${jobId}`) {
      state.gets++;
      // The server snapshots the job status when it PROCESSES the GET —
      // modeled at ISSUE time (v20), before the response travels back — so
      // it cannot report a completion that happens in flight. The loop's
      // strict acceptance check then rejects a response landing at/after the
      // deadline.
      const snapshot = clock.now;
      clock.now += typeof rtt === "function" ? rtt(state.gets) : rtt; // lands
      if (snapshot >= jobS) {
        return json(200, {
          job_id: jobId,
          status: "completed",
          inbound_result: {
            allowed: true,
            blocked: false,
            filtered_content: "ok",
            triggered_rules: [],
          },
        });
      }
      const pending: Record<string, unknown> = { job_id: jobId, status: "processing_inbound" };
      // The server computes status AND hint in ONE response at the SNAPSHOT
      // time (v21), not at landing.
      const hint = typeof opts.hintMs === "function" ? opts.hintMs(snapshot) : opts.hintMs;
      if (hint !== undefined) pending.suggested_poll_ms = hint;
      return json(200, pending);
    }
    return json(404, { error: { message: "nf", type: "not_found" } });
  };
  return { handler, state };
}

function clockClient(clock: VClock, handler: Handler, rand: () => number = () => 0.5) {
  return makeClient(handler, {
    sleep: clock.sleep,
    monotonic: clock.monotonic,
    rand,
  });
}

// --------------------------------------------------- wiring + simulator parity

test.each([0.3, 8, 11])("moderation loop matches the simulator (job %ss)", async (jobS) => {
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, jobS);
  const collie = clockClient(clock, handler);
  const result = await collie.moderate.input({ prompt: "hi" });
  expect(result.allowed).toBe(true);
  expect(state.gets).toBe(SIM.expectedPolls[jobS]);
});

test.each([0.3, 8])("_pollJob loop matches the simulator (job %ss)", async (jobS) => {
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, jobS);
  const collie = clockClient(clock, handler);
  const data = await collie._pollJob("job_t", {
    sdkOrigin: "test",
    terminal: new Set(["completed"]),
    failure: new Set(["failed"]),
  });
  expect(data.status).toBe("completed");
  expect(state.gets).toBe(SIM.expectedPolls[jobS]);
});

test("fast-job latency gate", async () => {
  // Ceilings alone can hide over-backoff: a 300 ms job must complete within
  // job + caller_interval + 2*RTT of virtual time.
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, 0.3);
  const collie = clockClient(clock, handler);
  await collie.moderate.input({ prompt: "hi" });
  expect(clock.now).toBeLessThanOrEqual(0.3 + 0.05 + 2 * RTT);
  expect(state.gets).toBeLessThanOrEqual(8);
});

test("worst jitter stays within the ceilings", async () => {
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, 8);
  const collie = clockClient(clock, handler, () => 0);
  await collie.moderate.input({ prompt: "hi" });
  expect(state.gets).toBe(SIM.worstPolls8s);
  expect(state.gets).toBeLessThanOrEqual(45);
});

// ------------------------------------------------------------------ hint flows

test("hint ignored in warm, honored in ramp", async () => {
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, 8, { hintMs: 5000 });
  const collie = clockClient(clock, handler);
  await collie.moderate.input({ prompt: "hi" });
  // Warm phase unaffected by the hint (still ~25 tiny polls in the first
  // 1.5 s), then the hint stretches ramp sleeps toward 5 s.
  expect(state.gets).toBe(SIM.hintedPolls8s);
  expect(SIM.hintedPolls8s).toBeLessThan(SIM.expectedPolls[8]!);
});

test("a job finishing just inside the budget with a large hint succeeds", async () => {
  // v10 final-poll reserve: pre-v10 the final sleep reached the deadline and
  // the loop timed out without ever issuing the last GET.
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, 29, { hintMs: 5000 });
  const collie = clockClient(clock, handler);
  const result = await collie.moderate.input({ prompt: "hi" });
  expect(result.allowed).toBe(true);
  expect(state.gets).toBe(canonPolls("job_29s_draw_0.5_hint_5000"));
});

test("the final-poll reserve boundary is strict (v12): 99 ms lands, 100 ms is rejected", async () => {
  // The reserve guarantees the final GET is ISSUED with 100 ms remaining; it
  // is accepted iff the full round trip completes STRICTLY within the
  // reserve — at exactly 100 ms the response lands AT the deadline and the
  // strict acceptance boundary rejects it.
  // v15 pins it on the plain 29.89 s job: both runs are poll-for-poll
  // identical and differ only in the reserve GET's landing (29.999 vs 30.0).
  const ok = canonCase("job_29.89s_plain_rtt_0.099");
  const okClock = new VClock();
  const okJob = timedJobHandler(okClock, 29.89, { rtt: ok.rtt_s });
  const result = await clockClient(okClock, okJob.handler).moderate.input({ prompt: "hi" });
  expect(result.allowed).toBe(true);
  expect(ok.timed_out).toBe(false);
  expect(okJob.state.gets).toBe(ok.polls);

  const slow = canonCase("job_29.89s_plain_rtt_0.1");
  const slowClock = new VClock();
  const slowJob = timedJobHandler(slowClock, 29.89, { rtt: slow.rtt_s });
  await expect(
    clockClient(slowClock, slowJob.handler).moderate.input({ prompt: "hi" }),
  ).rejects.toThrow(/timed out/);
  expect(slow.timed_out).toBe(true);
  expect(slowJob.state.gets).toBe(slow.polls);
});

/** Schedule lookup per the canonical semantics (v13): value of the last
 * [t_from, ms] step with t_from STRICTLY < t — transitions are exclusive at
 * the boundary, matching the backend ladder's `raw <= step` (at exactly
 * 16.0 s the age floor is still 2000). */
function scheduleHint(schedule: Array<[number, number]>): (t: number) => number {
  return (t: number): number => {
    let value = schedule[0]![1];
    for (const [tFrom, ms] of schedule) if (t > tFrom) value = ms;
    return value;
  };
}

/** Variable round trip (v18): rtt for poll index i (1-based) is the last
 * step with pollFrom STRICTLY < i, in seconds — mirrors the simulator's
 * rtt_schedule_fn. */
function scheduleRtt(schedule: Array<[number, number]>): (pollIndex: number) => number {
  return (i: number): number => {
    let ms = schedule[0]![1];
    for (const [pollFrom, v] of schedule) if (i > pollFrom) ms = v;
    return ms / 1000;
  };
}

test("a dynamic hint schedule (the backend age floor) through the real loop", async () => {
  // The hint CHANGES between polls — the loop must follow each response's
  // own value, not just a constant. The schedule comes from the canonical
  // case itself (single source).
  const spec = canonCase("job_11s_draw_0.5_age_schedule");
  if (!spec.hint_schedule) throw new Error("age-schedule case lost its hint_schedule");
  const hint = scheduleHint(spec.hint_schedule);
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, 11, { hintMs: hint });
  const result = await clockClient(clock, handler).moderate.input({ prompt: "hi" });
  expect(result.allowed).toBe(true);
  expect(spec.timed_out).toBe(false);
  expect(state.gets).toBe(spec.polls);
});

test("boundary and dynamic cases through the SECOND loop (_pollJob)", async () => {
  // Hint extraction and deadline handling are duplicated per loop — the
  // moderation-loop replays alone can't catch a divergence in _pollJob.
  // (a) strict reserve boundary: 99 ms round trip accepted, 100 ms rejected.
  const ok = canonCase("job_29.89s_plain_rtt_0.099");
  const okClock = new VClock();
  const okJob = timedJobHandler(okClock, 29.89, { rtt: ok.rtt_s });
  const data = await clockClient(okClock, okJob.handler)._pollJob("job_t", {
    sdkOrigin: "test",
    terminal: new Set(["completed"]),
    failure: new Set(["failed"]),
  });
  expect(data.status).toBe("completed");
  expect(okJob.state.gets).toBe(ok.polls);

  const slow = canonCase("job_29.89s_plain_rtt_0.1");
  const slowClock = new VClock();
  const slowJob = timedJobHandler(slowClock, 29.89, { rtt: slow.rtt_s });
  await expect(
    clockClient(slowClock, slowJob.handler)._pollJob("job_t", {
      sdkOrigin: "test",
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
    }),
  ).rejects.toMatchObject({ code: "poll_timeout" });
  expect(slow.timed_out).toBe(true);
  expect(slowJob.state.gets).toBe(slow.polls);
});

test("boundary pairs through BOTH loops: v13 rescued (outside W), ninth-review inside W (v20)", async () => {
  // Two boundary pairs under the honest server-snapshot model. The v13 pair
  // (age schedule, rtt == reserve) completes 700 ms before the deadline —
  // OUTSIDE W — and is genuinely rescued (both succeed). The ninth-review
  // pair (max(host@6->5000, age), rtt 80 ms) completes 30 ms before the
  // deadline — INSIDE W (= 180 ms); v14's "rescue" was an artifact of the
  // pre-v20 landing-snapshot model, so under the honest model BOTH schedules
  // TIME OUT (the guarantee permits it — inside W is a lottery). Poll counts
  // match the simulator either way.
  const pairs: Array<[string, string, number]> = [
    ["job_29.3s_rtt_0.1_plain", "job_29.3s_age_schedule_rtt_0.1", 29.3],
    ["job_29.97s_rtt_0.08_plain", "job_29.97s_host5000_at_6_rtt_0.08", 29.97],
  ];
  const loops = [
    (clock: VClock, handler: Handler) =>
      clockClient(clock, handler).moderate.input({ prompt: "hi" }),
    (clock: VClock, handler: Handler) =>
      clockClient(clock, handler)._pollJob("job_t", {
        sdkOrigin: "test",
        terminal: new Set(["completed"]),
        failure: new Set(["failed"]),
      }),
  ];
  const replay = async (
    run: (clock: VClock, handler: Handler) => Promise<unknown>,
    clock: VClock,
    job: { handler: Handler; state: { gets: number } },
    spec: { polls: number; timed_out: boolean },
  ) => {
    if (spec.timed_out) await expect(run(clock, job.handler)).rejects.toThrow(/timed out/);
    else await run(clock, job.handler);
    expect(job.state.gets).toBe(spec.polls);
  };
  for (const [plainName, hintedName, jobS] of pairs) {
    const plainSpec = canonCase(plainName);
    const hintedSpec = canonCase(hintedName);
    if (!hintedSpec.hint_schedule) throw new Error(`${hintedName} lost its hint_schedule`);
    for (const run of loops) {
      const plainClock = new VClock();
      await replay(
        run,
        plainClock,
        timedJobHandler(plainClock, jobS, { rtt: plainSpec.rtt_s }),
        plainSpec,
      );
      const hintedClock = new VClock();
      await replay(
        run,
        hintedClock,
        timedJobHandler(hintedClock, jobS, {
          hintMs: scheduleHint(hintedSpec.hint_schedule),
          rtt: hintedSpec.rtt_s,
        }),
        hintedSpec,
      );
    }
  }
});

test.each([
  ["job_5s_interval_0.2_draw_0.5", 5, 0.2, undefined],  // custom interval, no hint (v16)
  ["job_8s_interval_0.2_hint_5000", 8, 0.2, 5000],      // custom interval x hint (v17)
  ["job_25s_interval_10_draw_0.5", 25, 10, undefined],  // interval above the 5 s cap (v17)
])(
  "custom caller interval %s matches the simulator through both loops",
  async (caseName, jobS, intervalS, hintMs) => {
    // Non-default intervals through the REAL loops — the warm phase, ramp
    // floor, jitter range and cap_eff all key off the caller interval, so
    // the interval sweep being simulator-only left the loops' own interval
    // plumbing unchecked (interval, interval x hint, interval > cap).
    const spec = canonCase(caseName as string);
    expect(spec.timed_out).toBe(false);

    const modClock = new VClock();
    const mod = timedJobHandler(modClock, jobS as number, { hintMs: hintMs as number | undefined });
    const result = await clockClient(modClock, mod.handler).moderate.input({
      prompt: "hi",
      pollIntervalS: intervalS as number,
    });
    expect(result.allowed).toBe(true);
    expect(mod.state.gets).toBe(spec.polls);

    const secondClock = new VClock();
    const second = timedJobHandler(secondClock, jobS as number, {
      hintMs: hintMs as number | undefined,
    });
    const data = await clockClient(secondClock, second.handler)._pollJob("job_t", {
      sdkOrigin: "test",
      pollIntervalS: intervalS as number,
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
    });
    expect(data.status).toBe("completed");
    expect(second.state.gets).toBe(spec.polls);
  },
);

test.each([
  ["job_8s_rtt_schedule", 8, false],                  // variable rtt, no hint (v18)
  ["job_29.5s_rtt_schedule_age_hint", 29.5, true],    // tail: variable rtt + age hint (v20)
])(
  "variable rtt schedule %s matches the simulator through both loops",
  async (caseName, jobS, hasHint) => {
    // A per-poll VARYING round trip (bounded under the reserve) through the
    // REAL loops — a constant rtt models zero variance and can't catch a loop
    // that mishandles a changing round trip. The tail case adds a changing
    // production age hint at once.
    const spec = canonCase(caseName as string);
    if (!spec.rtt_schedule) throw new Error(`${caseName} lost its rtt_schedule`);
    expect(spec.timed_out).toBe(false);
    const rtt = scheduleRtt(spec.rtt_schedule);
    const hintMs =
      hasHint && spec.hint_schedule ? scheduleHint(spec.hint_schedule) : undefined;

    const modClock = new VClock();
    const mod = timedJobHandler(modClock, jobS as number, { rtt, hintMs });
    const result = await clockClient(modClock, mod.handler).moderate.input({ prompt: "hi" });
    expect(result.allowed).toBe(true);
    expect(mod.state.gets).toBe(spec.polls);

    const secondClock = new VClock();
    const second = timedJobHandler(secondClock, jobS as number, { rtt, hintMs });
    const data = await clockClient(secondClock, second.handler)._pollJob("job_t", {
      sdkOrigin: "test",
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
    });
    expect(data.status).toBe("completed");
    expect(second.state.gets).toBe(spec.polls);
  },
);

test("the eleventh-review counterexamples are rescued through both loops (v15)", async () => {
  // Ramp-in bound + caller-paced tail fix the eleventh review's pair:
  // - post-warm cliff: a host rung of 5000 jumped a 1.6 s job straight
  //   into a 4.5 s sleep (completion 6.02 s instead of 1.62 s); the
  //   ramp-in bound observes it at 2.275 s with poll parity;
  // - custom timeout: a 2.914 s job under a 3 s budget with the age
  //   schedule timed out with 86 ms still on the clock; the caller-paced
  //   tail catches it in-budget with plain-parity polls.
  const cliff = canonCase("job_1.6s_host5000");
  expect(cliff.timed_out).toBe(false);
  const loops = [
    (clock: VClock, handler: Handler) =>
      clockClient(clock, handler).moderate.input({ prompt: "hi" }),
    (clock: VClock, handler: Handler) =>
      clockClient(clock, handler)._pollJob("job_t", {
        sdkOrigin: "test",
        terminal: new Set(["completed"]),
        failure: new Set(["failed"]),
      }),
  ];
  for (const run of loops) {
    const clock = new VClock();
    const { handler, state } = timedJobHandler(clock, 1.6, { hintMs: 5000 });
    await run(clock, handler);
    expect(state.gets).toBe(cliff.polls);
    expect(clock.now).toBeCloseTo(2.275, 9);
  }

  const ct = canonCase("job_2.914s_age_schedule_timeout_3");
  if (!ct.hint_schedule) throw new Error("custom-timeout case lost its hint_schedule");
  expect(ct.timed_out).toBe(false);
  const hint = scheduleHint(ct.hint_schedule);

  const modClock = new VClock();
  const mod = timedJobHandler(modClock, 2.914, { hintMs: hint });
  const result = await clockClient(modClock, mod.handler, () => 0).moderate.input({
    prompt: "hi",
    timeoutS: 3,
  });
  expect(result.allowed).toBe(true);
  expect(mod.state.gets).toBe(ct.polls);

  const secondClock = new VClock();
  const second = timedJobHandler(secondClock, 2.914, { hintMs: hint });
  const data = await clockClient(secondClock, second.handler, () => 0)._pollJob("job_t", {
    sdkOrigin: "test",
    timeoutS: 3,
    terminal: new Set(["completed"]),
    failure: new Set(["failed"]),
  });
  expect(data.status).toBe("completed");
  expect(second.state.gets).toBe(ct.polls);
});

// --------------------------------------------------------------------- timeout

test("wall-clock timeout in the virtual model", async () => {
  // Virtual-clock view of the budget: sleeps are bounded by the remaining
  // budget, so virtual time never overshoots by more than one RTT. (Strict
  // real-time enforcement — cancelling a GET that hangs past the deadline —
  // is covered by the real-clock tests below; the virtual clock can't drive
  // AbortSignal.timeout.)
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, 999);
  const collie = clockClient(clock, handler);
  await expect(collie.moderate.input({ prompt: "hi" })).rejects.toThrow(/timed out/);
  expect(clock.now).toBeLessThanOrEqual(30 + RTT);
  expect(state.gets).toBe(SIM.timeoutPolls);
  expect(state.gets).toBeLessThanOrEqual(75);
});

/** POST answers instantly; every GET hangs far past any test budget. If the
 * deadline bound is not wired (no signal on the GET), the promise rejects
 * immediately with a distinguishable error instead of hanging the suite. */
function hangingGetHandler(): Handler {
  return (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    return new Promise<Response>((_, reject) => {
      const signal = init.signal;
      if (!signal) {
        reject(new Error("poll GET carried no AbortSignal — deadline bound not wired"));
        return;
      }
      // Spec-compliant fetch behavior: reject with the signal's abort reason.
      signal.addEventListener("abort", () => reject(signal.reason));
    });
  };
}

test("deadline cancels a hung GET — _pollJob (real clock)", async () => {
  // The budget is absolute wall clock: a GET that hangs past the deadline is
  // cancelled and surfaces as poll_timeout. Real clocks on purpose —
  // transport timeouts are per-phase and can't cap total request time, so
  // only the AbortSignal.timeout bound can end this request.
  const collie = makeClient(hangingGetHandler());
  const start = performance.now();
  await expect(
    collie._pollJob("j", {
      sdkOrigin: "test",
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
      timeoutS: 0.25,
    }),
  ).rejects.toMatchObject({ code: "poll_timeout" });
  expect(performance.now() - start).toBeLessThan(5000);
});

test("deadline cancels a hung GET — moderation loop (real clock)", async () => {
  const collie = makeClient(hangingGetHandler());
  const start = performance.now();
  await expect(collie.moderate.input({ prompt: "hi", timeoutS: 0.25 })).rejects.toThrow(
    /timed out/,
  );
  expect(performance.now() - start).toBeLessThan(5000);
});

/** A GET that outruns cancellation: it advances the virtual clock past the
 * deadline (ignoring the abort signal, like a non-compliant fetch or a
 * late-firing timer) and still returns a terminal payload. */
function lateTerminalHandler(clock: VClock, advance: number): Handler {
  return (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    clock.now += advance;
    return json(200, {
      job_id: "j",
      status: "completed",
      inbound_result: {
        allowed: true,
        blocked: false,
        filtered_content: "ok",
        triggered_rules: [],
      },
    });
  };
}

test("a late terminal response is rejected — _pollJob (strict acceptance)", async () => {
  // Strict acceptance boundary: the deadline timer can fire late (integer
  // ceil, event-loop lag) or be ignored entirely — a terminal response
  // landing after the deadline is a poll-timeout, not a late success.
  const clock = new VClock();
  const collie = clockClient(clock, lateTerminalHandler(clock, 1.0));
  await expect(
    collie._pollJob("j", {
      sdkOrigin: "test",
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
      timeoutS: 0.5,
    }),
  ).rejects.toMatchObject({ code: "poll_timeout" });
});

test("a late terminal response is rejected — moderation loop (strict acceptance)", async () => {
  const clock = new VClock();
  const collie = clockClient(clock, lateTerminalHandler(clock, 1.0));
  await expect(collie.moderate.input({ prompt: "hi", timeoutS: 0.5 })).rejects.toThrow(
    /timed out/,
  );
});

test("a late HTTP error response is rejected — strict acceptance", async () => {
  // A 500 landing after the deadline (transport outran the abort) is this
  // flow's poll-timeout, not a late typed HTTP error — the primitive throws
  // before the loop's post-check, so the catch path enforces the boundary.
  const clock = new VClock();
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    clock.now += 1.0; // lands 0.5 s past the 0.5 s budget
    return errorJson(500, "server_error", "late");
  };
  const collie = clockClient(clock, handler);
  await expect(
    collie._pollJob("j", {
      sdkOrigin: "test",
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
      timeoutS: 0.5,
    }),
  ).rejects.toMatchObject({ code: "poll_timeout" });
});

test("a late HTTP error response is rejected — moderation loop", async () => {
  // The late-HTTP-error catch exists in BOTH loops — through moderation too.
  const clock = new VClock();
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    clock.now += 1.0;
    return errorJson(500, "server_error", "late");
  };
  const collie = clockClient(clock, handler);
  await expect(collie.moderate.input({ prompt: "hi", timeoutS: 0.5 })).rejects.toThrow(
    /timed out/,
  );
});

test("an unrelated abort after the deadline stays a connection error", async () => {
  // Structural taxonomy: only the deadline signal's own reason (by identity
  // or cause) maps to poll_timeout. A foreign AbortError arriving after the
  // deadline fired — e.g. a transport's internal abort — must not be
  // reclassified by a generic name check.
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    return new Promise<Response>((_, reject) => {
      init.signal!.addEventListener("abort", () =>
        reject(new DOMException("independent transport abort", "AbortError")),
      );
    });
  };
  const collie = makeClient(handler);
  await expect(
    collie._pollJob("j", {
      sdkOrigin: "test",
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
      timeoutS: 0.25,
    }),
  ).rejects.toBeInstanceOf(CollieConnectionError);
});

test("a transport failure while reading a 2xx body stays a connection error", async () => {
  // A socket dropped after the headers is a transport failure, not a
  // malformed response (invalid_response).
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    return new Response(
      new ReadableStream({
        start(controller) {
          controller.error(new Error("socket reset by peer"));
        },
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  };
  const collie = makeClient(handler);
  await expect(
    collie._pollJob("j", {
      sdkOrigin: "test",
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
    }),
  ).rejects.toBeInstanceOf(CollieConnectionError);
});

test("transport failure coinciding with deadline exhaustion stays a connection error", async () => {
  // Taxonomy is structural: a transport failure is a connection problem even
  // when the poll budget happens to be exhausted at that moment — only the
  // deadline signal itself firing maps to poll_timeout.
  const clock = new VClock();
  let calls = 0;
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    calls++;
    if (calls === 1) {
      clock.now += RTT;
      return json(200, { job_id: "j", status: "processing_inbound" });
    }
    clock.now += 1.0; // virtual budget (0.5 s) is now exhausted
    throw new TypeError("fetch failed");
  };
  const collie = clockClient(clock, handler);
  await expect(
    collie._pollJob("j", {
      sdkOrigin: "test",
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
      timeoutS: 0.5,
    }),
  ).rejects.toBeInstanceOf(CollieConnectionError);
});

test("transport error with budget left stays a connection error", async () => {
  const clock = new VClock();
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    throw new TypeError("boom");
  };
  const collie = clockClient(clock, handler);
  await expect(
    collie._pollJob("j", {
      sdkOrigin: "test",
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
    }),
  ).rejects.toBeInstanceOf(CollieConnectionError);
});

// ------------------------------------------------------------------------ 429s

function handler429ThenDone(
  clock: VClock,
  opts: { retryAfter?: number } = {},
): { handler: Handler; state: { gets: number } } {
  const state = { gets: 0 };
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    state.gets++;
    clock.now += RTT;
    if (state.gets === 1) {
      const headers: Record<string, string> = {};
      if (opts.retryAfter !== undefined) headers["retry-after"] = String(opts.retryAfter);
      return errorJson(429, "rate_limit", "rl", headers);
    }
    return json(200, {
      job_id: "j",
      status: "completed",
      inbound_result: {
        allowed: true,
        blocked: false,
        filtered_content: "ok",
        triggered_rules: [],
      },
    });
  };
  return { handler, state };
}

test("429 with Retry-After waits and continues", async () => {
  const clock = new VClock();
  const { handler, state } = handler429ThenDone(clock, { retryAfter: 2 });
  const collie = clockClient(clock, handler);
  const result = await collie.moderate.input({ prompt: "hi" });
  expect(result.allowed).toBe(true);
  expect(state.gets).toBe(2);
  // Retry-After is a minimum: the wait was >= 2 s (upward-only jitter).
  expect(clock.now).toBeGreaterThanOrEqual(2);
});

test("a slow 429 body cancel never stalls the poll loop", async () => {
  // cancel() may legally return a slow or never-settling promise; cleanup
  // is fire-and-forget, so the flow must still complete on schedule.
  const clock = new VClock();
  let gets = 0;
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    gets++;
    clock.now += RTT;
    if (gets === 1) {
      const body = new ReadableStream({
        cancel: () => new Promise<void>(() => {}), // never settles
      });
      return new Response(body, {
        status: 429,
        headers: { "retry-after": "2", "content-type": "application/json" },
      });
    }
    return json(200, {
      job_id: "j",
      status: "completed",
      inbound_result: {
        allowed: true,
        blocked: false,
        filtered_content: "ok",
        triggered_rules: [],
      },
    });
  };
  const collie = clockClient(clock, handler);
  const result = await collie.moderate.input({ prompt: "hi" });
  expect(result.allowed).toBe(true);
  expect(gets).toBe(2);
});

test("a continued 429 releases its response body before sleeping", async () => {
  // Undici keeps the connection busy until the body is consumed or
  // cancelled — a continued 429 that just slept would pin a pool
  // connection per attempt.
  const clock = new VClock();
  let resp429: Response | null = null;
  let gets = 0;
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    gets++;
    clock.now += RTT;
    if (gets === 1) {
      resp429 = errorJson(429, "rate_limit", "rl", { "retry-after": "2" });
      return resp429;
    }
    return json(200, {
      job_id: "j",
      status: "completed",
      inbound_result: {
        allowed: true,
        blocked: false,
        filtered_content: "ok",
        triggered_rules: [],
      },
    });
  };
  const collie = clockClient(clock, handler);
  await collie.moderate.input({ prompt: "hi" });
  // Cancelled (disturbed) before the sleep — not left pinning a connection.
  expect(resp429!.bodyUsed).toBe(true);
});

test("429 with Retry-After beyond the budget surfaces the typed 429", async () => {
  const clock = new VClock();
  const { handler, state } = handler429ThenDone(clock, { retryAfter: 60 });
  const collie = clockClient(clock, handler);
  const err = await collie.moderate.input({ prompt: "hi" }).catch((e: unknown) => e);
  expect(err).toBeInstanceOf(CollieApiError);
  expect((err as CollieApiError).statusCode).toBe(429);
  expect(state.gets).toBe(1); // no pointless sleep-then-timeout
});

test("429 without a header ends warm at the pressure floor", async () => {
  const clock = new VClock();
  const { handler } = handler429ThenDone(clock); // no Retry-After
  const collie = clockClient(clock, handler);
  await collie.moderate.input({ prompt: "hi" });
  // Pressure floor: the single wait was >= 1 s despite being inside warm.
  expect(clock.now).toBeGreaterThanOrEqual(1);
});

test("_pollJob 429 with Retry-After waits and continues", async () => {
  // The 429 path through the SECOND loop, not just moderation.
  const clock = new VClock();
  let gets = 0;
  const handler: Handler = (url, init) => {
    gets++;
    clock.now += RTT;
    if (gets === 1) return errorJson(429, "rate_limit", "rl", { "retry-after": "2" });
    return json(200, { job_id: "j", status: "completed" });
  };
  const collie = clockClient(clock, handler);
  const data = await collie._pollJob("j", {
    sdkOrigin: "test",
    terminal: new Set(["completed"]),
    failure: new Set(["failed"]),
  });
  expect(data.status).toBe("completed");
  expect(gets).toBe(2);
  expect(clock.now).toBeGreaterThanOrEqual(2); // Retry-After is a minimum
});

test("Retry-After strict decimal parse (RFC 9110 delay-seconds)", () => {
  // Number() admitted "1.5", "1e2", "0x10" and "" (as 0) and diverged from
  // the other SDKs; malformed values are unusable (pressure-floor path).
  const collie = makeClient(() => json(200, {}));
  const mk = (v: string) => new Response("", { status: 429, headers: { "retry-after": v } });
  expect(collie._parseRetryAfter(mk("2"))).toBe(2);
  expect(collie._parseRetryAfter(mk("0"))).toBe(0);
  // A VALID huge delta parses — the pacer's feasibility check then surfaces
  // the typed 429; it must never fall to the headerless pressure floor
  // (cross-SDK divergence, v9).
  expect(collie._parseRetryAfter(mk("2147483648"))).toBe(2147483648);
  expect(collie._parseRetryAfter(mk("9".repeat(400)))).toBe(Infinity);
  for (const bad of ["1.5", "1e2", "0x10", "", "-1", "Infinity"]) {
    expect(collie._parseRetryAfter(mk(bad)), bad).toBeNull();
  }
});

test("429 with Retry-After: 0 floors, not tight-loops", async () => {
  // Retry-After: 0 must not produce a zero-sleep re-poll loop — any 429 is
  // rate pressure and the wait floors at 1 s.
  const clock = new VClock();
  const { handler, state } = handler429ThenDone(clock, { retryAfter: 0 });
  const collie = clockClient(clock, handler);
  const result = await collie.moderate.input({ prompt: "hi" });
  expect(result.allowed).toBe(true);
  expect(state.gets).toBe(2);
  expect(clock.now).toBeGreaterThanOrEqual(1);
});

test("429 with a large interval is not sped up, through both loops (v18)", async () => {
  // Flow-level (the pacer-unit test alone doesn't prove the loops honor it):
  // with a 10 s interval, a Retry-After: 0 429 must sleep the caller's 10 s,
  // NOT the 1 s pressure floor. Both loops.
  const loops = [
    (clock: VClock, handler: Handler) =>
      clockClient(clock, handler).moderate.input({ prompt: "hi", pollIntervalS: 10 }),
    (clock: VClock, handler: Handler) =>
      clockClient(clock, handler)._pollJob("j", {
        sdkOrigin: "test",
        pollIntervalS: 10,
        terminal: new Set(["completed"]),
        failure: new Set(["failed"]),
      }),
  ];
  for (const run of loops) {
    const clock = new VClock();
    const { handler, state } = handler429ThenDone(clock, { retryAfter: 0 });
    await run(clock, handler);
    expect(state.gets).toBe(2);
    expect(clock.now).toBeGreaterThanOrEqual(10); // the 10 s interval floor, not 1 s
  }
});

test("429 whose interval floor won't fit the budget surfaces the typed 429, both loops (v20)", async () => {
  // A 10 s interval with Retry-After: 0 under a 5 s budget: the 10 s floor
  // doesn't fit, so the rate-limit error surfaces on the first GET. Verified
  // through BOTH loops (the v19 claim of "both loops" covered only the
  // sleep-floor case, not this feasibility case).
  const loops = [
    (clock: VClock, handler: Handler) =>
      clockClient(clock, handler).moderate.input({ prompt: "hi", pollIntervalS: 10, timeoutS: 5 }),
    (clock: VClock, handler: Handler) =>
      clockClient(clock, handler)._pollJob("j", {
        sdkOrigin: "test",
        pollIntervalS: 10,
        timeoutS: 5,
        terminal: new Set(["completed"]),
        failure: new Set(["failed"]),
      }),
  ];
  for (const run of loops) {
    const clock = new VClock();
    const { handler, state } = handler429ThenDone(clock, { retryAfter: 0 });
    await expect(run(clock, handler)).rejects.toMatchObject({ statusCode: 429 });
    expect(state.gets).toBe(1); // surfaced immediately, no 1 s tight loop
  }
});

// --------------------------------------------------- SDK-owned request timeout
// These use REAL timers (a small requestTimeoutS) so the AbortSignal.timeout
// genuinely FIRES — the v20 tests only checked the signal was present and
// threw synchronously, which couldn't catch the poll-GET / body-read /
// retry-ceiling defects. A "hang" reject only when the signal aborts (like a
// real fetch), so the request-timeout is what ends it.

/** A GET/POST that hangs until its signal aborts, then rejects with the
 * signal's reason — like a real fetch on a stuck connection. `onRequest`
 * can short-circuit specific requests (e.g. return 202 for job creation). */
function hangUntilAbort(onRequest?: (url: string, init: RequestInit) => Response | null): Handler {
  return (url, init) => {
    const early = onRequest?.(url, init);
    if (early) return early;
    return new Promise<Response>((_resolve, reject) => {
      const sig = init.signal;
      if (!sig) return; // caller guarantees a signal is present
      if (sig.aborted) return reject(sig.reason);
      sig.addEventListener("abort", () => reject(sig.reason), { once: true });
    });
  };
}

/** A response whose HEADERS arrive OK (given status) but whose BODY stream
 * hangs until the request's signal aborts, then errors — so `response.text()`
 * rejects with the abort, exercising the body-read timeout path. */
function bodyHangsBody(status: number, init: RequestInit): Response {
  const sig = init.signal!;
  const stream = new ReadableStream({
    start(controller) {
      if (sig.aborted) return controller.error(sig.reason);
      sig.addEventListener("abort", () => controller.error(sig.reason), { once: true });
    },
  });
  return new Response(stream, {
    status,
    headers: { "content-type": "application/json" },
  });
}

test("a hung job-creation POST fires the request timeout as a connection error (v21)", async () => {
  const collie = makeClient(hangUntilAbort(), { requestTimeoutS: 0.02 });
  await expect(collie.moderate.input({ prompt: "hi" })).rejects.toBeInstanceOf(
    CollieConnectionError,
  );
});

test("a hung create-POST BODY read is a connection error, not invalid_response (v21)", async () => {
  // Headers-OK 202, then the body read hangs — the request timeout aborts it,
  // and the taxonomy must be a connection error (NOT invalid_response, which
  // is reserved for a body that arrived but was empty/malformed).
  const collie = makeClient((_url, init) => bodyHangsBody(202, init), {
    requestTimeoutS: 0.02,
  });
  const err = await collie.moderate.input({ prompt: "hi" }).catch((e) => e);
  expect(err).toBeInstanceOf(CollieConnectionError);
});

test.each([
  ["moderation loop", (collie: ReturnType<typeof makeClient>) =>
    collie.moderate.input({ prompt: "hi", timeoutS: 5 })],
  ["poll-job loop", (collie: ReturnType<typeof makeClient>) =>
    collie._pollJob("j", {
      sdkOrigin: "test",
      timeoutS: 5,
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
    })],
])(
  "a hung status poll fires the request timeout (< deadline) as a connection error via %s (v21)",
  async (_name, run) => {
    // requestTimeoutS 20 ms fires long before the 5 s poll deadline, so this
    // is a CONNECTION error — not the flow's poll-timeout.
    const collie = makeClient(
      hangUntilAbort((url, init) =>
        (init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs"
          ? json(202, { job_id: "j", status: "processing_inbound" })
          : null,
      ),
      { requestTimeoutS: 0.02 },
    );
    await expect(run(collie)).rejects.toBeInstanceOf(CollieConnectionError);
  },
);

test.each([
  ["moderation loop", (collie: ReturnType<typeof makeClient>) =>
    collie.moderate.input({ prompt: "hi", timeoutS: 0.02 })],
  ["poll-job loop", (collie: ReturnType<typeof makeClient>) =>
    collie._pollJob("j", {
      sdkOrigin: "test",
      timeoutS: 0.02,
      terminal: new Set(["completed"]),
      failure: new Set(["failed"]),
    })],
])(
  "when the deadline (< request timeout) fires it is the flow's poll-timeout, not a connection error, via %s (v21)",
  async (_name, run) => {
    // Deadline 20 ms fires before the 30 s request timeout, so the abort is
    // the DEADLINE — classified as the flow's poll-timeout (NOT a connection
    // error). Composition must not blur the taxonomy.
    const collie = makeClient(
      hangUntilAbort((url, init) =>
        (init.method ?? "GET") === "POST" && pathOf(url) === "/v1/jobs"
          ? json(202, { job_id: "j", status: "processing_inbound" })
          : null,
      ),
    );
    const err = await run(collie).then(
      () => new Error("expected a rejection"),
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(Error);
    expect(err).not.toBeInstanceOf(CollieConnectionError); // deadline -> poll-timeout
  },
);

test("a hung chunk attempt fires the request timeout and retries to exhaustion (v21)", async () => {
  let posts = 0;
  const collie = makeClient(
    hangUntilAbort((url, init) => {
      const method = init.method ?? "GET";
      if (method === "POST" && pathOf(url) === "/v1/jobs") {
        return json(202, { job_id: "j", status: "processing_inbound" });
      }
      if (method === "POST" && pathOf(url).endsWith("/chunks")) posts++;
      return null; // hang
    }),
    { requestTimeoutS: 0.02, retryMaxPerChunkS: 0.08, maxRetries: 5, baseBackoffS: 0.001, maxBackoffS: 0.001 },
  );
  const session = collie.streaming.session({ input: "hi" });
  await expect(session.submit("hello", true)).rejects.toBeInstanceOf(ChunkRetryExhausted);
  expect(posts).toBeGreaterThanOrEqual(2); // retried, not one-and-done
});

test("chunk retry ceiling is a STRICT boundary: no request once the budget is spent (v21)", async () => {
  // First attempt returns 429 Retry-After: 1; ceiling is exactly 1 s. The
  // pre-sleep >= check must refuse to sleep to the boundary and issue a
  // second POST — exactly ONE POST happens.
  let posts = 0;
  const collie = makeClient(
    (url, init) => {
      const method = init.method ?? "GET";
      if (method === "POST" && pathOf(url) === "/v1/jobs") {
        return json(202, { job_id: "j", status: "processing_inbound" });
      }
      posts++;
      return errorJson(429, "rate_limit", "rl", { "retry-after": "1" });
    },
    { retryMaxPerChunkS: 1, maxRetries: 5 },
  );
  const session = collie.streaming.session({ input: "hi" });
  await expect(session.submit("hello", true)).rejects.toBeInstanceOf(ChunkRetryExhausted);
  expect(posts).toBe(1);
});

test.each([0, -1, Number.NaN, Number.POSITIVE_INFINITY, 1e12, "x" as unknown as number])(
  "requestTimeoutS=%s is rejected at construction, before any request (v21)",
  (bad) => {
    expect(() => makeClient((_u, _i) => json(200, {}), { requestTimeoutS: bad })).toThrow(
      RangeError,
    );
  },
);

test.each<[string, Partial<import("../src/client.js").CollieClientOptions>]>([
  ["maxRetries=0", { maxRetries: 0 }],
  ["maxRetries=1.5", { maxRetries: 1.5 }],
  ["retryMaxPerChunkS=-1", { retryMaxPerChunkS: -1 }],
  ["baseBackoffS=-1", { baseBackoffS: -1 }],
  ["maxBackoffS<baseBackoffS", { baseBackoffS: 2, maxBackoffS: 1 }],
])("bad numeric option %s is rejected at construction (v23)", (_name, opts) => {
  expect(() => makeClient((_u, _i) => json(200, {}), opts)).toThrow(RangeError);
});

test("a 2xx chunk that lands after the retry ceiling is rejected (v23)", async () => {
  // The monotonic clock jumps past the ceiling DURING the chunk request (the
  // handler bumps it), so the 2xx arrives late. The strict TOTAL ceiling must
  // reject it rather than accept a late success.
  let mono = 0;
  const collie = makeClient(
    (url, init) => {
      const method = init.method ?? "GET";
      if (method === "POST" && pathOf(url) === "/v1/jobs") {
        return json(202, { job_id: "j", status: "processing_inbound" });
      }
      if (method === "POST" && pathOf(url).endsWith("/chunks")) {
        mono = 0.2; // elapsed jumps past the 0.1s ceiling mid-flight
        return json(200, { sequence: 0, accepted: true, emits: [], finished: true });
      }
      return json(404, { error: { message: "nf", type: "not_found" } });
    },
    { retryMaxPerChunkS: 0.1, monotonic: () => mono },
  );
  const session = collie.streaming.session({ input: "hi" });
  await expect(session.submit("hello", true)).rejects.toBeInstanceOf(ChunkRetryExhausted);
});

test("a hung NON-2xx (400) body read is a connection error, not the HTTP status (v23)", async () => {
  // A 400 whose error body hangs: the request timeout aborts the read, which
  // _apiError's parser swallows — the classification must still be a
  // connection error (request timeout), NOT CollieApiError HTTP 400.
  const collie = makeClient((_url, init) => bodyHangsBody(400, init), { requestTimeoutS: 0.02 });
  const err = await collie.moderate.input({ prompt: "hi" }).catch((e) => e);
  expect(err).toBeInstanceOf(CollieConnectionError);
});

// ------------------------------------------------------ protectBuffered (public)

test("protectBuffered's outbound poll matches the simulator", async () => {
  // Public-wrapper wiring: the private _pollJob parity test alone can't
  // catch a wrapper that bypasses or misconfigures the pacer.
  const clock = new VClock();
  const state = { gets: 0 };
  const handler: Handler = (url, init) => {
    const method = init.method ?? "GET";
    const path = pathOf(url);
    if (method === "POST" && path === "/v1/jobs") {
      return json(202, { job_id: "jb", status: "processing_outbound" });
    }
    if (method === "GET" && path === "/v1/jobs/jb") {
      state.gets++;
      clock.now += RTT;
      if (clock.now >= 8) {
        return json(200, {
          job_id: "jb",
          status: "completed",
          // Real servers send BOTH allowed and blocked; the buffered parser
          // requires the explicit allow (tech-debt #22).
          outbound_result: { allowed: true, blocked: false, filtered_content: "hello", triggered_rules: [] },
        });
      }
      return json(200, { job_id: "jb", status: "processing_outbound" });
    }
    return json(404, { error: { message: "nf", type: "not_found" } });
  };
  const collie = clockClient(clock, handler);
  async function* gen(): AsyncGenerator<string> {
    yield "hello";
  }
  const result = await collie.streaming.protectBuffered({
    input: "hi",
    rawStreamFactory: gen,
    checkInput: false,
  });
  expect(result.blocked).toBe(false);
  expect(state.gets).toBe(SIM.expectedPolls[8]);
});

// ------------------------------------------------------------------ validation

test("knob validation fails before any request", async () => {
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, 0.3);
  const collie = clockClient(clock, handler);
  await expect(collie.moderate.input({ prompt: "hi", pollIntervalS: 0 })).rejects.toThrow(
    RangeError,
  );
  await expect(collie.moderate.input({ prompt: "hi", timeoutS: 0 })).rejects.toThrow(
    RangeError,
  );
  expect(state.gets).toBe(0);
});

test("an explicit null knob fails validation, not silently defaults", async () => {
  // Defaults apply to `undefined` only — a JS caller passing null must get
  // the RangeError before any request, not a silent 30 s default.
  const clock = new VClock();
  const { handler, state } = timedJobHandler(clock, 0.3);
  const collie = clockClient(clock, handler);
  await expect(
    collie.moderate.input({ prompt: "hi", timeoutS: null as unknown as number }),
  ).rejects.toThrow(RangeError);
  await expect(
    collie.moderate.input({ prompt: "hi", pollIntervalS: null as unknown as number }),
  ).rejects.toThrow(RangeError);
  expect(state.gets).toBe(0);
});

test("protectBuffered validates knobs before any side effect", async () => {
  // An invalid timeoutS must fail before the provider factory runs and
  // before any job is created — not down in _pollJob after both.
  const posts = { n: 0 };
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      posts.n++;
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    return json(200, { job_id: "j", status: "completed" });
  };
  const collie = makeClient(handler);
  const factoryCalls = { n: 0 };
  function factory(): AsyncIterable<string> {
    factoryCalls.n++;
    return (async function* () {
      yield "hello";
    })();
  }
  await expect(
    collie.streaming.protectBuffered({
      input: "hi",
      rawStreamFactory: factory,
      timeoutS: 0,
    }),
  ).rejects.toThrow(RangeError);
  await expect(
    collie.streaming.protectBuffered({
      input: "hi",
      rawStreamFactory: factory,
      timeoutS: null as unknown as number,
    }),
  ).rejects.toThrow(RangeError);
  expect(factoryCalls.n).toBe(0);
  expect(posts.n).toBe(0);
});

// ---------------------------------------------------------------- independence

test("sequential operations on one client get fresh pacers", async () => {
  // Same CLIENT (not just same clock): a pacer accidentally kept on the
  // client instance would carry ramp state into the second call.
  const clock = new VClock();
  const jobDoneAt = { at: 0.3 };
  let gets = 0;
  const handler: Handler = (url, init) => {
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    gets++;
    clock.now += RTT;
    if (clock.now >= jobDoneAt.at) {
      return json(200, {
        job_id: "j",
        status: "completed",
        inbound_result: {
          allowed: true,
          blocked: false,
          filtered_content: "ok",
          triggered_rules: [],
        },
      });
    }
    return json(200, { job_id: "j", status: "processing_inbound" });
  };
  const collie = clockClient(clock, handler);
  await collie.moderate.input({ prompt: "one" });
  const first = gets;
  jobDoneAt.at = clock.now + 0.3;
  await collie.moderate.input({ prompt: "two" });
  expect(gets - first).toBe(first);
});

test("sequential operations get fresh pacers", async () => {
  const clock = new VClock();
  const first = timedJobHandler(clock, 0.3);
  const collie = clockClient(clock, first.handler);
  await collie.moderate.input({ prompt: "one" });
  // Reset the virtual job for a second, identical run.
  const base = clock.now;
  const second = timedJobHandler(clock, base + 0.3);
  const collie2 = clockClient(clock, second.handler);
  await collie2.moderate.input({ prompt: "two" });
  // A carried-over ramp would poll far fewer times; a fresh pacer repeats
  // the warm-phase pattern.
  expect(second.state.gets).toBe(first.state.gets);
});
