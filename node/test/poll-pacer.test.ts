/**
 * PollPacer unit matrix (Poll Backoff Contract, amendment v27): warm/ramp
 * boundary, post-jitter caller floor, cap continuity, capEff, hint parsing
 * and placement, 429 semantics, ramp advancement, budget bounding, knob
 * validation. Mirrors sdk/python/tests/test_poll_pacer.py.
 */
import { expect, test } from "vitest";

import {
  HARD_CAP_S,
  MAX_TIMEOUT_S,
  PRESSURE_FLOOR_S,
  PollPacer,
  parseSuggestedPollMs,
} from "../src/pacing.js";

class Clock {
  now: number;
  constructor(now = 0) {
    this.now = now;
  }
  fn = (): number => this.now;
}

/** Scripted U(0,1) draws; throws if consumed when it must not be. */
class Rand {
  private readonly draws: number[];
  private readonly forbid: boolean;
  constructor(draws: number[] = [], forbid = false) {
    this.draws = [...draws];
    this.forbid = forbid;
  }
  fn = (): number => {
    if (this.forbid) throw new Error("jitter rng consumed where none is allowed");
    return this.draws.length ? this.draws.shift()! : 0.5;
  };
}

function make(
  opts: { interval?: number; timeout?: number; now?: number; rand?: Rand } = {},
): { pacer: PollPacer; clock: Clock } {
  const clock = new Clock(opts.now ?? 0);
  const rand = opts.rand ?? new Rand();
  const pacer = new PollPacer({
    intervalS: opts.interval ?? 0.05,
    timeoutS: opts.timeout ?? 30,
    monotonic: clock.fn,
    rand: rand.fn,
  });
  return { pacer, clock };
}

const P = 10; // toBeCloseTo digits — the default (2) is far too loose here

// ------------------------------------------------------------------ warm phase

test("warm is literal: no jitter, no hint", () => {
  const { pacer, clock } = make({ rand: new Rand([], true) });
  for (const t of [0.0, 0.5, 1.49]) {
    clock.now = t;
    // Hint present but MUST be ignored in warm; rng must not be consumed.
    expect(pacer.nextSleep(5000)).toBeCloseTo(0.05, P);
  }
});

test("warm to ramp boundary", () => {
  const { pacer, clock } = make({ rand: new Rand([0, 0]) });
  clock.now = 1.49;
  expect(pacer.nextSleep()).toBeCloseTo(0.05, P); // still warm
  clock.now = 1.5;
  // Ramp: base=max(0.1, 0.05)=0.1 → lo=max(0.08, 0.05)=0.08 with draw 0.
  expect(pacer.nextSleep()).toBeCloseTo(0.08, P);
});

// --------------------------------------------------------------- floors & caps

test("post-jitter caller floor", () => {
  // caller 500 ms: draw 0.0 must NOT produce 400 ms.
  const { pacer, clock } = make({ interval: 0.5, rand: new Rand([0, 1]) });
  clock.now = 2.0;
  expect(pacer.nextSleep()).toBeCloseTo(0.5, P); // base=max(0.1,0.5) → lo=max(0.4,0.5)
  expect(pacer.nextSleep()).toBeCloseTo(0.6, P); // draw 1.0 → hi=min(0.6, 5)
});

test("cap continuity: no point mass at the cap", () => {
  // target pinned at cap via hint: range must be U(0.8*cap, cap), continuous.
  // elapsed 10 s so the v15 ramp-in bound (elapsed/2 = cap) does not bind.
  const { pacer, clock } = make({ rand: new Rand([0, 0.5, 1]) });
  clock.now = 10.0;
  expect(pacer.nextSleep(5000)).toBeCloseTo(0.8 * HARD_CAP_S, P);
  expect(pacer.nextSleep(5000)).toBeCloseTo(0.9 * HARD_CAP_S, P);
  expect(pacer.nextSleep(5000)).toBeCloseTo(HARD_CAP_S, P);
});

test("capEff honors a large caller interval verbatim", () => {
  const { pacer, clock } = make({ interval: 10, timeout: 60, rand: new Rand([0, 1]) });
  clock.now = 2.0;
  expect(pacer.nextSleep()).toBeCloseTo(10, P);
  expect(pacer.nextSleep()).toBeCloseTo(10, P);
});

test("capEff is isolated by the headerless-429 path", () => {
  // The nextSleep assertions above can't FAIL on a capEff regression: with
  // a 10 s interval the post-jitter caller floor reconstructs 10 even if
  // capEff were wrongly 5. The one place capEff is observable in isolation
  // is the headerless-429 sleep min(max(base, pressureFloor), capEff) — no
  // caller floor follows, so a capEff broken to 5 s returns 5 where the
  // contract requires 10.
  const { pacer, clock } = make({ interval: 10, timeout: 300 });
  clock.now = 2.0;
  expect(pacer.sleepFor429(null)).toBeCloseTo(10, P);
});

// -------------------------------------------------------------------- the hint

test("hint parsing strictness", () => {
  expect(parseSuggestedPollMs(100)).toBeCloseTo(0.1, P);
  expect(parseSuggestedPollMs(50)).toBeCloseTo(0.05, P);
  expect(parseSuggestedPollMs(5000)).toBeCloseTo(5.0, P);
  // (Python also rejects the float spelling 100.0 — JS numbers can't
  // distinguish it, so 100.5 covers the fractional case instead.)
  for (const bad of [49, 5001, true, false, "100", 100.5, null, undefined, [100], -100]) {
    expect(parseSuggestedPollMs(bad)).toBeNull();
  }
});

test("hint honored in ramp", () => {
  const { pacer, clock } = make({ rand: new Rand([0.5]) });
  clock.now = 4.0; // elapsed/2 = 2.0: the v15 ramp-in bound does not bind
  // target = min(max(0.1, 2.0), 5) = 2.0 → U(1.6, 2.4) at 0.5 → 2.0
  expect(pacer.nextSleep(2000)).toBeCloseTo(2.0, P);
});

test("ramp-in bound halves the target after warm (v15)", () => {
  // The target never exceeds half the operation's own elapsed time — a
  // host rung of 5000 must not jump a job straight from the warm phase
  // into 5 s sleeps (the post-warm cliff: a 1.6 s job cost +4.4 s).
  const { pacer, clock } = make({ rand: new Rand([0.5, 0.5]) });
  pacer.endWarm();
  clock.now = 1.6; // elapsed 1.6 -> bound 0.8, U(0.64, 0.96) at 0.5 = 0.8
  expect(pacer.nextSleep(5000)).toBeCloseTo(0.8, P);
  clock.now = 6.0; // elapsed 6.0 -> bound 3.0, U(2.4, 3.6) at 0.5 = 3.0
  expect(pacer.nextSleep(5000)).toBeCloseTo(3.0, P);
});

test("ramp advances once per sleep, including hint-stretched sleeps", () => {
  const { pacer, clock } = make({ rand: new Rand([0, 0, 0]) });
  clock.now = 2.0;
  pacer.nextSleep(5000); // base 0.1 consumed, i -> 1
  pacer.nextSleep(5000); // base 0.2 consumed, i -> 2
  // No hint now: base must be 0.4 (advanced twice above, never reset).
  expect(pacer.nextSleep()).toBeCloseTo(Math.max(0.8 * 0.4, 0.05), P);
});

test("concurrent pacers are independent", () => {
  const a = make({ rand: new Rand([0, 0]) });
  const b = make({ rand: new Rand([0]) });
  a.clock.now = 2.0;
  a.pacer.nextSleep(); // advances a's ramp only
  b.clock.now = 2.0;
  expect(b.pacer.nextSleep()).toBeCloseTo(0.08, P); // b still at ramp[0]
});

// ---------------------------------------------------------------------- budget

test("final-poll reserve shortens a deadline-reaching sleep", () => {
  // v10: a sleep that would cross into the reserve is shortened so one
  // last GET is issued with the reserve remaining. Post-v14 this branch is
  // reached via the warm/caller-interval path — the densified ramp target
  // is capped at half the remaining budget and no longer crosses the tail
  // on its own.
  const { pacer, clock } = make({ timeout: 1 });
  clock.now = 0.87; // warm; interval 0.05 > tail 0.03 -> shortened
  expect(pacer.nextSleep()).toBeCloseTo(0.03, P);
  // The shortened sleep SPENDS the tail poll; after it polling continues
  // at the CALLER'S OWN interval (v15) — not a sleep to the deadline,
  // which forfeited round trips that still fit the budget.
  clock.now = 0.93; // remaining 0.07 <= reserve, tail spent
  expect(pacer.nextSleep()).toBeCloseTo(0.05, P);
});

test("densification halves the target near the deadline (v14)", () => {
  // The target never exceeds half the remaining budget (floored at the
  // caller's interval) — a 5 s hint with 4 s remaining paces at 2 s.
  const { pacer, clock } = make({ timeout: 30, rand: new Rand([0.5, 0.5]) });
  pacer.endWarm();
  clock.now = 26.0; // remaining 4.0 -> target 2.0, U(1.6, 2.4) at 0.5 = 2.0
  expect(pacer.nextSleep(5000)).toBeCloseTo(2.0, P);
  clock.now = 29.0; // remaining 1.0 -> target 0.5, U(0.4, 0.6) at 0.5 = 0.5
  expect(pacer.nextSleep(5000)).toBeCloseTo(0.5, P);
});

test("first inside-reserve landing polls immediately, then caller-paced (v15)", () => {
  // Stateful tail poll (v14) + caller-paced tail (v15): a pending response
  // LANDING inside the reserve (the sleep-shortened path never ran, so the
  // final poll is still unspent) polls again immediately; afterwards
  // polling continues at min(interval, remaining) until the deadline.
  const { pacer, clock } = make({ timeout: 1 });
  clock.now = 0.96; // remaining 0.04 <= reserve; tail poll unspent
  expect(pacer.nextSleep()).toBe(0);
  expect(pacer.nextSleep()).toBeCloseTo(0.04, P); // min(interval, 0.04)
  expect(pacer.nextSleep()).toBeCloseTo(0.04, P);
  clock.now = 0.93; // remaining 0.07 > interval: full caller interval
  expect(pacer.nextSleep()).toBeCloseTo(0.05, P);
});

test("sleep bounded by the remaining budget", () => {
  const { pacer, clock } = make({ timeout: 1.0 });
  clock.now = 0.97;
  expect(pacer.nextSleep()).toBe(0); // v14: the unspent tail poll fires
  expect(pacer.nextSleep()).toBeCloseTo(0.03, P);
  clock.now = 1.5;
  expect(pacer.nextSleep()).toBe(0);
  expect(pacer.remaining()).toBeLessThan(0);
});

// ------------------------------------------------------------------ validation

test.each([0, -1, 0.009, NaN, Infinity, true, "x"])(
  "interval validation rejects %s",
  (interval) => {
    expect(() => PollPacer.validate(interval, 30)).toThrow(RangeError);
  },
);

test.each([0, -5, NaN, Infinity, false, "x"])(
  "timeout validation rejects %s",
  (timeout) => {
    expect(() => PollPacer.validate(0.05, timeout)).toThrow(RangeError);
  },
);

test("timeout beyond the runtime timer bound is rejected explicitly", () => {
  // AbortSignal.timeout's delay is a uint32 (2^31-1 ms): a bigger budget
  // must fail validation loudly, not silently end ~24.8 days in.
  expect(() => PollPacer.validate(0.05, MAX_TIMEOUT_S + 1)).toThrow(RangeError);
  expect(() => PollPacer.validate(0.05, MAX_TIMEOUT_S)).not.toThrow();
});

// ------------------------------------------------------------------------ 429s

test("429 never polls faster than the caller interval (v18)", () => {
  // A 429 is rate pressure — it must never make the caller poll FASTER than
  // their own interval. With a 10 s interval, a VALID Retry-After: 0 used to
  // collapse to the 1 s pressure floor. (The headerless path was already
  // correct — it floors at max(base, 1 s) with base >= interval; only the
  // with-header path lacked the interval term.) The with-header floor now
  // includes the caller interval; default-interval tests are unaffected.
  {
    const { pacer } = make({ interval: 10, timeout: 300, rand: new Rand([0]) });
    expect(pacer.sleepFor429(0)).toBeCloseTo(10, P); // max(0, 1, 10)
  }
  {
    const { pacer } = make({ interval: 10, timeout: 300, rand: new Rand([0]) });
    expect(pacer.sleepFor429(1)).toBeCloseTo(10, P); // max(1, 1, 10)
  }
  {
    const { pacer } = make({ interval: 10, timeout: 300, rand: new Rand([0]) });
    expect(pacer.sleepFor429(30)).toBeCloseTo(30, P); // server minimum honored
  }
});

test("429 Retry-After is a minimum with upward jitter", () => {
  const { pacer } = make({ rand: new Rand([0, 1]) });
  expect(pacer.sleepFor429(2.0)).toBeCloseTo(2.0, P);
  expect(pacer.sleepFor429(2.0)).toBeCloseTo(2.2, P);
});

test("429 Retry-After beyond the budget surfaces", () => {
  const { pacer } = make({ timeout: 1.0 });
  expect(pacer.sleepFor429(2.0)).toBeNull();
});

test("429 wait must leave the final-poll reserve", () => {
  // v11: feasibility includes the final-poll reserve. remaining=2.05,
  // Retry-After=2 leaves only 50 ms for the final GET — surface the typed
  // 429; with remaining=2.2 the floor fits and jitter falls back to it.
  const { pacer } = make({ timeout: 2.05, rand: new Rand([1]) });
  expect(pacer.sleepFor429(2.0)).toBeNull();
  const { pacer: pacer2 } = make({ timeout: 2.2, rand: new Rand([1]) });
  expect(pacer2.sleepFor429(2.0)).toBeCloseTo(2.0, P);
});

test("429 Retry-After equal to remaining surfaces", () => {
  // The wait must complete STRICTLY within the budget: equality leaves no
  // room for the next GET, so the typed 429 surfaces now.
  const { pacer } = make({ timeout: 2.0 });
  expect(pacer.sleepFor429(2.0)).toBeNull();
});

test("429 headerless ends warm and floors at pressure", () => {
  const { pacer, clock } = make({ rand: new Rand([0]) });
  clock.now = 0.2; // inside warm
  expect(pacer.sleepFor429(null)).toBeCloseTo(PRESSURE_FLOOR_S, P);
  // Warm has ended: the next sleep is ramp-phase (jittered, not 50 ms).
  expect(pacer.inWarm()).toBe(false);
  expect(pacer.nextSleep()).not.toBeCloseTo(0.05, P);
});

test("429 headerless beyond the budget surfaces", () => {
  const { pacer } = make({ timeout: 0.5 });
  expect(pacer.sleepFor429(null)).toBeNull();
});

test("429 TOCTOU: a floor that no longer fits at return time surfaces", () => {
  // The feasibility decision is re-taken against a fresh reading AFTER the
  // jitter draw: if time advanced between the two readings and the floor no
  // longer fits, surface the typed 429 instead of returning a floor that
  // sleeps into a guaranteed poll-timeout.
  const ticks = [0.0, 8.85, 9.0006]; // ctor, first check, final check
  let i = 0;
  const pacer = new PollPacer({
    intervalS: 0.05,
    timeoutS: 10,
    monotonic: () => ticks[i++]!,
    rand: () => 1.0,
  });
  expect(pacer.sleepFor429(0)).toBeNull();
});

test("429 Retry-After: 0 floors at the pressure floor", () => {
  // Retry-After: 0 is a minimum, not a license to tight-loop: any 429 is
  // rate pressure, so the wait floors at PRESSURE_FLOOR_S (still jittered
  // upward-only).
  const { pacer } = make({ rand: new Rand([0, 1]) });
  expect(pacer.sleepFor429(0)).toBeCloseTo(PRESSURE_FLOOR_S, P);
  expect(pacer.sleepFor429(0)).toBeCloseTo(PRESSURE_FLOOR_S * 1.1, P);
});

test("429 Retry-After: 0 with a tiny budget surfaces", () => {
  // Under pressure with less than the floor remaining, an immediate re-poll
  // would violate the floor and a floored sleep would overrun the budget —
  // surface the typed 429.
  const { pacer } = make({ timeout: 0.5 });
  expect(pacer.sleepFor429(0)).toBeNull();
});
