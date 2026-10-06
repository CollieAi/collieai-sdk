/**
 * Poll pacing per the Poll Backoff Contract.
 *
 * Normative spec: the Poll Backoff Contract (v27, 2026-07-23). One
 * `PollPacer` instance per
 * polling OPERATION — it is stateful (start time, deadline, ramp index) and
 * must never be shared across concurrent jobs. The pacer owns timing only;
 * the poll loops keep their flow-specific terminal checks and typed errors.
 *
 * Shape of the schedule:
 *
 * - warm phase (first 1.5 s): the caller's exact interval — no jitter, no
 *   server hint. Zero-regression for fast jobs is unconditional.
 * - ramp: doubling 100 → 1000 ms, floored at the caller's interval, with a
 *   continuous jitter range whose lower bound is ALSO floored at the caller's
 *   interval (the caller is never polled faster than they asked, post-jitter).
 * - `suggested_poll_ms` server hints apply in the ramp only, strictly parsed.
 * - 429 handling: any 429 is rate pressure — it ends the warm phase and the
 *   wait is floored at `PRESSURE_FLOOR_S` (`Retry-After` is a *minimum*, so
 *   waiting longer is always compliant — `Retry-After: 0` must not
 *   tight-loop). The jitter is upward-only, and a wait that could not
 *   complete strictly within the remaining budget tells the caller to
 *   surface the typed 429 instead of sleeping into a guaranteed
 *   poll-timeout.
 * - every pending-poll sleep is bounded by the remaining wall-clock budget.
 *
 * All times are SECONDS from a monotonic source — the unit is normative;
 * the client's default seam is `performance.now() / 1000`.
 */

export const WARM_S = 1.5;
export const RAMP_S = [0.1, 0.2, 0.4, 0.8, 1.0] as const;
export const HARD_CAP_S = 5.0;
export const MIN_INTERVAL_S = 0.01;
export const HINT_MIN_MS = 50;
export const HINT_MAX_MS = 5000;
// A 429 is rate pressure: never poll faster than this until the server says
// otherwise, even when Retry-After says 0.
export const PRESSURE_FLOOR_S = 1.0;
// Upward-only jitter factor for Retry-After sleeps — the server's minimum is
// never violated.
export const RETRY_AFTER_JITTER = 0.1;
// Final-poll reserve (v10): a pending-path sleep that would otherwise reach
// the deadline is shortened so ONE last GET is ISSUED with the reserve
// remaining. The reserve guarantees the ISSUE, not the completion: the
// response is accepted only if the full round trip (network + body read +
// the loop's acceptance check) completes STRICTLY within the reserve — at
// exactly 100 ms the strict boundary rejects it (v12). v14 makes the
// reserve STATEFUL: the first pending response LANDING inside the reserve
// polls again immediately — one preserved final poll, no tail burst. v15:
// after that tail poll, polling continues at the CALLER'S OWN interval
// until the deadline (sleeping the whole remainder forfeited round trips
// that still fit the budget under small custom timeouts). Together with
// the two densification bounds, for round trips strictly inside the
// reserve any job completing at least rtt + min(RESERVE, interval + rtt)
// before the deadline succeeds under ANY schedule — the universal window
// guarantee.
export const FINAL_POLL_RESERVE_S = 0.1;
// setTimeout's TIMEOUT_MAX — AbortSignal.timeout validates its delay as a
// uint32 and larger values throw.
const MAX_TIMER_MS = 2 ** 31 - 1;
// The public timeoutS knob is capped at the runtime's timer bound (~24.8
// days) EXPLICITLY — validate() rejects bigger budgets instead of letting
// deadlineSignal() silently shorten them (v5).
export const MAX_TIMEOUT_S = Math.floor(MAX_TIMER_MS / 1000);

/**
 * Strict wire parse of `suggested_poll_ms`: a JSON integer within
 * [50, 5000] ms → seconds; anything else → `null` (ignored). The strictness
 * is normative — a lax parse would let `true` or `"100"` steer pacing.
 * (`typeof` excludes booleans/strings; `Number.isInteger` excludes
 * fractional values. JS numbers can't distinguish Python's rejected `100.0`
 * float spelling — JSON.parse yields the same integer.)
 */
export function parseSuggestedPollMs(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isInteger(value)) return null;
  if (value < HINT_MIN_MS || value > HINT_MAX_MS) return null;
  return value / 1000;
}

/**
 * Per-attempt COOPERATIVE deadline bound (PBC-08). `AbortSignal.timeout` requires an
 * INTEGER delay (Node validates uint32); ceil so the bound never undercuts
 * the remaining budget. The clamp is defense in depth only — validate()
 * rejects budgets beyond MAX_TIMEOUT_S. The timer can fire late (integer
 * ceil, event-loop lag), which is why the loops also enforce the strict
 * acceptance boundary (v5) after every GET.
 */
export function deadlineSignal(remainingS: number): AbortSignal {
  return AbortSignal.timeout(Math.min(Math.ceil(remainingS * 1000), MAX_TIMER_MS));
}

/**
 * Structural taxonomy check (v5): is this rejection the per-attempt deadline
 * bound firing? A spec-compliant fetch rejects with the aborting signal's
 * `reason` — a unique object per signal — so identity is exact; an error
 * whose `cause` is that reason covers wrapped body-read failures.
 * Deliberately NO name-based fallback: an AbortError/TimeoutError from an
 * unrelated source arriving after the deadline fired must stay a connection
 * error (a generic-name check would re-admit exactly that ambiguity).
 */
export function isDeadlineAbort(e: unknown, deadlineSignal: AbortSignal): boolean {
  if (!deadlineSignal.aborted) return false;
  if (e === deadlineSignal.reason) return true;
  return e instanceof Error && e.cause === deadlineSignal.reason;
}

export interface PollPacerOptions {
  intervalS: number;
  timeoutS: number;
  monotonic: () => number;
  rand: () => number;
}

/** Per-operation poll pacing state machine. */
export class PollPacer {
  private readonly intervalS: number;
  private readonly monotonicFn: () => number;
  private readonly randFn: () => number;
  private readonly t0: number;
  readonly deadline: number;
  private readonly capEff: number;
  private rampI = 0;
  private warmEnded = false;
  private tailPollSpent = false;

  /** Knob validation, callable BEFORE any side effect (job creation). */
  static validate(intervalS: unknown, timeoutS: unknown): void {
    if (
      typeof intervalS !== "number" ||
      !Number.isFinite(intervalS) ||
      intervalS < MIN_INTERVAL_S
    ) {
      throw new RangeError(
        `pollIntervalS must be a finite number >= ${MIN_INTERVAL_S}s, ` +
          `got ${String(intervalS)}`,
      );
    }
    if (typeof timeoutS !== "number" || !Number.isFinite(timeoutS) || timeoutS <= 0) {
      throw new RangeError(`timeoutS must be a finite number > 0, got ${String(timeoutS)}`);
    }
    if (timeoutS > MAX_TIMEOUT_S) {
      throw new RangeError(
        `timeoutS must be <= ${MAX_TIMEOUT_S}s (the runtime's 2^31-1 ms timer ` +
          `bound), got ${String(timeoutS)}`,
      );
    }
  }

  constructor(opts: PollPacerOptions) {
    PollPacer.validate(opts.intervalS, opts.timeoutS);
    this.intervalS = opts.intervalS;
    this.monotonicFn = opts.monotonic;
    this.randFn = opts.rand;
    this.t0 = opts.monotonic();
    this.deadline = this.t0 + opts.timeoutS;
    // An explicit caller interval above the hard cap is honored verbatim.
    this.capEff = Math.max(HARD_CAP_S, this.intervalS);
  }

  // ------------------------------------------------------------------ state

  remaining(): number {
    return this.deadline - this.monotonicFn();
  }

  inWarm(): boolean {
    return !this.warmEnded && this.monotonicFn() - this.t0 < WARM_S;
  }

  /** Force warm exit (used on any 429 — rate pressure). */
  endWarm(): void {
    this.warmEnded = true;
  }

  /** Advance the ramp exactly once per completed ramp-phase sleep —
   * including sleeps stretched by a hint or a 429. Never resets. */
  private bump(): void {
    this.rampI = Math.min(this.rampI + 1, RAMP_S.length - 1);
  }

  /** Bound by the remaining budget WITH the final-poll reserve (v10): a
   * sleep that would otherwise reach the deadline is shortened so one last
   * GET is ISSUED with the reserve remaining (accepted only if its round
   * trip completes strictly within the reserve — v12). STATEFUL (v14): the
   * first pending response that LANDS inside the reserve polls again
   * immediately (the sleep-shortened path never ran, so the final poll is
   * still unspent). After that tail poll, polling continues at the
   * caller's own interval until the deadline (v15) — the caller-sanctioned
   * density is not a burst, and sleeping the whole remainder forfeited
   * round trips that still fit the budget. */
  private bound(sleepS: number): number {
    const remaining = this.remaining();
    const tail = Math.max(0, remaining - FINAL_POLL_RESERVE_S);
    if (sleepS <= tail) return Math.max(0, sleepS);
    if (remaining > FINAL_POLL_RESERVE_S) {
      this.tailPollSpent = true;
      return tail;
    }
    if (!this.tailPollSpent) {
      this.tailPollSpent = true;
      return 0;
    }
    return Math.min(this.intervalS, Math.max(0, remaining));
  }

  // ----------------------------------------------------------------- sleeps

  /** Sleep before the next status GET, after a pending 2xx response. */
  nextSleep(suggestedPollMs?: unknown): number {
    if (this.inWarm()) {
      // Literal warm phase: exact caller interval — no jitter, no hint.
      return this.bound(this.intervalS);
    }
    const base = Math.max(RAMP_S[this.rampI]!, this.intervalS);
    this.bump();
    const hintS = parseSuggestedPollMs(suggestedPollMs);
    let target = Math.min(hintS !== null ? Math.max(base, hintS) : base, this.capEff);
    // Deadline-aware densification (v14): the target never exceeds HALF
    // the remaining budget (floored at the caller's interval) — as the
    // deadline nears, the schedule geometrically approaches the plain
    // schedule's landing density instead of gambling a job completing
    // just inside the budget on one sparse final landing.
    target = Math.min(target, Math.max(this.remaining() / 2, this.intervalS));
    // Ramp-in bound (v15, symmetric to densification): the target never
    // exceeds HALF the operation's own elapsed time either — a slow host
    // median used to jump a young job straight from the warm phase into
    // 5 s sleeps (a 1.6 s job cost +4.4 s). The geometric ramp-in caps
    // hint overhead at ~60 % of the job's own duration while converging
    // to the full hint within a few sleeps on long jobs.
    target = Math.min(
      target,
      Math.max((this.monotonicFn() - this.t0) / 2, this.intervalS),
    );
    // Continuous jitter range: the lower bound is floored at the caller's
    // interval (post-jitter floor), the upper bound at capEff — no point
    // mass at the cap. A degenerate lo >= hi means "intentionally exact"
    // (caller-pinned interval).
    const lo = Math.max(0.8 * target, this.intervalS);
    const hi = Math.min(1.2 * target, this.capEff);
    const raw = hi <= lo ? lo : lo + (hi - lo) * this.randFn();
    return this.bound(raw);
  }

  /**
   * 429 handling. Returns the sleep to take, or `null` when the caller must
   * surface the typed 429 — the required wait could not complete strictly
   * within the remaining budget, and sleeping into a guaranteed
   * poll-timeout would only mask the rate limit.
   *
   * Any 429 is rate pressure, so the wait is floored at `PRESSURE_FLOOR_S`
   * even for `Retry-After: 0` — the header is a *minimum*, waiting longer
   * is always compliant. It is ALSO floored at the caller's interval (v18):
   * a 429 is rate pressure and must never make the caller poll FASTER than
   * they asked — with a 10 s interval, `Retry-After: 0` used to collapse to
   * the 1 s pressure floor, a 10x speed-up in response to backpressure. The
   * upward-only jitter is a courtesy: when the jittered value would reach
   * the remaining budget (leaving no room for one more GET), it falls back
   * to the un-jittered floor rather than consuming the final poll.
   */
  sleepFor429(retryAfterS: number | null): number | null {
    this.endWarm();
    this.bump();
    if (retryAfterS !== null && retryAfterS >= 0) {
      const floor = Math.max(retryAfterS, PRESSURE_FLOOR_S, this.intervalS);
      // Feasibility includes the final-poll reserve (v11): the wait AND the
      // final GET's issue reserve must both fit the budget — a wait leaving
      // less than the reserve would launch a GET doomed by the strict
      // acceptance boundary.
      if (floor >= this.remaining() - FINAL_POLL_RESERVE_S) return null;
      const jittered = floor * (1.0 + RETRY_AFTER_JITTER * this.randFn());
      // Re-decided against ONE fresh reading AFTER the jitter draw (TOCTOU).
      const budget = this.remaining() - FINAL_POLL_RESERVE_S;
      if (jittered < budget) return jittered;
      return floor < budget ? floor : null;
    }
    // No usable header: poll no faster than the pressure floor.
    const base = Math.max(RAMP_S[this.rampI]!, this.intervalS);
    const target = Math.min(Math.max(base, PRESSURE_FLOOR_S), this.capEff);
    return target < this.remaining() - FINAL_POLL_RESERVE_S ? target : null;
  }
}
