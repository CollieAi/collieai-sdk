"""Poll pacing per the Poll Backoff Contract.

Normative spec: the Poll Backoff Contract (v27, 2026-07-23). One :class:`PollPacer` instance per polling
OPERATION — it is stateful (start time, deadline, ramp index) and must never be
shared across concurrent jobs. The pacer owns timing only; the poll loops keep
their flow-specific terminal checks and typed errors.

Shape of the schedule:

- warm phase (first 1.5 s): the caller's exact interval — no jitter, no server
  hint. Zero-regression for fast jobs is unconditional.
- ramp: doubling 100 → 1000 ms, floored at the caller's interval, with a
  continuous jitter range whose lower bound is ALSO floored at the caller's
  interval (the caller is never polled faster than they asked, post-jitter).
- ``suggested_poll_ms`` server hints apply in the ramp only, strictly parsed.
- 429 handling: any 429 is rate pressure — it ends the warm phase and the wait
  is floored at :data:`PRESSURE_FLOOR_S` (``Retry-After`` is a *minimum*, so
  waiting longer is always compliant — ``Retry-After: 0`` must not tight-loop).
  The jitter is upward-only, and a wait that could not complete strictly
  within the remaining budget tells the caller to surface the typed 429
  instead of sleeping into a guaranteed poll-timeout.
- every pending-poll sleep is bounded by the remaining wall-clock budget.
"""
from __future__ import annotations

import math
from typing import Callable, Optional

WARM_S = 1.5
RAMP_S = (0.1, 0.2, 0.4, 0.8, 1.0)
HARD_CAP_S = 5.0
MIN_INTERVAL_S = 0.01
HINT_MIN_MS = 50
HINT_MAX_MS = 5000
# A 429 without a usable Retry-After is rate pressure: never poll faster than
# this until the server says otherwise.
PRESSURE_FLOOR_S = 1.0
# Upward-only jitter factor for Retry-After sleeps — the server's minimum is
# never violated.
RETRY_AFTER_JITTER = 0.1
# Final-poll reserve (v10): a pending-path sleep that would otherwise reach
# the deadline is shortened so ONE last GET is ISSUED with the reserve
# remaining. The reserve guarantees the ISSUE, not the completion: the
# response is accepted only if the full round trip (network + body read +
# the loop's acceptance check) completes STRICTLY within the reserve — at
# exactly 100 ms the strict boundary rejects it (v12). v14 makes the
# reserve STATEFUL: a pending response can LAND inside the reserve window
# naturally (long hinted sleep + real RTT); the FIRST such landing polls
# again immediately — one preserved final poll either way, no tail burst.
# v15: after that tail poll, polling continues at the CALLER'S OWN interval
# until the deadline (sleeping the whole remainder forfeited round trips
# that still fit the budget under small custom timeouts). Together with the
# two densification bounds (next_sleep), for round trips strictly inside
# the reserve any job completing at least
# rtt + min(RESERVE, interval + rtt) before the deadline succeeds under ANY
# schedule, hinted or plain — the universal window guarantee.
FINAL_POLL_RESERVE_S = 0.1


def parse_suggested_poll_ms(value: object) -> Optional[float]:
    """Strict wire parse of ``suggested_poll_ms``: a JSON integer (bool is NOT
    an integer here) within [50, 5000] ms → seconds; anything else → ``None``
    (ignored). The strictness is normative — a lax parse would let ``true`` or
    ``"100"`` steer pacing."""
    if type(value) is not int:  # noqa: E721 — `type is` deliberately excludes bool
        return None
    if value < HINT_MIN_MS or value > HINT_MAX_MS:
        return None
    return value / 1000.0


class PollPacer:
    """Per-operation poll pacing state machine."""

    @staticmethod
    def validate(interval_s: float, timeout_s: float) -> None:
        """Knob validation, callable BEFORE any side effect (job creation)."""
        if (
            not isinstance(interval_s, (int, float))
            or isinstance(interval_s, bool)
            or not math.isfinite(interval_s)
            or interval_s < MIN_INTERVAL_S
        ):
            raise ValueError(
                f"poll_interval_s must be a finite number >= {MIN_INTERVAL_S}s, "
                f"got {interval_s!r}"
            )
        if (
            not isinstance(timeout_s, (int, float))
            or isinstance(timeout_s, bool)
            or not math.isfinite(timeout_s)
            or timeout_s <= 0
        ):
            raise ValueError(
                f"timeout_s must be a finite number > 0, got {timeout_s!r}"
            )

    def __init__(
        self,
        *,
        interval_s: float,
        timeout_s: float,
        monotonic: Callable[[], float],
        rand: Callable[[], float],
    ) -> None:
        self.validate(interval_s, timeout_s)
        self._interval = float(interval_s)
        self._monotonic = monotonic
        self._rand = rand
        self._t0 = monotonic()
        self.deadline = self._t0 + float(timeout_s)
        # An explicit caller interval above the hard cap is honored verbatim.
        self._cap_eff = max(HARD_CAP_S, self._interval)
        self._ramp_i = 0
        self._warm_ended = False
        self._tail_poll_spent = False

    # ------------------------------------------------------------------ state

    def remaining(self) -> float:
        return self.deadline - self._monotonic()

    def in_warm(self) -> bool:
        return (not self._warm_ended) and (
            self._monotonic() - self._t0
        ) < WARM_S

    def end_warm(self) -> None:
        """Force warm exit (used on a headerless 429 — rate pressure)."""
        self._warm_ended = True

    def _bump(self) -> None:
        """Advance the ramp exactly once per completed ramp-phase sleep —
        including sleeps stretched by a hint or a 429. Never resets."""
        self._ramp_i = min(self._ramp_i + 1, len(RAMP_S) - 1)

    def _bound(self, sleep_s: float) -> float:
        """Bound by the remaining budget WITH the final-poll reserve (v10):
        a sleep that would otherwise reach the deadline is shortened so one
        last GET is ISSUED with the reserve remaining (accepted only if its
        round trip completes strictly within the reserve — v12). STATEFUL
        (v14): the first pending response that LANDS inside the reserve
        polls again immediately (the sleep-shortened path never ran, so the
        final poll is still unspent). After that tail poll, polling
        continues at the caller's own interval until the deadline (v15) —
        the caller-sanctioned density is not a burst, and sleeping the
        whole remainder forfeited round trips that still fit the budget."""
        remaining = self.remaining()
        tail = max(0.0, remaining - FINAL_POLL_RESERVE_S)
        if sleep_s <= tail:
            return max(0.0, sleep_s)
        if remaining > FINAL_POLL_RESERVE_S:
            self._tail_poll_spent = True
            return tail
        if not self._tail_poll_spent:
            self._tail_poll_spent = True
            return 0.0
        return min(self._interval, max(0.0, remaining))

    # ------------------------------------------------------------------ sleeps

    def next_sleep(self, *, suggested_poll_ms: object = None) -> float:
        """Sleep before the next status GET, after a pending 2xx response."""
        if self.in_warm():
            # Literal warm phase: exact caller interval — no jitter, no hint.
            return self._bound(self._interval)
        base = max(RAMP_S[self._ramp_i], self._interval)
        self._bump()
        hint_s = parse_suggested_poll_ms(suggested_poll_ms)
        target = min(
            max(base, hint_s) if hint_s is not None else base, self._cap_eff
        )
        # Deadline-aware densification (v14): the target never exceeds HALF
        # the remaining budget (floored at the caller's interval) — as the
        # deadline nears, the schedule geometrically approaches the plain
        # schedule's landing density instead of gambling a job completing
        # just inside the budget on one sparse final landing.
        target = min(target, max(self.remaining() / 2.0, self._interval))
        # Ramp-in bound (v15, symmetric to densification): the target never
        # exceeds HALF the operation's own elapsed time either — a slow
        # host median used to jump a young job straight from the warm phase
        # into 5 s sleeps (a 1.6 s job cost +4.4 s). The geometric ramp-in
        # caps hint overhead at ~60 % of the job's own duration while
        # converging to the full hint within a few sleeps on long jobs.
        target = min(
            target, max((self._monotonic() - self._t0) / 2.0, self._interval)
        )
        # Continuous jitter range: the lower bound is floored at the caller's
        # interval (post-jitter floor), the upper bound at cap_eff — no point
        # mass at the cap. A degenerate lo >= hi means "intentionally exact"
        # (caller-pinned interval).
        lo = max(0.8 * target, self._interval)
        hi = min(1.2 * target, self._cap_eff)
        raw = lo if hi <= lo else lo + (hi - lo) * self._rand()
        return self._bound(raw)

    def sleep_for_429(self, retry_after_s: Optional[float]) -> Optional[float]:
        """429 handling. Returns the sleep to take, or ``None`` when the caller
        must surface the typed 429 — the required wait could not complete
        strictly within the remaining budget, and sleeping into a guaranteed
        poll-timeout would only mask the rate limit.

        Any 429 is rate pressure, so the wait is floored at
        :data:`PRESSURE_FLOOR_S` even for ``Retry-After: 0`` — the header is a
        *minimum*, waiting longer is always compliant. It is ALSO floored at
        the caller's interval (v18): a 429 is rate pressure and must never
        make the caller poll FASTER than they asked — with a 10 s interval,
        ``Retry-After: 0`` used to collapse to the 1 s pressure floor, a
        10x speed-up in response to backpressure. The upward-only jitter
        is a courtesy: when the jittered value would reach the remaining
        budget (leaving no room for one more GET), it falls back to the
        un-jittered floor rather than consuming the final poll."""
        self.end_warm()
        self._bump()
        if retry_after_s is not None and retry_after_s >= 0:
            floor = max(retry_after_s, PRESSURE_FLOOR_S, self._interval)
            # Feasibility includes the final-poll reserve (v11): the wait AND
            # the final GET's issue reserve must both fit the budget — a wait
            # that leaves less than the reserve would launch a GET doomed by
            # the strict acceptance boundary.
            if floor >= self.remaining() - FINAL_POLL_RESERVE_S:
                return None
            jittered = floor * (1.0 + RETRY_AFTER_JITTER * self._rand())
            # Re-decided against ONE fresh reading AFTER the jitter draw
            # (TOCTOU): the first check can be stale by now.
            budget = self.remaining() - FINAL_POLL_RESERVE_S
            if jittered < budget:
                return jittered
            return floor if floor < budget else None
        # No usable header: poll no faster than the pressure floor.
        base = max(RAMP_S[self._ramp_i], self._interval)
        target = min(max(base, PRESSURE_FLOOR_S), self._cap_eff)
        return target if target < self.remaining() - FINAL_POLL_RESERVE_S else None
