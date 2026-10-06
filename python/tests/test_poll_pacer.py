"""PollPacer unit matrix (Poll Backoff Contract, amendment v27): warm/ramp
boundary, post-jitter caller floor, cap continuity, cap_eff, hint parsing and
placement, 429 semantics, ramp advancement, budget bounding, knob validation."""
import math

import pytest

from collieai._pacing import (
    HARD_CAP_S,
    MIN_INTERVAL_S,
    PRESSURE_FLOOR_S,
    PollPacer,
    parse_suggested_poll_ms,
)


class Clock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class Rand:
    """Scripted U(0,1) draws; raises if consumed when it must not be."""

    def __init__(self, *draws: float, forbid: bool = False) -> None:
        self.draws = list(draws)
        self.forbid = forbid
        self.calls = 0

    def __call__(self) -> float:
        assert not self.forbid, "jitter rng consumed where none is allowed"
        self.calls += 1
        return self.draws.pop(0) if self.draws else 0.5


def make(interval=0.05, timeout=30.0, now=0.0, rand=None):
    clock = Clock(now)
    pacer = PollPacer(
        interval_s=interval,
        timeout_s=timeout,
        monotonic=clock,
        rand=rand if rand is not None else Rand(),
    )
    return pacer, clock


# ---------------------------------------------------------------- warm phase

def test_warm_is_literal_no_jitter_no_hint():
    pacer, clock = make(rand=Rand(forbid=True))
    for t in (0.0, 0.5, 1.49):
        clock.now = t
        # Hint present but MUST be ignored in warm; rng must not be consumed.
        assert pacer.next_sleep(suggested_poll_ms=5000) == pytest.approx(0.05)


def test_warm_to_ramp_boundary():
    pacer, clock = make(rand=Rand(0.0, 0.0))
    clock.now = 1.49
    assert pacer.next_sleep() == pytest.approx(0.05)  # still warm
    clock.now = 1.5
    # Ramp: base=max(0.1, 0.05)=0.1 → lo=max(0.08, 0.05)=0.08 with draw 0.
    assert pacer.next_sleep() == pytest.approx(0.08)


# ------------------------------------------------------------- floors & caps

def test_post_jitter_caller_floor():
    # caller 500 ms: draw 0.0 must NOT produce 400 ms.
    pacer, clock = make(interval=0.5, rand=Rand(0.0, 1.0))
    clock.now = 2.0
    s1 = pacer.next_sleep()  # base=max(0.1,0.5)=0.5 → lo=max(0.4,0.5)=0.5
    assert s1 == pytest.approx(0.5)
    s2 = pacer.next_sleep()  # draw 1.0 → hi=min(0.6, 5)=0.6
    assert s2 == pytest.approx(0.6)


def test_cap_continuity_no_point_mass():
    # target pinned at cap via hint: range must be U(0.8*cap, cap), continuous.
    # elapsed 10 s so the v15 ramp-in bound (elapsed/2 = cap) does not bind.
    pacer, clock = make(rand=Rand(0.0, 0.5, 1.0))
    clock.now = 10.0
    sleeps = [pacer.next_sleep(suggested_poll_ms=5000) for _ in range(3)]
    assert sleeps[0] == pytest.approx(0.8 * HARD_CAP_S)
    assert sleeps[1] == pytest.approx(0.9 * HARD_CAP_S)
    assert sleeps[2] == pytest.approx(HARD_CAP_S)


def test_cap_eff_honors_large_caller_interval():
    # Explicit caller interval above the hard cap is honored verbatim.
    pacer, clock = make(interval=10.0, timeout=60.0, rand=Rand(0.0, 1.0))
    clock.now = 2.0
    assert pacer.next_sleep() == pytest.approx(10.0)
    assert pacer.next_sleep() == pytest.approx(10.0)


def test_cap_eff_is_isolated_by_the_headerless_429_path():
    """The next_sleep assertions above can't actually FAIL on a cap_eff
    regression: with a 10 s interval the post-jitter caller floor
    (lo = max(0.8·target, interval) = 10) reconstructs 10 even if cap_eff
    were wrongly 5 (the degenerate window returns lo). The ONE place cap_eff
    is observable in isolation is the headerless-429 sleep
    `min(max(base, pressure_floor), cap_eff)` — no caller floor follows, so
    a cap_eff broken to 5 s returns 5 here where the contract requires 10."""
    pacer, clock = make(interval=10.0, timeout=300.0)
    clock.now = 2.0
    assert pacer.sleep_for_429(None) == pytest.approx(10.0)


# ----------------------------------------------------------------- the hint

def test_hint_parsing_strictness():
    assert parse_suggested_poll_ms(100) == pytest.approx(0.1)
    assert parse_suggested_poll_ms(50) == pytest.approx(0.05)
    assert parse_suggested_poll_ms(5000) == pytest.approx(5.0)
    for bad in (49, 5001, True, False, "100", 100.0, None, [100], -100):
        assert parse_suggested_poll_ms(bad) is None


def test_hint_honored_in_ramp():
    pacer, clock = make(rand=Rand(0.5))
    clock.now = 4.0  # elapsed/2 = 2.0: the v15 ramp-in bound does not bind
    # target = min(max(0.1, 2.0), 5) = 2.0 → U(1.6, 2.4) at 0.5 → 2.0
    assert pacer.next_sleep(suggested_poll_ms=2000) == pytest.approx(2.0)


def test_ramp_in_bound_halves_the_target_after_warm():
    """v15: the target never exceeds half the operation's own elapsed time
    — a host rung of 5000 must not jump a job straight from the warm phase
    into 5 s sleeps (the post-warm cliff: a 1.6 s job cost +4.4 s)."""
    pacer, clock = make(rand=Rand(0.5, 0.5))
    pacer.end_warm()
    clock.now = 1.6  # elapsed 1.6 -> bound 0.8, U(0.64, 0.96) at 0.5 = 0.8
    assert pacer.next_sleep(suggested_poll_ms=5000) == pytest.approx(0.8)
    clock.now = 6.0  # elapsed 6.0 -> bound 3.0, U(2.4, 3.6) at 0.5 = 3.0
    assert pacer.next_sleep(suggested_poll_ms=5000) == pytest.approx(3.0)


def test_ramp_advances_once_per_sleep_including_hint_stretched():
    pacer, clock = make(rand=Rand(0.0, 0.0, 0.0))
    clock.now = 2.0
    pacer.next_sleep(suggested_poll_ms=5000)  # base 0.1 consumed, i -> 1
    pacer.next_sleep(suggested_poll_ms=5000)  # base 0.2 consumed, i -> 2
    # No hint now: base must be 0.4 (advanced twice above, never reset).
    assert pacer.next_sleep() == pytest.approx(max(0.8 * 0.4, 0.05))


def test_concurrent_pacers_are_independent():
    a, clock_a = make(rand=Rand(0.0, 0.0))
    b, clock_b = make(rand=Rand(0.0))
    clock_a.now = 2.0
    a.next_sleep()  # advances a's ramp only
    clock_b.now = 2.0
    assert b.next_sleep() == pytest.approx(0.08)  # b still at ramp[0]


# ------------------------------------------------------------------- budget

def test_final_poll_reserve_shortens_a_deadline_reaching_sleep():
    """v10: a sleep that would cross into the reserve is shortened so one
    last GET is issued with the reserve remaining. Post-v14 this branch is
    reached via the warm/caller-interval path — the densified ramp target
    is capped at half the remaining budget and no longer crosses the tail
    on its own."""
    pacer, clock = make(timeout=1.0)
    clock.now = 0.87  # warm; interval 0.05 > tail 0.03 -> shortened
    assert pacer.next_sleep() == pytest.approx(0.03)
    # The shortened sleep SPENDS the tail poll; after it polling continues
    # at the CALLER'S OWN interval (v15) — not a sleep to the deadline,
    # which forfeited round trips that still fit the budget.
    clock.now = 0.93  # remaining 0.07 <= reserve, tail spent
    assert pacer.next_sleep() == pytest.approx(0.05)


def test_densification_halves_the_target_near_the_deadline():
    """v14: the target never exceeds half the remaining budget (floored at
    the caller's interval) — a 5 s hint with 4 s remaining paces at 2 s;
    the jitter range follows the densified target."""
    pacer, clock = make(timeout=30.0, rand=Rand(0.5, 0.5))
    pacer.end_warm()
    clock.now = 26.0  # remaining 4.0 -> target 2.0, U(1.6, 2.4) at 0.5 = 2.0
    assert pacer.next_sleep(suggested_poll_ms=5000) == pytest.approx(2.0)
    clock.now = 29.0  # remaining 1.0 -> target 0.5, U(0.4, 0.6) at 0.5 = 0.5
    assert pacer.next_sleep(suggested_poll_ms=5000) == pytest.approx(0.5)


def test_first_inside_reserve_landing_polls_immediately_then_caller_paced():
    """v14 stateful tail poll + v15 caller-paced tail: a pending response
    LANDING inside the reserve (the sleep-shortened path never ran, so the
    final poll is still unspent) polls again immediately; afterwards
    polling continues at min(interval, remaining) until the deadline."""
    pacer, clock = make(timeout=1.0)
    clock.now = 0.96  # remaining 0.04 <= reserve; tail poll unspent
    assert pacer.next_sleep() == 0.0
    assert pacer.next_sleep() == pytest.approx(0.04)  # min(interval, 0.04)
    assert pacer.next_sleep() == pytest.approx(0.04)
    clock.now = 0.93  # remaining 0.07 > interval: full caller interval
    assert pacer.next_sleep() == pytest.approx(0.05)


def test_sleep_bounded_by_remaining():
    pacer, clock = make(timeout=1.0)
    clock.now = 0.97
    assert pacer.next_sleep() == 0.0  # v14: the unspent tail poll fires
    assert pacer.next_sleep() == pytest.approx(0.03)
    clock.now = 1.5
    assert pacer.next_sleep() == 0.0
    assert pacer.remaining() < 0


# --------------------------------------------------------------- validation

@pytest.mark.parametrize("interval", [0.0, -1.0, 0.009, math.nan, math.inf, True, "x"])
def test_interval_validation(interval):
    with pytest.raises(ValueError):
        PollPacer.validate(interval, 30.0)


@pytest.mark.parametrize("timeout", [0.0, -5.0, math.nan, math.inf, False, "x"])
def test_timeout_validation(timeout):
    with pytest.raises(ValueError):
        PollPacer.validate(0.05, timeout)


# --------------------------------------------------------------------- 429s

def test_429_retry_after_is_a_minimum_with_upward_jitter():
    pacer, clock = make(rand=Rand(0.0, 1.0))
    assert pacer.sleep_for_429(2.0) == pytest.approx(2.0)
    assert pacer.sleep_for_429(2.0) == pytest.approx(2.2)


def test_429_never_polls_faster_than_the_caller_interval():
    """v18: a 429 is rate pressure — it must never make the caller poll
    FASTER than their own interval. With a 10 s interval, a VALID
    `Retry-After: 0` used to collapse to the 1 s pressure floor — a 10x
    speed-up in response to backpressure. (The headerless path was already
    correct: it floors at `max(base, 1 s)` where `base ≥ interval`; only the
    with-header path lacked the interval term.) The with-header floor now
    includes the caller interval. Default-interval 429 tests are unaffected:
    `max(ra, 1, 0.05) == max(ra, 1)`."""
    pacer, clock = make(interval=10.0, timeout=300.0, rand=Rand(0.0))
    # Retry-After: 0 -> floor = max(0, 1, 10) = 10, jitter 0.0 keeps it.
    assert pacer.sleep_for_429(0.0) == pytest.approx(10.0)
    # Retry-After: 1 -> still floored at the 10 s interval.
    pacer2, _ = make(interval=10.0, timeout=300.0, rand=Rand(0.0))
    assert pacer2.sleep_for_429(1.0) == pytest.approx(10.0)
    # A Retry-After ABOVE the interval is honored verbatim (server minimum).
    pacer3, _ = make(interval=10.0, timeout=300.0, rand=Rand(0.0))
    assert pacer3.sleep_for_429(30.0) == pytest.approx(30.0)


def test_429_retry_after_beyond_budget_surfaces():
    pacer, clock = make(timeout=1.0)
    assert pacer.sleep_for_429(2.0) is None


def test_429_wait_must_leave_the_final_poll_reserve():
    # v11: feasibility includes the final-poll reserve. remaining=2.05,
    # Retry-After=2 leaves only 50 ms for the final GET — less than the
    # reserve — so the typed 429 surfaces; with remaining=2.2 the floor
    # fits and the jitter falls back to it (never repolling before
    # Retry-After).
    pacer, clock = make(timeout=2.05, rand=Rand(1.0))
    assert pacer.sleep_for_429(2.0) is None
    pacer2, _ = make(timeout=2.2, rand=Rand(1.0))
    assert pacer2.sleep_for_429(2.0) == pytest.approx(2.0)


def test_429_retry_after_equal_to_remaining_surfaces():
    # The wait must complete STRICTLY within the budget: equality leaves no
    # room for the next GET, so the typed 429 surfaces now.
    pacer, clock = make(timeout=2.0)
    assert pacer.sleep_for_429(2.0) is None


def test_429_headerless_ends_warm_and_floors_at_pressure():
    pacer, clock = make(rand=Rand(0.0))
    clock.now = 0.2  # inside warm
    sleep = pacer.sleep_for_429(None)
    assert sleep == pytest.approx(PRESSURE_FLOOR_S)
    # Warm has ended: the next sleep is ramp-phase (jittered, not 50 ms).
    assert not pacer.in_warm()
    nxt = pacer.next_sleep()
    assert nxt != pytest.approx(0.05)


def test_429_headerless_beyond_budget_surfaces():
    pacer, clock = make(timeout=0.5)
    assert pacer.sleep_for_429(None) is None


def test_429_toctou_floor_no_longer_fitting_surfaces():
    """The feasibility decision is re-taken against a fresh reading AFTER the
    jitter draw: if time advanced between the two readings and the floor no
    longer fits, surface the typed 429 instead of returning a floor that
    sleeps into a guaranteed poll-timeout."""
    ticks = iter([0.0, 8.85, 9.0006])  # ctor, first check, final check
    pacer = PollPacer(
        interval_s=0.05, timeout_s=10.0,
        monotonic=lambda: next(ticks), rand=lambda: 1.0,
    )
    assert pacer.sleep_for_429(0.0) is None


def test_429_zero_retry_after_floors_at_pressure():
    # Retry-After: 0 is a minimum, not a license to tight-loop: any 429 is
    # rate pressure, so the wait floors at PRESSURE_FLOOR_S (still jittered
    # upward-only).
    pacer, clock = make(rand=Rand(0.0, 1.0))
    assert pacer.sleep_for_429(0.0) == pytest.approx(PRESSURE_FLOOR_S)
    assert pacer.sleep_for_429(0.0) == pytest.approx(PRESSURE_FLOOR_S * 1.1)


def test_429_zero_retry_after_with_tiny_budget_surfaces():
    # Under pressure with less than the floor remaining, an immediate re-poll
    # would violate the floor and a floored sleep would overrun the budget —
    # surface the typed 429.
    pacer, clock = make(timeout=0.5)
    assert pacer.sleep_for_429(0.0) is None


# ---- mutation-testing kills (v26): mutmut over _pacing.py found these gaps ----

def test_elapsed_is_relative_to_t0_not_the_absolute_clock():
    """The warm check and the ramp-in bound key off ELAPSED (`monotonic - t0`),
    not the absolute clock. A virtual clock started at t0=0 makes `-t0` and
    `+t0` identical, so those mutants survived — pin them with a non-zero t0."""
    # Warm: 0.5 s of ELAPSED time, but the absolute clock reads ~100.
    p, clk = make(interval=0.05, now=100.0)
    clk.now = 100.5
    assert p.in_warm()
    assert p.next_sleep() == pytest.approx(0.05)
    # Ramp-in: post-warm at small ELAPSED (1.6 s), a big hint is bounded by
    # elapsed/2 = 0.8, NOT by the absolute clock (~100, which would not bind).
    p2, clk2 = make(interval=0.05, now=100.0, rand=Rand(0.0))
    clk2.now = 101.6
    assert p2.next_sleep(suggested_poll_ms=5000) <= 0.8 + 1e-9


def test_min_interval_boundary_is_inclusive():
    """Exactly MIN_INTERVAL_S is ACCEPTED; just below raises — pins the
    `< MIN_INTERVAL_S` boundary against a `<=` mutant."""
    PollPacer.validate(MIN_INTERVAL_S, 30.0)  # must not raise
    with pytest.raises(ValueError):
        PollPacer.validate(MIN_INTERVAL_S - 1e-6, 30.0)


def test_pressure_floor_is_literally_one_second():
    """The 429 pressure floor is exactly 1 s, pinned to a LITERAL (not the
    imported constant, which changes with a mutant) — a Retry-After: 0 with a
    fast interval sleeps 1 s, so mutating PRESSURE_FLOOR_S to 2 s fails here."""
    p, clk = make(interval=0.05, rand=Rand(0.0))
    clk.now = 2.0
    assert p.sleep_for_429(0.0) == pytest.approx(1.0)


def test_429_feasibility_uses_remaining_minus_reserve_not_plus():
    """PBC-11: feasibility is `floor >= remaining - RESERVE` (headerless:
    `target < remaining - RESERVE`). At remaining 1.05 s with a 1.0 s wait the
    429 must SURFACE; the `remaining - RESERVE -> + RESERVE` sign mutants would
    sleep 1 s instead (1.0 < 1.15)."""
    p, clk = make(interval=0.05, timeout=30.0)
    clk.now = 30.0 - 1.05  # remaining 1.05 s
    assert p.sleep_for_429(1.0) is None        # with a valid Retry-After header
    q, clk2 = make(interval=0.05, timeout=30.0)
    clk2.now = 30.0 - 1.05
    assert q.sleep_for_429(None) is None       # headerless path
