"""Flow-level poll-backoff tests (Poll Backoff Contract, amendment v27): both
Python poll loops (moderate.input inline loop + AsyncCollie._poll_job) are
wired to the pacer — observed poll counts EQUAL the reference simulator's
output for identical parameters (wiring + math in one assertion), plus the
latency gate, hint flows, 429 flows, wall-clock stop, and the error taxonomy.
"""
import asyncio
import importlib.util
import json
import pathlib
import sys
import time

import httpx
import pytest

from collieai import CollieAPIError, CollieConnectionError, ModerationError

# The reference simulator is the canonical source (an INDEPENDENT
# implementation of the contract — comparing the SDK against it is a real
# cross-check, not a tautology).
_SIM_PATH = pathlib.Path(__file__).resolve().parents[2] / "conformance" / "poll_sim.py"
_spec = importlib.util.spec_from_file_location("poll_sim", _SIM_PATH)
poll_sim = importlib.util.module_from_spec(_spec)
sys.modules["poll_sim"] = poll_sim  # dataclasses resolve __module__ via sys.modules
_spec.loader.exec_module(poll_sim)

RTT = 0.01


class VClock:
    """Virtual time: the injected sleep advances it; handlers add RTT."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


def timed_job_handler(clock: VClock, job_s: float, *, job_id="job_t",
                      inbound_result=None, hint_ms=None, rtt=RTT):
    """Job that becomes terminal when the GET *response* lands at/after
    ``job_s`` — the same counting model as the simulator."""
    state = {"gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": job_id, "status": "processing_inbound"})
        if request.method == "GET" and path == f"/v1/jobs/{job_id}":
            state["gets"] += 1
            # The server snapshots the job status when it PROCESSES the GET —
            # modeled at ISSUE time (v20), BEFORE the response travels back —
            # so it cannot report a completion that happens in flight. The
            # loop's strict acceptance check then rejects a response landing
            # at/after the deadline.
            snapshot = clock.now
            # rtt may be a callable(poll_index) — a VARIABLE round trip
            # (rtt_schedule), same convention as the simulator's callable
            # rtt_s. poll_index is 1-based (this GET's number).
            clock.now += rtt(state["gets"]) if callable(rtt) else rtt  # lands
            if snapshot >= job_s:
                payload = {
                    "job_id": job_id, "status": "completed",
                    "inbound_result": inbound_result or {
                        "allowed": True, "blocked": False,
                        "filtered_content": "ok", "triggered_rules": [],
                    },
                }
                return httpx.Response(200, json=payload)
            pending = {"job_id": job_id, "status": "processing_inbound"}
            # hint_ms may be a callable — a DYNAMIC schedule (the backend age
            # floor). The server computes status AND hint in ONE response at
            # the SNAPSHOT time (v21), not at landing — same convention as the
            # simulator.
            hint = hint_ms(snapshot) if callable(hint_ms) else hint_ms
            if hint is not None:
                pending["suggested_poll_ms"] = hint
            return httpx.Response(200, json=pending)
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    handler.state = state
    return handler


def make_client(build_client, clock: VClock, handler, *, rand=lambda: 0.5):
    return build_client(
        handler, _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=rand
    )


# ------------------------------------------------- wiring + simulator parity

@pytest.mark.asyncio
@pytest.mark.parametrize("job_s", [0.3, 8.0, 11.0])
async def test_moderation_loop_matches_simulator(build_client, job_s):
    clock = VClock()
    handler = timed_job_handler(clock, job_s)
    collie = make_client(build_client, clock, handler)
    result = await collie.moderate.input(prompt="hi")
    assert result.allowed is True
    expected = poll_sim.simulate(job_s, draw=0.5, rtt_s=RTT)
    assert handler.state["gets"] == expected.polls


@pytest.mark.asyncio
@pytest.mark.parametrize("job_s", [0.3, 8.0])
async def test_poll_job_loop_matches_simulator(build_client, job_s):
    clock = VClock()
    handler = timed_job_handler(clock, job_s)
    collie = make_client(build_client, clock, handler)
    data = await collie._poll_job(
        "job_t", sdk_origin="test",
        terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
    )
    assert data["status"] == "completed"
    expected = poll_sim.simulate(job_s, draw=0.5, rtt_s=RTT)
    assert handler.state["gets"] == expected.polls


@pytest.mark.asyncio
async def test_fast_job_latency_gate(build_client):
    """Ceilings alone can hide over-backoff: a 300 ms job must complete within
    job + caller_interval + 2*RTT of virtual time."""
    clock = VClock()
    handler = timed_job_handler(clock, 0.3)
    collie = make_client(build_client, clock, handler)
    await collie.moderate.input(prompt="hi")
    assert clock.now <= 0.3 + 0.05 + 2 * RTT
    assert handler.state["gets"] <= 8


@pytest.mark.asyncio
async def test_worst_jitter_stays_within_ceilings(build_client):
    clock = VClock()
    handler = timed_job_handler(clock, 8.0)
    collie = make_client(build_client, clock, handler, rand=lambda: 0.0)
    await collie.moderate.input(prompt="hi")
    worst = poll_sim.simulate(8.0, draw=0.0, rtt_s=RTT)
    assert handler.state["gets"] == worst.polls <= 45


# ---------------------------------------------------------------- hint flows

@pytest.mark.asyncio
async def test_hint_ignored_in_warm_honored_in_ramp(build_client):
    clock = VClock()
    handler = timed_job_handler(clock, 8.0, hint_ms=5000)
    collie = make_client(build_client, clock, handler)
    await collie.moderate.input(prompt="hi")
    expected = poll_sim.simulate(8.0, draw=0.5, rtt_s=RTT, hint_ms=5000)
    assert handler.state["gets"] == expected.polls
    # Warm phase unaffected by the hint: still ~25 polls in the first 1.5 s,
    # then the hint stretches ramp sleeps to ~5 s.
    assert expected.polls < poll_sim.simulate(8.0, draw=0.5, rtt_s=RTT).polls


@pytest.mark.asyncio
async def test_job_finishing_just_inside_budget_with_large_hint_succeeds(build_client):
    """v10 final-poll reserve: a 29 s job with a 5 s hint and a 30 s budget
    must SUCCEED — pre-v10 the final sleep reached the deadline and the loop
    timed out without ever issuing the last GET."""
    clock = VClock()
    handler = timed_job_handler(clock, 29.0, hint_ms=5000)
    collie = make_client(build_client, clock, handler)
    result = await collie.moderate.input(prompt="hi")
    assert result.allowed is True
    expected = poll_sim.simulate(29.0, draw=0.5, rtt_s=RTT, hint_ms=5000)
    assert not expected.timed_out
    assert handler.state["gets"] == expected.polls


def test_final_poll_reserve_boundary_is_strict():
    """The reserve guarantees the final GET is ISSUED with 100 ms remaining;
    it is accepted iff the full round trip completes STRICTLY within the
    reserve (v12 — at exactly 100 ms the response lands AT the deadline and
    the strict acceptance boundary rejects it). v15's pinning scenario is
    the plain 29.89 s job: both runs are poll-for-poll identical (41) and
    differ only in the reserve GET's landing — 29.9 + rtt flips
    accepted/rejected across the 99/100 ms line. Real deployments also
    spend scheduler/body-parse time inside the round trip, so the
    practical network budget is slightly under the reserve."""
    ok = poll_sim.simulate(29.89, draw=0.5, rtt_s=0.099)
    assert not ok.timed_out and ok.latency_s == pytest.approx(29.999)
    assert poll_sim.simulate(29.89, draw=0.5, rtt_s=0.100).timed_out
    assert poll_sim.simulate(29.89, draw=0.5, rtt_s=0.101).timed_out
    assert poll_sim.simulate(29.89, draw=0.5, rtt_s=0.3).timed_out


@pytest.mark.asyncio
async def test_rtt_boundary_through_the_real_loop(build_client):
    """The strict reserve boundary exercised through the REAL moderation
    loop, not just the simulator: a 99 ms round trip on the final GET is
    accepted, a 100 ms one is rejected as the flow's poll-timeout — both at
    canonical poll counts."""
    ok_clock = VClock()
    ok = timed_job_handler(ok_clock, 29.89, rtt=0.099)
    result = await make_client(build_client, ok_clock, ok).moderate.input(prompt="hi")
    assert result.allowed is True
    expected_ok = poll_sim.simulate(29.89, draw=0.5, rtt_s=0.099)
    assert ok.state["gets"] == expected_ok.polls

    slow_clock = VClock()
    slow = timed_job_handler(slow_clock, 29.89, rtt=0.100)
    with pytest.raises(ModerationError, match="timed out"):
        await make_client(build_client, slow_clock, slow).moderate.input(prompt="hi")
    expected_slow = poll_sim.simulate(29.89, draw=0.5, rtt_s=0.100)
    assert expected_slow.timed_out
    assert slow.state["gets"] == expected_slow.polls


@pytest.mark.asyncio
async def test_dynamic_hint_schedule_through_the_real_loop(build_client):
    """A hint that CHANGES between polls (the backend age floor under a low
    host median) through the real loop — parity with the canonical
    `job_11s_draw_0.5_age_schedule` case, not just a constant hint."""
    clock = VClock()
    schedule = poll_sim.schedule_fn(poll_sim.AGE_FLOOR_SCHEDULE)
    handler = timed_job_handler(clock, 11.0, hint_ms=schedule)
    collie = make_client(build_client, clock, handler)
    result = await collie.moderate.input(prompt="hi")
    assert result.allowed is True
    expected = poll_sim.simulate(11.0, draw=0.5, rtt_s=RTT, hint_ms=schedule)
    assert not expected.timed_out
    assert handler.state["gets"] == expected.polls


async def _second_loop(collie):
    return await collie._poll_job(
        "job_t", sdk_origin="test",
        terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
    )


@pytest.mark.asyncio
async def test_boundary_and_dynamic_cases_through_the_second_loop(build_client):
    """Hint extraction and deadline handling are duplicated per loop — the
    moderation-loop replays alone can't catch a divergence in _poll_job
    (v13). Same canonical cases, other loop."""
    ok_clock = VClock()
    ok = timed_job_handler(ok_clock, 29.89, rtt=0.099)
    data = await _second_loop(make_client(build_client, ok_clock, ok))
    assert data["status"] == "completed"
    assert ok.state["gets"] == poll_sim.simulate(
        29.89, draw=0.5, rtt_s=0.099).polls

    slow_clock = VClock()
    slow = timed_job_handler(slow_clock, 29.89, rtt=0.100)
    with pytest.raises(CollieAPIError) as exc_info:
        await _second_loop(make_client(build_client, slow_clock, slow))
    assert exc_info.value.code == "poll_timeout"
    assert slow.state["gets"] == poll_sim.simulate(
        29.89, draw=0.5, rtt_s=0.100).polls

    schedule = poll_sim.schedule_fn(poll_sim.AGE_FLOOR_SCHEDULE)
    dyn_clock = VClock()
    dyn = timed_job_handler(dyn_clock, 11.0, hint_ms=schedule)
    data = await _second_loop(make_client(build_client, dyn_clock, dyn))
    assert data["status"] == "completed"
    assert dyn.state["gets"] == poll_sim.simulate(
        11.0, draw=0.5, rtt_s=RTT, hint_ms=schedule).polls


def _age_plus_host_at_6():
    """Production-shaped hint: max(age floor, host rung jumping to 5000 at
    t=6) — the canonical `job_29.97s_host5000_at_6_rtt_0.08` schedule."""
    age = poll_sim.schedule_fn(poll_sim.AGE_FLOOR_SCHEDULE)
    return lambda t: max(age(t), 5000 if t >= 6.0 else 50)


@pytest.mark.asyncio
async def _replay_expecting_sim(run, clock, handler, sim):
    """Run a loop over the handler and assert its observed poll count EQUALS
    the simulator's — whether the sim says success or timeout (a timeout
    surfaces as ModerationError/CollieAPIError, but the handler still counted
    the GETs)."""
    if sim.timed_out:
        with pytest.raises((ModerationError, CollieAPIError)):
            await run((clock, handler))
    else:
        await run((clock, handler))
    assert handler.state["gets"] == sim.polls


@pytest.mark.asyncio
async def test_boundary_pairs_through_both_loops_v13_rescued_ninth_inside_W(build_client):
    """Two boundary pairs through BOTH loops under the honest server-snapshot
    model (v20):

    - v13's pair (29.3 s, rtt == reserve 100 ms): completes 700 ms before
      the deadline — OUTSIDE W — so v14's stateful tail poll + densification
      genuinely RESCUE it: plain and dynamic age hint both SUCCEED.
    - the ninth-review pair (29.97 s, rtt 80 ms): completes 30 ms before the
      deadline — INSIDE W (= 180 ms). v14 claimed the production hint was
      "rescued" here, but that was an artifact of the pre-v20 landing-snapshot
      model; under the honest model the server snapshots at GET-processing
      time, so BOTH schedules TIME OUT — which the guarantee permits (inside
      W is a lottery). Poll counts match the simulator either way."""
    age = poll_sim.schedule_fn(poll_sim.AGE_FLOOR_SCHEDULE)
    prod = _age_plus_host_at_6()
    cases = [
        (29.3, 0.1, age),      # v13 pair — outside W, rescued (both succeed)
        (29.97, 0.08, prod),   # ninth-review pair — inside W (both time out)
    ]
    for job_s, rtt, hint in cases:
        for run in (
            lambda c: make_client(build_client, c[0], c[1]).moderate.input(prompt="hi"),
            lambda c: _second_loop(make_client(build_client, c[0], c[1])),
        ):
            plain_clock = VClock()
            plain = timed_job_handler(plain_clock, job_s, rtt=rtt)
            await _replay_expecting_sim(
                run, plain_clock, plain, poll_sim.simulate(job_s, draw=0.5, rtt_s=rtt)
            )

            hinted_clock = VClock()
            hinted = timed_job_handler(hinted_clock, job_s, hint_ms=hint, rtt=rtt)
            await _replay_expecting_sim(
                run, hinted_clock, hinted,
                poll_sim.simulate(job_s, draw=0.5, rtt_s=rtt, hint_ms=hint),
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("job_s,interval_s,hint_ms", [
    (5.0, 0.2, None),    # non-default interval, no hint (v16)
    (8.0, 0.2, 5000),    # custom interval TOGETHER WITH a hint (v17)
    (25.0, 10.0, None),  # interval ABOVE the 5 s cap: cap_eff honors it (v17)
])
async def test_custom_interval_matches_the_simulator_through_both_loops(
    build_client, job_s, interval_s, hint_ms
):
    """NON-DEFAULT caller intervals through the REAL loops: the warm phase,
    ramp floor, jitter range and cap_eff all key off caller_interval, so the
    interval sweep being simulator-only left the loops' own interval plumbing
    unchecked. Covers a plain custom interval, a custom interval WITH a hint
    (interval x hint), and an interval above the 5 s hard cap (cap_eff)."""
    spec = poll_sim.simulate(
        job_s, draw=0.5, rtt_s=RTT, interval_s=interval_s, hint_ms=hint_ms
    )
    assert not spec.timed_out

    mod_clock = VClock()
    mod = timed_job_handler(mod_clock, job_s, hint_ms=hint_ms)
    result = await make_client(build_client, mod_clock, mod).moderate.input(
        prompt="hi", poll_interval_s=interval_s
    )
    assert result.allowed is True
    assert mod.state["gets"] == spec.polls

    second_clock = VClock()
    second = timed_job_handler(second_clock, job_s, hint_ms=hint_ms)
    data = await make_client(build_client, second_clock, second)._poll_job(
        "job_t", sdk_origin="test", poll_interval_s=interval_s,
        terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
    )
    assert data["status"] == "completed"
    assert second.state["gets"] == spec.polls


@pytest.mark.asyncio
@pytest.mark.parametrize("job_s,rtt_sched,hint_sched", [
    (8.0, [[0, 10], [20, 50]], None),                        # variable rtt, no hint (v18)
    (29.5, [[0, 10], [15, 50], [30, 20]], "age"),            # tail: variable rtt + age hint (v20)
])
async def test_variable_rtt_schedule_matches_the_simulator_through_both_loops(
    build_client, job_s, rtt_sched, hint_sched,
):
    """A per-poll VARYING round trip (bounded under the reserve) through the
    REAL loops — a constant rtt models zero variance and can't catch a loop
    that mishandles a changing round trip. Covers a plain variable-rtt job
    and (v20) a deadline-TAIL job combining a changing rtt AND a changing
    production age hint at once."""
    rtt = poll_sim.rtt_schedule_fn(rtt_sched)
    hint = poll_sim.schedule_fn(poll_sim.AGE_FLOOR_SCHEDULE) if hint_sched else None
    spec = poll_sim.simulate(job_s, draw=0.5, rtt_s=rtt, hint_ms=hint)
    assert not spec.timed_out

    mod_clock = VClock()
    mod = timed_job_handler(mod_clock, job_s, rtt=rtt, hint_ms=hint)
    result = await make_client(build_client, mod_clock, mod).moderate.input(prompt="hi")
    assert result.allowed is True
    assert mod.state["gets"] == spec.polls

    second_clock = VClock()
    second = timed_job_handler(second_clock, job_s, rtt=rtt, hint_ms=hint)
    data = await make_client(build_client, second_clock, second)._poll_job(
        "job_t", sdk_origin="test",
        terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
    )
    assert data["status"] == "completed"
    assert second.state["gets"] == spec.polls


@pytest.mark.asyncio
async def test_eleventh_review_counterexamples_are_rescued_through_both_loops(
    build_client,
):
    """v15 (ramp-in bound + caller-paced tail) FIXES the eleventh review's
    two counterexamples — verified through BOTH loops:

    - post-warm cliff: a host rung of 5000 jumped a 1.6 s job straight
      into a 4.5 s sleep (completion 6.02 s instead of 1.62 s); the
      ramp-in bound observes it at 2.275 s with poll parity;
    - custom timeout: a 2.914 s job under a 3 s budget with the age
      schedule timed out with 86 ms still on the clock; the caller-paced
      tail catches it at ~2.97 s with plain-parity polls."""
    # Cliff case — default knobs, constant host hint.
    for run in (
        lambda c: make_client(build_client, c[0], c[1]).moderate.input(prompt="hi"),
        lambda c: _second_loop(make_client(build_client, c[0], c[1])),
    ):
        clock = VClock()
        handler = timed_job_handler(clock, 1.6, hint_ms=5000)
        await run((clock, handler))
        sim = poll_sim.simulate(1.6, draw=0.5, rtt_s=RTT, hint_ms=5000)
        assert not sim.timed_out
        assert handler.state["gets"] == sim.polls == 27
        assert clock.now == pytest.approx(sim.latency_s) == pytest.approx(2.275)

    # Custom-timeout case — 3 s budget, worst-jitter draw, dynamic age hint.
    schedule = poll_sim.schedule_fn(poll_sim.AGE_FLOOR_SCHEDULE)
    sim_ct = poll_sim.simulate(
        2.914, timeout_s=3.0, draw=0.0, rtt_s=RTT, hint_ms=schedule
    )
    assert not sim_ct.timed_out and sim_ct.polls == 34

    mod_clock = VClock()
    mod = timed_job_handler(mod_clock, 2.914, hint_ms=schedule)
    result = await make_client(
        build_client, mod_clock, mod, rand=lambda: 0.0
    ).moderate.input(prompt="hi", timeout_s=3.0)
    assert result.allowed is True
    assert mod.state["gets"] == sim_ct.polls

    second_clock = VClock()
    second = timed_job_handler(second_clock, 2.914, hint_ms=schedule)
    collie = make_client(build_client, second_clock, second, rand=lambda: 0.0)
    data = await collie._poll_job(
        "job_t", sdk_origin="test", timeout_s=3.0,
        terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
    )
    assert data["status"] == "completed"
    assert second.state["gets"] == sim_ct.polls


# ------------------------------------------------------------------ timeout

@pytest.mark.asyncio
async def test_wall_clock_timeout_in_virtual_model(build_client):
    """Virtual-clock view of the budget: sleeps are bounded by the remaining
    budget, so virtual time never overshoots by more than one RTT. (Strict
    real-time enforcement — cancelling a GET that hangs past the deadline —
    is covered by the real-clock tests below; the virtual clock can't drive
    asyncio.wait_for.)"""
    clock = VClock()
    handler = timed_job_handler(clock, 999.0)
    collie = make_client(build_client, clock, handler)
    with pytest.raises(ModerationError, match="timed out"):
        await collie.moderate.input(prompt="hi")
    assert clock.now <= 30.0 + RTT
    expected = poll_sim.simulate(999.0, draw=0.5, rtt_s=RTT)
    assert expected.timed_out
    assert handler.state["gets"] == expected.polls <= 75


def _hanging_get_handler():
    """POST answers instantly; every GET hangs far past any test budget.
    If the deadline bound fails to cancel it, the test itself times out."""

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "j", "status": "processing_inbound"})
        await asyncio.sleep(30)
        return httpx.Response(200, json={"job_id": "j", "status": "processing_inbound"})

    return handler


@pytest.mark.asyncio
async def test_deadline_cancels_hung_get_poll_job(build_client):
    """The budget is absolute wall clock: a GET that hangs past the deadline
    is cancelled and surfaces as poll_timeout. Real clocks on purpose — httpx
    timeouts are per-phase and can't cap total request time, so only the
    outer asyncio.wait_for bound can end this request."""
    collie = build_client(_hanging_get_handler())
    start = time.monotonic()
    with pytest.raises(CollieAPIError) as exc_info:
        await collie._poll_job(
            "j", sdk_origin="test", timeout_s=0.25,
            terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
        )
    assert exc_info.value.code == "poll_timeout"
    assert time.monotonic() - start < 5.0


@pytest.mark.asyncio
async def test_deadline_cancels_hung_get_moderation(build_client):
    """Same absolute bound through the moderation loop: the hung GET is
    cancelled and surfaces as this flow's ModerationError timeout."""
    collie = build_client(_hanging_get_handler())
    start = time.monotonic()
    with pytest.raises(ModerationError, match="timed out"):
        await collie.moderate.input(prompt="hi", timeout_s=0.25)
    assert time.monotonic() - start < 5.0


def _late_terminal_handler(clock: VClock, *, advance: float):
    """A GET that outruns cancellation: it advances the virtual clock past
    the deadline and still returns a terminal payload — modeling a transport
    that suppressed wait_for's cancellation (3.9-3.11 returns its result)."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "j", "status": "processing_inbound"})
        clock.now += advance
        return httpx.Response(200, json={
            "job_id": "j", "status": "completed",
            "inbound_result": {"allowed": True, "blocked": False,
                               "filtered_content": "ok", "triggered_rules": []},
        })

    return handler


@pytest.mark.asyncio
async def test_late_terminal_is_rejected_poll_job(build_client):
    """Strict acceptance boundary: a terminal response landing after the
    deadline is a poll-timeout, not a late success."""
    clock = VClock()
    collie = build_client(
        _late_terminal_handler(clock, advance=1.0),
        _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=lambda: 0.5,
    )
    with pytest.raises(CollieAPIError) as exc_info:
        await collie._poll_job(
            "j", sdk_origin="test", timeout_s=0.5,
            terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
        )
    assert exc_info.value.code == "poll_timeout"


@pytest.mark.asyncio
async def test_late_terminal_is_rejected_moderation(build_client):
    clock = VClock()
    collie = build_client(
        _late_terminal_handler(clock, advance=1.0),
        _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=lambda: 0.5,
    )
    with pytest.raises(ModerationError, match="timed out"):
        await collie.moderate.input(prompt="hi", timeout_s=0.5)


@pytest.mark.asyncio
async def test_late_http_error_is_rejected_poll_job(build_client):
    """Strict acceptance applies to error responses too: a 500 landing after
    the deadline (a transport that outran cancellation) is a poll-timeout,
    not a late typed HTTP error."""
    clock = VClock()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "j", "status": "processing_inbound"})
        clock.now += 1.0  # lands 0.5 s past the 0.5 s budget
        return httpx.Response(500, json={"error": {"message": "late", "type": "server_error"}})

    collie = build_client(
        handler, _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=lambda: 0.5
    )
    with pytest.raises(CollieAPIError) as exc_info:
        await collie._poll_job(
            "j", sdk_origin="test", timeout_s=0.5,
            terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
        )
    assert exc_info.value.code == "poll_timeout"


@pytest.mark.asyncio
async def test_late_http_error_is_rejected_moderation(build_client):
    """The late-HTTP-error catch exists in BOTH loops — through moderation too."""
    clock = VClock()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "j", "status": "processing_inbound"})
        clock.now += 1.0
        return httpx.Response(500, json={"error": {"message": "late", "type": "server_error"}})

    collie = build_client(
        handler, _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=lambda: 0.5
    )
    with pytest.raises(ModerationError, match="timed out"):
        await collie.moderate.input(prompt="hi", timeout_s=0.5)


@pytest.mark.asyncio
async def test_transport_owned_builtin_timeout_is_connection_error(build_client):
    """A transport-raised builtin TimeoutError is a transport failure. On
    3.11+ it is the same class as asyncio.TimeoutError — without the
    normalization inside the request primitive, the loop's wait_for catch
    would misread it as the deadline firing (budget untouched here)."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "j", "status": "processing_inbound"})
        raise TimeoutError("transport-owned")

    collie = build_client(handler)
    with pytest.raises(CollieConnectionError):
        await collie._poll_job(
            "j", sdk_origin="test",
            terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
        )


def test_simulator_implements_strict_acceptance():
    """The oracle must reject a terminal response landing at/after the
    deadline, same as the SDK loops — otherwise ports built against it
    would re-open the late-acceptance hole."""
    result = poll_sim.simulate(0.065, timeout_s=0.065)
    assert result.timed_out


def test_poll_cases_json_matches_the_simulator():
    """sdk/conformance/poll_cases.json is the generated cross-SDK source of
    canonical counts (the Node and .NET suites load it instead of hand-copied
    numbers); it must never drift from the live simulator. Regenerate with
    `python poll_sim.py --json > poll_cases.json`."""
    path = _SIM_PATH.parent / "poll_cases.json"
    assert json.loads(path.read_text()) == poll_sim.canonical_cases()


def test_retry_after_strict_decimal_parse():
    """RFC 9110 delay-seconds: a non-negative decimal integer, strictly —
    float() admitted 1.5 / 1e2 / Infinity and diverged from the other SDKs.
    Malformed values are unusable (headerless pressure-floor path)."""
    from collieai._client import AsyncCollie

    def resp(value: str) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": value})

    assert AsyncCollie._parse_retry_after(resp("2")) == 2.0
    assert AsyncCollie._parse_retry_after(resp("0")) == 0.0
    # A VALID huge delta (e.g. Int32 overflow territory) parses — the pacer's
    # feasibility check then surfaces the typed 429; it must never fall to
    # the headerless pressure floor (cross-SDK divergence, v9).
    assert AsyncCollie._parse_retry_after(resp("2147483648")) == 2147483648.0
    assert AsyncCollie._parse_retry_after(resp("9" * 400)) == float("inf")
    for bad in ("1.5", "1e2", "0x10", "", "-1", "Infinity"):
        assert AsyncCollie._parse_retry_after(resp(bad)) is None, bad


@pytest.mark.asyncio
async def test_transport_timeout_is_connection_error_even_at_deadline(build_client):
    """Taxonomy is structural, not clock-inferred: an httpx-level timeout is
    the client's own transport timeout (a connection problem) even when the
    poll budget happens to be exhausted at that moment — only the deadline
    bound itself firing maps to poll_timeout."""
    clock = VClock()
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "j", "status": "processing_inbound"})
        calls["n"] += 1
        if calls["n"] == 1:
            clock.now += RTT
            return httpx.Response(200, json={"job_id": "j", "status": "processing_inbound"})
        clock.now += 1.0  # virtual budget (0.5 s) is now exhausted
        raise httpx.ReadTimeout("transport read timeout")

    collie = build_client(
        handler, _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=lambda: 0.5
    )
    with pytest.raises(CollieConnectionError):
        await collie._poll_job(
            "j", sdk_origin="test", timeout_s=0.5,
            terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
        )


@pytest.mark.asyncio
async def test_transport_error_with_budget_left_stays_connection_error(build_client):
    clock = VClock()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "j", "status": "processing_inbound"})
        raise httpx.ConnectError("boom")

    collie = build_client(
        handler, _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=lambda: 0.5
    )
    with pytest.raises(CollieConnectionError):
        await collie._poll_job(
            "j", sdk_origin="test",
            terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
        )


# --------------------------------------------------------------------- 429s

def _handler_429_then_done(clock, *, retry_after=None, job_id="j"):
    state = {"gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": job_id, "status": "processing_inbound"})
        state["gets"] += 1
        clock.now += RTT
        if state["gets"] == 1:
            headers = {}
            if retry_after is not None:
                headers["Retry-After"] = str(retry_after)
            return httpx.Response(429, headers=headers,
                                  json={"error": {"message": "rl", "type": "rate_limit"}})
        return httpx.Response(200, json={
            "job_id": job_id, "status": "completed",
            "inbound_result": {"allowed": True, "blocked": False,
                               "filtered_content": "ok", "triggered_rules": []},
        })

    handler.state = state
    return handler


@pytest.mark.asyncio
async def test_429_with_retry_after_waits_and_continues(build_client):
    clock = VClock()
    handler = _handler_429_then_done(clock, retry_after=2)
    collie = make_client(build_client, clock, handler)
    result = await collie.moderate.input(prompt="hi")
    assert result.allowed is True
    assert handler.state["gets"] == 2
    # Retry-After is a minimum: the wait was >= 2 s (upward-only jitter).
    assert clock.now >= 2.0


@pytest.mark.asyncio
async def test_429_retry_after_beyond_budget_surfaces_typed(build_client):
    clock = VClock()
    handler = _handler_429_then_done(clock, retry_after=60)
    collie = make_client(build_client, clock, handler)
    with pytest.raises(CollieAPIError) as exc_info:
        await collie.moderate.input(prompt="hi")
    assert exc_info.value.status_code == 429
    assert handler.state["gets"] == 1  # no pointless sleep-then-timeout


@pytest.mark.asyncio
async def test_429_headerless_ends_warm(build_client):
    clock = VClock()
    handler = _handler_429_then_done(clock)  # no Retry-After
    collie = make_client(build_client, clock, handler)
    await collie.moderate.input(prompt="hi")
    # Pressure floor: the single wait was >= 1 s despite being inside warm.
    assert clock.now >= 1.0


@pytest.mark.asyncio
async def test_poll_job_429_with_retry_after_waits_and_continues(build_client):
    """The 429 path through the SECOND loop (_poll_job), not just moderation."""
    clock = VClock()
    state = {"gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["gets"] += 1
        clock.now += RTT
        if state["gets"] == 1:
            return httpx.Response(429, headers={"Retry-After": "2"},
                                  json={"error": {"message": "rl", "type": "rate_limit"}})
        return httpx.Response(200, json={"job_id": "j", "status": "completed"})

    collie = build_client(
        handler, _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=lambda: 0.5
    )
    data = await collie._poll_job(
        "j", sdk_origin="test",
        terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
    )
    assert data["status"] == "completed"
    assert state["gets"] == 2
    assert clock.now >= 2.0  # Retry-After is a minimum


@pytest.mark.asyncio
async def test_429_zero_retry_after_floors_not_tight_loops(build_client):
    """Retry-After: 0 must not produce a zero-sleep re-poll loop — any 429 is
    rate pressure and the wait floors at 1 s."""
    clock = VClock()
    handler = _handler_429_then_done(clock, retry_after=0)
    collie = make_client(build_client, clock, handler)
    result = await collie.moderate.input(prompt="hi")
    assert result.allowed is True
    assert handler.state["gets"] == 2
    assert clock.now >= 1.0


@pytest.mark.asyncio
async def test_429_large_interval_not_sped_up_through_both_loops(build_client):
    """v18 flow-level (the pacer-unit test alone doesn't prove the loops
    honor it): with a 10 s interval, a `Retry-After: 0` 429 must sleep the
    caller's 10 s, NOT the 1 s pressure floor — a 429 is backpressure and
    must never speed the caller up. Both loops."""
    for run in (
        lambda c, h: make_client(build_client, c, h).moderate.input(
            prompt="hi", poll_interval_s=10.0
        ),
        lambda c, h: make_client(build_client, c, h)._poll_job(
            "j", sdk_origin="test", poll_interval_s=10.0,
            terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
        ),
    ):
        clock = VClock()
        handler = _handler_429_then_done(clock, retry_after=0)
        await run(clock, handler)
        assert handler.state["gets"] == 2
        assert clock.now >= 10.0  # the 10 s interval floor, not 1 s


@pytest.mark.asyncio
async def test_429_interval_floor_beyond_budget_surfaces_typed_through_both_loops(
    build_client,
):
    """v18/v20: when the interval floor itself can't fit the remaining budget,
    the typed 429 must surface rather than sleeping into a guaranteed
    poll-timeout. A 10 s interval with a `Retry-After: 0` under a 5 s budget:
    the 10 s floor doesn't fit, so the rate-limit error surfaces on the first
    GET (no pointless sleep). Verified through BOTH loops (the v19 claim of
    'both loops' covered only the sleep-floor case, not this feasibility
    case)."""
    for run in (
        lambda c, h: make_client(build_client, c, h).moderate.input(
            prompt="hi", poll_interval_s=10.0, timeout_s=5.0
        ),
        lambda c, h: make_client(build_client, c, h)._poll_job(
            "j", sdk_origin="test", poll_interval_s=10.0, timeout_s=5.0,
            terminal=frozenset({"completed"}), failure=frozenset({"failed"}),
        ),
    ):
        clock = VClock()
        handler = _handler_429_then_done(clock, retry_after=0)
        with pytest.raises(CollieAPIError) as exc_info:
            await run(clock, handler)
        assert exc_info.value.status_code == 429
        assert handler.state["gets"] == 1  # surfaced immediately, no 1 s tight loop


# ------------------------------------------------- protect_buffered (public)

@pytest.mark.asyncio
async def test_protect_buffered_output_poll_matches_simulator(build_client):
    """Public-wrapper wiring: protect_buffered's outbound poll is paced (the
    private _poll_job parity test alone can't catch a wrapper that bypasses
    or misconfigures it)."""
    clock = VClock()
    state = {"gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "jb", "status": "processing_outbound"})
        if request.method == "GET" and request.url.path == "/v1/jobs/jb":
            state["gets"] += 1
            clock.now += RTT
            if clock.now >= 8.0:
                return httpx.Response(200, json={
                    "job_id": "jb", "status": "completed",
                    # The real server always sends BOTH allowed and blocked
                    # (jobs.py _build_filtering_result); the buffered parser
                    # now requires the explicit allow (tech-debt #22).
                    "outbound_result": {"allowed": True, "blocked": False,
                                        "filtered_content": "hello",
                                        "triggered_rules": []},
                })
            return httpx.Response(200, json={"job_id": "jb", "status": "processing_outbound"})
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(
        handler, _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=lambda: 0.5
    )

    async def gen():
        yield "hello"

    result = await collie.streaming.protect_buffered(
        input="hi", raw_stream_factory=gen, check_input=False
    )
    assert result.blocked is False
    expected = poll_sim.simulate(8.0, draw=0.5, rtt_s=RTT)
    assert state["gets"] == expected.polls


# --------------------------------------------------------------- validation

@pytest.mark.asyncio
async def test_knob_validation_fails_before_any_request(build_client):
    clock = VClock()
    handler = timed_job_handler(clock, 0.3)
    collie = make_client(build_client, clock, handler)
    with pytest.raises(ValueError):
        await collie.moderate.input(prompt="hi", poll_interval_s=0.0)
    with pytest.raises(ValueError):
        await collie.moderate.input(prompt="hi", timeout_s=0.0)
    assert handler.state["gets"] == 0


@pytest.mark.asyncio
async def test_protect_buffered_validates_knobs_before_side_effects(build_client):
    """An invalid timeout_s must fail before the provider factory runs and
    before any job is created — not down in _poll_job after both."""
    posts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            posts["n"] += 1
            return httpx.Response(202, json={"job_id": "j", "status": "processing_inbound"})
        return httpx.Response(200, json={"job_id": "j", "status": "completed"})

    collie = build_client(handler)
    factory_calls = {"n": 0}

    def factory():
        factory_calls["n"] += 1

        async def gen():
            yield "hello"

        return gen()

    with pytest.raises(ValueError):
        await collie.streaming.protect_buffered(
            input="hi", raw_stream_factory=factory, timeout_s=0.0
        )
    assert factory_calls["n"] == 0
    assert posts["n"] == 0


# ------------------------------------------------------------- independence

@pytest.mark.asyncio
async def test_sequential_operations_on_one_client_get_fresh_pacers(build_client):
    """Same CLIENT (not just same clock): a pacer accidentally kept on the
    client instance would carry ramp state into the second call."""
    clock = VClock()
    job_done_at = [0.3]
    state = {"gets": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json={"job_id": "j", "status": "processing_inbound"})
        state["gets"] += 1
        clock.now += RTT
        if clock.now >= job_done_at[0]:
            return httpx.Response(200, json={
                "job_id": "j", "status": "completed",
                "inbound_result": {"allowed": True, "blocked": False,
                                   "filtered_content": "ok", "triggered_rules": []},
            })
        return httpx.Response(200, json={"job_id": "j", "status": "processing_inbound"})

    collie = build_client(
        handler, _sleep=clock.sleep, _monotonic=clock.monotonic, _rand=lambda: 0.5
    )
    await collie.moderate.input(prompt="one")
    first = state["gets"]
    job_done_at[0] = clock.now + 0.3
    await collie.moderate.input(prompt="two")
    assert state["gets"] - first == first


@pytest.mark.asyncio
async def test_sequential_operations_get_fresh_pacers(build_client):
    clock = VClock()
    handler = timed_job_handler(clock, 0.3)
    collie = make_client(build_client, clock, handler)
    await collie.moderate.input(prompt="one")
    first = handler.state["gets"]
    # Reset the virtual job for a second, identical run.
    base = clock.now
    handler2 = timed_job_handler(clock, base + 0.3)
    collie2 = make_client(build_client, clock, handler2)
    await collie2.moderate.input(prompt="two")
    # A carried-over ramp would poll far fewer times; a fresh pacer repeats
    # the warm-phase pattern.
    assert handler2.state["gets"] == first
