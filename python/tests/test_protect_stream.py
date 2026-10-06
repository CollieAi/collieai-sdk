"""protect_stream / protect_buffered: the ergonomic safe-streaming wrapper."""
import asyncio
import contextlib

import httpx
import pytest

from collieai import (
    Blocked,
    CollieAPIError,
    Finished,
    InputBlocked,
    InputModerationResult,
    ProviderStreamFactoryRequired,
    SafeDelta,
)
from conftest import FakeChunkBackend, body_of, error_response


# --------------------------------------------------------------------------
# A backend that serves BOTH the moderation job (inbound_only) and the
# streaming session (chunks). Configurable input verdict + output masking +
# transient chunk failures.
# --------------------------------------------------------------------------
def make_backend(*, input_blocked=False, mask_to=None, block_output=False,
                 chunk_fail=None, chunk_fail_times=0, context_result=None, blocked_by=None):
    state = {"factory_calls": 0, "chunk_attempts": 0, "origins": [],
             "created_jobs": []}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        state["origins"].append(request.headers.get("X-CollieAi-SDK-Origin"))
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            state["created_jobs"].append(body)
            if body.get("inbound_only"):
                return httpx.Response(202, json={"job_id": "job_mod"})
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path == "/v1/jobs/job_mod":
            inbound = {
                "allowed": not input_blocked,
                "blocked": input_blocked,
                "block_message": "input blocked" if input_blocked else None,
                "triggered_rules": [],
            }
            payload = {
                "job_id": "job_mod",
                "status": "inbound_blocked" if input_blocked else "completed",
                "inbound_result": inbound,
            }
            if context_result is not None:
                payload["context_result"] = context_result
            if blocked_by is not None:
                payload["blocked_by"] = blocked_by
            return httpx.Response(200, json=payload)
        if request.method == "POST" and path == "/v1/jobs/job_stream/chunks":
            state["chunk_attempts"] += 1
            body = body_of(request)
            if chunk_fail is not None and state["chunk_attempts"] <= chunk_fail_times:
                return chunk_fail
            content = body.get("content", "")
            is_final = bool(body.get("is_final"))
            if block_output and content:
                emits = [{"content": "", "blocked": True, "final": True,
                          "block_message": "Custom rule block message",
                          "triggered_rules": [
                    {"rule_id": "r1", "rule_name": "blocker", "rule_type": "regex", "decision": "block"}
                ]}]
                return httpx.Response(200, json={"sequence": body["sequence"], "accepted": True, "emits": emits, "finished": True})
            out = mask_to if (mask_to is not None and content) else content
            emits = []
            if content or is_final:
                emits = [{"content": out, "blocked": False, "final": is_final, "triggered_rules": []}]
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True, "emits": emits, "finished": is_final,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    return handler, state


def make_factory(deltas, state=None):
    async def factory():
        if state is not None:
            state["factory_calls"] += 1
        for d in deltas:
            yield d
    return factory


async def collect(aiter):
    return [event async for event in aiter]


@pytest.mark.asyncio
async def test_input_block_skips_factory_and_yields_input_blocked(build_client):
    handler, _ = make_backend(input_blocked=True)
    collie = build_client(handler)
    state = {"factory_calls": 0}
    factory = make_factory(["never"], state)

    events = await collect(collie.streaming.protect_stream(input="hi", raw_stream_factory=factory))

    assert state["factory_calls"] == 0          # provider never invoked
    assert len(events) == 1
    assert isinstance(events[0], InputBlocked)
    assert events[0].block_message == "input blocked"


@pytest.mark.asyncio
async def test_success_invokes_factory_and_yields_safe_deltas(build_client):
    handler, _ = make_backend()
    collie = build_client(handler)
    state = {"factory_calls": 0}
    factory = make_factory(["hello world"], state)

    events = await collect(collie.streaming.protect_stream(input="hi", raw_stream_factory=factory))

    assert state["factory_calls"] == 1
    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert "".join(deltas) == "hello world"
    assert isinstance(events[-1], Finished)


@pytest.mark.asyncio
async def test_raw_provider_chunks_never_yielded(build_client):
    # Backend masks every chunk to "SAFE"; the raw "RAW" text must not appear.
    handler, _ = make_backend(mask_to="SAFE")
    collie = build_client(handler)
    factory = make_factory(["RAWTEXT"])

    events = await collect(collie.streaming.protect_stream(input="hi", raw_stream_factory=factory))
    texts = [e.text for e in events if isinstance(e, SafeDelta)]
    assert texts == ["SAFE"]
    assert all("RAW" not in t for t in texts)


@pytest.mark.asyncio
async def test_timeout_retry_does_not_duplicate_deltas(build_client):
    handler, state = make_backend(chunk_fail=error_response(504, "chunk_filter_timeout"), chunk_fail_times=1)
    collie = build_client(handler)
    factory = make_factory(["hello"])

    events = await collect(collie.streaming.protect_stream(input="hi", raw_stream_factory=factory))
    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert deltas == ["hello"]               # exactly once despite the retry
    assert state["chunk_attempts"] >= 2      # the push was retried


@pytest.mark.asyncio
async def test_factory_called_exactly_once_with_retries(build_client):
    handler, _ = make_backend(chunk_fail=error_response(503, "chunk_persistence_unavailable"), chunk_fail_times=1)
    collie = build_client(handler)
    state = {"factory_calls": 0}
    factory = make_factory(["a", "b"], state)

    await collect(collie.streaming.protect_stream(input="hi", raw_stream_factory=factory))
    assert state["factory_calls"] == 1


@pytest.mark.asyncio
async def test_blocked_output_yields_blocked_terminal(build_client):
    handler, _ = make_backend(block_output=True)
    collie = build_client(handler)
    factory = make_factory(["secret stuff"])

    events = await collect(collie.streaming.protect_stream(input="hi", raw_stream_factory=factory))
    assert isinstance(events[-1], Blocked)
    assert events[-1].block_message == "Custom rule block message"  # F1
    assert not any(isinstance(e, Finished) for e in events)  # block is terminal


@pytest.mark.asyncio
async def test_push_finished_without_block_still_yields_finished(build_client):
    """F2: if the backend finalizes the session on a push (finished=true, no
    blocked emit), the iterator must still end with a terminal Finished event
    and must not re-submit via finish()."""
    chunk_posts = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "POST" and path == "/v1/jobs/job_stream/chunks":
            chunk_posts["count"] += 1
            body = body_of(request)
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True,
                "emits": [{"content": body.get("content", ""), "blocked": False,
                           "final": False, "triggered_rules": []}],
                "finished": True,  # backend finalized early, without a block
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    factory = make_factory(["hello"])

    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory, check_input=False))

    assert [e.text for e in events if isinstance(e, SafeDelta)] == ["hello"]
    assert isinstance(events[-1], Finished)     # terminal event guaranteed
    assert chunk_posts["count"] == 1            # no finish() re-submit / re-emit


@pytest.mark.asyncio
async def test_batching_by_count(build_client):
    handler, state = make_backend()
    collie = build_client(handler)
    factory = make_factory(["a", "b", "c"])

    # max_deltas=2 -> "ab" flushes as one push, "c" flushes at provider end.
    await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory, max_deltas=2,
    ))
    # 2 content pushes + 1 final chunk = 3 chunk submits.
    assert state["chunk_attempts"] == 3


@pytest.mark.asyncio
async def test_provider_stream_factory_required_raised_synchronously(build_client):
    handler, _ = make_backend()
    collie = build_client(handler)

    async def already_started():  # an async generator OBJECT, not a factory
        yield "x"

    # Must raise at call time, before any iteration.
    with pytest.raises(ProviderStreamFactoryRequired):
        collie.streaming.protect_stream(input="hi", raw_stream_factory=already_started())


@pytest.mark.asyncio
async def test_non_async_iterable_factory_return_raises(build_client):
    # callable, but returns a sync list -> not an async iterable.
    handler, _ = make_backend()
    collie = build_client(handler)
    with pytest.raises(ProviderStreamFactoryRequired):
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=lambda: ["not async"], check_input=False,
        ))


@pytest.mark.asyncio
async def test_protect_buffered_non_async_iterable_factory_return_raises(build_client):
    handler, _ = make_buffered_backend()
    collie = build_client(handler)
    with pytest.raises(ProviderStreamFactoryRequired):
        await collie.streaming.protect_buffered(
            input="hi", raw_stream_factory=lambda: ["not async"], check_input=False,
        )


@pytest.mark.filterwarnings("error::RuntimeWarning")
@pytest.mark.asyncio
async def test_async_def_factory_rejected_without_warning(build_client):
    """An async-def (coroutine) factory is rejected AND its coroutine is closed,
    so no 'coroutine was never awaited' RuntimeWarning escapes."""
    import gc

    handler, _ = make_backend()
    collie = build_client(handler)

    async def stream():
        yield "x"

    async def factory():        # coroutine function (no yield) -> returns a coroutine
        return stream()

    with pytest.raises(ProviderStreamFactoryRequired):
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=factory, check_input=False,
        ))
    gc.collect()  # would surface a never-awaited-coroutine warning (→ error) if not closed


@pytest.mark.asyncio
async def test_broken_aiter_returns_non_iterator_raises(build_client):
    """An object whose __aiter__ doesn't return a real async iterator is a typed
    error, not a raw AttributeError on iteration."""
    handler, _ = make_backend()
    collie = build_client(handler)

    class BadIterable:
        def __aiter__(self):
            return object()  # no __anext__

    with pytest.raises(ProviderStreamFactoryRequired):
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=lambda: BadIterable(), check_input=False,
        ))


@pytest.mark.asyncio
async def test_non_callable_anext_rejected(build_client):
    handler, _ = make_backend()
    collie = build_client(handler)

    class NoneAnext:
        def __aiter__(self):
            return self
        __anext__ = None  # present but not callable

    with pytest.raises(ProviderStreamFactoryRequired):
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=lambda: NoneAnext(), check_input=False,
        ))


# An async iterable whose __aiter__ returns a SEPARATE iterator, with a
# closable provider — exercises "close the original provider too".
class _SeparateIterable:
    def __init__(self, deltas):
        self._deltas = deltas
        self.closed = False

    def __aiter__(self):
        return _SeparateIterator(self._deltas)

    async def aclose(self):
        self.closed = True


class _SeparateIterator:
    def __init__(self, deltas):
        self._it = iter(deltas)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


@pytest.mark.asyncio
async def test_protect_stream_closes_distinct_provider(build_client):
    handler, _ = make_backend()
    collie = build_client(handler)
    provider = _SeparateIterable(["a", "b"])
    await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=lambda: provider, check_input=False,
    ))
    assert provider.closed is True


@pytest.mark.asyncio
async def test_protect_buffered_closes_distinct_provider(build_client):
    handler, _ = make_buffered_backend()
    collie = build_client(handler)
    provider = _SeparateIterable(["a", "b"])
    await collie.streaming.protect_buffered(
        input="hi", raw_stream_factory=lambda: provider, check_input=False,
    )
    assert provider.closed is True


@pytest.mark.asyncio
async def test_provider_closed_when_validation_fails(build_client):
    """If validation fails after the factory built a closable provider, the
    provider is still closed (paid work doesn't leak)."""
    handler, _ = make_backend()
    collie = build_client(handler)

    class BadButClosable:
        def __init__(self):
            self.closed = False

        def __aiter__(self):
            return object()  # invalid -> ProviderStreamFactoryRequired

        async def aclose(self):
            self.closed = True

    provider = BadButClosable()
    with pytest.raises(ProviderStreamFactoryRequired):
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=lambda: provider, check_input=False,
        ))
    assert provider.closed is True


def _chunk_recording_handler(chunks):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = body_of(request) if request.method == "POST" else {}
        if request.method == "POST" and path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_mod" if body.get("inbound_only") else "job_stream"})
        if path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={"status": "completed", "inbound_result": {
                "allowed": True, "blocked": False, "triggered_rules": []}})
        if path == "/v1/jobs/job_stream/chunks":
            chunks.append(body)
            content = body.get("content", "")
            emits = ([{"content": content, "blocked": False, "final": bool(body.get("is_final")),
                       "triggered_rules": []}] if content or body.get("is_final") else [])
            return httpx.Response(200, json={"sequence": body["sequence"], "accepted": True,
                                             "emits": emits, "finished": bool(body.get("is_final"))})
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})
    return handler


@pytest.mark.asyncio
async def test_factory_failure_does_not_fake_finalize(build_client):
    """A job that never streamed a chunk (the factory raised) must NOT be sent an
    empty is_final — that would mark it 'completed with empty output' and mask the
    failure in the audit log. It is left to expire instead."""
    chunks = []
    collie = build_client(_chunk_recording_handler(chunks))

    def factory():
        raise RuntimeError("provider open failed")

    with pytest.raises(RuntimeError):
        await collect(collie.streaming.protect_stream(input="hi", raw_stream_factory=factory))
    assert chunks == []  # no chunk submitted at all — no spurious finalize


@pytest.mark.asyncio
async def test_midstream_provider_error_finalizes_after_chunk(build_client):
    """If the upstream model raises AFTER >=1 accepted chunk, the streamed-so-far
    content is finalized (is_final) so the job reaches a terminal state."""
    chunks = []
    collie = build_client(_chunk_recording_handler(chunks))

    def factory():
        async def gen():
            yield "hello"
            raise RuntimeError("upstream blew up")
        return gen()

    with pytest.raises(RuntimeError):
        await collect(
            collie.streaming.protect_stream(input="hi", raw_stream_factory=factory, max_deltas=1)
        )
    assert any(not c.get("is_final") for c in chunks)  # the "hello" chunk landed
    assert any(c.get("is_final") for c in chunks)       # then it was finalized


@pytest.mark.asyncio
async def test_session_create_carries_gate_reference(build_client):
    """The duplicate-inbound fix:
    the session job proves its prompt was gated by referencing the
    moderation job — input_job_id on the create body — instead of paying
    a second inbound pass. message_input stays (the chunk path's guard
    prompt context reads it)."""
    handler, state = make_backend()
    collie = build_client(handler)
    factory = make_factory(["ok"])

    await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory,
    ))

    session_bodies = [b for b in state["created_jobs"] if not b.get("inbound_only")]
    assert len(session_bodies) == 1
    assert session_bodies[0]["input_job_id"] == "job_mod"
    assert session_bodies[0]["message_input"] == "hi"


@pytest.mark.asyncio
async def test_no_gate_reference_without_input_check(build_client):
    """check_input=False means no gate ran, so there is no proof to send —
    the session job must NOT claim one (the server would reject a bogus
    reference loudly, and an absent field means it filters as before)."""
    handler, state = make_backend()
    collie = build_client(handler)
    factory = make_factory(["ok"])

    await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory, check_input=False,
    ))

    session_bodies = [b for b in state["created_jobs"] if not b.get("inbound_only")]
    assert len(session_bodies) == 1
    assert "input_job_id" not in session_bodies[0]


@pytest.mark.asyncio
async def test_precomputed_input_result_supplies_its_job_id(build_client):
    """A reused input_result carries ITS gate's job id into the session
    create — the reference follows the verdict that actually gated the
    text, not a fresh moderation call."""
    handler, state = make_backend()
    collie = build_client(handler)
    factory = make_factory(["ok"])
    ir = InputModerationResult(
        allowed=True, blocked=False, original_text="hi", job_id="job_pre",
    )

    await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory, input_result=ir,
    ))

    session_bodies = [b for b in state["created_jobs"] if not b.get("inbound_only")]
    assert len(session_bodies) == 1
    assert session_bodies[0]["input_job_id"] == "job_pre"


@pytest.mark.asyncio
async def test_precomputed_input_result_without_job_id_omits_reference(build_client):
    """An input_result with no job_id (constructed by hand) has no proof to
    forward — the field is omitted, not sent empty."""
    handler, state = make_backend()
    collie = build_client(handler)
    factory = make_factory(["ok"])
    ir = InputModerationResult(allowed=True, blocked=False, original_text="hi")

    await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory, input_result=ir,
    ))

    session_bodies = [b for b in state["created_jobs"] if not b.get("inbound_only")]
    assert "input_job_id" not in session_bodies[0]


# --------------------------------------------------------------------------
# Masked-input fail-closed (round 5)
# --------------------------------------------------------------------------
def make_masking_backend(filtered_text):
    """Input gate ALLOWS but returns a filtered (masked) prompt."""
    state = {"created_jobs": []}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            state["created_jobs"].append(body)
            return httpx.Response(202, json={
                "job_id": "job_mod" if body.get("inbound_only") else "job_stream"
            })
        if request.method == "GET" and path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={
                "job_id": "job_mod", "status": "completed",
                "inbound_result": {
                    "allowed": True, "blocked": False,
                    "filtered_content": filtered_text,
                    "triggered_rules": [],
                },
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    return handler, state


@pytest.mark.asyncio
@pytest.mark.parametrize("filtered", ["my [MASKED] prompt", ""])
async def test_masked_input_fails_closed_before_provider(build_client, filtered):
    """Round 5: the wrapper's own gate MASKED the prompt, but the zero-arg
    factory closes over the ORIGINAL — streaming would send unmasked
    content (PII included) to the model, against the documented promise.
    Fail closed: typed error, zero provider calls, no session created.
    The `""` row is the full wipe — `!=`, never truthiness."""
    from collieai import MaskedInputError

    handler, state = make_masking_backend(filtered)
    collie = build_client(handler)
    factory_state = {"factory_calls": 0}
    factory = make_factory(["never"], factory_state)

    with pytest.raises(MaskedInputError):
        await collect(collie.streaming.protect_stream(
            input="my secret prompt", raw_stream_factory=factory,
        ))

    assert factory_state["factory_calls"] == 0
    session_bodies = [b for b in state["created_jobs"] if not b.get("inbound_only")]
    assert session_bodies == []  # refused before any session create


@pytest.mark.asyncio
async def test_masked_input_fails_closed_in_buffered_too(build_client):
    from collieai import MaskedInputError

    handler, _ = make_masking_backend("my [MASKED] prompt")
    collie = build_client(handler)
    factory_state = {"factory_calls": 0}

    with pytest.raises(MaskedInputError):
        await collie.streaming.protect_buffered(
            input="my secret prompt",
            raw_stream_factory=make_factory(["never"], factory_state),
        )
    assert factory_state["factory_calls"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("filtered", ["my [MASKED] prompt", ""])
async def test_regate_mask_fails_closed_before_provider(build_client, filtered):
    """Round 6: the mask check must hold on the LADDER's re-gate too —
    initial gate allows unmasked, the create 409s stale, the RE-GATE
    (under the new policy) masks. Streaming would still leak the
    original; MaskedInputError, zero provider calls."""
    from collieai import MaskedInputError

    state = {"mod_calls": 0, "stale_served": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            if body.get("inbound_only"):
                state["mod_calls"] += 1
                return httpx.Response(
                    202, json={"job_id": f"job_mod{state['mod_calls']}"}
                )
            if body.get("input_job_id") and state["stale_served"] < 1:
                state["stale_served"] += 1
                return httpx.Response(409, json={"error": {
                    "message": "stale", "type": "input_gate_stale",
                    "code": "input_gate_stale",
                }})
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path.startswith("/v1/jobs/job_mod"):
            # The RE-GATE (job_mod2) masks; the original gate did not.
            masked = path.endswith("job_mod2")
            inbound = {"allowed": True, "blocked": False, "triggered_rules": []}
            if masked:
                inbound["filtered_content"] = filtered
            return httpx.Response(200, json={
                "job_id": path.rsplit("/", 1)[-1], "status": "completed",
                "inbound_result": inbound,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    factory_state = {"factory_calls": 0}

    with pytest.raises(MaskedInputError):
        await collect(collie.streaming.protect_stream(
            input="my secret prompt",
            raw_stream_factory=make_factory(["never"], factory_state),
        ))
    assert state["mod_calls"] == 2
    assert factory_state["factory_calls"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("filtered", ["my [MASKED] prompt", ""])
async def test_masking_recipe_streams_the_filtered_prompt(build_client, filtered):
    """The POSITIVE half of the recipe the fail-closed error prescribes
    (round 6): gate manually, build the factory over `filtered_text`
    (null-safe — `""` is a legitimate full wipe), pass `input_result`.
    The captured prompt PROVES the model gets the filtered text, not
    the original."""
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    original = "my secret prompt"
    ir = InputModerationResult(
        allowed=True, blocked=False, original_text=original,
        filtered_text=filtered, job_id="job_pre",
    )

    prompt_for_model = (
        ir.filtered_text if ir.filtered_text is not None else original
    )
    used_prompts = []

    def factory():
        async def gen():
            used_prompts.append(prompt_for_model)
            yield "ok"
        return gen()

    events = await collect(collie.streaming.protect_stream(
        input=original, raw_stream_factory=factory, input_result=ir,
    ))

    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert "".join(deltas) == "ok"
    assert used_prompts == [filtered]  # the model saw the FILTERED prompt
    assert original not in used_prompts


@pytest.mark.asyncio
async def test_masked_input_result_path_is_exempt(build_client):
    """The input_result path presumes the caller honored filtered_text (it
    holds both halves and owns the factory's contents) — no fail-closed."""
    backend = FakeChunkBackend()
    collie = build_client(backend.handler)
    ir = InputModerationResult(
        allowed=True, blocked=False, original_text="my secret prompt",
        filtered_text="my [MASKED] prompt", job_id="job_pre",
    )

    events = await collect(collie.streaming.protect_stream(
        input="my secret prompt",
        raw_stream_factory=make_factory(["ok"]),
        input_result=ir,
    ))
    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert "".join(deltas) == "ok"


# --------------------------------------------------------------------------
# The 409 input_gate_stale protocol (server claim contract §10.2)
# --------------------------------------------------------------------------
def make_stale_backend(*, stale_times=1, block_on_regate=False,
                       code="input_gate_stale"):
    """Session creates WITH an input_job_id 409 with `code` the first
    `stale_times` times; a claimless create always succeeds. Each
    moderation gate gets a distinct job id (job_mod1, job_mod2, …) so the
    re-gate's fresh proof is distinguishable from the original's."""
    state = {"created_jobs": [], "mod_calls": 0, "stale_served": 0,
             "factory_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            state["created_jobs"].append(body)
            if body.get("inbound_only"):
                state["mod_calls"] += 1
                return httpx.Response(
                    202, json={"job_id": f"job_mod{state['mod_calls']}"}
                )
            if body.get("input_job_id") and state["stale_served"] < stale_times:
                state["stale_served"] += 1
                return httpx.Response(409, json={"error": {
                    "message": "claim refused",
                    "type": code, "code": code,
                }})
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path.startswith("/v1/jobs/job_mod"):
            blocked = block_on_regate and path.endswith("job_mod2")
            return httpx.Response(200, json={
                "job_id": path.rsplit("/", 1)[-1],
                "status": "inbound_blocked" if blocked else "completed",
                "inbound_result": {
                    "allowed": not blocked, "blocked": blocked,
                    "block_message": "blocked by the CURRENT policy" if blocked else None,
                    "triggered_rules": [],
                },
            })
        if request.method == "POST" and path == "/v1/jobs/job_stream/chunks":
            body = body_of(request)
            is_final = bool(body.get("is_final"))
            emits = []
            if body.get("content") or is_final:
                emits = [{"content": body.get("content", ""), "blocked": False,
                          "final": is_final, "triggered_rules": []}]
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True,
                "emits": emits, "finished": is_final,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    return handler, state


@pytest.mark.asyncio
async def test_stale_gate_regates_once_before_provider(build_client):
    """§10.2: a 409 input_gate_stale on the session create triggers ONE fresh
    moderate.input — before the provider factory runs — and the retried
    create carries the NEW gate's job id as its proof."""
    handler, state = make_stale_backend(stale_times=1)
    collie = build_client(handler)
    factory_state = {"factory_calls": 0}
    factory = make_factory(["ok"], factory_state)

    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory,
    ))

    assert state["mod_calls"] == 2  # original gate + exactly one re-gate
    session_bodies = [b for b in state["created_jobs"] if not b.get("inbound_only")]
    assert [b["input_job_id"] for b in session_bodies] == ["job_mod1", "job_mod2"]
    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert "".join(deltas) == "ok"
    assert factory_state["factory_calls"] == 1


@pytest.mark.asyncio
async def test_second_stale_raises_with_zero_provider_calls(build_client):
    """Round 5: a 409 stale on the RETRIED create is POSITIVE PROOF the
    policy is churning faster than we can verify — the round-4 claimless
    downgrade would have raced the provider against a proven drift. The
    typed error surfaces with the provider never started; the caller
    decides."""
    handler, state = make_stale_backend(stale_times=2)
    collie = build_client(handler)
    factory_state = {"factory_calls": 0}
    factory = make_factory(["never"], factory_state)

    with pytest.raises(CollieAPIError) as exc_info:
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=factory,
        ))

    assert exc_info.value.code == "input_gate_stale"
    assert state["mod_calls"] == 2  # exactly one re-gate, no loop
    assert factory_state["factory_calls"] == 0  # provider NEVER started


@pytest.mark.asyncio
async def test_stale_then_unverifiable_downgrades_to_claimless(build_client):
    """The one second-409 that still downgrades: a pin outage beginning
    mid-turn (`input_gate_unverifiable` on the retried create). Unlike a
    second stale, nothing says the fresh re-gate verdict is wrong — the
    claimless session is the documented availability carve-out."""
    codes = iter(["input_gate_stale", "input_gate_unverifiable"])
    state = {"created_jobs": [], "mod_calls": 0, "factory_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            state["created_jobs"].append(body)
            if body.get("inbound_only"):
                state["mod_calls"] += 1
                return httpx.Response(
                    202, json={"job_id": f"job_mod{state['mod_calls']}"}
                )
            if body.get("input_job_id"):
                code = next(codes)
                return httpx.Response(409, json={"error": {
                    "message": "claim refused", "type": code, "code": code,
                }})
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path.startswith("/v1/jobs/job_mod"):
            return httpx.Response(200, json={
                "job_id": path.rsplit("/", 1)[-1], "status": "completed",
                "inbound_result": {"allowed": True, "blocked": False,
                                   "triggered_rules": []},
            })
        if request.method == "POST" and path == "/v1/jobs/job_stream/chunks":
            body = body_of(request)
            is_final = bool(body.get("is_final"))
            emits = ([{"content": body.get("content", ""), "blocked": False,
                       "final": is_final, "triggered_rules": []}]
                     if body.get("content") or is_final else [])
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True,
                "emits": emits, "finished": is_final,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=make_factory(["ok"]),
    ))
    session_bodies = [b for b in state["created_jobs"] if not b.get("inbound_only")]
    assert [b.get("input_job_id") for b in session_bodies] == [
        "job_mod1", "job_mod2", None,
    ]
    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert "".join(deltas) == "ok"


@pytest.mark.asyncio
async def test_unverifiable_downgrades_immediately_without_regate(build_client):
    """§10.2 round 4: input_gate_unverifiable means a pin outage — a fresh
    gate would be just as unpinned, so re-gating is pointless. The wrapper
    goes straight to the claimless session (v1 contract; the client-side
    gate with context just ran) and the provider starts only after that
    create succeeds."""
    handler, state = make_stale_backend(
        stale_times=1, code="input_gate_unverifiable",
    )
    collie = build_client(handler)
    factory_state = {"factory_calls": 0}
    factory = make_factory(["ok"], factory_state)

    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory,
    ))

    assert state["mod_calls"] == 1  # the original gate only — NO re-gate
    session_bodies = [b for b in state["created_jobs"] if not b.get("inbound_only")]
    assert [b.get("input_job_id") for b in session_bodies] == ["job_mod1", None]
    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert "".join(deltas) == "ok"
    assert factory_state["factory_calls"] == 1


@pytest.mark.asyncio
async def test_regate_block_yields_input_blocked_without_provider(build_client):
    """The stale protocol's point: when the CURRENT policy blocks the input,
    the verdict lands BEFORE any provider spend."""
    handler, state = make_stale_backend(stale_times=1, block_on_regate=True)
    collie = build_client(handler)
    factory_state = {"factory_calls": 0}
    factory = make_factory(["never"], factory_state)

    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory,
    ))

    assert len(events) == 1
    assert isinstance(events[0], InputBlocked)
    assert events[0].block_message == "blocked by the CURRENT policy"
    assert factory_state["factory_calls"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["input_gate_stale", "input_gate_unverifiable"])
async def test_claim_refusal_with_external_input_result_raises(build_client, code):
    """Rounds 3–4: an external input_result may have been produced WITH a
    context the wrapper never saw (context+input_result is rejected at
    the API edge). Neither a prompt-only auto re-gate nor a silent
    claimless downgrade is an honest remedy — the typed error surfaces
    (before any provider spend) and the CALLER re-gates with its own
    context."""
    handler, state = make_stale_backend(stale_times=1, code=code)
    collie = build_client(handler)
    factory_state = {"factory_calls": 0}
    factory = make_factory(["never"], factory_state)
    ir = InputModerationResult(
        allowed=True, blocked=False, original_text="hi", job_id="job_pre",
    )

    with pytest.raises(CollieAPIError) as exc_info:
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=factory, input_result=ir,
        ))

    assert exc_info.value.code == code
    assert state["mod_calls"] == 0  # NO automatic re-gate happened
    assert factory_state["factory_calls"] == 0  # provider never started


@pytest.mark.asyncio
async def test_claimed_gate_error_propagates(build_client):
    """input_gate_claimed is NOT retried — the gate is spent; surfacing the
    conflict is the only honest move. The counters pin "untouched": adding
    the code to the refusal ladder would run a billed re-gate + a second
    create before the same exception, and only the counts catch that (the
    post-round-10 workflow review found the code-only assert survived
    exactly that mutation)."""
    counts = {"gate_creates": 0, "session_creates": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            if body_of(request).get("inbound_only"):
                counts["gate_creates"] += 1
                return httpx.Response(202, json={"job_id": "job_mod"})
            counts["session_creates"] += 1
            return httpx.Response(409, json={"error": {
                "message": "already claimed", "type": "input_gate_claimed",
                "code": "input_gate_claimed",
            }})
        if request.method == "GET" and path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={
                "job_id": "job_mod", "status": "completed",
                "inbound_result": {"allowed": True, "blocked": False,
                                   "triggered_rules": []},
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    factory_state = {"factory_calls": 0}
    with pytest.raises(CollieAPIError) as exc_info:
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=make_factory(["never"], factory_state),
        ))
    assert exc_info.value.code == "input_gate_claimed"
    assert counts["gate_creates"] == 1  # the wrapper's own gate — no re-gate
    assert counts["session_creates"] == 1  # no second create attempt
    assert factory_state["factory_calls"] == 0  # provider never started


@pytest.mark.asyncio
async def test_input_result_mismatch_raises_synchronously(build_client):
    handler, _ = make_backend()
    collie = build_client(handler)
    factory = make_factory(["x"])
    ir = InputModerationResult(allowed=True, blocked=False, original_text="DIFFERENT")

    with pytest.raises(ValueError):
        collie.streaming.protect_stream(input="hi", raw_stream_factory=factory, input_result=ir)


@pytest.mark.asyncio
async def test_input_result_with_check_input_false_raises_synchronously(build_client):
    """Round 10: input_result + check_input=False used to be silently ignored
    (no claim sent, the session re-filtered the prompt) — exactly the shape a
    CheckInput=false integration would produce by accident. Now loud, in both
    wrappers, before any side effect."""
    handler, backend = make_backend()
    collie = build_client(handler)
    state = {"factory_calls": 0}
    factory = make_factory(["x"], state)
    ir = InputModerationResult(allowed=True, blocked=False, original_text="hi")

    with pytest.raises(ValueError, match="check_input=True"):
        collie.streaming.protect_stream(
            input="hi", raw_stream_factory=factory,
            input_result=ir, check_input=False,
        )
    with pytest.raises(ValueError, match="check_input=True"):
        await collie.streaming.protect_buffered(
            input="hi", raw_stream_factory=factory,
            input_result=ir, check_input=False,
        )
    assert state["factory_calls"] == 0
    assert backend["created_jobs"] == []


@pytest.mark.asyncio
async def test_input_result_reused_skips_moderation_call(build_client):
    handler, _ = make_backend()
    collie = build_client(handler)
    state = {"factory_calls": 0}
    factory = make_factory(["ok"], state)
    ir = InputModerationResult(allowed=True, blocked=False, original_text="hi")

    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=factory, input_result=ir,
    ))
    # No GET /v1/jobs/job_mod was needed (input_result supplied).
    deltas = [e.text for e in events if isinstance(e, SafeDelta)]
    assert "".join(deltas) == "ok"
    assert state["factory_calls"] == 1


@pytest.mark.asyncio
async def test_protect_stream_forwards_context_to_input_gate(build_client):
    """context passed to protect_stream rides the inbound moderation job."""
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            if body.get("inbound_only"):
                captured["body"] = body
                return httpx.Response(202, json={"job_id": "job_mod"})
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={
                "job_id": "job_mod", "status": "completed",
                "inbound_result": {"allowed": True, "blocked": False, "triggered_rules": []},
            })
        if request.method == "POST" and path == "/v1/jobs/job_stream/chunks":
            body = body_of(request)
            content, is_final = body.get("content", ""), bool(body.get("is_final"))
            emits = ([{"content": content, "blocked": False, "final": is_final, "triggered_rules": []}]
                     if (content or is_final) else [])
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True, "emits": emits, "finished": is_final,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    await collect(collie.streaming.protect_stream(
        input="hi",
        raw_stream_factory=make_factory(["ok"]),
        context={"transaction": {"title": "x"}},
        context_format="json",
    ))
    assert captured["body"]["context"] == {"transaction": {"title": "x"}}
    assert captured["body"]["context_format"] == "json"


@pytest.mark.asyncio
async def test_protect_stream_rejects_context_with_input_result(build_client):
    """input_result skips the input gate, so context would be dropped -> reject."""
    handler, _ = make_backend()
    collie = build_client(handler)
    ir = InputModerationResult(allowed=True, blocked=False, original_text="hi")

    with pytest.raises(ValueError):
        collie.streaming.protect_stream(
            input="hi", raw_stream_factory=make_factory(["x"]),
            input_result=ir, context={"a": "b"},
        )


@pytest.mark.asyncio
async def test_protect_buffered_rejects_context_with_input_result(build_client):
    handler, _ = make_backend()
    collie = build_client(handler)
    ir = InputModerationResult(allowed=True, blocked=False, original_text="hi")

    with pytest.raises(ValueError):
        await collie.streaming.protect_buffered(
            input="hi", raw_stream_factory=make_factory(["x"]),
            input_result=ir, context={"a": "b"},
        )


@pytest.mark.asyncio
async def test_protect_buffered_external_result_is_trusted_client_reuse(build_client):
    """The positive wire pin of the buffered trusted-client contract (round
    10): an external allowed result — carrying a REAL job_id — skips the
    input pass entirely and claims nothing. Provider runs once, the ONLY job
    created is the message_output one, and input_job_id is sent nowhere (the
    absence that lets the server neither verify nor consume the result)."""
    handler, backend = make_buffered_backend()
    collie = build_client(handler)
    state = {"factory_calls": 0}
    ir = InputModerationResult(
        allowed=True, blocked=False, original_text="hi", job_id="job_gate",
    )

    result = await collie.streaming.protect_buffered(
        input="hi", raw_stream_factory=make_factory(["ok"], state),
        input_result=ir,
    )

    assert result.blocked is False
    assert state["factory_calls"] == 1
    jobs = backend["created_jobs"]
    assert len(jobs) == 1  # no inbound moderation job
    assert "message_output" in jobs[0]
    assert not jobs[0].get("inbound_only")
    assert all("input_job_id" not in j for j in jobs)


# --------------------------------------------------------------------------
# protect_buffered
# --------------------------------------------------------------------------
def make_buffered_backend(*, input_blocked=False, output_blocked=False, filtered="CLEAN",
                          context_result=None, blocked_by=None):
    state = {"origins": [], "created_jobs": []}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        state["origins"].append(request.headers.get("X-CollieAi-SDK-Origin"))
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            state["created_jobs"].append(body)
            if body.get("inbound_only"):
                return httpx.Response(202, json={"job_id": "job_mod"})
            return httpx.Response(202, json={"job_id": "job_buf"})
        if request.method == "GET" and path == "/v1/jobs/job_mod":
            payload = {
                "job_id": "job_mod",
                "status": "inbound_blocked" if input_blocked else "completed",
                "inbound_result": {"allowed": not input_blocked, "blocked": input_blocked,
                                   "block_message": "input blocked", "triggered_rules": []},
            }
            if context_result is not None:
                payload["context_result"] = context_result
            if blocked_by is not None:
                payload["blocked_by"] = blocked_by
            return httpx.Response(200, json=payload)
        if request.method == "GET" and path == "/v1/jobs/job_buf":
            outbound = {
                "allowed": not output_blocked, "blocked": output_blocked,
                "block_message": "output blocked" if output_blocked else None,
                "filtered_content": None if output_blocked else filtered,
                "triggered_rules": [],
            }
            return httpx.Response(200, json={
                "job_id": "job_buf",
                "status": "outbound_blocked" if output_blocked else "completed",
                "outbound_result": outbound,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})
    return handler, state


@pytest.mark.asyncio
async def test_protect_buffered_allowed_returns_filtered_text(build_client):
    handler, _ = make_buffered_backend(filtered="clean answer")
    collie = build_client(handler)
    factory = make_factory(["raw ", "answer"])
    result = await collie.streaming.protect_buffered(input="hi", raw_stream_factory=factory)
    assert result.blocked is False
    assert result.filtered_text == "clean answer"


@pytest.mark.asyncio
async def test_protect_buffered_output_blocked(build_client):
    handler, _ = make_buffered_backend(output_blocked=True)
    collie = build_client(handler)
    factory = make_factory(["bad answer"])
    result = await collie.streaming.protect_buffered(input="hi", raw_stream_factory=factory)
    assert result.blocked is True
    assert result.input_blocked is False
    assert result.block_message == "output blocked"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,outbound,expect_blocked",
    [
        # The #22 fix itself: an EMPTY outbound_result used to fail-OPEN
        # (blocked=False); under the v24 table ambiguity is blocked.
        ("completed", {}, True),
        ("completed", {"blocked": False}, True),                     # absent allowed
        ("completed", {"allowed": False}, True),                     # deny, blocked absent
        ("completed", {"allowed": True, "blocked": False}, False),   # explicit allow
        ("completed", {"allowed": True, "blocked": True}, True),     # inconsistent
        ("outbound_blocked", {"allowed": True, "blocked": False}, True),  # status wins
        ("completed", {"allowed": "yes", "blocked": False}, True),   # non-bool ≠ True
    ],
)
async def test_buffered_verdict_table_is_fail_safe(
    build_client, status, outbound, expect_blocked
):
    """The v24 fail-safe table on the buffered path (tech-debt #22): NOT
    blocked requires an EXPLICIT allowed=true and no hard-block signal;
    everything ambiguous resolves to blocked — same rows as
    moderate.output's table, mirrored per-SDK."""
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            return httpx.Response(202, json={"job_id": "job_buf"})
        if request.method == "GET" and path == "/v1/jobs/job_buf":
            return httpx.Response(200, json={
                "job_id": "job_buf", "status": status,
                "outbound_result": outbound,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    result = await collie.streaming.protect_buffered(
        input="hi", raw_stream_factory=make_factory(["text"]), check_input=False,
    )
    assert result.blocked is expect_blocked


@pytest.mark.asyncio
async def test_protect_buffered_input_blocked_skips_factory(build_client):
    handler, _ = make_buffered_backend(input_blocked=True)
    collie = build_client(handler)
    state = {"factory_calls": 0}
    factory = make_factory(["never"], state)
    result = await collie.streaming.protect_buffered(input="hi", raw_stream_factory=factory)
    assert result.blocked is True
    assert result.input_blocked is True
    assert state["factory_calls"] == 0


@pytest.mark.asyncio
async def test_protect_stream_finished_carries_input_context(build_client):
    """A monitored/degraded-but-allowed context is observable on the success
    terminal (Finished), not only on a block (Slice 1f review)."""
    handler, _ = make_backend(
        context_result={"status": "monitored", "blocked": False},
        blocked_by="none",
    )
    collie = build_client(handler)
    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=make_factory(["hello"]), context={"a": "b"},
    ))
    finished = events[-1]
    assert isinstance(finished, Finished)
    assert finished.blocked_by == "none"
    assert finished.context is not None
    assert finished.context.status == "monitored"


@pytest.mark.asyncio
async def test_protect_buffered_success_carries_input_context(build_client):
    handler, _ = make_buffered_backend(
        filtered="clean",
        context_result={"status": "degraded", "inference_degraded": True},
        blocked_by="none",
    )
    collie = build_client(handler)
    result = await collie.streaming.protect_buffered(
        input="hi", raw_stream_factory=make_factory(["x"]), context={"a": "b"},
    )
    assert result.blocked is False
    assert result.context is not None
    assert result.context.status == "degraded"
    assert result.context.inference_degraded is True


# --------------------------------------------------------------------------
# Review-round fixes: origin attribution, factory-after-session, missing
# terminal result objects.
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_protect_stream_attributes_origin(build_client):
    handler, state = make_backend()
    collie = build_client(handler)
    await collect(collie.streaming.protect_stream(input="hi", raw_stream_factory=make_factory(["x"])))
    assert state["origins"]
    assert set(state["origins"]) == {"protect_stream"}  # input check + job + chunks


@pytest.mark.asyncio
async def test_protect_buffered_attributes_origin(build_client):
    handler, state = make_buffered_backend()
    collie = build_client(handler)
    await collie.streaming.protect_buffered(input="hi", raw_stream_factory=make_factory(["x"]))
    assert state["origins"]
    assert set(state["origins"]) == {"protect_buffered"}


@pytest.mark.asyncio
async def test_session_create_failure_does_not_invoke_factory(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        body = body_of(request) if request.method == "POST" else {}
        if request.method == "POST" and path == "/v1/jobs":
            if body.get("inbound_only"):
                return httpx.Response(202, json={"job_id": "job_mod"})
            return error_response(500, "server_error", "boom")  # stream-job create fails
        if path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={"status": "completed", "inbound_result": {
                "allowed": True, "blocked": False, "triggered_rules": []}})
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    state = {"factory_calls": 0}
    factory = make_factory(["never"], state)
    with pytest.raises(CollieAPIError):
        await collect(collie.streaming.protect_stream(input="hi", raw_stream_factory=factory))
    assert state["factory_calls"] == 0  # provider never opened — no paid LLM work


@pytest.mark.asyncio
async def test_protect_buffered_missing_outbound_result_raises(build_client):
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            return httpx.Response(202, json={"job_id": "job_mod" if body.get("inbound_only") else "job_buf"})
        if path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={"status": "completed", "inbound_result": {
                "allowed": True, "blocked": False, "triggered_rules": []}})
        if path == "/v1/jobs/job_buf":
            return httpx.Response(200, json={"status": "completed"})  # no outbound_result
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as ei:
        await collie.streaming.protect_buffered(input="hi", raw_stream_factory=make_factory(["x"]))
    assert ei.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_provider_closed_on_external_cancellation(build_client):
    """Cancelling the consumer while the batcher awaits the next provider delta
    must still finalize the provider (its finally runs)."""
    handler, _ = make_backend()
    collie = build_client(handler)
    closed = {"v": False}
    started = asyncio.Event()

    async def factory():
        try:
            yield "first"
            started.set()
            await asyncio.Event().wait()  # block on the next delta forever
            yield "never"  # pragma: no cover
        finally:
            closed["v"] = True

    async def run():
        async for _ in collie.streaming.protect_stream(
            input="hi",
            raw_stream_factory=factory,
            max_deltas=99,         # don't flush by count
            flush_interval_s=100,  # don't flush by timer
        ):
            pass

    task = asyncio.ensure_future(run())
    await started.wait()        # provider yielded "first", now blocked on next read
    await asyncio.sleep(0)       # let the batcher start awaiting the next delta
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert closed["v"] is True   # provider finalized despite the in-flight read


# ---------------------------------------------------------------------------
# Context verdict carried on the wrapper input-block paths (Slice 1f review)
# ---------------------------------------------------------------------------

def _ctx_input_block_handler():
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            jid = "job_mod" if body.get("inbound_only") else "job_stream"
            return httpx.Response(202, json={"job_id": jid})
        if request.method == "GET" and path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={
                "job_id": "job_mod", "status": "inbound_blocked",
                "inbound_result": {"allowed": False, "blocked": True,
                                   "block_message": "ctx", "triggered_rules": []},
                "blocked_by": "context",
                "context_result": {"status": "blocked", "blocked": True,
                                   "triggering_pointer": "/transaction/title",
                                   "triggering_rule_type": "lightweight_model"},
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "x"}})
    return handler


@pytest.mark.asyncio
async def test_protect_stream_input_block_carries_context(build_client):
    collie = build_client(_ctx_input_block_handler())
    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=make_factory(["x"]),
        context={"transaction": {"title": "y"}},
    ))
    assert len(events) == 1
    ev = events[0]
    assert isinstance(ev, InputBlocked)
    assert ev.blocked_by == "context"
    assert ev.context is not None
    assert ev.context.status == "blocked"
    assert ev.context.triggering_pointer == "/transaction/title"
    assert ev.context.triggering_rule_type == "lightweight_model"


@pytest.mark.asyncio
async def test_protect_buffered_input_block_carries_context(build_client):
    collie = build_client(_ctx_input_block_handler())
    result = await collie.streaming.protect_buffered(
        input="hi", raw_stream_factory=make_factory(["x"]), context={"a": "b"},
    )
    assert result.input_blocked is True
    assert result.blocked_by == "context"
    assert result.context is not None
    assert result.context.status == "blocked"
    assert result.context.triggering_pointer == "/transaction/title"


# --------------------------------------------------------------------------
# Mutation-kill tests (#26 streaming target): each test exists because a
# specific surviving mutant proved the class unpinned (the maintainers'
# mutation-testing register, "Python streaming").
# --------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_not_allowed_external_input_result_rejected_at_the_edge_streaming(build_client):
    """Reachability proof for the v23 explicit-allow gate's `or` (mutant
    261): an external input_result that is not affirmatively allowed is
    rejected at the API EDGE with a loud ValueError — before the gate,
    before any session, before the factory. Together with the parser's
    ambiguity-normalization this makes the gate's `not allowed` half
    pure defense-in-depth (documented equivalent), and THIS test pins
    the guard that makes it so."""
    handler, state = make_backend()
    collie = build_client(handler)
    factory_state = {"factory_calls": 0}
    ir = InputModerationResult(allowed=False, blocked=False, original_text="hi")

    with pytest.raises(ValueError, match="allowed"):
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=make_factory(["never"], factory_state),
            input_result=ir,
        ))

    assert factory_state["factory_calls"] == 0
    assert state["created_jobs"] == []


@pytest.mark.asyncio
async def test_not_allowed_external_input_result_rejected_at_the_edge_buffered(build_client):
    """The buffered twin of the edge guard."""
    handler, state = make_buffered_backend()
    collie = build_client(handler)
    factory_state = {"factory_calls": 0}
    ir = InputModerationResult(allowed=False, blocked=False, original_text="hi")

    with pytest.raises(ValueError, match="allowed"):
        await collie.streaming.protect_buffered(
            input="hi", raw_stream_factory=make_factory(["never"], factory_state),
            input_result=ir,
        )
    assert factory_state["factory_calls"] == 0
    assert state["created_jobs"] == []


@pytest.mark.asyncio
async def test_finished_carries_the_regate_context(build_client):
    """When the stale ladder re-gates and the turn then SUCCEEDS, the
    Finished terminal must carry the RE-GATE's context verdict (the
    observability half) — survivors 272/273 nulled the carried variables
    and only the blocked path was pinned."""
    state = {"mod_calls": 0, "stale_served": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            if body.get("inbound_only"):
                state["mod_calls"] += 1
                return httpx.Response(202, json={"job_id": f"job_mod{state['mod_calls']}"})
            if body.get("input_job_id") and state["stale_served"] < 1:
                state["stale_served"] += 1
                return httpx.Response(409, json={"error": {
                    "message": "stale", "type": "input_gate_stale",
                    "code": "input_gate_stale",
                }})
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path.startswith("/v1/jobs/job_mod"):
            payload = {
                "job_id": path.rsplit("/", 1)[-1], "status": "completed",
                "inbound_result": {"allowed": True, "blocked": False, "triggered_rules": []},
            }
            if path.endswith("job_mod2"):  # the RE-GATE ran context analysis
                payload["blocked_by"] = "none"
                payload["context_result"] = {"status": "monitored", "blocked": False}
            return httpx.Response(200, json=payload)
        if request.method == "POST" and path == "/v1/jobs/job_stream/chunks":
            body = body_of(request)
            is_final = bool(body.get("is_final"))
            content = body.get("content", "")
            emits = []
            if content or is_final:
                emits = [{"content": content, "blocked": False, "final": is_final,
                          "triggered_rules": []}]
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True,
                "emits": emits, "finished": is_final,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=make_factory(["ok"]),
    ))

    finished = events[-1]
    assert type(finished).__name__ == "Finished"
    assert state["mod_calls"] == 2  # the ladder re-gated exactly once
    assert finished.context is not None and finished.context.status == "monitored"
    assert finished.blocked_by == "none"  # survivor 272: the carried half


@pytest.mark.asyncio
async def test_regate_block_carries_blocked_by_and_context(build_client):
    """When the RE-GATE blocks, the InputBlocked event must carry the
    re-gate's blocked_by and typed context verdict — survivors 272/273
    nulled both and nothing failed (the context pointer is the whole
    reason a caller can distinguish a context block)."""
    state = {"mod_calls": 0, "stale_served": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            if body.get("inbound_only"):
                state["mod_calls"] += 1
                return httpx.Response(202, json={"job_id": f"job_mod{state['mod_calls']}"})
            if body.get("input_job_id") and state["stale_served"] < 1:
                state["stale_served"] += 1
                return httpx.Response(409, json={"error": {
                    "message": "stale", "type": "input_gate_stale",
                    "code": "input_gate_stale",
                }})
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path.startswith("/v1/jobs/job_mod"):
            if path.endswith("job_mod2"):  # the re-gate blocks, by CONTEXT
                return httpx.Response(200, json={
                    "job_id": "job_mod2", "status": "inbound_blocked",
                    "inbound_result": {"allowed": False, "blocked": True,
                                       "block_message": "ctx", "triggered_rules": []},
                    "blocked_by": "context",
                    "context_result": {"status": "blocked", "blocked": True,
                                       "block_message": "ctx",
                                       "triggering_pointer": "/page"},
                })
            return httpx.Response(200, json={
                "job_id": "job_mod1", "status": "completed",
                "inbound_result": {"allowed": True, "blocked": False, "triggered_rules": []},
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    events = await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=make_factory(["never"]),
    ))

    assert len(events) == 1 and isinstance(events[0], InputBlocked)
    assert events[0].blocked_by == "context"
    assert events[0].context is not None and events[0].context.status == "blocked"


def _chunk_recording_backend(*, block_on_first=False, finish_on_first=False):
    """Happy-path chunk backend that RECORDS every chunk body, so wire
    shape (is_final / finish_reason) is assertable."""
    state = {"created_jobs": [], "chunk_bodies": []}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path == "/v1/jobs":
            body = body_of(request)
            state["created_jobs"].append(body)
            if body.get("inbound_only"):
                return httpx.Response(202, json={"job_id": "job_mod"})
            return httpx.Response(202, json={"job_id": "job_stream"})
        if request.method == "GET" and path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={
                "job_id": "job_mod", "status": "completed",
                "inbound_result": {"allowed": True, "blocked": False, "triggered_rules": []},
            })
        if request.method == "POST" and path == "/v1/jobs/job_stream/chunks":
            body = body_of(request)
            state["chunk_bodies"].append(body)
            content = body.get("content", "")
            is_final = bool(body.get("is_final"))
            if block_on_first and content:
                return httpx.Response(200, json={
                    "sequence": body["sequence"], "accepted": True,
                    "emits": [{"content": "", "blocked": True, "final": True,
                               "block_message": "blocked", "triggered_rules": []}],
                    "finished": True,
                })
            if finish_on_first and content:
                return httpx.Response(200, json={
                    "sequence": body["sequence"], "accepted": True,
                    "emits": [{"content": content, "blocked": False, "final": True,
                               "triggered_rules": []}],
                    "finished": True,
                })
            emits = []
            if content or is_final:
                emits = [{"content": content, "blocked": False, "final": is_final,
                          "triggered_rules": []}]
            return httpx.Response(200, json={
                "sequence": body["sequence"], "accepted": True,
                "emits": emits, "finished": is_final,
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    return handler, state


def _endless_factory():
    """An UNBOUNDED provider: if a terminal emit fails to break the
    submit loop (`break`→`continue`), the wrapper submits forever — the
    loop contract is the pin, not the prefetch count (the batcher
    legitimately prefetches). The callers wrap in asyncio.wait_for so a
    real regression is a DETERMINISTIC failure in plain CI, not a hang
    that only a mutation harness's timeout would catch."""
    async def factory():
        i = 0
        while True:
            yield f"d{i}"
            i += 1
            await asyncio.sleep(0)
    return factory


@pytest.mark.asyncio
async def test_midstream_block_stops_the_submit_loop_and_skips_final_flush(build_client):
    """A mid-stream block is TERMINAL: exactly ONE chunk submission
    (survivor 293 turned `break` into `continue`) and no final flush
    after it (survivors 297/301 flipped the blocked flag/flush
    condition — a flush after a block would re-open a closed verdict)."""
    handler, state = _chunk_recording_backend(block_on_first=True)
    collie = build_client(handler)

    events = await asyncio.wait_for(collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=_endless_factory(),
    )), timeout=1)

    assert [type(e).__name__ for e in events] == ["Blocked"]
    assert len(state["chunk_bodies"]) == 1  # the block broke the loop
    assert all(not b.get("is_final") for b in state["chunk_bodies"])


@pytest.mark.asyncio
async def test_server_finish_stops_the_submit_loop_and_skips_final_flush(build_client):
    """The server finishing the session early has the same loop contract
    as a block (survivors 288/289/296): one submission, no is_final
    flush, terminal Finished."""
    handler, state = _chunk_recording_backend(finish_on_first=True)
    collie = build_client(handler)

    events = await asyncio.wait_for(collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=_endless_factory(),
    )), timeout=1)

    assert type(events[-1]).__name__ == "Finished"
    assert len(state["chunk_bodies"]) == 1
    assert all(not b.get("is_final") for b in state["chunk_bodies"])


@pytest.mark.asyncio
async def test_final_flush_carries_stop_and_only_the_final_chunk(build_client):
    """Wire shape of the finish: exactly one is_final chunk, carrying
    finish_reason="stop"; non-final chunks carry NO finish_reason
    (survivors 304/599 mutated both halves)."""
    handler, state = _chunk_recording_backend()
    collie = build_client(handler)

    await collect(collie.streaming.protect_stream(
        input="hi", raw_stream_factory=make_factory(["a", "b"]),
    ))

    finals = [b for b in state["chunk_bodies"] if b.get("is_final")]
    non_finals = [b for b in state["chunk_bodies"] if not b.get("is_final")]
    assert len(finals) == 1
    assert finals[0].get("finish_reason") == "stop"
    assert all("finish_reason" not in b for b in non_finals)


@pytest.mark.asyncio
async def test_require_streaming_reuses_the_cached_preflight(build_client):
    """require_streaming consults the preflight CACHE (survivor 244
    flipped force_refresh to True): two protected turns within the
    validity window must cost exactly ONE preflight call."""
    state = {"preflights": 0}
    base, base_state = _chunk_recording_backend()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/streaming/preflight":
            state["preflights"] += 1
            return httpx.Response(200, json={
                "mode": "streaming", "recommended_client_behavior": "stream",
                "project_id": "proj_test", "valid_until": "2999-01-01T00:00:00Z",
                "rules": [],
            })
        return base(request)

    collie = build_client(handler)
    for _ in range(2):
        await collect(collie.streaming.protect_stream(
            input="hi", raw_stream_factory=make_factory(["a"]),
            require_streaming=True,
        ))
    assert state["preflights"] == 1


@pytest.mark.asyncio
async def test_buffered_result_parses_triggered_rules(build_client):
    """The buffered verdict's triggered_rules must survive parsing
    (survivor 405 read a wrong key and every test still passed —
    nothing asserted a NON-EMPTY rule list on the allowed path)."""
    handler, state = make_buffered_backend()
    collie = build_client(handler)

    def with_rules(request: httpx.Request) -> httpx.Response:
        resp = handler(request)
        if request.url.path == "/v1/jobs/job_buf" and request.method == "GET":
            payload = resp.json()
            payload["outbound_result"]["triggered_rules"] = [
                {"rule_id": "r9", "rule_name": "masker", "rule_type": "regex",
                 "decision": "mask"},
            ]
            return httpx.Response(200, json=payload)
        return resp

    collie = build_client(with_rules)
    result = await collie.streaming.protect_buffered(
        input="hi", raw_stream_factory=make_factory(["a"]),
    )
    assert [r.rule_name for r in result.triggered_rules] == ["masker"]


@pytest.mark.asyncio
async def test_buffered_create_without_job_id_is_invalid_response(build_client):
    """The buffered create's malformed-response CODE is wire contract
    (survivor 374 mutated code="invalid_response" and nothing failed)."""
    from collieai import CollieAPIError

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/jobs":
            body = body_of(request)
            if body.get("inbound_only"):
                return httpx.Response(202, json={"job_id": "job_mod"})
            return httpx.Response(202, json={})  # no job_id
        if request.method == "GET" and request.url.path == "/v1/jobs/job_mod":
            return httpx.Response(200, json={
                "job_id": "job_mod", "status": "completed",
                "inbound_result": {"allowed": True, "blocked": False, "triggered_rules": []},
            })
        return httpx.Response(404, json={"error": {"message": "nf", "type": "not_found"}})

    collie = build_client(handler)
    with pytest.raises(CollieAPIError) as excinfo:
        await collie.streaming.protect_buffered(
            input="hi", raw_stream_factory=make_factory(["a"]),
        )
    assert excinfo.value.code == "invalid_response"


@pytest.mark.asyncio
async def test_buffered_result_carries_request_id_and_input_blocked_by(build_client):
    """The buffered result's request_id (survivor 375) and the
    input-phase blocked_by carried onto the result (survivor 378) are
    both readable contract."""
    handler, state = make_buffered_backend(input_blocked=True, blocked_by="prompt")

    def with_rid(request: httpx.Request) -> httpx.Response:
        resp = handler(request)
        if request.method == "POST" and request.url.path == "/v1/jobs":
            resp.headers["x-request-id"] = "rid-77"
        return resp

    collie = build_client(with_rid)
    result = await collie.streaming.protect_buffered(
        input="hi", raw_stream_factory=make_factory(["never"]),
    )
    assert result.blocked and result.blocked_by == "prompt"
    assert result.request_id == "rid-77"
