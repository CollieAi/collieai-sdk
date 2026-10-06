# CollieAi Python SDK

[![PyPI](https://img.shields.io/pypi/v/collieai)](https://pypi.org/project/collieai/)
[![CI](https://github.com/CollieAi/collieai-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/CollieAi/collieai-sdk/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/CollieAi/collieai-sdk/blob/main/LICENSE)

The Python client for [CollieAi](https://collieai.io), the AI guardrails service for LLM
applications: prompt injection detection, PII masking, input and output
moderation. You keep calling your own model; the SDK checks the prompt before
the call and streams back **only text CollieAi has released**, never raw model
output. You need a CollieAi API key and a project whose policy defines the
rules.

## Install

```bash
pip install collieai
```

## Quick start

```python
import asyncio
from collieai import AsyncCollie

async def main() -> None:
    async with AsyncCollie(api_key="clai_...", project_id="project_123") as collie:
        user_prompt = "My card is 4111 1111 1111 1111, why was I charged twice?"
        result = await collie.moderate.input(prompt=user_prompt)
        if result.blocked:
            print(result.block_message or "Input blocked by policy.")
            return
        # Masking rules may have removed PII: the model gets the FILTERED prompt.
        prompt_for_model = result.filtered_text if result.filtered_text is not None else user_prompt
        print(prompt_for_model)  # now call your LLM with it

asyncio.run(main())
```

To stream the model's answer through CollieAi as well, use `protect_stream`
(see "Stream safely" below).

## Links

- Documentation: https://docs.collieai.io/sdks/python-sdk
- Examples: https://github.com/CollieAi/collieai-sdk/tree/main/python/examples
- Changelog: https://github.com/CollieAi/collieai-sdk/blob/main/python/CHANGELOG.md
- Node.js SDK: https://github.com/CollieAi/collieai-sdk/tree/main/node
- .NET SDK: https://github.com/CollieAi/collieai-sdk/tree/main/dotnet

## Usage

### Construct the client

```python
from collieai import AsyncCollie

collie = AsyncCollie(
    api_key="clai_...",
    base_url="https://app.collieai.io",
    project_id="project_123",
)
```

The client reuses one pooled HTTP connection set. Close it when done
(`await collie.aclose()`), or use it as an async context manager.

### Check an input before calling your LLM

```python
result = await collie.moderate.input(
    prompt=user_prompt,
    conversation_id=conversation_id,   # optional, groups a conversation
    correlation_id=chat_turn_id,       # optional, pins one turn
)

if result.blocked:
    return result.block_message or "Input blocked by policy."

# If your input policy MASKS, send the FILTERED prompt to your model —
# `is not None`, never truthiness: "" is a legitimate full wipe.
prompt_for_model = (
    result.filtered_text if result.filtered_text is not None else user_prompt
)
```

A policy block is a normal result (`result.blocked is True`), not an exception.
No webhook is required. `filtered_text` is the post-mask prompt — the raw
values a mask rule removed must never reach your model, so it is
`prompt_for_model`, not `user_prompt`, that goes into your LLM call.

### Analyze context alongside the prompt

Pass `context` to analyze the structured data (or raw string) you're about to
feed the model — retrieved documents, tool output, a transaction record —
alongside the prompt, so an injection hidden in that data is caught too, not just
one in the prompt. Structured `context` travels as JSON; `context_format`
(`"auto"` | `"json"` | `"text"`) is an optional parsing hint for a raw string.

```python
result = await collie.moderate.input(
    prompt=user_prompt,
    context={"transaction": {"title": retrieved_title, "memo": retrieved_memo}},
)

if result.context and result.context.status in ("monitored", "blocked", "degraded"):
    print(result.context.status,
          result.context.triggering_pointer,     # e.g. "/transaction/memo" — a path, never a value
          result.context.triggering_rule_type)

if result.blocked:
    # result.blocked_by is "prompt" or "context" — which surface blocked
    return result.block_message or "Blocked by policy."
```

`result.context` (a `ContextModerationResult`) carries the closed `status` enum
(`not_provided` / `disabled` / `not_run` / `clean` / `monitored` / `blocked` /
`degraded`), the triggering JSON Pointer + rule, and degraded-coverage markers
(`parse_degraded` / `limit_exceeded` / `inference_degraded`). The pointer is a
**path, never a value** — no analyzed content is echoed back.

The same `context` / `context_format` arguments work on `protect_stream` and
`protect_buffered` **when input checking is enabled** (the default): context is
analyzed by the input gate, so a wrapper called with `check_input=False` skips it
entirely and never analyzes context. With the gate on, the verdict rides the
terminal event (`InputBlocked` on a context block, `Finished` on success) and
`protect_buffered`'s result, and `to_sse(...)` serializes it onto the
`input_blocked` / `finished` relay frames.

> **Until a policy turns context analysis on**, `context` is inert — sending it
> is a safe no-op (`status` is `not_provided` / `disabled`). Enable it per policy
> server-side to activate detection.

### Stream safely — the recommended path

`protect_stream` checks the input, calls your LLM **only if it passes**, batches
the output, and yields only CollieAi-released events. Pass a *factory* (a
zero-arg callable returning your stream), not an already-started stream — the
SDK calls it once, after the input check.

```python
from collieai import SafeDelta, Blocked, InputBlocked

async for event in collie.streaming.protect_stream(
    input=user_prompt,
    raw_stream_factory=lambda: your_llm_stream(user_prompt),
):
    if isinstance(event, SafeDelta):
        yield event.text                       # forward ONLY safe text
    elif isinstance(event, (Blocked, InputBlocked)):
        yield event.block_message or "Blocked by policy."
        break
```

A block — of the input or the output — is a normal terminal **event**, not an
exception. You never touch chunk sequence numbers, retries, or batching.

#### FastAPI

```python
from fastapi.responses import StreamingResponse
from openai import AsyncOpenAI
from collieai import SafeDelta, Blocked, InputBlocked
from collieai.adapters.openai import openai_factory   # pip install "collieai[openai]"

openai_client = AsyncOpenAI()

@app.post("/chat")
async def chat(prompt: str):
    factory = openai_factory(
        openai_client, model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
    )
    async def body():
        async for event in collie.streaming.protect_stream(
            input=prompt, raw_stream_factory=factory,
        ):
            if isinstance(event, SafeDelta):
                yield event.text
            elif isinstance(event, (Blocked, InputBlocked)):
                yield event.block_message or "Blocked by policy."
                return
    return StreamingResponse(body(), media_type="text/plain")
```

### Choose the UX up front (preflight)

Ask whether the policy can stream *before* you call the LLM, and branch into a
token-stream UI or a "checking response…" UI accordingly:

```python
cap = await collie.streaming.preflight()   # cached until cap.valid_until

if cap.recommended_client_behavior == "stream":
    async for event in collie.streaming.protect_stream(
        input=user_prompt, raw_stream_factory=lambda: your_llm_stream(user_prompt),
    ):
        ...   # token-stream UI
elif cap.recommended_client_behavior == "buffer_then_show":
    result = await collie.streaming.protect_buffered(
        input=user_prompt, raw_stream_factory=lambda: your_llm_stream(user_prompt),
    )
    ...       # "checking response…" then show result
else:
    raise RuntimeError(cap.reason_detail or cap.reason)   # fail_fast
```

If you'd rather not branch yourself, pass `require_streaming=True` to
`protect_stream`: it preflights first and raises `BufferedFallbackRequired`
(policy must buffer) or a `PreflightError` (can't be served) **before** calling
your LLM.

If your input policy MASKS the prompt, `protect_stream`/`protect_buffered`
raise `MaskedInputError` **before** your factory runs — the zero-arg factory
closes over the original text, and streaming it would send unmasked content
to your model. The recipe: gate manually with `moderate.input`, build the
factory over `result.filtered_text` (`""` is a legitimate full wipe), and
pass `input_result=result` (that path is exempt). Keep `check_input`
at its default `True`: combining `input_result` with
`check_input=False` is a contradiction and raises `ValueError` (in
2.0 it was silently ignored).

### Buffered fallback

When a policy can't stream, check the whole response at once — same input-gate
and factory contract, but it returns a single result instead of events:

```python
result = await collie.streaming.protect_buffered(
    input=user_prompt,
    raw_stream_factory=lambda: your_llm_stream(user_prompt),
)
return result.block_message if result.blocked else result.filtered_text
```

Reusing an `input_result` here differs from `protect_stream`: the streaming
path turns a result carrying a `job_id` into a server-verified, expiring,
single-use claim against a 2.1+ server (without a `job_id`, or on an older
server, the session runs claimless and re-filters the input in its own
async pass — the pre-2.1 shape: billed, and the verdict can land after
streaming has started), while the
buffered path is a **trusted-client reuse** — the server neither verifies nor
consumes the result and no `input_gate_*` error can occur. Obtain it in the
same turn, same client and project, immediately before the call; never cache
or reuse it.

### Relay to a browser (SSE)

A backend that submits chunks for a job can also subscribe to that job's
CollieAi SSE stream and relay safe events to a browser, with automatic
reconnect:

```python
from collieai import Blocked, Finished, StreamInterrupted, to_sse

async for event in session.stream_events():   # auto-resumes from Last-Event-ID
    if isinstance(event, StreamInterrupted):
        continue                              # reconnecting; nothing to forward
    yield to_sse(event)                       # SSE bytes for your text/event-stream
    if isinstance(event, (Blocked, Finished)):
        break
```

Replayed frames are deduplicated across reconnects, so a delta is never shown
twice. Pass `auto_resume=False` to stop at the first `StreamInterrupted` and
resume yourself with `stream_events(last_event_id=...)`.

To let a **browser** subscribe directly, mint a short-lived, job-scoped token
(your API key stays server-side):

```python
st = await session.mint_stream_token()
# hand st.url to the browser: new EventSource(st.url)
```

The token is read-only, valid only for that one job's stream, and expires in
`st.expires_in` seconds — re-mint before it lapses.

### Low-level session (advanced)

If you need to drive batching yourself, use the session directly. It does
**not** check the input — call `moderate.input(...)` first.

```python
check = await collie.moderate.input(prompt=user_prompt)
if check.blocked:
    return check.block_message or "Input blocked by policy."

# The split that matters when your input policy masks:
# - the SESSION gets the ORIGINAL prompt (it must byte-match the gate);
# - your MODEL gets the FILTERED prompt (`is not None`, never truthiness —
#   "" is a legitimate full wipe).
prompt_for_model = (
    check.filtered_text if check.filtered_text is not None else user_prompt
)

# input_job_id: proves the prompt was just gated, so the server skips
# re-filtering it on the session job (one inbound pass per turn).
async with collie.streaming.session(
    input=user_prompt, input_job_id=check.job_id
) as session:
    async for raw_delta in your_llm_stream(prompt_for_model):
        result = await session.push(raw_delta)
        for emit in result.emits:
            yield emit.text          # forward ONLY safe emits
        if result.finished:
            break
    await session.finish()
```

You never touch chunk sequence numbers, retries, or idempotency — the session
owns them. Forward `emit.text`, never the raw model delta.

Manual sessions do not auto-retry the claim protocol: a `CollieAPIError`
with code `input_gate_stale` means the policy changed since your gate ran —
call `moderate.input` again (with the same context, if any) and open a new
session with the fresh job id; `input_gate_unverifiable` means a policy pin is missing server-side — retry without the gate reference; `input_gate_claimed` means that gate was
already consumed. `protect_stream` handles the stale re-gate for you — but only when it runs its own gate; with an external `input_result` the typed errors surface to your code by design.

## Output moderation (text produced outside a wrapper)

Use `moderate.output` for assistant text that never went through
`protect_stream`/`protect_buffered` — proactive notifications,
escalations, any side channel. Don't route such text through
`moderate.input`: that evaluates it with INPUT rules (masking and
output-safety silently never run; injection detectors can false-block
assistant-style imperatives).

```python
result = await collie.moderate.output(response=assistant_text)
if result.blocked:
    ...  # don't send it
safe = result.filtered_text if result.filtered_text is not None else assistant_text
send(safe)
```

`filtered_text` carries the masked output — send it, not the original
(`is not None`, not `or`: an empty string is a real mask result).

## Errors

Catch typed exceptions instead of parsing strings. A few you'll see:

| Exception | Meaning |
|---|---|
| `ChunkRetryExhausted` | transient failures exceeded the retry budget |
| `ChunkPolicyChanged` | policy changed mid-stream — start a new session |
| `ChunkQuotaExceeded` | rate-limited with no usable `Retry-After` |
| `ChunkSessionFinished` | the session already reached a terminal state |
| `ConcurrentSessionUseError` | overlapping `push()` calls on one session |
| `ModerationError` | `moderate.input` job failed/expired or timed out |
| `MaskedInputError` | the wrapper's own gate MASKED the prompt — gate manually, build the factory over `filtered_text`, pass `input_result` |

All inherit from `CollieError`.

## Retry behavior

The session retries the **same** chunk sequence on transient failures (network
timeouts, `503`, `504 chunk_filter_timeout`, `429` with a usable `Retry-After`)
with exponential backoff + jitter — default base 250 ms, max 4 s, 3 attempts per
chunk, 10 s ceiling (`AsyncCollie(retry_max_per_chunk_s=...)`). A retried chunk
never produces a duplicate visible emit.

## License

MIT. See [LICENSE](https://github.com/CollieAi/collieai-sdk/blob/main/LICENSE). Releases up to and including 2.1.0 are under Apache-2.0.
