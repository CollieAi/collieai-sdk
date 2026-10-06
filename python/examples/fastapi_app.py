"""FastAPI safe-streaming example.

Streams ONLY CollieAi-released text to the client — never raw model output —
with the input checked before the LLM is called.

This example lives in the SDK source tree and is NOT part of the installed
wheel. Run it from `sdk/python/` so the `examples` package is importable (or
copy these two files into your own project):

    cd sdk/python
    pip install -e . fastapi uvicorn openai
    export COLLIEAI_API_KEY=clai_... COLLIEAI_PROJECT_ID=project_... OPENAI_API_KEY=sk-...
    uvicorn examples.fastapi_app:app --reload

Then:
    curl -N -X POST localhost:8000/chat -H 'content-type: application/json' \
         -d '{"prompt": "Tell me about quarterly earnings"}'
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from openai import AsyncOpenAI

from collieai import (
    AsyncCollie,
    Blocked,
    BufferedFallbackRequired,
    ChunkStreamingUnsupported,
    CollieError,
    InputBlocked,
    SafeDelta,
)
from collieai.adapters.openai import openai_factory  # from the collieai[openai] extra

collie = AsyncCollie(
    api_key=os.environ["COLLIEAI_API_KEY"],
    project_id=os.environ["COLLIEAI_PROJECT_ID"],
)
openai_client = AsyncOpenAI()  # reads OPENAI_API_KEY


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Release pooled HTTP connections on shutdown — both the SDK's and OpenAI's.
    await collie.aclose()
    await openai_client.close()


app = FastAPI(lifespan=lifespan)


@app.post("/chat")
async def chat(body: dict):
    """The recommended path: protect_stream checks the input, then streams only
    safe deltas. A block (of input or output) ends the stream."""
    prompt = body["prompt"]
    factory = openai_factory(
        openai_client,
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
    )

    async def events():
        try:
            async for event in collie.streaming.protect_stream(
                input=prompt, raw_stream_factory=factory,
            ):
                if isinstance(event, SafeDelta):
                    yield event.text
                elif isinstance(event, (Blocked, InputBlocked)):
                    yield event.block_message or "Blocked by policy."
                    return
        except ChunkStreamingUnsupported:
            # The policy can't stream; re-run through the buffered path.
            yield "This policy requires buffered response checking."
        except CollieError:
            # FAIL-CLOSED: nothing unverified is ever released. Failing OPEN on
            # an outage is a deliberate risk decision, not a default — see
            # "Failure policy: fail-closed by default" in the Python SDK docs
            # for how to do it safely (catch only CollieConnectionError / 5xx
            # CollieAPIError, and only before the first delta was written).
            yield "Could not verify the response. Please try again."

    return StreamingResponse(events(), media_type="text/plain")


@app.post("/chat/auto")
async def chat_auto(body: dict):
    """Preflight first, then branch into token-streaming or a buffered
    'checking response…' UX before calling the LLM."""
    prompt = body["prompt"]
    factory = openai_factory(
        openai_client,
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
    )
    capability = await collie.streaming.preflight()

    if capability.recommended_client_behavior == "stream":
        async def events():
            try:
                async for event in collie.streaming.protect_stream(
                    input=prompt, raw_stream_factory=factory, require_streaming=True,
                ):
                    if isinstance(event, SafeDelta):
                        yield event.text
                    elif isinstance(event, (Blocked, InputBlocked)):
                        yield event.block_message or "Blocked by policy."
                        return
            except BufferedFallbackRequired:
                yield "This policy requires buffered response checking."

        return StreamingResponse(events(), media_type="text/plain")

    if capability.recommended_client_behavior == "buffer_then_show":
        result = await collie.streaming.protect_buffered(
            input=prompt, raw_stream_factory=factory,
        )
        text = result.block_message if result.blocked else result.filtered_text
        return {"text": text, "blocked": result.blocked}

    # fail_fast — the policy can't be served as configured.
    return {"error": capability.reason_detail or capability.reason}
