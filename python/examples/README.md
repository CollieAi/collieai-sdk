# CollieAi SDK examples

Runnable, copyable examples for safe customer-owned streaming.

| File | Shows |
|---|---|
| [`fastapi_app.py`](fastapi_app.py) | A FastAPI `/chat` endpoint that streams **only** CollieAi-released text via `protect_stream` (using the packaged `collieai.adapters.openai` factory), plus `/chat/auto` that preflights and branches into streaming vs. buffered UX. |

The OpenAI/Anthropic stream adapters now ship in the package — install the
extra and import them:

```python
from collieai.adapters.openai import openai_factory       # pip install "collieai[openai]"
from collieai.adapters.anthropic import anthropic_factory  # pip install "collieai[anthropic]"
```

## Run the FastAPI example

These examples live in the SDK source tree and are **not** shipped in the
`collieai` wheel, so run them from `sdk/python/` (where the `examples` package is
importable) — or copy the two files into your own project.

```bash
cd sdk/python
pip install -e . fastapi uvicorn openai     # -e . installs collieai from this tree
export COLLIEAI_API_KEY=clai_...
export COLLIEAI_PROJECT_ID=project_...
export OPENAI_API_KEY=sk-...
uvicorn examples.fastapi_app:app --reload
```

```bash
curl -N -X POST localhost:8000/chat \
  -H 'content-type: application/json' \
  -d '{"prompt": "Tell me about quarterly earnings"}'
```

## The one rule

Forward `SafeDelta.text` to your users — **never** the raw model delta. The
examples only ever yield SDK events; the raw OpenAI stream stays inside the
factory.
