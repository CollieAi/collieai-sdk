# @collieai/sdk

[![npm](https://img.shields.io/npm/v/@collieai/sdk)](https://www.npmjs.com/package/@collieai/sdk)
[![CI](https://github.com/CollieAi/collieai-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/CollieAi/collieai-sdk/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/CollieAi/collieai-sdk/blob/main/LICENSE)

The Node.js / TypeScript client for [CollieAi](https://collieai.io), the AI guardrails service for LLM
applications: prompt injection detection, PII masking, input and output
moderation. You keep calling your own model; the SDK checks the prompt before
the call and streams back **only text CollieAi has released**, never raw model
output. You need a CollieAi API key and a project whose policy defines the
rules.

## Install

```bash
npm install @collieai/sdk
```

## Quick start

```ts
import { CollieClient } from "@collieai/sdk";

const collie = new CollieClient({ apiKey: "clai_...", projectId: "project_123" });

const userPrompt = "My card is 4111 1111 1111 1111, why was I charged twice?";
const result = await collie.moderate.input({ prompt: userPrompt });
if (result.blocked) {
  console.log(result.blockMessage ?? "Input blocked by policy.");
} else {
  // Masking rules may have removed PII: the model gets the FILTERED prompt.
  const promptForModel = result.filteredText ?? userPrompt;
  console.log(promptForModel); // now call your LLM with it
}
```

To stream the model's answer through CollieAi as well, use `protectStream`
(see "Stream safely" below). Zero runtime dependencies: the SDK uses the
global `fetch` (Node.js 18+).

## Links

- Documentation: https://docs.collieai.io/sdks/node-sdk
- Examples: https://github.com/CollieAi/collieai-sdk/tree/main/node/examples
- Changelog: https://github.com/CollieAi/collieai-sdk/blob/main/node/CHANGELOG.md
- Python SDK: https://github.com/CollieAi/collieai-sdk/tree/main/python
- .NET SDK: https://github.com/CollieAi/collieai-sdk/tree/main/dotnet

## Usage

```ts
import { CollieClient } from "@collieai/sdk";

const collie = new CollieClient({
  apiKey: "clai_...",
  baseUrl: "https://app.collieai.io",
  projectId: "project_123",
});
```

### Check an input before calling your LLM

```ts
const result = await collie.moderate.input({ prompt: userPrompt });
if (result.blocked) return result.blockMessage ?? "Input blocked by policy.";

// If your input policy MASKS, send the FILTERED prompt to your model —
// null-check, never truthiness: "" is a legitimate full wipe.
const promptForModel = result.filteredText ?? userPrompt;
```

A policy block is a normal result (`result.blocked === true`), not an error.
`filteredText` is the post-mask prompt — the raw values a mask rule removed
must never reach your model, so it is `promptForModel`, not `userPrompt`,
that goes into your LLM call.

### Analyze context alongside the prompt

Pass `context` to analyze the structured data (or raw string) you're about to
feed the model — retrieved documents, tool output, a transaction record —
alongside the prompt, so an injection hidden in that data is caught too. Structured
`context` travels as JSON; `contextFormat` (`"auto"` | `"json"` | `"text"`) is an
optional parsing hint for a raw string.

```ts
const result = await collie.moderate.input({
  prompt: userPrompt,
  context: { transaction: { title, memo } },
});

const ctx = result.context;
if (ctx && ctx.status !== "clean" && ctx.status !== "not_provided") {
  console.log(ctx.status, ctx.triggeringPointer, ctx.triggeringRuleType); // pointer is a path, never a value
}
if (result.blocked) return result.blockMessage ?? "Blocked by policy.";
// result.blockedBy is "prompt" | "context" | "none" — which surface blocked
```

`result.context` (a `ContextModerationResult`) carries the closed `status` enum
(`not_provided` / `disabled` / `not_run` / `clean` / `monitored` / `blocked` /
`degraded`), the triggering JSON Pointer + rule, and degraded markers
(`parseDegraded` / `limitExceeded` / `inferenceDegraded`). The pointer is a
**path, never a value**.

The same `context` / `contextFormat` work on `protectStream` / `protectBuffered`
**when input checking is enabled** (the default) — context is analyzed by the
input gate, so `checkInput: false` skips it and never analyzes context. With the
gate on, the verdict rides the `input_blocked` / `finished` event and the
buffered result, and `toSse(...)` serializes it onto the relay frames.

> **Until a policy enables context analysis**, `context` is inert — a safe no-op
> (`status` is `not_provided` / `disabled`). Enable it per policy server-side.

### Stream safely (Express)

`protectStream` checks the input, calls your LLM **only if it passes**, batches
the output, and yields only safe events. Pass a *factory* — a zero-arg function
returning your stream — not an already-started stream.

```ts
import { openaiFactory } from "@collieai/sdk/adapters/openai"; // optional helper

const factory = openaiFactory(openai, {
  model: "gpt-4o-mini",
  messages: [{ role: "user", content: prompt }],
});

for await (const event of collie.streaming.protectStream({ input: prompt, rawStreamFactory: factory })) {
  if (event.type === "delta") res.write(event.text);            // forward ONLY safe text
  else if (event.type === "blocked" || event.type === "input_blocked") {
    res.write(event.blockMessage ?? "Blocked by policy.");
    break;
  }
}
```

See [`examples/express.ts`](https://github.com/CollieAi/collieai-sdk/blob/main/node/examples/express.ts) and
[`examples/next-route.ts`](https://github.com/CollieAi/collieai-sdk/blob/main/node/examples/next-route.ts).

### Choose the UX up front (preflight)

```ts
const cap = await collie.streaming.preflight();           // cached until cap.validUntil
if (cap.recommendedClientBehavior === "stream") { /* protectStream */ }
else if (cap.recommendedClientBehavior === "buffer_then_show") {
  const r = await collie.streaming.protectBuffered({ input: prompt, rawStreamFactory: factory });
} else throw new Error(cap.reasonDetail ?? cap.reason ?? "unavailable");
```

Or pass `requireStreaming: true` to `protectStream` — it preflights and throws
`BufferedFallbackRequired` / a `PreflightError` before calling your LLM.

If your input policy MASKS the prompt, `protectStream`/`protectBuffered`
throw `MaskedInputError` **before** your factory runs — the zero-arg factory
closes over the original text, and streaming it would send unmasked content
to your model. The recipe: gate manually with `moderate.input`, build the
factory over `result.filteredText` (`""` is a legitimate full wipe), and
pass `inputResult: result` (that path is exempt). Leave `checkInput`
unset/true: combining `inputResult` with `checkInput: false` is a
contradiction and throws `TypeError` (in 2.0 it was silently
ignored). Note the reuse semantics differ per path: `protectStream`
turns a result carrying a `jobId` into a server-verified, expiring,
single-use claim against a 2.1+ server (the `input_gate_*` 409s can
surface; without a `jobId` or on an older server the session runs
claimless and re-filters the input in its own async pass — the pre-2.1
shape: billed, and the verdict can land after streaming has started),
while `protectBuffered` is a
**trusted-client reuse** — the server neither verifies nor consumes
the result and no `input_gate_*` error can occur. Obtain it in the
same turn, same client and project, immediately before the call;
never cache or reuse it.

### Low-level session (advanced)

If you drive batching yourself, use the session directly. It does **not**
check the input — call `moderate.input({...})` first, and pass its job id
so the server skips re-filtering the prompt on the session job (one
inbound pass per turn):

```ts
const check = await collie.moderate.input({ prompt: userPrompt });
if (check.blocked) return check.blockMessage ?? "Input blocked by policy.";

// The split that matters when your input policy masks:
// - the SESSION gets the ORIGINAL prompt (it must byte-match the gate);
// - your MODEL gets the FILTERED prompt ("" is a legitimate full wipe).
const promptForModel = check.filteredText ?? userPrompt;

const session = collie.streaming.session({
  input: userPrompt,
  inputJobId: check.jobId ?? undefined,
});
await session.open();
```

Manual sessions do not auto-retry the claim protocol: a `CollieApiError`
with code `input_gate_stale` means the policy changed since your gate ran
— call `moderate.input` again (with the same context, if any) and open a
new session with the fresh job id; `input_gate_unverifiable` means a policy pin is missing server-side — retry without the gate reference; `input_gate_claimed` means that gate
was already consumed. `protectStream` handles the stale re-gate for you — but only when it runs its own gate; with an external `inputResult` the typed errors surface to your code by design.

### Relay to a browser (SSE)

```ts
import { toSse } from "@collieai/sdk";

for await (const event of session.streamEvents()) {       // auto-resumes from Last-Event-ID
  if (event.type === "interrupted") continue;             // reconnecting; nothing to forward
  res.write(toSse(event));                                // SSE bytes for text/event-stream
  if (event.type === "blocked" || event.type === "finished") break;
}
```

For direct browser subscription, mint a short-lived, job-scoped token
(`const st = await session.mintStreamToken()`) and open `new EventSource(st.url)`.

## Output moderation (text produced outside a wrapper)

Use `moderate.output` for assistant text that never went through
`protectStream`/`protectBuffered` — proactive notifications, escalations,
any side channel. Don't route such text through `moderate.input`: that
evaluates it with INPUT rules (masking and output-safety silently never
run; injection detectors can false-block assistant-style imperatives).

```ts
const result = await collie.moderate.output({ response: assistantText });
if (result.blocked) return; // don't send it
send(result.filteredText ?? assistantText); // masking arrives here
```

`filteredText` carries the masked output — send it, not the original.

## Timeouts

`moderate.input({ ... })` takes `pollIntervalS` (default `0.05`) and `timeoutS`
(default `30`) — the base cadence and the polling-wait deadline. Separately, the
**client** takes `requestTimeoutS` (default `30`): `fetch` has no built-in
timeout, so this bounds every individual non-streaming request — job creation,
each status poll (composed with the poll deadline), and each chunk-submit
attempt — including the response-body read. A request that exceeds it surfaces
as a `CollieConnectionError`. The long-lived output SSE stream is not bounded by
it. Prefer this knob over injecting a `fetch` wrapper that forces its own
`signal` — that would clobber the deadline signal the poll loop and stream rely
on.

```ts
const collie = new CollieClient({ apiKey: "clai_...", requestTimeoutS: 10 });
```

## Errors

Catch typed errors (all extend `CollieError`): `ChunkRetryExhausted`,
`ChunkPolicyChanged`, `ChunkQuotaExceeded`, `ChunkSessionUnrecoverable`,
`ChunkStreamingUnsupported`, `BufferedFallbackRequired`, `ProjectNotFound` /
`PlanNotEntitled` / … (`PreflightError`), `ProviderStreamFactoryRequired`,
`ConcurrentSessionUseError`, `ModerationError`, `CollieConnectionError`,
`CollieApiError`.

## Develop

```bash
npm install
npm run typecheck   # tsc --noEmit
npm test            # vitest
npm run build       # tsc -> dist/
```

## License

MIT. See [LICENSE](https://github.com/CollieAi/collieai-sdk/blob/main/LICENSE). Releases up to and including 2.1.0 are under Apache-2.0.
