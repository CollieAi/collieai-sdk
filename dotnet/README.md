# CollieAi .NET SDK (`CollieAi.Client`)

[![NuGet](https://img.shields.io/nuget/v/CollieAi.Client)](https://www.nuget.org/packages/CollieAi.Client)
[![CI](https://github.com/CollieAi/collieai-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/CollieAi/collieai-sdk/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/CollieAi/collieai-sdk/blob/main/LICENSE)

The .NET 8 client for [CollieAi](https://collieai.io), the AI guardrails service for LLM
applications: prompt injection detection, PII masking, input and output
moderation. You keep calling your own model; the SDK checks the prompt before
the call and streams back **only text CollieAi has released**, never raw model
output. You need a CollieAi API key and a project whose policy defines the
rules.

## Install

```bash
dotnet add package CollieAi.Client
```

## Quick start

```csharp
using CollieAi;

await using var collie = new CollieClient(new CollieClientOptions
{
    ApiKey = "clai_...",
    ProjectId = "project_123",
});

var userPrompt = "My card is 4111 1111 1111 1111, why was I charged twice?";
var result = await collie.Moderation.CheckInputAsync(new InputModerationRequest { Prompt = userPrompt });
if (result.Blocked)
{
    Console.WriteLine(result.BlockMessage ?? "Input blocked by policy.");
    return;
}
// Masking rules may have removed PII: the model gets the FILTERED prompt.
var promptForModel = result.FilteredText ?? userPrompt;
Console.WriteLine(promptForModel); // now call your LLM with it
```

To stream the model's answer through CollieAi as well, use
`ProtectStreamAsync` (see "Protected streaming" below): the provider stream
is created inside a deferred `RawStreamFactory` that runs only after the
input check passes, so paid LLM work cannot run ahead of a block.

## Links

- Documentation: https://docs.collieai.io/sdks/dotnet-sdk
- Examples: https://github.com/CollieAi/collieai-sdk/tree/main/dotnet/examples
- Changelog: https://github.com/CollieAi/collieai-sdk/blob/main/dotnet/CHANGELOG.md
- Python SDK: https://github.com/CollieAi/collieai-sdk/tree/main/python
- Node.js SDK: https://github.com/CollieAi/collieai-sdk/tree/main/node

## Usage

## Construct the client

### ASP.NET Core (dependency injection)

```csharp
builder.Services.AddCollieAi(options =>
{
    options.ApiKey = builder.Configuration["CollieAi:ApiKey"]!;
    options.BaseUrl = new Uri("https://app.collieai.io");
    options.ProjectId = "project_123";
});

// then inject ICollieClient
public sealed class ChatService(ICollieClient collie) { /* ... */ }
```

`AddCollieAi` registers a named `HttpClient` (`"CollieAi.Client"`) through
`IHttpClientFactory`. Attach your own logging/tracing/auth delegating handlers to
that named client — but **do not** add retry/circuit-breaker/timeout handlers on
the chunk endpoints; they conflict with the SDK's own retry semantics.

### Console / worker (direct)

```csharp
await using var collie = new CollieClient(new CollieClientOptions
{
    ApiKey = Environment.GetEnvironmentVariable("COLLIEAI_API_KEY")!,
    BaseUrl = new Uri("https://app.collieai.io"),
    ProjectId = "project_123",
});
```

`CollieClient` is `IAsyncDisposable`; dispose with `await using` so the owned
HTTP resources are released.

## Input moderation (pre-generation gate)

```csharp
var input = await collie.Moderation.CheckInputAsync(new InputModerationRequest
{
    Prompt = userPrompt,
    ConversationId = conversationId,
    CorrelationId = chatTurnId,
});

if (input.Blocked)
    return input.BlockMessage ?? "Input blocked by policy.";

// If your input policy MASKS, send the FILTERED prompt to your model —
// null-check, never string emptiness: "" is a legitimate full wipe.
var promptForModel = input.FilteredText ?? userPrompt;
```

A policy block is a normal result (`Blocked = true`), **not** an exception.
`FilteredText` is the post-mask prompt — the raw values a mask rule removed
must never reach your model, so it is `promptForModel`, not `userPrompt`,
that goes into your LLM call.

Both moderation requests accept an optional `TimeoutS` — a wall-clock
budget in seconds for polling the verdict (starts after the job-create
POST, which `CollieClientOptions.Timeout` bounds separately). On expiry:
`ModerationException`, the same died-without-verdict error a fail-open
wrapper already handles.

### Analyze context alongside the prompt

Set `Context` to analyze the structured data (or raw string) you're about to feed
the model — retrieved documents, tool output, a transaction record — alongside the
prompt, so an injection hidden in that data is caught too. It travels as JSON with
your keys preserved verbatim; `ContextFormat` (`"auto"` / `"json"` / `"text"`) is
an optional parsing hint for a raw string.

```csharp
var input = await collie.Moderation.CheckInputAsync(new InputModerationRequest
{
    Prompt = userPrompt,
    Context = new Dictionary<string, object>
    {
        ["transaction"] = new Dictionary<string, object> { ["memo"] = retrievedMemo },
    },
});

if (input.Context is { Status: not "clean" and not "not_provided" } ctx)
    Console.WriteLine($"{ctx.Status} {ctx.TriggeringPointer} {ctx.TriggeringRuleType}"); // pointer is a path, never a value

if (input.Blocked)
    // input.BlockedBy is "prompt", "context", or "none" — which surface blocked
    return input.BlockMessage ?? "Blocked by policy.";
```

`input.Context` (a `ContextModerationResult`) carries the closed `Status` string
(`not_provided` / `disabled` / `not_run` / `clean` / `monitored` / `blocked` /
`degraded`), the triggering JSON Pointer + rule, and degraded markers
(`ParseDegraded` / `LimitExceeded` / `InferenceDegraded`). The pointer is a
**path, never a value**.

`Context` / `ContextFormat` work the same on `ProtectStreamAsync` /
`ProtectBufferedAsync` **when input checking is enabled** (the default) — context
is analyzed by the input gate, so `CheckInput = false` skips it and never
analyzes context. With the gate on, the verdict rides the `InputBlocked` /
`Finished` event and the buffered result, and `CollieSse.ToFrame(...)` serializes
it onto the relay frames.

> **Until a policy enables context analysis**, `Context` is inert — a safe no-op
> (`Status` is `not_provided` / `disabled`). Enable it per policy server-side.

## Output moderation (text produced outside a wrapper)

`ProtectStreamAsync`/`ProtectBufferedAsync` already moderate the streamed
answer. Use `CheckOutputAsync` for assistant text that never went through
a wrapper — proactive notifications, escalation messages, any side
channel. Routing such text through `CheckInputAsync` evaluates it with
INPUT rules: output-safety and masking rules silently never run, and
injection detectors can false-block assistant-style imperatives.

```csharp
var result = await collie.Moderation.CheckOutputAsync(new OutputModerationRequest
{
    Response = assistantText,
});

if (result.Blocked) return;                  // don't send it
Send(result.FilteredText ?? assistantText);  // masking arrives here
```

`FilteredText` carries the **masked** output — send it, not the original.
`OutputModerationRequest` deliberately has no `Context`: context is an
input surface; context-aware output filtering is the protect wrappers.

## Protected streaming (the safe path)

```csharp
await foreach (var ev in collie.Streaming.ProtectStreamAsync(new ProtectStreamRequest
{
    Input = userPrompt,
    RawStreamFactory = ct => myLlm.StreamTextAsync(userPrompt, ct), // runs only after the gate
    ConversationId = conversationId,
    CorrelationId = chatTurnId,
}, cancellationToken))
{
    switch (ev)
    {
        case SafeDelta d:        yield return d.Text; break;        // forward ONLY this
        case InputBlocked ib:    yield return ib.BlockMessage ?? "Input blocked."; yield break;
        case Blocked b:          yield return b.BlockMessage ?? "Response blocked."; yield break;
        case Finished:           break;
    }
}
// A non-streamable policy throws ChunkStreamingUnsupportedException on the default
// path — catch it and switch to ProtectBufferedAsync (or set RequireStreaming = true).
```

> ⚠️ **Forward `SafeDelta.Text` only — never the raw provider deltas.** The SDK
> cannot enforce this outside its own iterator, so the safe path must be the
> obvious path in your code.

`RawStreamFactory` is invoked **exactly once**, after the input check passes;
CollieAi chunk retries re-submit existing chunks and never re-invoke it. The
token it receives is cancelled when the SDK tears the stream down — forward it
to your provider so paid work stops promptly.

### Preflight + buffered fallback

By default `ProtectStreamAsync` streams optimistically (no preflight) and lets the
chunk endpoint be the source of truth — a policy that can't stream surfaces as a
fatal `ChunkStreamingUnsupportedException` on the first push. To decide the UX
*before* generation, either set `RequireStreaming = true` (which preflights and
throws `BufferedFallbackRequiredException` when the policy buffers) or call
`PreflightAsync(...)` yourself. When the policy buffers, switch to the buffered
helper:

```csharp
var result = await collie.Streaming.ProtectBufferedAsync(new ProtectBufferedRequest
{
    Input = userPrompt,
    RawStreamFactory = ct => myLlm.StreamTextAsync(userPrompt, ct),
});
return result.Blocked ? result.BlockMessage : result.FilteredText;
```

Reusing an `InputResult` here differs from `ProtectStreamAsync`: the
streaming path turns a result carrying a `JobId` into a server-verified,
expiring, single-use claim against a 2.1+ server (without a `JobId`, or on
an older server, the session runs claimless and re-filters the input in
its own async pass — the pre-2.1 shape: billed, and the verdict can land
after streaming has started),
while the buffered path is a **trusted-client reuse** — the server neither
verifies nor consumes the result and no `input_gate_*` error can occur.
Obtain it in the same turn, same client and project, immediately before the
call; never cache or reuse it.

You can also call `collie.Streaming.PreflightAsync(...)` yourself to choose the
UX (token stream vs "checking response…") before generation starts.

### Low-level session (advanced)

```csharp
var check = await collie.Moderation.CheckInputAsync(
    new InputModerationRequest { Prompt = userPrompt });
if (check.Blocked)
    return;

// The split that matters when your input policy masks:
// - the SESSION gets the ORIGINAL prompt (it must byte-match the gate);
// - your MODEL gets the FILTERED prompt ("" is a legitimate full wipe).
var promptForModel = check.FilteredText ?? userPrompt;

// InputJobId: proves the prompt was just gated, so the server skips
// re-filtering it on the session job (one inbound pass per turn).
await using var session = await collie.Streaming.CreateSessionAsync(
    new StreamingSessionRequest { Input = userPrompt, InputJobId = check.JobId });

await foreach (var rawDelta in YourLlmStream(promptForModel))
{
    var push = await session.PushAsync(rawDelta);
    foreach (var emit in push.Emits)
        yield return emit.Text;          // forward ONLY safe emits
}
await session.FinishAsync();
```

Manual sessions do not auto-retry the claim protocol: a
`CollieApiException` with `Code == "input_gate_stale"` means the policy
changed since your gate ran — call `CheckInputAsync` again (with the same
context, if any) and open a new session with the fresh job id;
`"input_gate_unverifiable"` means a policy pin is missing server-side —
retry without the gate reference; `"input_gate_claimed"` means that gate
was already consumed. `ProtectStreamAsync` handles the stale re-gate for
you — but only when it runs its own gate; with an external `InputResult`
the typed errors surface to your code by design. When you do pass an
external `InputResult`, keep `CheckInput` at its default `true`:
combining it with `CheckInput = false` is a contradiction and throws
`ArgumentException` (in 2.0 it was silently ignored).

The low-level session does not run input filtering — call
`Moderation.CheckInputAsync(...)` yourself if you need it. It owns sequence
numbers, serializes submits (overlapping calls throw
`ConcurrentSessionUseException`), and `FinishAsync` is idempotent. For browser
delivery, `session.MintStreamTokenAsync()` returns a short-lived, job-scoped
`StreamToken` (hand the browser its `Url`, keep your API key server-side) and
`session.StreamEventsAsync(...)` consumes the CollieAi SSE stream with
`Last-Event-ID` resume (replayed deltas are de-duplicated).

## Error handling

Typed exceptions map directly to API error codes — never parse strings. Policy
decisions are **events** (`Blocked` / `InputBlocked`), not exceptions.
`OperationCanceledException` is **not** wrapped (catch it before `CollieException`).

| Exception | Meaning | What to do |
|---|---|---|
| `ProviderStreamFactoryRequiredException` | High-level stream called without a deferred factory | Pass `RawStreamFactory`, not an already-started stream |
| `BufferedFallbackRequiredException` | `RequireStreaming=true` but the policy buffers | Use `ProtectBufferedAsync` / a buffered UX |
| `ChunkStreamingUnsupportedException` | Default path hit a policy that can't stream (400) | Switch to `ProtectBufferedAsync`, or preflight first |
| `ProjectNotFoundException`, `PlanNotEntitledException`, `UnknownRuleTypeException`, `StreamingFeatureDisabledException`, `PolicyNotStreamableException` | Preflight `unsupported` reasons | Fail fast; surface the reason / fix policy or plan |
| `ChunkPolicyChangedException`, `ChunkSessionUnrecoverableException`, `ChunkSessionFinishedException` | Fatal for the current job (409) | Start a new job |
| `ChunkQuotaExceededException` | 429 with no usable `Retry-After` | Back off and retry the turn |
| `ChunkRetryExhaustedException` | Per-chunk retry budget/ceiling hit | Show "try again" |
| `ConcurrentSessionUseException` | Overlapping `PushAsync`/`FinishAsync` | Serialize submits per session |
| `ModerationException` | `CheckInputAsync` job died/timed out | Retry; not a policy block |
| `MaskedInputException` | the wrapper's own gate MASKED the prompt | Gate manually, build the factory over `FilteredText`, pass `InputResult` |
| `CollieApiException` | Unmapped HTTP / malformed body | Inspect `StatusCode` / `Code` |
| `CollieConnectionException` | Transport failure before a response | Network/retry |

## Retry & batching defaults

- **Batching** (high-level stream): flush on 50 ms / 16 deltas / 2048 chars, or
  on provider end. Override via `ProtectStreamRequest.Batching`.
- **Per-chunk retry**: same sequence + content, exponential backoff with jitter,
  base 250 ms, max 4 s, 3 attempts, 10 s ceiling. A retry never duplicates a
  safe delta already yielded. Tune on `CollieClientOptions`.

## Build & test

```bash
cd sdk/dotnet
dotnet build                 # builds the library, tests, and example
dotnet test                  # runs the xUnit suite (no network — fully mocked)
dotnet run --project examples/AspNetCoreChat
```

Tests inject a fake `HttpMessageHandler` plus deterministic clock/delay/jitter
seams (via the assembly's `InternalsVisibleTo`), so the suite is hermetic and
fast.

## Layout

```
sdk/dotnet/
  CollieAi.sln
  src/CollieAi.Client/        # the SDK
  test/CollieAi.Client.Tests/ # xUnit tests (RFC Test Matrix)
  examples/AspNetCoreChat/    # minimal API streaming example
```

## License

MIT. See [LICENSE](https://github.com/CollieAi/collieai-sdk/blob/main/LICENSE). Releases up to and including 2.1.0 are under Apache-2.0.
