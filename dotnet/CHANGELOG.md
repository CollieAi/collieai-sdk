# Changelog

All notable changes to the CollieAi .NET SDK (`CollieAi.Client`).

## [Unreleased]

### Changed

- License: MIT from 2.2.0 (Copyright (c) 2025 B25 B.V.). Earlier releases,
  up to and including 2.1.0, remain under Apache-2.0.

### Added

- `McpPayloadCanonicalizer` / `McpCanonicalPayload` /
  `McpCanonicalizationException` — the .NET reference implementation of
  the MCP payload canonical form `mcpc1`.
  A datapath adapter uses it to recompute `evaluated_payload_digest` over
  the payload it is about to forward and abort on mismatch. Conformance
  is pinned against the shared vector file
  `sdk/conformance/mcp_canonical_vectors.json`, which the Python server
  implementation must match vector-by-vector. Additive; no existing
  member changed.

## [2.1.0] — 2026-08-08

Backward compatible with one deliberate exception: the previously
accepted — and silently ignored — `InputResult` + `CheckInput = false`
combination now throws (see Changed). No interface members changed,
nothing removed (`StreamingSessionRequest.InputJobId` is an optional
init property on a sealed record). Against a 2.1 server `ProtectStreamAsync` DOES gain
new behavior: it may perform one extra input check (the stale re-gate,
billed like any gate call) and may surface the new `input_gate_stale`
/ `input_gate_unverifiable` / `input_gate_claimed` error codes.
Against older servers the claim protocol is inert — but the
masked-prompt fail-closed (see Changed) is client-side and applies
against ANY server.

### Added

- The streaming session job now carries `input_job_id` — the id of the
  `Moderation.CheckInputAsync` gate that already filtered the prompt.
  A server implementing the claim contract verifies the reference and
  skips the session job's second inbound pass: one filtering pass and
  **one billed usage unit** per streaming turn instead of two
  (previously every `ProtectStreamAsync` turn double-filtered and
  double-billed). The claim is **single-use** server-side (an
  idempotent replay of the same create returns the existing session).
  `ProtectStreamAsync` forwards the id automatically (from its own gate
  call or a reused `InputResult`); `StreamingSessionRequest.InputJobId`
  exposes the knob for callers who gate themselves. Older servers
  ignore the field and behave exactly as before. NOTE for
  dashboards/quota alerts: recorded usage and inbound-check counts for
  streaming projects DROP by half on adoption — that is the overcount
  ending, not lost traffic. (The rare stale re-gate below adds one more
  billed gate call to a turn — the price of a current verdict.)
- The claim-refusal protocol in `ProtectStreamAsync` (all steps before
  your provider stream is started): on `409 input_gate_stale` (policy
  changed / gate aged out) the wrapper re-runs the input check ONCE —
  same context — and retries with the fresh verdict; a block by the
  current policy surfaces as `InputBlocked` with zero provider spend;
  a SECOND stale on the retried create RAISES with zero provider calls (proven policy churn must never race the provider); only an `input_gate_unverifiable` on the retried create downgrades to a claimless session (the
  pre-2.1 behavior — two passes for that turn). On
  `409 input_gate_unverifiable` (a policy pin is missing server-side)
  it downgrades to the claimless session immediately — re-gating
  cannot help during a pin outage. Bounded, never a loop.
  `input_gate_claimed` (gate already consumed) always throws. With an
  EXTERNAL `InputResult` neither remedy is automatic — that result may
  have been produced with a context the wrapper cannot see, so the
  typed error surfaces and the caller re-gates with its own context.
  Manual `CreateSessionAsync` users receive the typed errors and
  re-gate themselves — documented on `InputJobId`.

### Changed

- **`ProtectStreamAsync` / `ProtectBufferedAsync` FAIL CLOSED on a
  masked prompt.** When the wrapper's own input gate returns a
  `FilteredText` differing from the input (including `""` — a full
  wipe), the wrappers now throw the new `MaskedInputException` BEFORE
  your provider factory runs. Previously they streamed anyway — sending
  the ORIGINAL (unmasked) prompt, PII included, to your model, against
  the documented promise that masked values never reach it. If your
  input policy masks: call `CheckInputAsync` yourself, build the
  factory over `result.FilteredText`, and pass `InputResult = result`
  (that path is exempt — you demonstrably hold the filtered text).
  Policies that never mask input are unaffected.
- **`InputResult` combined with `CheckInput = false` now throws
  `ArgumentException`** in both `ProtectStreamAsync` and
  `ProtectBufferedAsync`. Previously the result was silently ignored:
  in `ProtectStreamAsync` no claim was sent and the session
  re-filtered the prompt — the combination quietly kept the double
  pass 2.1 exists to remove; in `ProtectBufferedAsync` the input pass
  was simply skipped, as if the result had never been passed. The
  validation runs before any provider or network side effect
  (`ProtectStreamAsync` throws before returning the enumerable;
  `ProtectBufferedAsync` faults its task). Pass the result with
  `CheckInput = true` (the default), or drop one of the two.

### Fixed

- `ProtectBufferedAsync` verdict gating is now fail-safe (the v24
  table, same as `CheckOutputAsync`): a terminal job whose
  `outbound_result` is an empty object — or carries `allowed: false`
  with `blocked` absent — now resolves to `Blocked = true`. Previously
  such an ambiguous verdict failed OPEN. Not-blocked requires an
  explicit `allowed: true` with no hard-block signal. Real CollieAi
  servers always send both fields, so well-formed responses are
  unaffected.

## [2.0.0] — 2026-08-05

### Added

- `Moderation.CheckOutputAsync(...)`: standalone OUTPUT moderation — check
  assistant text produced outside a protect wrapper (proactive
  notifications, escalations) against the project's OUTPUT-direction
  rules via a `message_output`-only job. Same fail-safe verdict
  resolution and error taxonomy as the input check; `FilteredText` carries
  the masked output (send it, not the original). Deliberately no context
  parameter: context is an input surface (with context analysis enabled on the
  host, the server rejects an outbound-only job carrying one; with it
  off, raw-API context is ignored, never persisted).

- `TimeoutS` on `InputModerationRequest` and `OutputModerationRequest`:
  an optional wall-clock budget in seconds for
  polling the moderation job to a verdict — previously an internal 30 s
  constant, which made a fail-open latency budget impossible to express
  (a self-imposed linked-CTS timeout surfaces as cancellation, which a
  correct wrapper must rethrow — failing CLOSED). Bounds the POLL phase
  only; the job-create POST stays bounded by
  `CollieClientOptions.Timeout`, so the worst case is roughly the sum of
  the two. On expiry: `ModerationException`, same as any
  died-without-verdict outcome. Brings the .NET SDK to parity with
  Python/Node, whose `timeout_s`/`timeoutS` were already public.

### Breaking change (why this is 2.0.0)

`CheckOutputAsync` is a new abstract member on the public
`ICollieModerationClient`. **Source- and binary-breaking for external
implementors of that interface** (hand-written test doubles, decorators):
such types stop compiling on upgrade, and pre-built assemblies
implementing the old interface fail to load (`TypeLoadException`) — add
the one method to fix. Callers-only consumers are unaffected and need no
code changes.

## [1.2.0] — 2026-07-27

Releases together with the server-side monitor-mode streaming rollout.
Fully backward compatible: no signatures changed, nothing removed;
upgrading from 1.1.1 requires no code changes.

### Added

- `RuleCapability.ExecutionRole` on the preflight response: which of four
  roles a rule plays in a streaming request — `enforce_streaming`,
  `enforce_postflight`, `stream_observed`, `postflight_observed`. This is
  what `StreamingSupported` cannot express: it is `false` for every
  monitor rule, so a stream-capable observer and a full-context one look
  identical without this field. Kept as a string so a future server-side
  role does not break older clients; `null` when the rule cannot be
  planned at all.
- `ChunkResolutionUnavailableException` — typed exception for the
  server's 503 `chunk_resolution_unavailable` (the server could not
  *resolve* the policy because a dependency was unreachable, as opposed
  to the policy being unable to stream). Retried exactly as before; when
  the outage outlasts the retry budget it is the `InnerException` of the
  `ChunkRetryExhaustedException` the SDK throws.

### Changed

- Against servers with monitor-mode streaming deployed, monitor policies
  now stream: preflight answers `Mode="streaming"` and monitor rules
  report `ExecutionRole="stream_observed"` or `"postflight_observed"`;
  their findings appear in the CollieAi audit log only, never on the
  wire. Against older servers nothing changes — they still answer
  `Mode="buffered"`, `Reason="monitor_mode"`, and the SDK handles both.
- The `InnerException` of `ChunkRetryExhaustedException` for a 503
  `chunk_resolution_unavailable` is now the typed
  `ChunkResolutionUnavailableException` (a `ChunkException` subclass)
  instead of the generic `CollieApiException`. This is the release's only
  observable behavior change; code that only catches
  `ChunkRetryExhaustedException` is unaffected.

## [1.1.1] — 2026-07-24

- Poll Backoff Contract (server-paced polling via `retry_after` hints,
  strict retry ceilings, idempotent chunk replay hardening).
