# Changelog

All notable changes to the CollieAi Python SDK.

## [2.2.0] — 2026-10-06

No functional changes: the client behaves exactly as 2.1.0. The source
now lives in the public repository
[CollieAi/collieai-sdk](https://github.com/CollieAi/collieai-sdk).

### Changed

- License: MIT from 2.2.0 (Copyright (c) 2025 B25 B.V.). Earlier releases,
  up to and including 2.1.0, remain under Apache-2.0.
- Package metadata: new description and keywords; the Repository, Issues
  and Changelog links point to the public repository.
- README: a new first screen with installation, a quick start and links
  to the documentation.

## [2.1.0] — 2026-08-08

Backward compatible with one deliberate exception: the previously
accepted — and silently ignored — `input_result` + `check_input=False`
combination now raises (see Changed). Nothing removed;
`streaming.session(...)` gains one optional parameter. Against a 2.1 server `protect_stream` DOES gain
new behavior: it may perform one extra `moderate.input` call (the stale
re-gate, billed like any gate call) and may surface the new
`input_gate_stale` / `input_gate_unverifiable` / `input_gate_claimed` error codes. Against older
servers the claim protocol is inert — but the masked-prompt fail-closed (see Changed) is client-side and applies against ANY server.

### Added

- The streaming session job now carries `input_job_id` — the id of the
  `moderate.input` gate that already filtered the prompt. A server
  implementing the claim contract verifies the reference and skips the
  session job's second inbound pass: one filtering pass and **one
  billed usage unit** per streaming turn instead of two (previously
  every `protect_stream` turn double-filtered and double-billed). The
  claim is **single-use** server-side (an idempotent replay of the same
  create returns the existing session). `protect_stream` forwards the
  id automatically (from its own gate call or a reused `input_result`);
  `streaming.session(...)` exposes an optional `input_job_id` parameter
  for callers who gate themselves. Older servers ignore the field and
  behave exactly as before. NOTE for dashboards/quota alerts: recorded
  usage and inbound-check counts for streaming projects DROP by half on
  adoption — that is the overcount ending, not lost traffic. (The rare
  stale re-gate below adds one more billed gate call to a turn — the
  price of a current verdict.)
- The claim-refusal protocol in `protect_stream` (all steps before your
  provider stream is started): on `409 input_gate_stale` (policy
  changed / gate aged out) the wrapper re-runs `moderate.input` ONCE —
  same context — and retries with the fresh verdict; a block by the
  current policy surfaces as `InputBlocked` with zero provider spend;
  a SECOND stale on the retried create RAISES with zero provider calls (proven policy churn must never race the provider); only an `input_gate_unverifiable` on the retried create downgrades to a claimless session (the
  pre-2.1 behavior — two passes for that turn). On
  `409 input_gate_unverifiable` (a policy pin is missing server-side)
  it downgrades to the claimless session immediately — re-gating
  cannot help during a pin outage. Bounded, never a loop.
  `input_gate_claimed` (gate already consumed) always raises. With an
  EXTERNAL `input_result` neither remedy is automatic — that result
  may have been produced with a context the wrapper cannot see, so the
  typed error surfaces and the caller re-gates with its own context.
  Manual `streaming.session(...)` users receive the typed errors and
  re-gate themselves — documented on the method.

### Changed

- **`protect_stream` / `protect_buffered` FAIL CLOSED on a masked
  prompt.** When the wrapper's own input gate returns a `filtered_text`
  differing from the input (including `""` — a full wipe), the wrappers
  now raise the new `MaskedInputError` BEFORE your provider factory
  runs. Previously they streamed anyway — sending the ORIGINAL
  (unmasked) prompt, PII included, to your model, against the
  documented promise that masked values never reach it. If your input
  policy masks: call `moderate.input` yourself, build the factory over
  `result.filtered_text`, and pass `input_result=result` (that path is
  exempt — you demonstrably hold the filtered text). Policies that
  never mask input are unaffected.
- **`input_result` combined with `check_input=False` now raises
  `ValueError`** in both `protect_stream` and `protect_buffered`.
  Previously the result was silently ignored: in `protect_stream` no
  claim was sent and the session re-filtered the prompt — the
  combination quietly kept the double pass 2.1 exists to remove; in
  `protect_buffered` the input pass was simply skipped, as if the
  result had never been passed. The validation runs before any
  provider or network side effect (`protect_stream` raises before
  returning the iterator; `protect_buffered` rejects on await). Pass
  the result with `check_input=True` (the default), or drop one of
  the two.

### Fixed

- `protect_buffered` verdict gating is now fail-safe (the v24 table,
  same as `moderate.output`): a terminal job whose `outbound_result` is
  an empty object — or carries `allowed: false` with `blocked` absent —
  now resolves to `blocked=True`. Previously such an ambiguous verdict
  failed OPEN. Not-blocked requires an explicit `allowed: true` with no
  hard-block signal. Real CollieAi servers always send both fields, so
  well-formed responses are unaffected.

## [2.0.0] — 2026-08-05

Fully backward compatible for callers — the major matches the
cross-SDK version lockstep (the .NET SDK's interface change makes
2.0.0 there; the three SDKs release in step).

### Added

- `moderate.output(...)`: standalone OUTPUT moderation — check
  assistant text produced outside a protect wrapper (proactive
  notifications, escalations) against the project's OUTPUT-direction
  rules via a `message_output`-only job. Same fail-safe verdict
  resolution and error taxonomy as the input check; `filtered_text` carries
  the masked output (send it, not the original). Deliberately no context
  parameter: context is an input surface (with context analysis enabled on the
  host, the server rejects an outbound-only job carrying one; with it
  off, raw-API context is ignored, never persisted).

## [1.2.0] — 2026-07-27

Releases together with the server-side monitor-mode streaming rollout.
Fully backward compatible: no signatures changed, nothing removed;
upgrading from 1.1.1 requires no code changes.

### Added

- `RuleCapability.execution_role` on the preflight response: which of four
  roles a rule plays in a streaming request — `enforce_streaming`,
  `enforce_postflight`, `stream_observed`, `postflight_observed`. This is
  what `streaming_supported` cannot express: it is `False` for every
  monitor rule, so a stream-capable observer and a full-context one look
  identical without this field. Kept as a plain string so a future
  server-side role does not break older clients; `None` when the rule
  cannot be planned at all.
- `ChunkResolutionUnavailable` — typed error for the server's 503
  `chunk_resolution_unavailable` (the server could not *resolve* the
  policy because a dependency was unreachable, as opposed to the policy
  being unable to stream). Retried exactly as before; when the outage
  outlasts the retry budget it is the `__cause__` of the
  `ChunkRetryExhausted` the SDK raises.

### Changed

- Against servers with monitor-mode streaming deployed, monitor policies
  now stream: preflight answers `mode="streaming"` and monitor rules
  report `execution_role="stream_observed"` or `"postflight_observed"`;
  their findings appear in the CollieAi audit log only, never on the
  wire. Against older servers nothing changes — they still answer
  `mode="buffered"`, `reason="monitor_mode"`, and the SDK handles both.
- The `__cause__` of `ChunkRetryExhausted` for a 503
  `chunk_resolution_unavailable` is now the typed
  `ChunkResolutionUnavailable` (a `ChunkError` subclass) instead of the
  generic `CollieAPIError`. This is the release's only observable
  behavior change; code that only catches `ChunkRetryExhausted` is
  unaffected.

## [1.1.1] — 2026-07-24

- Poll Backoff Contract (server-paced polling via `retry_after` hints,
  strict retry ceilings, idempotent chunk replay hardening).
