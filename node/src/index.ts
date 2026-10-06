/**
 * @collieai/sdk — Node/TypeScript SDK for safe customer-owned LLM streaming and
 * input moderation. Mirrors the Python SDK's API.
 *
 *   import { CollieClient } from "@collieai/sdk";
 *   const collie = new CollieClient({ apiKey: "clai_...", projectId: "project_123" });
 */
export { CollieClient, VERSION } from "./client.js";
export type { CollieClientOptions } from "./client.js";
export { StreamingClient, StreamingSession, toSse } from "./streaming.js";
export type { ProtectStreamOptions, ProtectBufferedOptions } from "./streaming.js";
export { ModerationClient } from "./moderation.js";
export type { ModerateInputOptions, ModerateOutputOptions } from "./moderation.js";
export * from "./errors.js";
export type {
  TriggeredRule,
  InputModerationResult,
  OutputModerationResult,
  ContextModerationResult,
  ContextStatus,
  SafeEmit,
  ChunkResult,
  SafeDelta,
  Blocked,
  InputBlocked,
  Finished,
  BufferedFallback,
  StreamInterrupted,
  StreamEvent,
  BufferedResult,
  RuleCapability,
  StreamingCapability,
  StreamToken,
  RawStreamFactory,
} from "./types.js";
