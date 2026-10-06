/**
 * Anthropic provider adapter (optional helper). Extracts text from the
 * Anthropic SDK's raw streaming events (`content_block_delta` → delta.text),
 * skipping tool-use JSON deltas and lifecycle events. Duck-typed — no hard
 * dependency on `@anthropic-ai/sdk`.
 */
import type { RawStreamFactory } from "../types.js";
import { acloseStream } from "./_common.js";

export async function* anthropicTextDeltas(
  stream: AsyncIterable<unknown>,
): AsyncGenerator<string> {
  const it = stream[Symbol.asyncIterator]();
  try {
    while (true) {
      const r = await it.next();
      if (r.done) break;
      const ev = r.value as { type?: string; delta?: { text?: unknown } };
      if (ev?.type !== "content_block_delta") continue;
      const text = ev.delta?.text;
      if (typeof text === "string" && text) yield text;
    }
  } finally {
    if (typeof it.return === "function") {
      try {
        await it.return();
      } catch {
        /* best-effort */
      }
    }
    await acloseStream(stream);
  }
}

export interface AnthropicFactoryOptions {
  model: string;
  messages: Array<Record<string, unknown>>;
  maxTokens: number;
  [key: string]: unknown;
}

/** Build a zero-arg `rawStreamFactory` from an Anthropic client. `maxTokens` is
 * required (Anthropic's API requires it; no silent default that could truncate). */
export function anthropicFactory(
  client: {
    messages: {
      create: (
        args: Record<string, unknown>,
        options?: { signal?: AbortSignal },
      ) => Promise<AsyncIterable<unknown>>;
    };
  },
  opts: AnthropicFactoryOptions,
): RawStreamFactory {
  // Drop any caller-supplied signal/stream from the body: the SDK's runtime
  // signal (factory arg) and stream:true are authoritative.
  const { model, messages, maxTokens, signal: _callerSignal, stream: _callerStream, ...rest } = opts;
  if (maxTokens == null) {
    throw new Error("anthropicFactory requires maxTokens");
  }
  return async function* (signal?: AbortSignal) {
    // signal belongs in the SDK's RequestOptions (2nd arg), NOT the JSON body.
    // ...rest first so caller extras can't override the forced contract fields.
    const response = await client.messages.create(
      { ...rest, model, max_tokens: maxTokens, messages, stream: true },
      signal ? { signal } : undefined,
    );
    yield* anthropicTextDeltas(response);
  };
}
