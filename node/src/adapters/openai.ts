/**
 * OpenAI provider adapter (optional helper). Normalizes the OpenAI Node SDK's
 * streaming chunks into text deltas. Duck-types the client/stream — no hard
 * dependency on `openai`.
 */
import type { RawStreamFactory } from "../types.js";
import { acloseStream } from "./_common.js";

/** Yield each non-empty content delta from an OpenAI chat-completion stream.
 * Closes the underlying stream when iteration ends or is abandoned early. */
export async function* openaiTextDeltas(
  stream: AsyncIterable<unknown>,
): AsyncGenerator<string> {
  const it = stream[Symbol.asyncIterator]();
  try {
    while (true) {
      const r = await it.next();
      if (r.done) break;
      const chunk = r.value as { choices?: Array<{ delta?: { content?: unknown } }> };
      const content = chunk?.choices?.[0]?.delta?.content;
      if (typeof content === "string" && content) yield content;
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

export interface OpenAIFactoryOptions {
  model: string;
  messages: Array<Record<string, unknown>>;
  [key: string]: unknown;
}

/** Build a zero-arg `rawStreamFactory` for protectStream/protectBuffered.
 * Opens the OpenAI stream lazily (after the input check) and once. */
export function openaiFactory(
  client: {
    chat: {
      completions: {
        create: (
          args: Record<string, unknown>,
          options?: { signal?: AbortSignal },
        ) => Promise<AsyncIterable<unknown>>;
      };
    };
  },
  opts: OpenAIFactoryOptions,
): RawStreamFactory {
  // Drop any caller-supplied signal/stream from the body: the SDK's runtime
  // signal (factory arg) and stream:true are authoritative.
  const { model, messages, signal: _callerSignal, stream: _callerStream, ...rest } = opts;
  return async function* (signal?: AbortSignal) {
    // signal belongs in the SDK's RequestOptions (2nd arg), NOT the JSON body.
    // ...rest first so caller extras can't override the forced contract fields.
    const response = await client.chat.completions.create(
      { ...rest, model, messages, stream: true },
      signal ? { signal } : undefined,
    );
    yield* openaiTextDeltas(response);
  };
}
