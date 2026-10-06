import { test, expect } from "vitest";
import { openaiFactory, openaiTextDeltas } from "../src/adapters/openai.js";
import { anthropicFactory, anthropicTextDeltas } from "../src/adapters/anthropic.js";
import type { SafeDelta, StreamEvent } from "../src/types.js";
import { makeClient, echoHandler, collect } from "./helpers.js";

/** Async-iterable that tracks whether its iterator was closed (return()ed). */
function closableStream(items: unknown[]): { stream: AsyncIterable<unknown>; closed: () => boolean } {
  let closed = false;
  const stream: AsyncIterable<unknown> = {
    [Symbol.asyncIterator]() {
      let i = 0;
      return {
        async next() {
          return i < items.length ? { value: items[i++], done: false } : { value: undefined, done: true };
        },
        async return() {
          closed = true;
          return { value: undefined, done: true };
        },
      };
    },
  };
  return { stream, closed: () => closed };
}

const oaiChunk = (content: unknown) => ({ choices: [{ delta: { content } }] });
const anthropicText = (text: string) => ({ type: "content_block_delta", delta: { type: "text_delta", text } });
const anthropicJson = (partial: string) => ({ type: "content_block_delta", delta: { type: "input_json_delta", partial_json: partial } });

const texts = (events: StreamEvent[]) =>
  events.filter((e): e is SafeDelta => e.type === "delta").map((e) => e.text);

test("openaiTextDeltas extracts and skips empty/role/no-choices", async () => {
  const { stream } = closableStream([
    oaiChunk(null),
    oaiChunk("Hello"),
    oaiChunk(""),
    oaiChunk(", world"),
    { choices: [] },
  ]);
  expect(await collect(openaiTextDeltas(stream))).toEqual(["Hello", ", world"]);
});

test("openaiTextDeltas closes the stream on early exit", async () => {
  const { stream, closed } = closableStream([oaiChunk("a"), oaiChunk("b")]);
  const gen = openaiTextDeltas(stream);
  expect((await gen.next()).value).toBe("a");
  await gen.return(undefined);
  expect(closed()).toBe(true);
});

test("openaiFactory is lazy and composes with protectStream", async () => {
  const opened = { n: 0 };
  const client = {
    chat: {
      completions: {
        async create() {
          opened.n += 1;
          return (async function* () {
            yield oaiChunk("Hel");
            yield oaiChunk("lo");
          })();
        },
      },
    },
  };
  const factory = openaiFactory(client, { model: "gpt-4o-mini", messages: [{ role: "user", content: "hi" }] });
  expect(opened.n).toBe(0);
  const collie = makeClient(echoHandler());
  const events = await collect(collie.streaming.protectStream({ input: "hi", rawStreamFactory: factory }));
  expect(texts(events).join("")).toBe("Hello");
  expect(opened.n).toBe(1);
});

test("anthropicTextDeltas extracts content_block_delta, skips others", async () => {
  const { stream } = closableStream([
    { type: "message_start" },
    { type: "content_block_start" },
    anthropicText("Hello"),
    anthropicJson('{"x":'),
    anthropicText(", world"),
    { type: "message_stop" },
  ]);
  expect(await collect(anthropicTextDeltas(stream))).toEqual(["Hello", ", world"]);
});

test("openaiFactory forces stream:true and puts the SDK signal in RequestOptions", async () => {
  let seen: Record<string, unknown> = {};
  let seenOptions: { signal?: AbortSignal } | undefined;
  const client = {
    chat: {
      completions: {
        async create(args: Record<string, unknown>, options?: { signal?: AbortSignal }) {
          seen = args;
          seenOptions = options;
          return (async function* () {
            yield oaiChunk("x");
          })();
        },
      },
    },
  };
  const callerSignal = new AbortController().signal;
  const factory = openaiFactory(client, {
    model: "m",
    messages: [],
    stream: false, // caller tries to disable streaming...
    signal: callerSignal, // ...and inject their own signal into the body
  } as never);
  const sdkSignal = new AbortController().signal;
  await collect(factory(sdkSignal));
  expect(seen.stream).toBe(true); // forced on, despite stream:false
  expect(seen.signal).toBeUndefined(); // signal is NOT a body param
  expect(seenOptions?.signal).toBe(sdkSignal); // it lives in RequestOptions, SDK signal wins
});

test("anthropicFactory forces stream:true and puts the SDK signal in RequestOptions", async () => {
  let seen: Record<string, unknown> = {};
  let seenOptions: { signal?: AbortSignal } | undefined;
  const client = {
    messages: {
      async create(args: Record<string, unknown>, options?: { signal?: AbortSignal }) {
        seen = args;
        seenOptions = options;
        return (async function* () {
          yield anthropicText("x");
        })();
      },
    },
  };
  const factory = anthropicFactory(client, {
    model: "m",
    maxTokens: 8,
    messages: [],
    stream: false,
  } as never);
  const sdkSignal = new AbortController().signal;
  await collect(factory(sdkSignal));
  expect(seen.stream).toBe(true);
  expect(seen.signal).toBeUndefined();
  expect(seen.max_tokens).toBe(8);
  expect(seenOptions?.signal).toBe(sdkSignal);
});

test("anthropicFactory requires maxTokens", () => {
  // @ts-expect-error maxTokens is required
  expect(() => anthropicFactory({} as never, { model: "m", messages: [] })).toThrow();
});

test("anthropicFactory passes max_tokens and composes", async () => {
  const opened = { n: 0 };
  const client = {
    messages: {
      async create(args: Record<string, unknown>) {
        opened.n += 1;
        expect(args.max_tokens).toBe(512);
        expect(args.stream).toBe(true);
        return (async function* () {
          yield anthropicText("Hel");
          yield anthropicText("lo");
        })();
      },
    },
  };
  const factory = anthropicFactory(client, {
    model: "claude-3-5-sonnet-latest",
    maxTokens: 512,
    messages: [{ role: "user", content: "hi" }],
  });
  const collie = makeClient(echoHandler());
  const events = await collect(collie.streaming.protectStream({ input: "hi", rawStreamFactory: factory }));
  expect(texts(events).join("")).toBe("Hello");
  expect(opened.n).toBe(1);
});
