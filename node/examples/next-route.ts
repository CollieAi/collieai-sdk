/**
 * Next.js (App Router) safe-streaming route — drop in at app/api/chat/route.ts.
 * Streams ONLY CollieAi-released text to the browser.
 *
 *   npm i openai @collieai/sdk
 *   # env: COLLIEAI_API_KEY, COLLIEAI_PROJECT_ID, OPENAI_API_KEY
 */
import OpenAI from "openai";
import { ChunkStreamingUnsupported, CollieClient, CollieError } from "@collieai/sdk";
import { openaiFactory } from "@collieai/sdk/adapters/openai";

const collie = new CollieClient({
  apiKey: process.env.COLLIEAI_API_KEY!,
  projectId: process.env.COLLIEAI_PROJECT_ID!,
});
const openai = new OpenAI();

export async function POST(req: Request): Promise<Response> {
  const { prompt } = (await req.json()) as { prompt: string };
  const factory = openaiFactory(openai, {
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: prompt }],
  });
  const encoder = new TextEncoder();

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      let errored = false;
      try {
        for await (const event of collie.streaming.protectStream({
          input: prompt,
          rawStreamFactory: factory,
        })) {
          if (event.type === "delta") {
            controller.enqueue(encoder.encode(event.text));
          } else if (event.type === "blocked" || event.type === "input_blocked") {
            controller.enqueue(encoder.encode(event.blockMessage ?? "Blocked by policy."));
            break;
          }
        }
      } catch (e) {
        if (e instanceof ChunkStreamingUnsupported) {
          // The policy can't stream; re-run through protectBuffered instead.
          controller.enqueue(encoder.encode("This policy requires buffered response checking."));
        } else if (e instanceof CollieError) {
          // FAIL-CLOSED: nothing unverified is ever released. Failing OPEN on
          // an outage is a deliberate risk decision, not a default — see
          // "Failure policy: fail-closed by default" in the Node SDK docs for
          // how to do it safely (match only CollieConnectionError / 5xx
          // CollieApiError, and only before the first delta was enqueued).
          controller.enqueue(encoder.encode("Could not verify the response. Please try again."));
        } else {
          // Not ours: error the stream. `errored` keeps the finally below from
          // calling close() on an already-errored controller (a TypeError).
          errored = true;
          controller.error(e);
        }
      } finally {
        if (!errored) controller.close();
      }
    },
  });

  return new Response(stream, { headers: { "content-type": "text/plain" } });
}
