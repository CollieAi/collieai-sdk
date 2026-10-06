/**
 * Express safe-streaming example. Streams ONLY CollieAi-released text — never
 * raw model output — with the input checked before the LLM is called.
 *
 *   npm i express openai @collieai/sdk
 *   COLLIEAI_API_KEY=clai_... COLLIEAI_PROJECT_ID=project_... OPENAI_API_KEY=sk-... \
 *     node --import tsx examples/express.ts
 */
import express from "express";
import OpenAI from "openai";
import { ChunkStreamingUnsupported, CollieClient, CollieError } from "@collieai/sdk";
import { openaiFactory } from "@collieai/sdk/adapters/openai";

const collie = new CollieClient({
  apiKey: process.env.COLLIEAI_API_KEY!,
  projectId: process.env.COLLIEAI_PROJECT_ID!,
});
const openai = new OpenAI();

const app = express();
app.use(express.json());

app.post("/chat", async (req, res) => {
  const prompt: string = req.body.prompt;
  const factory = openaiFactory(openai, {
    model: "gpt-4o-mini",
    messages: [{ role: "user", content: prompt }],
  });

  res.setHeader("content-type", "text/plain");
  try {
    for await (const event of collie.streaming.protectStream({
      input: prompt,
      rawStreamFactory: factory,
    })) {
      if (event.type === "delta") {
        res.write(event.text); // forward ONLY safe text
      } else if (event.type === "blocked" || event.type === "input_blocked") {
        res.write(event.blockMessage ?? "Blocked by policy.");
        break;
      }
    }
  } catch (e) {
    if (e instanceof ChunkStreamingUnsupported) {
      // The policy can't stream; re-run through protectBuffered instead.
      res.write("This policy requires buffered response checking.");
    } else if (e instanceof CollieError) {
      // FAIL-CLOSED: nothing unverified is ever released. Failing OPEN on an
      // outage is a deliberate risk decision, not a default — see "Failure
      // policy: fail-closed by default" in the Node SDK docs for how to do it
      // safely (match only CollieConnectionError / 5xx CollieApiError, and
      // only before the first delta was written).
      res.write("Could not verify the response. Please try again.");
    } else {
      throw e;
    }
  } finally {
    res.end();
  }
});

app.listen(3000, () => console.log("listening on :3000"));
