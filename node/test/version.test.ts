/**
 * Version-identity invariant: the wire User-Agent is how the backend (and
 * the release gate) identifies the SDK version, and VERSION is a
 * manual copy of package.json — this test is what keeps the two from
 * drifting on the next bump (the publish workflow only checks package.json
 * against the tag).
 */
import { readFileSync } from "node:fs";
import { expect, test } from "vitest";

import { VERSION } from "../src/client.js";
import { json, makeClient, type Handler } from "./helpers.js";

const pkg = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
) as { version: string };

test("VERSION matches package.json and reaches the wire as the User-Agent", async () => {
  expect(VERSION).toBe(pkg.version);

  let userAgent: string | null = null;
  const handler: Handler = (url, init) => {
    userAgent ??= (init.headers as Record<string, string>)["user-agent"] ?? null;
    if ((init.method ?? "GET") === "POST") {
      return json(202, { job_id: "j", status: "processing_inbound" });
    }
    return json(200, {
      job_id: "j",
      status: "completed",
      inbound_result: { allowed: true, blocked: false, triggered_rules: [] },
    });
  };
  const collie = makeClient(handler);
  await collie.moderate.input({ prompt: "hi" });
  expect(userAgent).toBe(`collieai-node/${pkg.version}`);
});
