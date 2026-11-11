/**
 * File analysis (preview): the files client, attachments on moderate.input,
 * FileAnalysisUnavailable and ModerationTimeout. Test names
 * match the Python and .NET suites.
 */
import { createServer, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import { expect, test } from "vitest";

import { CollieClient } from "../src/client.js";
import { CollieApiError, FileAnalysisUnavailable, ModerationError, ModerationTimeout } from "../src/errors.js";
import { bodyOf, errorJson, json, makeClient, pathOf, type Handler } from "./helpers.js";

const FILE = {
  id: "file_0123456789abcdef0123456789abcdef", object: "file", bytes: 5, sha256: "ab".repeat(32),
  filename: "a.pdf", declared_media_type: "application/pdf", state: "uploaded", retain: false,
  created_at: "2026-11-10T12:00:00Z", expires_at: "2026-11-10T13:00:00Z",
};
const NOW = new Date("2026-11-10T12:00:00.000Z");

function job(extra: Record<string, unknown> = {}, status = "completed") {
  return { job_id: "job_f", status, inbound_result: { allowed: true, blocked: false, triggered_rules: [] }, ...extra };
}

function jobsHandler(seen: Array<{ url: string; init: RequestInit }>, result: Record<string, unknown>): Handler {
  return (url, init) => {
    seen.push({ url, init });
    if (init.method === "POST" && pathOf(url) === "/v1/jobs") return json(202, { job_id: "job_f", status: "processing_inbound" });
    return json(200, result);
  };
}

// --------------------------------------------------------------------- files

test("upload sends multipart with a length and retain", async () => {
  // A real local server: the Content-Length the runtime actually sends is
  // what the API's 411 checks.
  const got: { headers?: IncomingMessage["headers"]; body?: Buffer } = {};
  const server = createServer((req, res) => {
    const parts: Buffer[] = [];
    req.on("data", (c: Buffer) => parts.push(c));
    req.on("end", () => {
      got.headers = req.headers;
      got.body = Buffer.concat(parts);
      // `connection: close`: Node 18's server.close() otherwise waits out the
      // client's idle keep-alive socket (~5 s).
      res.writeHead(201, { "content-type": "application/json", connection: "close" });
      res.end(JSON.stringify({ ...FILE, retain: true }));
    });
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  try {
    const port = (server.address() as AddressInfo).port;
    const collie = new CollieClient({ apiKey: "clai_test", baseUrl: `http://127.0.0.1:${port}` });
    const file = await collie.files.upload(new TextEncoder().encode("%PDF-"), { filename: "a.pdf", retain: true });
    expect(file.id).toBe(FILE.id);
    expect(file.retain).toBe(true);
    expect(file.declaredMediaType).toBe("application/pdf");
    expect(got.headers?.["content-type"]).toMatch(/^multipart\/form-data; boundary=/);
    expect(Number(got.headers?.["content-length"])).toBe(got.body?.length);
    expect(got.headers?.["transfer-encoding"]).toBeUndefined();
    expect(got.headers?.["x-collieai-sdk-origin"]).toBe("files");
    const text = got.body!.toString("latin1");
    expect(text).toContain('name="file"; filename="a.pdf"');
    expect(text).toContain("%PDF-");
    expect(text).toMatch(/name="retain"\r\n\r\ntrue/);
  } finally {
    await new Promise<void>((r) => server.close(() => r()));
  }
});

test("get and delete", async () => {
  const seen: Array<[string | undefined, string]> = [];
  const collie = makeClient((url, init) => {
    seen.push([init.method, pathOf(url)]);
    return init.method === "DELETE" ? new Response(null, { status: 204 }) : json(200, FILE);
  });
  expect((await collie.files.get(FILE.id)).state).toBe("uploaded");
  expect(await collie.files.delete(FILE.id)).toBeUndefined();
  expect(seen).toEqual([["GET", `/v1/files/${FILE.id}`], ["DELETE", `/v1/files/${FILE.id}`]]);
});

test("file analysis unavailable on its code only", async () => {
  const bytes = new TextEncoder().encode("x");
  const off = makeClient(() => errorJson(422, "file_analysis_unavailable"));
  const err = await off.files.upload(bytes, { filename: "a.txt" }).catch((e) => e);
  expect(err).toBeInstanceOf(FileAnalysisUnavailable);
  expect(err).toBeInstanceOf(CollieApiError);
  expect(err.statusCode).toBe(422);
  const other = makeClient(() => errorJson(422, "invalid_retain"));
  const err2 = await other.files.upload(bytes, { filename: "a.txt" }).catch((e) => e);
  expect(err2).toBeInstanceOf(CollieApiError);
  expect(err2).not.toBeInstanceOf(FileAnalysisUnavailable);
});

test("a malformed file object is a typed error", async () => {
  const collie = makeClient(() => json(200, { id: "x" }));
  const err = await collie.files.get("x").catch((e) => e);
  expect(err).toBeInstanceOf(CollieApiError);
  expect(err.code).toBe("invalid_response");
});

// --------------------------------------------------------------- attachments

test("attachments carry the deadline computed before the post", async () => {
  const seen: Array<{ url: string; init: RequestInit }> = [];
  const collie = makeClient(jobsHandler(seen, job({ file_result: { status: "rejected", files: [] } })), {
    requestTimeoutS: 20, utcNow: () => NOW,
  });
  const result = await collie.moderate.input({
    prompt: "summarize", attachments: [FILE.id], timeoutS: 10,
  });
  const body = bodyOf(seen[0]!.init);
  expect(body.attachments).toEqual([{ file_id: FILE.id }]);
  expect(body.client_now).toBe("2026-11-10T12:00:00.000Z");
  expect(body.client_deadline_at).toBe("2026-11-10T12:00:30.000Z"); // request timeout + timeoutS
  expect(body.message_input).toBe("summarize");
  expect(result.fileResult).toEqual({ status: "rejected", files: [] });
});

test("without attachments the body is unchanged", async () => {
  const seen: Array<{ url: string; init: RequestInit }> = [];
  const collie = makeClient(jobsHandler(seen, job()), { utcNow: () => NOW });
  const result = await collie.moderate.input({ prompt: "hi" });
  expect(bodyOf(seen[0]!.init)).toEqual({ message_input: "hi", inbound_only: true });
  expect(result.fileResult).toBeNull();
});

test("an attachment-only check", async () => {
  const seen: Array<{ url: string; init: RequestInit }> = [];
  const collie = makeClient(jobsHandler(seen, job({ file_result: { status: "rejected" } })), { utcNow: () => NOW });
  const result = await collie.moderate.input({ attachments: [FILE.id] });
  const body = bodyOf(seen[0]!.init);
  expect(body.message_input).toBeUndefined();
  expect(body.inbound_only).toBe(true);
  expect(result.originalText).toBe("");
  expect(result.allowed).toBe(true);
});

test.each([
  [{}],
  [{ attachments: [] }],
  [{ attachments: "file_x" }],
  [{ attachments: [""] }],
])("refused before any request %#", async (opts) => {
  const seen: Array<{ url: string; init: RequestInit }> = [];
  const collie = makeClient(jobsHandler(seen, job()));
  await expect(collie.moderate.input(opts as never)).rejects.toThrow();
  expect(seen).toEqual([]);
});

test("a missing file result is file analysis unavailable", async () => {
  const seen: Array<{ url: string; init: RequestInit }> = [];
  const collie = makeClient(jobsHandler(seen, job())); // a server that ignored the attachments
  const err = await collie.moderate.input({ prompt: "hi", attachments: [FILE.id] }).catch((e) => e);
  expect(err).toBeInstanceOf(FileAnalysisUnavailable);
  expect(err.code).toBe("file_result_missing");
});

test("the attachments are copied when the call starts", async () => {
  // Emptying the caller's array mid-call must not switch off the check for a
  // server that ignored the attachments.
  const files = [FILE.id];
  const seen: Array<{ url: string; init: RequestInit }> = [];
  const inner = jobsHandler(seen, job());
  const collie = makeClient((url, init) => {
    files.length = 0;
    return inner(url, init);
  });
  await expect(collie.moderate.input({ prompt: "hi", attachments: files })).rejects.toBeInstanceOf(
    FileAnalysisUnavailable,
  );
});

test("attachments refused by the deployment", async () => {
  const collie = makeClient(() => errorJson(422, "file_analysis_unavailable"));
  await expect(collie.moderate.input({ prompt: "hi", attachments: [FILE.id] })).rejects.toBeInstanceOf(
    FileAnalysisUnavailable,
  );
});

// ------------------------------------------------------------------- timeout

test("sdk timeout raises moderation timeout subclass", async () => {
  // The SDK's own poll budget running out throws ModerationTimeout —
  // a ModerationError, so existing handlers keep catching it — with jobId, on
  // moderate.input and moderate.output alike. A virtual clock drives it.
  let now = 0;
  const clocked = (status: string, jobId: string) =>
    makeClient(
      (url, init) => {
        now += 0.01;
        return init.method === "POST" ? json(202, { job_id: jobId, status }) : json(200, { job_id: jobId, status });
      },
      { monotonic: () => now, sleep: async (s: number) => { now += s; } },
    );
  const err = await clocked("processing_inbound", "job_slow").moderate.input({ prompt: "hi", timeoutS: 5 }).catch((e) => e);
  expect(err).toBeInstanceOf(ModerationTimeout);
  expect(err).toBeInstanceOf(ModerationError);
  expect(err.jobId).toBe("job_slow");
  expect(err.message).toMatch(/timed out/);

  now = 0;
  const out = await clocked("processing_outbound", "job_o").moderate.output({ response: "text", timeoutS: 5 }).catch((e) => e);
  expect(out).toBeInstanceOf(ModerationTimeout);
  expect(out).toBeInstanceOf(ModerationError);
  expect(out.jobId).toBe("job_o");
});

test("the filename is escaped in the multipart header", async () => {
  let sent = "";
  const collie = makeClient((url, init) => {
    sent = new TextDecoder().decode(init.body as Uint8Array);
    expect((init.headers as Record<string, string>)["content-type"]).toMatch(/^multipart\/form-data; boundary=/);
    return json(201, FILE);
  });
  await collie.files.upload(new Uint8Array([1, 2]), { filename: 'a"b\r\nc.pdf' });
  expect(sent).toContain('filename="a%22b%0D%0Ac.pdf"');
  expect(sent).toContain("Content-Type: application/pdf");
});
