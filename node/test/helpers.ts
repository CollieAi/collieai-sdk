import { CollieClient, type CollieClientOptions } from "../src/client.js";

export type Handler = (url: string, init: RequestInit) => Response | Promise<Response>;

/** Build a CollieClient wired to a fake fetch + instant sleep. */
export function makeClient(
  handler: Handler,
  opts: Partial<CollieClientOptions> = {},
): CollieClient {
  const fetchImpl = (async (input: unknown, init?: RequestInit) =>
    handler(String(input), init ?? {})) as unknown as typeof fetch;
  return new CollieClient({
    apiKey: "clai_test",
    baseUrl: "http://test",
    projectId: "proj_1",
    fetch: fetchImpl,
    sleep: async () => {},
    ...opts,
  });
}

export function json(
  status: number,
  body: unknown,
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export function errorJson(
  status: number,
  code: string,
  message = "err",
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify({ error: { message, type: code, code } }), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

export function sse(...frames: string[]): Response {
  return new Response(frames.join(""), {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

export function pathOf(url: string): string {
  return new URL(url).pathname;
}

export function bodyOf(init: RequestInit): Record<string, unknown> {
  return init.body ? JSON.parse(String(init.body)) : {};
}

/** A backend that passes input checks and echoes chunk content as safe emits. */
export function echoHandler(): Handler {
  return (url, init) => {
    const path = pathOf(url);
    const method = init.method ?? "GET";
    if (method === "POST" && path === "/v1/jobs") {
      const body = bodyOf(init);
      return json(202, { job_id: body.inbound_only ? "job_mod" : "job_stream" });
    }
    if (method === "GET" && path === "/v1/jobs/job_mod") {
      return json(200, {
        status: "completed",
        inbound_result: { allowed: true, blocked: false, triggered_rules: [] },
      });
    }
    if (method === "POST" && path === "/v1/jobs/job_stream/chunks") {
      const body = bodyOf(init);
      const isFinal = Boolean(body.is_final);
      const content = (body.content as string) ?? "";
      const emits =
        content || isFinal
          ? [{ content, blocked: false, final: isFinal, triggered_rules: [] }]
          : [];
      return json(200, { sequence: body.sequence, accepted: true, emits, finished: isFinal });
    }
    return json(404, { error: { message: "nf", type: "not_found" } });
  };
}

export async function collect<T>(gen: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = [];
  for await (const x of gen) out.push(x);
  return out;
}

export async function* fromArray<T>(items: T[]): AsyncGenerator<T> {
  for (const it of items) yield it;
}
