/**
 * Files (preview): upload a file for analysis, read its metadata, release it
 * (`POST/GET/DELETE /v1/files`). The deployment must have file analysis
 * enabled; otherwise every call throws `FileAnalysisUnavailable`.
 *
 * Attach an uploaded file to an input check with
 * `collie.moderate.input({ prompt, attachments: [file.id] })`.
 *
 * `upload` takes the content, not a path: the SDK has no runtime
 * dependencies and runs wherever `fetch` exists. In
 * Node: `collie.files.upload(await readFile(p), { filename: basename(p) })`.
 */
import type { CollieClient } from "./client.js";
import { CollieApiError } from "./errors.js";
import type { FileObject } from "./types.js";

const SDK_ORIGIN = "files";

const MEDIA_TYPES: Record<string, string> = {
  pdf: "application/pdf",
  txt: "text/plain",
  csv: "text/csv",
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
};

export interface UploadOptions {
  /** The file's name; its extension also sets the media type unless `contentType` is given. */
  filename: string;
  contentType?: string;
  /** Keep the bytes for the upload TTL after the last job releases them. */
  retain?: boolean;
}

export class FilesClient {
  constructor(private readonly c: CollieClient) {}

  /** Upload `content` as `filename`. `retain: true` keeps the bytes for the
   * upload TTL after its last job releases it; otherwise they go as soon as no
   * job needs them. The request carries a Content-Length (the API refuses a
   * body without one). */
  async upload(content: Blob | Uint8Array | ArrayBuffer, opts: UploadOptions): Promise<FileObject> {
    if (!opts || typeof opts.filename !== "string" || !opts.filename) {
      throw new TypeError("files.upload needs a filename");
    }
    const ext = opts.filename.includes(".") ? opts.filename.split(".").pop()!.toLowerCase() : "";
    const type = opts.contentType
      ?? (content instanceof Blob && content.type ? content.type : undefined)
      ?? MEDIA_TYPES[ext] ?? "application/octet-stream";
    const bytes = content instanceof Blob
      ? new Uint8Array(await content.arrayBuffer())
      : content instanceof Uint8Array ? content : new Uint8Array(content);
    const { body, contentType } = multipart(bytes, opts.filename, type, opts.retain === true);
    const { response, signal } = await this.c._requestRaw("POST", "/v1/files", { sdkOrigin: SDK_ORIGIN, body, contentType });
    return toFileObject(await this.c._readJsonBody(response, signal), response.status);
  }

  /** The file's metadata (`state`: uploaded, leased, deleted, ...). */
  async get(fileId: string): Promise<FileObject> {
    const { data, response } = await this.c._requestJson("GET", `/v1/files/${encodeURIComponent(fileId)}`, {
      sdkOrigin: SDK_ORIGIN,
    });
    return toFileObject(data, response.status);
  }

  /** Release the file. Its bytes go once no job needs them; a job that
   * already holds it still finishes. */
  async delete(fileId: string): Promise<void> {
    await this.c._requestRaw("DELETE", `/v1/files/${encodeURIComponent(fileId)}`, { sdkOrigin: SDK_ORIGIN });
  }
}

function toFileObject(d: Record<string, unknown>, status: number): FileObject {
  const str = (v: unknown): string | null => (typeof v === "string" ? v : null);
  if (typeof d.id !== "string" || typeof d.bytes !== "number" || typeof d.sha256 !== "string"
      || typeof d.state !== "string" || typeof d.created_at !== "string") {
    throw new CollieApiError("API returned a malformed file object", { statusCode: status, code: "invalid_response" });
  }
  return {
    id: d.id,
    bytes: d.bytes,
    sha256: d.sha256,
    filename: str(d.filename),
    declaredMediaType: str(d.declared_media_type),
    state: d.state,
    retain: d.retain === true,
    createdAt: d.created_at,
    expiresAt: str(d.expires_at),
  };
}

/** The multipart body as bytes, so `fetch` sends an exact Content-Length
 * whatever a runtime does with a `FormData` body — the API refuses a body
 * without a length (411). The filename is escaped as the WHATWG multipart
 * encoder does. */
function multipart(file: Uint8Array, filename: string, type: string, retain: boolean): { body: Uint8Array; contentType: string } {
  const boundary = `----collieai${Math.random().toString(16).slice(2)}${Date.now().toString(16)}`;
  const name = filename.replace(/\r/g, "%0D").replace(/\n/g, "%0A").replace(/"/g, "%22");
  const enc = new TextEncoder();
  const head = enc.encode(
    `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${name}"\r\n` +
      `Content-Type: ${type}\r\n\r\n`,
  );
  const tail = enc.encode(
    `\r\n--${boundary}\r\nContent-Disposition: form-data; name="retain"\r\n\r\n` +
      `${retain ? "true" : "false"}\r\n--${boundary}--\r\n`,
  );
  const body = new Uint8Array(head.length + file.length + tail.length);
  body.set(head, 0);
  body.set(file, head.length);
  body.set(tail, head.length + file.length);
  return { body, contentType: `multipart/form-data; boundary=${boundary}` };
}
