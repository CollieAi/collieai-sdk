/** Shared adapter helper. */
export async function acloseStream(stream: unknown): Promise<void> {
  const s = stream as Record<string, unknown> | null;
  for (const name of ["aclose", "close"]) {
    const fn = s?.[name];
    if (typeof fn === "function") {
      try {
        const r = (fn as () => unknown).call(s);
        if (r && typeof (r as { then?: unknown }).then === "function") await r;
      } catch {
        /* best-effort */
      }
      return;
    }
  }
}
