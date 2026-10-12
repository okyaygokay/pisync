import type { PisyncConfig } from "./config.ts";

export type RetryConfig = PisyncConfig["retry"];

/** Delay before retry number `attempt` (1-based), without Retry-After. */
export function backoffDelay(attempt: number, cfg: RetryConfig, rand: () => number = Math.random): number {
  const exp = Math.min(cfg.maxDelayMs, cfg.baseDelayMs * 2 ** Math.min(attempt - 1, 30));
  if (cfg.jitter === 0) return Math.round(exp);
  const factor = 1 + (rand() * 2 - 1) * cfg.jitter;
  return Math.max(0, Math.round(exp * factor));
}

/** Parse Retry-After (seconds or HTTP date) into ms; undefined if absent/invalid. */
export function parseRetryAfter(value: string | string[] | undefined, now = Date.now()): number | undefined {
  const v = Array.isArray(value) ? value[0] : value;
  if (!v) return undefined;
  const secs = Number(v);
  if (Number.isFinite(secs)) return secs < 0 ? undefined : Math.round(secs * 1000);
  const at = Date.parse(v);
  if (Number.isNaN(at)) return undefined;
  return Math.max(0, at - now);
}

/** Final delay: the larger of backoff and Retry-After (capped). */
export function retryDelay(
  attempt: number,
  cfg: RetryConfig,
  retryAfterHeader?: string | string[],
  rand?: () => number,
): number {
  const base = backoffDelay(attempt, cfg, rand);
  const ra = parseRetryAfter(retryAfterHeader);
  if (ra === undefined) return base;
  return Math.max(base, Math.min(ra, cfg.retryAfterMaxMs));
}

const RETRYABLE_ERRNO = new Set([
  "ECONNRESET",
  "ECONNREFUSED",
  "ECONNABORTED",
  "ETIMEDOUT",
  "EPIPE",
  "EAI_AGAIN",
  "ENOTFOUND",
  "ENETUNREACH",
  "EHOSTUNREACH",
  "UND_ERR_SOCKET",
  "UND_ERR_CONNECT_TIMEOUT",
  "ERR_STREAM_PREMATURE_CLOSE",
  "PISYNC_TIMEOUT",
]);

export function isRetryableError(err: unknown): boolean {
  const code = (err as { code?: string } | null)?.code;
  if (code && RETRYABLE_ERRNO.has(code)) return true;
  const msg = String((err as Error | null)?.message ?? "");
  return /socket hang up|aborted|premature close|TLS|ssl|certificate has expired/i.test(msg) && !/AbortError/.test(msg);
}

export function isRetryableStatus(status: number, cfg: RetryConfig): boolean {
  return cfg.statuses.includes(status);
}

const NON_RETRYABLE_STREAM_ERRORS = new Set([
  "invalid_request_error",
  "authentication_error",
  "permission_error",
  "not_found_error",
  "request_too_large",
  "invalid_api_key",
  "context_length_exceeded",
  "insufficient_quota",
]);

/**
 * Look at the first bytes of an SSE response. Some providers answer HTTP 200
 * and then immediately emit an error event (Anthropic `overloaded_error`).
 * Returns true if that is a transient error worth retrying.
 */
export function sniffRetryableStreamError(firstChunk: Buffer): boolean {
  const text = firstChunk.subarray(0, 2048).toString("utf8");
  const t = text.trimStart();
  const isErrorEvent = /^event:\s*error\b/.test(t) || /^data:\s*\{\s*"type"\s*:\s*"error"/.test(t) || /^data:\s*\{\s*"error"\s*:/.test(t);
  if (!isErrorEvent) return false;
  const m = /"(?:type|code)"\s*:\s*"([a-z_]+)"/g;
  let hit: RegExpExecArray | null;
  while ((hit = m.exec(t))) {
    const code = hit[1]!;
    if (code === "error") continue;
    if (NON_RETRYABLE_STREAM_ERRORS.has(code)) return false;
  }
  return true;
}
