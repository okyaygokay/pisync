import http, { type IncomingMessage, type OutgoingHttpHeaders, type ServerResponse } from "node:http";
import https from "node:https";
import { pipeline } from "node:stream";
import type { PisyncConfig } from "./config.ts";
import type { Logger } from "./log.ts";
import { isRetryableError, isRetryableStatus, retryDelay, sniffRetryableStreamError } from "./retry.ts";

export interface Metrics {
  requests: number;
  retries: number;
  upstreamErrors: number;
  responses: Record<string, number>;
  failedAfterRetries: number;
}

export const newMetrics = (): Metrics => ({ requests: 0, retries: 0, upstreamErrors: 0, responses: {}, failedAfterRetries: 0 });

const HOP_BY_HOP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "proxy-connection",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "host",
  "content-length",
]);

/** Headers to send upstream: drop hop-by-hop and pisync-internal ones. */
export function filterRequestHeaders(h: IncomingMessage["headers"]): OutgoingHttpHeaders {
  const out: OutgoingHttpHeaders = {};
  for (const [k, v] of Object.entries(h)) {
    if (v === undefined || HOP_BY_HOP.has(k) || k.startsWith("x-pisync-")) continue;
    out[k] = v;
  }
  return out;
}

export function filterResponseHeaders(h: IncomingMessage["headers"]): OutgoingHttpHeaders {
  const out: OutgoingHttpHeaders = {};
  for (const [k, v] of Object.entries(h)) {
    if (v === undefined || (HOP_BY_HOP.has(k) && k !== "content-length")) continue;
    out[k] = v;
  }
  return out;
}

export interface ForwardArgs {
  cfg: PisyncConfig;
  target: URL;
  method: string;
  headers: OutgoingHttpHeaders;
  body: Buffer | undefined;
  clientRes: ServerResponse;
  /** Aborted when the client goes away. */
  signal: AbortSignal;
  log: Logger;
  metrics: Metrics;
  agents: { http: http.Agent; https: https.Agent };
  /** Test seams. */
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
  rand?: () => number;
  /** Label for logs only. */
  tag?: string;
}

interface AttemptOk {
  res: IncomingMessage;
  /** First body chunk (already consumed from `res`), or null if the body was empty. */
  first: Buffer | null;
  clear(): void;
}

const timeoutError = (what: string, ms: number): Error =>
  Object.assign(new Error(`upstream ${what} timeout after ${ms}ms`), { code: "PISYNC_TIMEOUT" });

const abortError = (): Error => Object.assign(new Error("client aborted"), { code: "PISYNC_CLIENT_ABORT", name: "AbortError" });

export const defaultSleep = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    if (signal.aborted) return reject(abortError());
    const t = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      reject(abortError());
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });

/** One upstream attempt, resolved once headers + the first body chunk are in. */
function attempt(a: ForwardArgs): Promise<AttemptOk> {
  const { cfg, target } = a;
  const t = cfg.timeouts;
  return new Promise<AttemptOk>((resolve, reject) => {
    const isTls = target.protocol === "https:";
    const timers = new Set<NodeJS.Timeout>();
    let done = false;
    let connectTimer: NodeJS.Timeout | undefined;
    let headersTimer: NodeJS.Timeout | undefined;
    let totalTimer: NodeJS.Timeout | undefined;
    let liveRes: IncomingMessage | undefined;

    const clear = () => {
      for (const x of [connectTimer, headersTimer, totalTimer]) if (x) clearTimeout(x);
      for (const x of timers) clearTimeout(x);
      a.signal.removeEventListener("abort", onAbort);
    };
    const fail = (err: Error) => {
      if (done) return;
      done = true;
      clear();
      req.destroy();
      reject(err);
    };
    const onAbort = () => fail(abortError());

    const headers: OutgoingHttpHeaders = { ...a.headers, host: target.host };
    if (a.body) headers["content-length"] = a.body.length;
    else if (a.method !== "GET" && a.method !== "HEAD") headers["content-length"] = 0;

    const req = (isTls ? https : http).request(
      {
        protocol: target.protocol,
        hostname: target.hostname,
        port: target.port || (isTls ? 443 : 80),
        path: target.pathname + target.search,
        method: a.method,
        headers,
        agent: isTls ? a.agents.https : a.agents.http,
      },
      (res) => {
        if (headersTimer) clearTimeout(headersTimer);
        liveRes = res;
        let idle: NodeJS.Timeout | undefined;
        const armIdle = () => {
          if (idle) clearTimeout(idle);
          idle = setTimeout(() => fail(timeoutError("idle", t.idleMs)), t.idleMs);
          timers.add(idle);
        };
        armIdle();
        const finish = (first: Buffer | null) => {
          if (done) return;
          if (idle) clearTimeout(idle);
          res.removeListener("error", onResErr);
          res.removeListener("data", onData);
          res.removeListener("end", onEnd);
          res.removeListener("close", onClose);
          done = true;
          if (a.signal) a.signal.removeEventListener("abort", onAbort);
          if (connectTimer) clearTimeout(connectTimer);
          for (const x of timers) clearTimeout(x);
          res.pause();
          resolve({
            res,
            first,
            clear: () => {
              if (totalTimer) clearTimeout(totalTimer);
            },
          });
        };
        const onData = (c: Buffer) => finish(c);
        const onEnd = () => finish(null);
        const onResErr = (e: Error) => fail(e);
        const onClose = () => fail(Object.assign(new Error("upstream closed before body"), { code: "ERR_STREAM_PREMATURE_CLOSE" }));
        res.on("data", onData);
        res.on("end", onEnd);
        res.on("error", onResErr);
        res.on("close", onClose);
      },
    );

    req.on("error", fail);
    req.on("socket", (sock) => {
      if (sock.connecting) {
        connectTimer = setTimeout(() => fail(timeoutError("connect", t.connectMs)), t.connectMs);
        sock.once(isTls ? "secureConnect" : "connect", () => {
          if (connectTimer) clearTimeout(connectTimer);
        });
      }
    });
    req.on("finish", () => {
      headersTimer = setTimeout(() => fail(timeoutError("headers", t.headersMs)), t.headersMs);
    });
    if (t.requestMs > 0)
      totalTimer = setTimeout(() => {
        const err = timeoutError("request", t.requestMs);
        if (!done) fail(err);
        else liveRes?.destroy(err); // already streaming: cut it
      }, t.requestMs);
    if (a.signal.aborted) return fail(abortError());
    a.signal.addEventListener("abort", onAbort, { once: true });
    req.end(a.body);
  });
}

function sendError(res: ServerResponse, status: number, type: string, message: string, extra?: OutgoingHttpHeaders): void {
  if (res.headersSent) {
    res.destroy();
    return;
  }
  const body = JSON.stringify({ type: "error", error: { type, message } });
  res.writeHead(status, { "content-type": "application/json", "content-length": Buffer.byteLength(body), ...extra });
  res.end(body);
}
export { sendError };

/**
 * Forward one request upstream with retries and stream the response back.
 * Resolves when the response has been fully delivered (or failed). The caller
 * holds the scheduler slot for exactly that duration.
 */
export async function forward(a: ForwardArgs): Promise<void> {
  const sleep = a.sleep ?? defaultSleep;
  const maxAttempts = a.cfg.retry.maxRetries + 1;
  const sse = (h: IncomingMessage["headers"]) => /text\/event-stream/i.test(String(h["content-type"] ?? ""));
  a.metrics.requests++;

  for (let n = 1; ; n++) {
    const last = n >= maxAttempts;
    let ok: AttemptOk;
    try {
      ok = await attempt(a);
    } catch (err) {
      if ((err as { code?: string }).code === "PISYNC_CLIENT_ABORT") return;
      a.metrics.upstreamErrors++;
      const msg = (err as Error).message;
      if (!last && isRetryableError(err)) {
        const delay = retryDelay(n, a.cfg.retry, undefined, a.rand);
        a.metrics.retries++;
        a.log.warn("upstream error, retrying", { tag: a.tag, attempt: n, delay, error: msg });
        try {
          await sleep(delay, a.signal);
        } catch {
          return;
        }
        continue;
      }
      a.metrics.failedAfterRetries++;
      a.log.error("upstream failed", { tag: a.tag, attempts: n, error: msg });
      sendError(a.clientRes, 502, "api_error", `pisync: upstream unreachable after ${n} attempt(s): ${msg}`);
      return;
    }

    const { res, first } = ok;
    const status = res.statusCode ?? 502;
    let retryReason: string | null = null;
    if (!last) {
      if (isRetryableStatus(status, a.cfg.retry)) retryReason = `status ${status}`;
      else if (
        a.cfg.retry.sniffStreamErrors &&
        first &&
        status === 200 &&
        sse(res.headers) &&
        !res.headers["content-encoding"] &&
        sniffRetryableStreamError(first)
      )
        retryReason = "error event in stream";
    }
    if (retryReason) {
      const delay = retryDelay(n, a.cfg.retry, res.headers["retry-after"], a.rand);
      a.metrics.retries++;
      a.log.warn("retrying", { tag: a.tag, attempt: n, delay, reason: retryReason });
      ok.clear();
      res.destroy();
      try {
        await sleep(delay, a.signal);
      } catch {
        return;
      }
      continue;
    }

    if (last && isRetryableStatus(status, a.cfg.retry)) a.metrics.failedAfterRetries++;
    a.metrics.responses[String(status)] = (a.metrics.responses[String(status)] ?? 0) + 1;
    await stream(a, ok);
    return;
  }
}

function stream(a: ForwardArgs, ok: AttemptOk): Promise<void> {
  const { res, first } = ok;
  const out = a.clientRes;
  return new Promise<void>((resolve) => {
    if (out.destroyed) {
      res.destroy();
      ok.clear();
      return resolve();
    }
    out.writeHead(res.statusCode ?? 502, res.statusMessage, filterResponseHeaders(res.headers));
    if (first) out.write(first);
    if (first === null) {
      ok.clear();
      out.end();
      return resolve();
    }
    let idle: NodeJS.Timeout | undefined;
    const armIdle = () => {
      if (idle) clearTimeout(idle);
      idle = setTimeout(() => res.destroy(Object.assign(new Error("upstream idle"), { code: "PISYNC_TIMEOUT" })), a.cfg.timeouts.idleMs);
    };
    armIdle();
    res.on("data", armIdle);
    const onAbort = () => res.destroy();
    a.signal.addEventListener("abort", onAbort, { once: true });
    pipeline(res, out, (err) => {
      if (idle) clearTimeout(idle);
      ok.clear();
      a.signal.removeEventListener("abort", onAbort);
      if (err && !a.signal.aborted) a.log.warn("stream ended with error", { tag: a.tag, error: err.message });
      resolve();
    });
    res.resume();
  });
}
