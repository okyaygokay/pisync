import http, { type IncomingMessage, type ServerResponse } from "node:http";
import https from "node:https";
import type { AddressInfo } from "node:net";
import type { PisyncConfig } from "./config.ts";
import { createLogger, type Logger } from "./log.ts";
import { filterRequestHeaders, forward, newMetrics, sendError, type Metrics } from "./proxy.ts";
import { Scheduler, SchedulerError, type SchedulerSnapshot } from "./scheduler.ts";

export const AGENT_ID_RE = /^[A-Za-z0-9._:@-]{1,128}$/;
const CONTROL_PREFIX = "/_pisync/";

export interface DaemonOptions {
  config: PisyncConfig;
  logger?: Logger;
  /** Test seams forwarded to the proxy. */
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
  rand?: () => number;
}

export interface Daemon {
  scheduler: Scheduler;
  server: http.Server;
  metrics: Metrics;
  /** Resolves with the bound port. */
  listen(): Promise<number>;
  close(graceMs?: number): Promise<void>;
  url(): string;
}

interface ControlBody {
  agentId?: unknown;
  parentId?: unknown;
  label?: unknown;
  pid?: unknown;
}

export function createDaemon(opts: DaemonOptions): Daemon {
  const cfg = opts.config;
  const log = opts.logger ?? createLogger(cfg.logLevel);
  const metrics = newMetrics();
  const startedAt = Date.now();
  const scheduler = new Scheduler({
    parentResume: cfg.queue.parentResume,
    maxTaskDurationMs: cfg.task.maxDurationMs,
    implicitTaskIdleMs: cfg.task.implicitIdleMs,
  });
  scheduler.onEvent((e) => {
    if (e.type === "granted" || e.type === "released") log.debug(e.type, { ...e });
    else log.info(e.type, { ...e });
  });

  const agents = {
    http: new http.Agent({ keepAlive: true, maxSockets: 16, keepAliveMsecs: 30_000 }),
    https: new https.Agent({ keepAlive: true, maxSockets: 16, keepAliveMsecs: 30_000 }),
  };
  const upstreams = new Map(Object.entries(cfg.upstreams).map(([k, v]) => [k, new URL(v.endsWith("/") ? v : v + "/")]));
  let anonSeq = 0;

  const server = http.createServer((req, res) => {
    handle(req, res).catch((err) => {
      log.error("unhandled", { error: (err as Error).message, stack: (err as Error).stack });
      sendError(res, 500, "api_error", "pisync: internal error");
    });
  });
  // LLM streams are long-lived; never let node time them out for us.
  server.requestTimeout = 0;
  server.headersTimeout = 60_000;
  server.keepAliveTimeout = 30_000;

  async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const rawUrl = req.url ?? "/";
    const q = rawUrl.indexOf("?");
    const path = q === -1 ? rawUrl : rawUrl.slice(0, q);

    if (path === "/healthz") {
      res.writeHead(200, { "content-type": "text/plain" }).end("ok");
      return;
    }
    if (path.startsWith(CONTROL_PREFIX)) return control(req, res, path.slice(CONTROL_PREFIX.length));
    return proxy(req, res, path, q === -1 ? "" : rawUrl.slice(q));
  }

  // ------------------------------------------------------------------ proxy

  async function proxy(req: IncomingMessage, res: ServerResponse, path: string, search: string): Promise<void> {
    // /a/<agent>/<upstream>/<rest>   or   /<upstream>/<rest> (+ x-pisync-agent header)
    let agentId: string | undefined;
    let rest = path;
    const m = /^\/a\/([^/]+)(\/.*)?$/.exec(path);
    if (m) {
      try {
        agentId = decodeURIComponent(m[1]!);
      } catch {
        return sendError(res, 400, "invalid_request_error", "pisync: bad agent id");
      }
      rest = m[2] ?? "/";
    } else if (typeof req.headers["x-pisync-agent"] === "string") {
      agentId = req.headers["x-pisync-agent"];
    }
    if (agentId !== undefined && !AGENT_ID_RE.test(agentId)) {
      return sendError(res, 400, "invalid_request_error", "pisync: bad agent id");
    }
    const parts = /^\/([^/]+)(\/.*)?$/.exec(rest);
    const base = parts && upstreams.get(parts[1]!);
    if (!parts || !base) {
      return sendError(res, 404, "not_found_error", `pisync: unknown upstream (known: ${[...upstreams.keys()].join(", ") || "none"})`);
    }
    const sub = (parts[2] ?? "").replace(/^\/+/, "");
    const target = new URL(sub + search, base);

    const anon = agentId === undefined;
    const id = agentId ?? `anon-${++anonSeq}`;

    const ac = new AbortController();
    const onClose = () => {
      if (!res.writableFinished) ac.abort();
    };
    res.on("close", onClose);

    try {
      const body = await readBody(req, cfg.queue.maxRequestBodyBytes, ac.signal);
      if (body === "too-large") return sendError(res, 413, "request_too_large", "pisync: request body too large");
      if (ac.signal.aborted) return;

      let release: () => void;
      const t0 = Date.now();
      try {
        release = await scheduler.acquireRequest(id, { signal: ac.signal });
      } catch (e) {
        if (e instanceof SchedulerError) {
          if (e.code === "aborted" || ac.signal.aborted) return;
          if (e.code === "task_expired") return sendError(res, 403, "permission_error", `pisync: ${e.message}`);
          return sendError(res, 503, "overloaded_error", `pisync: ${e.message}`, { "retry-after": "30" });
        }
        throw e;
      }
      try {
        if (ac.signal.aborted) return;
        const waited = Date.now() - t0;
        if (waited > 1000) log.info("request granted after wait", { agent: id, waitedMs: waited });
        await forward({
          cfg,
          target,
          method: req.method ?? "GET",
          headers: filterRequestHeaders(req.headers),
          body: body.length ? body : undefined,
          clientRes: res,
          signal: ac.signal,
          log,
          metrics,
          agents,
          tag: id,
          ...(opts.sleep ? { sleep: opts.sleep } : {}),
          ...(opts.rand ? { rand: opts.rand } : {}),
        });
      } finally {
        release();
        if (anon) scheduler.endTask(id);
      }
    } finally {
      res.off("close", onClose);
    }
  }

  // ---------------------------------------------------------------- control

  async function control(req: IncomingMessage, res: ServerResponse, name: string): Promise<void> {
    if (name === "status" && req.method === "GET") return json(res, 200, status());
    if (req.method !== "POST") return json(res, 405, { error: "method_not_allowed" });

    const ac = new AbortController();
    const onClose = () => {
      if (!res.writableFinished) ac.abort();
    };
    res.on("close", onClose);
    try {
      const raw = await readBody(req, 64 * 1024, ac.signal);
      if (raw === "too-large") return json(res, 413, { error: "too_large" });
      let b: ControlBody = {};
      if (raw.length) {
        try {
          b = JSON.parse(raw.toString("utf8")) as ControlBody;
        } catch {
          return json(res, 400, { error: "bad_json" });
        }
      }
      if (typeof b.agentId !== "string" || !AGENT_ID_RE.test(b.agentId)) return json(res, 400, { error: "bad_agent_id" });
      const id = b.agentId;
      const parentId = typeof b.parentId === "string" && AGENT_ID_RE.test(b.parentId) ? b.parentId : null;
      const info = {
        parentId,
        ...(typeof b.label === "string" ? { label: b.label.slice(0, 200) } : {}),
        ...(typeof b.pid === "number" ? { pid: b.pid } : {}),
        leaseMs: cfg.agent.leaseMs,
      };

      switch (name) {
        case "register":
          scheduler.registerAgent(id, info);
          return json(res, 200, { ok: true, leaseMs: cfg.agent.leaseMs, pollMs: cfg.agent.pollMs });
        case "heartbeat":
          if (!scheduler.hasAgent(id)) return json(res, 404, { error: "unknown_agent" });
          scheduler.touch(id);
          // `expired`: the task hit the hard limit; the extension aborts the run.
          return json(res, 200, { ok: true, expired: scheduler.isExpired(id) });
        case "acquire": {
          // Self-healing: a restarted daemon learns the agent from the acquire itself.
          const known = scheduler.parentOf(id);
          if (known === undefined || (parentId && known !== parentId)) {
            scheduler.registerAgent(id, info);
          }
          try {
            const granted = await scheduler.waitActive(id, { signal: ac.signal, timeoutMs: cfg.agent.pollMs });
            if (granted) return json(res, 200, { granted: true });
            return json(res, 200, { granted: false, position: scheduler.position(id), queueLength: scheduler.queueLength() });
          } catch (e) {
            if (e instanceof SchedulerError) {
              if (e.code === "aborted") return;
              return json(res, e.code === "task_expired" ? 409 : 503, { error: e.code, message: e.message });
            }
            throw e;
          }
        }
        case "release":
          scheduler.endTask(id);
          return json(res, 200, { ok: true });
        case "unregister":
          scheduler.unregisterAgent(id);
          return json(res, 200, { ok: true });
        default:
          return json(res, 404, { error: "unknown_endpoint" });
      }
    } finally {
      res.off("close", onClose);
    }
  }

  function status(): { pid: number; uptimeMs: number; scheduler: SchedulerSnapshot; metrics: Metrics; upstreams: string[] } {
    return { pid: process.pid, uptimeMs: Date.now() - startedAt, scheduler: scheduler.snapshot(), metrics, upstreams: [...upstreams.keys()] };
  }

  return {
    scheduler,
    server,
    metrics,
    listen() {
      return new Promise<number>((resolve, reject) => {
        server.once("error", reject);
        server.listen(cfg.listen.port, cfg.listen.host, () => {
          server.off("error", reject);
          const port = (server.address() as AddressInfo).port;
          log.info("pisyncd listening", { host: cfg.listen.host, port, upstreams: [...upstreams.keys()] });
          resolve(port);
        });
      });
    },
    url() {
      const a = server.address() as AddressInfo | null;
      return a ? `http://${cfg.listen.host === "0.0.0.0" ? "127.0.0.1" : cfg.listen.host}:${a.port}` : "";
    },
    async close(graceMs = 2000) {
      scheduler.close();
      const closed = new Promise<void>((r) => server.close(() => r()));
      server.closeIdleConnections();
      const t = setTimeout(() => server.closeAllConnections(), graceMs);
      await closed;
      clearTimeout(t);
      agents.http.destroy();
      agents.https.destroy();
    },
  };
}

function json(res: ServerResponse, status: number, body: unknown): void {
  if (res.headersSent) return;
  const s = JSON.stringify(body);
  res.writeHead(status, { "content-type": "application/json", "content-length": Buffer.byteLength(s) });
  res.end(s);
}

async function readBody(req: IncomingMessage, limit: number, signal: AbortSignal): Promise<Buffer | "too-large"> {
  const len = Number(req.headers["content-length"]);
  if (Number.isFinite(len) && len > limit) {
    req.resume();
    return "too-large";
  }
  const chunks: Buffer[] = [];
  let total = 0;
  try {
    for await (const c of req) {
      total += (c as Buffer).length;
      if (total > limit) {
        req.resume();
        return "too-large";
      }
      chunks.push(c as Buffer);
    }
  } catch (e) {
    if (signal.aborted) return Buffer.alloc(0);
    throw e;
  }
  return chunks.length === 1 ? chunks[0]! : Buffer.concat(chunks, total);
}
