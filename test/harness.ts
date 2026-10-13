import http, { type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { resolveConfig, type PisyncConfig } from "../src/config.ts";
import { createDaemon, type Daemon } from "../src/daemon.ts";
import { nullLogger } from "../src/log.ts";

export interface SeenRequest {
  method: string;
  url: string;
  headers: IncomingMessage["headers"];
  body: Buffer;
  n: number;
}

export interface FakeUpstream {
  url: string;
  seen: SeenRequest[];
  /** Max number of simultaneously open requests observed. */
  maxConcurrent: number;
  current: number;
  handler: (req: SeenRequest, res: ServerResponse, raw: IncomingMessage) => unknown;
  close(): Promise<void>;
}

export async function fakeUpstream(handler?: FakeUpstream["handler"]): Promise<FakeUpstream> {
  const up: FakeUpstream = {
    url: "",
    seen: [],
    maxConcurrent: 0,
    current: 0,
    handler:
      handler ??
      ((req, res) => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true, echo: req.body.toString() }));
      }),
    close: async () => {},
  };
  const sockets = new Set<import("node:net").Socket>();
  const server = http.createServer(async (raw, res) => {
    const chunks: Buffer[] = [];
    for await (const c of raw) chunks.push(c as Buffer);
    const seen: SeenRequest = { method: raw.method!, url: raw.url!, headers: raw.headers, body: Buffer.concat(chunks), n: up.seen.length + 1 };
    up.seen.push(seen);
    up.current++;
    up.maxConcurrent = Math.max(up.maxConcurrent, up.current);
    res.on("close", () => up.current--);
    try {
      await up.handler(seen, res, raw);
    } catch {
      res.destroy();
    }
  });
  server.on("connection", (s) => {
    sockets.add(s);
    s.on("close", () => sockets.delete(s));
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  up.url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  up.close = () =>
    new Promise<void>((r) => {
      for (const s of sockets) s.destroy();
      server.close(() => r());
    });
  return up;
}

export type ConfigOverride = Record<string, unknown>;

export async function startDaemon(up: FakeUpstream | null, over: ConfigOverride = {}, extra: Partial<Parameters<typeof createDaemon>[0]> = {}) {
  const base: ConfigOverride = {
    listen: { host: "127.0.0.1", port: 0 },
    upstreams: up ? { fake: up.url } : {},
    retry: { baseDelayMs: 1, maxDelayMs: 2, jitter: 0 },
    timeouts: { connectMs: 2000, headersMs: 2000, idleMs: 2000, requestMs: 0 },
    task: { implicitIdleMs: 0 },
    agent: { leaseMs: 5000, pollMs: 200 },
    logLevel: "silent",
  };
  const cfg: PisyncConfig = resolveConfig(deepMerge(base, over));
  const d: Daemon = createDaemon({ config: cfg, logger: nullLogger, ...extra });
  const port = await d.listen();
  return { daemon: d, port, base: `http://127.0.0.1:${port}`, cfg };
}

function deepMerge(a: ConfigOverride, b: ConfigOverride): ConfigOverride {
  const out: ConfigOverride = { ...a };
  for (const [k, v] of Object.entries(b)) {
    const av = out[k];
    out[k] =
      v && typeof v === "object" && !Array.isArray(v) && av && typeof av === "object" && k !== "upstreams"
        ? deepMerge(av as ConfigOverride, v as ConfigOverride)
        : v;
  }
  return out;
}

export async function control(base: string, name: string, body: Record<string, unknown>, signal?: AbortSignal) {
  const res = await fetch(`${base}/_pisync/${name}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    ...(signal ? { signal } : {}),
  });
  return { status: res.status, body: (await res.json()) as Record<string, any> };
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function statusOf(base: string): Promise<any> {
  return (await fetch(`${base}/_pisync/status`)).json();
}

/** Long-poll acquire until granted (a single poll may time out with granted:false). */
export async function acquireUntilGranted(base: string, agentId: string, timeoutMs = 10_000) {
  const t0 = Date.now();
  for (;;) {
    const r = await control(base, "acquire", { agentId });
    if (r.body.granted === true || r.status !== 200 || Date.now() - t0 > timeoutMs) return r;
  }
}
