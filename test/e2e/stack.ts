/**
 * Shared harness for end-to-end tests: REAL pi processes (from devDependencies)
 * -> pisync extension -> pisyncd -> fake Anthropic-compatible upstream.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveConfig } from "../../src/config.ts";
import { createDaemon, type Daemon } from "../../src/daemon.ts";
import { nullLogger } from "../../src/log.ts";

export const ROOT = new URL("../..", import.meta.url).pathname.replace(/\/$/, "");
export const PI = join(ROOT, "node_modules/.bin/pi");
export const EXT = join(ROOT, "extension/index.ts");
export const HAVE_PI = existsSync(PI);

export const PI_ARGS = [
  "--no-session", "--no-extensions", "-e", EXT, "--no-skills", "--no-prompt-templates", "--no-mcp",
  "--model", "anthropic/claude-sonnet-4-5",
];

export type Action =
  | { kind: "tool"; command: string; delayMs?: number }
  | { kind: "text"; text?: string; delayMs?: number }
  | { kind: "status"; status: number; headers?: Record<string, string>; body?: string }
  | { kind: "sse-error"; type?: string }
  | { kind: "reset" };

export interface Ctx {
  who: string;
  /** Last message is a tool result => the model should now finish. */
  final: boolean;
  /** 1-based count of requests seen from this agent. */
  nth: number;
  /** 1-based count of requests seen overall. */
  global: number;
  body: any;
}

export interface Entry {
  who: string;
  final: boolean;
  nth: number;
  t: number;
  apiKey: string | undefined;
}

export const defaultBehavior = (c: Ctx): Action =>
  c.final ? { kind: "text", text: "all done" } : { kind: "tool", command: "sleep 0.3; echo done" };

/** Shell command that runs a nested print-mode pi named `name` (inherits env => becomes a sub-agent). */
export const piCmd = (name: string) => `${PI} -p ${PI_ARGS.map((a) => JSON.stringify(a)).join(" ")} ${JSON.stringify("My name is " + name)} < /dev/null`;

export class Stack {
  log: Entry[] = [];
  current = 0;
  maxConcurrent = 0;
  behavior: (c: Ctx) => Action = defaultBehavior;
  upstream!: http.Server;
  upstreamPort = 0;
  daemon!: Daemon;
  daemonPort = 0;
  children = new Set<ChildProcess>();
  private counts = new Map<string, number>();
  private globalN = 0;

  static async start(opts: { daemon?: Record<string, unknown>; behavior?: Stack["behavior"] } = {}): Promise<Stack> {
    const s = new Stack();
    if (opts.behavior) s.behavior = opts.behavior;
    s.upstream = http.createServer((req, res) => void s.handle(req, res));
    await new Promise<void>((r) => s.upstream.listen(0, "127.0.0.1", r));
    s.upstreamPort = (s.upstream.address() as AddressInfo).port;
    s.daemon = createDaemon({
      config: resolveConfig({
        listen: { port: 0 },
        upstreams: { anthropic: `http://127.0.0.1:${s.upstreamPort}` },
        retry: { baseDelayMs: 5, maxDelayMs: 20, jitter: 0 },
        task: { implicitIdleMs: 0 },
        agent: { leaseMs: 30_000, pollMs: 5_000 },
        logLevel: "silent",
        ...opts.daemon,
      }),
      logger: nullLogger,
    });
    s.daemonPort = await s.daemon.listen();
    return s;
  }

  get url() {
    return `http://127.0.0.1:${this.daemonPort}`;
  }

  /** Request order as agent names, consecutive duplicates collapsed (= sequence of tasks). */
  tasks(): string[] {
    const out: string[] = [];
    for (const e of this.log) if (out[out.length - 1] !== e.who) out.push(e.who);
    return out;
  }
  requestsOf(who: string) {
    return this.log.filter((e) => e.who === who);
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse) {
    const chunks: Buffer[] = [];
    for await (const c of req) chunks.push(c as Buffer);
    let body: any = {};
    try {
      body = JSON.parse(Buffer.concat(chunks).toString() || "{}");
    } catch {}
    this.current++;
    this.maxConcurrent = Math.max(this.maxConcurrent, this.current);
    res.on("close", () => this.current--);
    const who = JSON.stringify(body.messages?.[0]?.content ?? "").match(/agent-[a-z0-9]+/)?.[0] ?? "?";
    const msgs: any[] = body.messages ?? [];
    const last = JSON.stringify(msgs[msgs.length - 1] ?? "");
    const final = last.includes("tool_result");
    const nth = (this.counts.get(who) ?? 0) + 1;
    this.counts.set(who, nth);
    const global = ++this.globalN;
    this.log.push({ who, final, nth, t: Date.now(), apiKey: req.headers["x-api-key"] as string | undefined });
    const action = this.behavior({ who, final, nth, global, body });
    const delay = "delayMs" in action ? (action.delayMs ?? 100) : 50;
    await new Promise((r) => setTimeout(r, delay));
    switch (action.kind) {
      case "status":
        res.writeHead(action.status, { "content-type": "application/json", ...action.headers });
        return void res.end(action.body ?? JSON.stringify({ type: "error", error: { type: "api_error", message: "boom" } }));
      case "reset":
        return void res.destroy();
      case "sse-error":
        res.writeHead(200, { "content-type": "text/event-stream" });
        return void res.end(`event: error\ndata: ${JSON.stringify({ type: "error", error: { type: action.type ?? "overloaded_error", message: "Overloaded" } })}\n\n`);
      default:
        return void sse(res, body.model, action);
    }
  }

  env(home: string, extra: Record<string, string> = {}): NodeJS.ProcessEnv {
    return {
      ...process.env,
      HOME: home,
      PI_CODING_AGENT_DIR: join(home, ".pi"),
      ANTHROPIC_API_KEY: "sk-fake",
      PISYNC_URL: this.url,
      PISYNC_NO_AUTOSTART: "1",
      ...extra,
    };
  }

  /** Print-mode pi. */
  runPi(name: string, o: { env?: Record<string, string>; args?: string[] } = {}): Promise<{ name: string; code: number | null; out: string; err: string }> {
    const home = mkdtempSync(join(tmpdir(), "pisync-e2e-"));
    const p = spawn(PI, ["-p", ...PI_ARGS, ...(o.args ?? []), `My name is ${name}`], {
      cwd: home,
      stdio: ["ignore", "pipe", "pipe"],
      env: this.env(home, { PISYNC_LABEL: name, ...o.env }),
    });
    this.children.add(p);
    return new Promise((resolve) => {
      let out = "";
      let err = "";
      p.stdout.on("data", (d) => (out += d));
      p.stderr.on("data", (d) => (err += d));
      p.on("close", (code) => {
        this.children.delete(p);
        resolve({ name, code, out: out.trim(), err: err.trim() });
      });
    });
  }

  /** Long-lived rpc-mode pi that can receive several prompts. */
  rpcPi(name: string, o: { env?: Record<string, string> } = {}): RpcPi {
    const home = mkdtempSync(join(tmpdir(), "pisync-e2e-"));
    const p = spawn(PI, ["--mode", "rpc", ...PI_ARGS], {
      cwd: home,
      stdio: ["pipe", "pipe", "pipe"],
      env: this.env(home, { PISYNC_LABEL: name, ...o.env }),
    });
    this.children.add(p);
    p.on("close", () => this.children.delete(p));
    return new RpcPi(name, p);
  }

  /** Wait until the daemon knows an agent labelled `name` with a started task. */
  async waitForTask(name: string, timeoutMs = 20_000): Promise<void> {
    await waitFor(() => this.daemon.scheduler.snapshot().agents.some((a) => a.label === name && a.state !== "idle"), timeoutMs, `task of ${name}`);
  }
  agentByLabel(name: string) {
    return this.daemon.scheduler.snapshot().agents.find((a) => a.label === name);
  }

  async stop() {
    for (const c of this.children) c.kill("SIGKILL");
    await this.daemon.close(100);
    this.upstream.closeAllConnections();
    this.upstream.close();
  }
}

export class RpcPi {
  private buf = "";
  private waiters: Array<(e: any) => void> = [];
  events: any[] = [];
  private nextId = 1;
  readonly name: string;
  readonly proc: ChildProcess;
  constructor(name: string, proc: ChildProcess) {
    this.name = name;
    this.proc = proc;
    proc.stdout!.on("data", (d) => {
      this.buf += d;
      let i;
      while ((i = this.buf.indexOf("\n")) >= 0) {
        const line = this.buf.slice(0, i);
        this.buf = this.buf.slice(i + 1);
        try {
          const ev = JSON.parse(line);
          this.events.push(ev);
          for (const w of [...this.waiters]) w(ev);
        } catch {}
      }
    });
    proc.stderr!.on("data", () => {});
  }
  private waitEvent(pred: (e: any) => boolean, timeoutMs: number): Promise<any> {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`${this.name}: timeout waiting for event`)), timeoutMs);
      const w = (e: any) => {
        if (pred(e)) {
          clearTimeout(t);
          this.waiters = this.waiters.filter((x) => x !== w);
          resolve(e);
        }
      };
      this.waiters.push(w);
    });
  }
  /** Send a prompt; resolves when the agent has fully settled. */
  async prompt(message: string, timeoutMs = 60_000): Promise<{ aborted: boolean }> {
    const settled = this.waitEvent((e) => e.type === "agent_settled", timeoutMs);
    this.proc.stdin!.write(JSON.stringify({ id: `p${this.nextId++}`, type: "prompt", message }) + "\n");
    return settled;
  }
  /** Fire a prompt without waiting. Returns the settle promise. */
  promptNoWait(message: string, timeoutMs = 60_000): Promise<{ aborted: boolean }> {
    return this.prompt(message, timeoutMs);
  }
  abort(): void {
    this.proc.stdin!.write(JSON.stringify({ id: `a${this.nextId++}`, type: "abort" }) + "\n");
  }
  async close(): Promise<void> {
    this.proc.stdin!.end();
    await new Promise<void>((r) => {
      const t = setTimeout(() => (this.proc.kill("SIGKILL"), r()), 5000);
      this.proc.on("close", () => (clearTimeout(t), r()));
    });
  }
}

function sse(res: http.ServerResponse, model: string, a: Extract<Action, { kind: "tool" | "text" }>) {
  const ev = (e: string, d: unknown) => `event: ${e}\ndata: ${JSON.stringify(d)}\n\n`;
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.write(ev("message_start", { type: "message_start", message: { id: "m", type: "message", role: "assistant", model, content: [], stop_reason: null, usage: { input_tokens: 5, output_tokens: 1 } } }));
  if (a.kind === "tool") {
    res.write(ev("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "tool_use", id: "toolu_" + Math.random().toString(36).slice(2, 10), name: "bash", input: {} } }));
    res.write(ev("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: JSON.stringify({ command: a.command }) } }));
  } else {
    res.write(ev("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }));
    res.write(ev("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: a.text ?? "all done" } }));
  }
  res.write(ev("content_block_stop", { type: "content_block_stop", index: 0 }));
  res.write(ev("message_delta", { type: "message_delta", delta: { stop_reason: a.kind === "tool" ? "tool_use" : "end_turn" }, usage: { output_tokens: 5 } }));
  res.end(ev("message_stop", { type: "message_stop" }));
}

export async function waitFor(cond: () => boolean, timeoutMs = 15_000, what = "condition"): Promise<void> {
  const t0 = Date.now();
  while (!cond()) {
    if (Date.now() - t0 > timeoutMs) throw new Error(`timeout waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

export function writeJson(dir: string, name: string, data: unknown): string {
  const p = join(dir, name);
  writeFileSync(p, JSON.stringify(data));
  return p;
}
