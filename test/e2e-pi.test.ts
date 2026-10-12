/**
 * End-to-end with REAL pi processes (the pi CLI from devDependencies) talking
 * to the daemon, which talks to a fake Anthropic-style upstream. Skipped when
 * the pi binary is not installed.
 */
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync } from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { resolveConfig } from "../src/config.ts";
import { createDaemon, type Daemon } from "../src/daemon.ts";
import { nullLogger } from "../src/log.ts";

const ROOT = new URL("..", import.meta.url).pathname.replace(/\/$/, "");
const PI = join(ROOT, "node_modules/.bin/pi");
const EXT = join(ROOT, "extension/index.ts");
const piArgs = (prompt: string) => [
  "-p", "--no-session", "--no-extensions", "-e", EXT, "--no-skills", "--no-prompt-templates", "--no-mcp",
  "--model", "anthropic/claude-sonnet-4-5", prompt,
];

let log: string[] = [];
let cur = 0;
let maxConcurrent = 0;
let upstream: http.Server;
let daemon: Daemon;
let daemonPort = 0;

const sseResponse = (res: http.ServerResponse, events: Array<[string, unknown]>) => {
  res.writeHead(200, { "content-type": "text/event-stream" });
  for (const [e, d] of events) res.write(`event: ${e}\ndata: ${JSON.stringify(d)}\n\n`);
  res.end();
};

beforeAll(async () => {
  if (!existsSync(PI)) return;
  upstream = http.createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const c of req) chunks.push(c as Buffer);
    const body = JSON.parse(Buffer.concat(chunks).toString() || "{}");
    cur++;
    maxConcurrent = Math.max(maxConcurrent, cur);
    res.on("close", () => cur--);
    const who = JSON.stringify(body.messages?.[0]?.content ?? "").match(/agent-[a-z0-9]+/)?.[0] ?? "?";
    const final = JSON.stringify(body.messages).includes("tool_result");
    log.push(`${who}:${final ? "final" : "tool"}`);
    await new Promise((r) => setTimeout(r, 150));
    const start: [string, unknown] = ["message_start", { type: "message_start", message: { id: "m", type: "message", role: "assistant", model: body.model, content: [], stop_reason: null, usage: { input_tokens: 5, output_tokens: 1 } } }];
    if (!final) {
      const command =
        who === "agent-master"
          ? `${PI} ${piArgs("My name is agent-child").map((a) => JSON.stringify(a)).join(" ")} < /dev/null`
          : "sleep 0.5; echo done";
      sseResponse(res, [
        start,
        ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "tool_use", id: "toolu_1", name: "bash", input: {} } }],
        ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "input_json_delta", partial_json: JSON.stringify({ command }) } }],
        ["content_block_stop", { type: "content_block_stop", index: 0 }],
        ["message_delta", { type: "message_delta", delta: { stop_reason: "tool_use" }, usage: { output_tokens: 5 } }],
        ["message_stop", { type: "message_stop" }],
      ]);
    } else {
      sseResponse(res, [
        start,
        ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
        ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "all done" } }],
        ["content_block_stop", { type: "content_block_stop", index: 0 }],
        ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 5 } }],
        ["message_stop", { type: "message_stop" }],
      ]);
    }
  });
  await new Promise<void>((r) => upstream.listen(0, "127.0.0.1", r));
  const upPort = (upstream.address() as AddressInfo).port;
  daemon = createDaemon({
    config: resolveConfig({
      listen: { port: 0 },
      upstreams: { anthropic: `http://127.0.0.1:${upPort}` },
      retry: { baseDelayMs: 20, maxDelayMs: 50 },
      logLevel: "silent",
    }),
    logger: nullLogger,
  });
  daemonPort = await daemon.listen();
});

afterAll(async () => {
  if (!existsSync(PI)) return;
  await daemon.close(100);
  upstream.closeAllConnections();
  upstream.close();
});

function runPi(name: string, delayMs = 0) {
  return new Promise<{ name: string; code: number | null; out: string }>((resolve) => {
    setTimeout(() => {
      const home = mkdtempSync(join(tmpdir(), "pisync-pi-"));
      const p = spawn(PI, piArgs(`My name is ${name}`), {
        cwd: home,
        stdio: ["ignore", "pipe", "pipe"],
        env: { ...process.env, HOME: home, PI_CODING_AGENT_DIR: join(home, ".pi"), ANTHROPIC_API_KEY: "sk-fake", PISYNC_URL: `http://127.0.0.1:${daemonPort}` },
      });
      let out = "";
      p.stdout.on("data", (d) => (out += d));
      p.on("close", (code) => resolve({ name, code, out: out.trim() }));
    }, delayMs);
  });
}

describe.skipIf(!existsSync(PI))("real pi processes", () => {
  it("three concurrent pi agents run one TASK at a time, in arrival order", async () => {
    log = [];
    maxConcurrent = 0;
    const rs = await Promise.all([runPi("agent-aaa"), runPi("agent-bbb", 500), runPi("agent-ccc", 1000)]);
    expect(rs.map((r) => r.code)).toEqual([0, 0, 0]);
    expect(rs.every((r) => r.out.includes("all done"))).toBe(true);
    // each agent's two requests (tool call, final) are contiguous: no interleaving between tasks
    expect(log).toEqual(["agent-aaa:tool", "agent-aaa:final", "agent-bbb:tool", "agent-bbb:final", "agent-ccc:tool", "agent-ccc:final"]);
    expect(maxConcurrent).toBe(1);
  }, 120_000);

  it("a master that spawns a sub-agent via bash and waits does not deadlock", async () => {
    log = [];
    maxConcurrent = 0;
    const events: Array<{ type: string; agent: string; parent?: string | null }> = [];
    const off = daemon.scheduler.onEvent((e) => events.push(e as never));
    const rs = await Promise.all([runPi("agent-master"), runPi("agent-other", 600)]);
    off();
    expect(rs.map((r) => r.code)).toEqual([0, 0]);
    expect(maxConcurrent).toBe(1);
    // master yielded while its child was queued, then resumed after the child finished
    expect(log[0]).toBe("agent-master:tool");
    expect(log.indexOf("agent-child:final")).toBeLessThan(log.indexOf("agent-master:final"));
    expect(log).toHaveLength(6);
    // the child was linked to the master automatically (env inheritance through the bash tool)
    const parents = events.filter((e) => e.type === "registered");
    const master = parents.find((e) => e.parent === null)!;
    expect(parents.filter((e) => e.parent !== null).map((e) => e.parent)).toContain(master.agent);
    expect(events.some((e) => e.type === "suspended")).toBe(true);
    expect(daemon.scheduler.snapshot().agents).toHaveLength(0);
  }, 120_000);
});
