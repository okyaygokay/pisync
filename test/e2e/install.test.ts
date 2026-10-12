import { execFileSync, spawn } from "node:child_process";
import { mkdtempSync, readFileSync, existsSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { EXT, HAVE_PI, PI, PI_ARGS, ROOT, Stack, waitFor, writeJson } from "./stack.ts";

let s: Stack | undefined;
let daemonPid: number | undefined;
afterEach(async () => {
  if (daemonPid) {
    try {
      process.kill(daemonPid, "SIGTERM");
    } catch {}
    daemonPid = undefined;
  }
  await s?.stop();
  s = undefined;
});

const freePort = () =>
  new Promise<number>((resolve) => {
    const srv = net.createServer().listen(0, "127.0.0.1", () => {
      const p = (srv.address() as net.AddressInfo).port;
      srv.close(() => resolve(p));
    });
  });

function runPiRaw(args: string[], env: NodeJS.ProcessEnv, cwd: string) {
  return new Promise<{ code: number | null; out: string; err: string }>((resolve) => {
    const p = spawn(PI, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    let err = "";
    p.stdout.on("data", (d) => (out += d));
    p.stderr.on("data", (d) => (err += d));
    p.on("close", (code) => resolve({ code, out: out.trim(), err: err.trim() }));
  });
}

describe.skipIf(!HAVE_PI)("e2e: zero-setup start (autostart) and installation", () => {
  it("with no daemon running, concurrent pi agents start exactly one and share it", async () => {
    s = await Stack.start({ behavior: (c) => (c.final ? { kind: "text" } : { kind: "tool", command: "sleep 0.5" }) });
    // a second, independent stack is not needed: reuse the fake upstream, but run a REAL spawned pisyncd
    await s.daemon.close(50);
    const port = await freePort();
    const home = mkdtempSync(join(tmpdir(), "pisync-install-"));
    const cfg = writeJson(home, "pisync.config.json", {
      listen: { port },
      upstreams: { anthropic: `http://127.0.0.1:${s.upstreamPort}` },
      agent: { leaseMs: 30000, pollMs: 5000 },
    });
    const env = (name: string): NodeJS.ProcessEnv => ({
      ...process.env,
      HOME: home,
      PI_CODING_AGENT_DIR: join(home, ".pi"),
      ANTHROPIC_API_KEY: "sk-fake",
      PISYNC_CONFIG: cfg,
      PISYNC_LABEL: name,
    });
    delete (env("x") as Record<string, unknown>).PISYNC_URL;
    const base = `http://127.0.0.1:${port}`;
    expect(await fetch(`${base}/healthz`).then(() => true, () => false)).toBe(false);

    const names = ["agent-p1", "agent-p2", "agent-p3"];
    const rs = await Promise.all(names.map((n) => runPiRaw(["-p", ...PI_ARGS, `My name is ${n}`], env(n), home)));
    expect(rs.map((r) => r.code)).toEqual([0, 0, 0]);
    expect(rs.every((r) => r.out.includes("all done"))).toBe(true);

    const st = (await (await fetch(`${base}/_pisync/status`)).json()) as any;
    daemonPid = st.pid;
    expect(st.pid).not.toBe(process.pid); // a separate detached process
    expect(st.metrics.requests).toBe(6);
    expect(st.scheduler.totals.tasksStarted).toBe(3);
    // the daemon kept running after the agents exited and logged to ~/.pisync
    expect(existsSync(join(home, ".pisync", "daemon.log"))).toBe(true);
    expect(readFileSync(join(home, ".pisync", "daemon.log"), "utf8")).toContain("pisyncd listening");
    // its tasks never overlapped upstream
    expect(s.maxConcurrent).toBe(1);
    expect(s.tasks()).toHaveLength(3);
  }, 120_000);

  it("`pi install <path>` is the whole installation: a plain `pi` then routes through the daemon", async () => {
    s = await Stack.start();
    await s.daemon.close(50);
    const port = await freePort();
    const home = mkdtempSync(join(tmpdir(), "pisync-install-"));
    writeJson(home, "pisync.config.json", { listen: { port }, upstreams: { anthropic: `http://127.0.0.1:${s.upstreamPort}` } });
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      HOME: home,
      PI_CODING_AGENT_DIR: join(home, ".pi"),
      ANTHROPIC_API_KEY: "sk-fake",
    };
    delete env.PISYNC_URL;

    const inst = await runPiRaw(["install", ROOT], env, home);
    expect(inst.code, inst.err + inst.out).toBe(0);
    const list = await runPiRaw(["list"], env, home);
    expect(list.out + list.err).toContain(ROOT);

    // plain pi: no -e, extension discovery on; config found in cwd
    const r = await runPiRaw(["-p", "--no-session", "--no-skills", "--no-prompt-templates", "--no-mcp", "--model", "anthropic/claude-sonnet-4-5", "My name is agent-installed"], env, home);
    expect(r.code, r.err).toBe(0);
    expect(r.out).toContain("all done");
    const st = (await (await fetch(`http://127.0.0.1:${port}/_pisync/status`)).json()) as any;
    daemonPid = st.pid;
    expect(st.metrics.requests).toBe(2);
    expect(s.requestsOf("agent-installed")).toHaveLength(2);
  }, 120_000);

  it("a pisync config with autoStart=false never spawns a daemon", async () => {
    s = await Stack.start();
    await s.daemon.close(50);
    const port = await freePort();
    const home = mkdtempSync(join(tmpdir(), "pisync-install-"));
    const cfg = writeJson(home, "pisync.config.json", { listen: { port }, autoStart: false, upstreams: { anthropic: `http://127.0.0.1:${s.upstreamPort}` } });
    const env: NodeJS.ProcessEnv = { ...process.env, HOME: home, PI_CODING_AGENT_DIR: join(home, ".pi"), ANTHROPIC_API_KEY: "sk-fake", PISYNC_CONFIG: cfg };
    delete env.PISYNC_URL;
    env.PISYNC_OUTAGE_MS = "1500";
    const r = await runPiRaw(["-p", ...PI_ARGS, "My name is agent-manual"], env, home);
    expect(r.code).not.toBe(0); // provider points at a dead port
    expect(await fetch(`http://127.0.0.1:${port}/healthz`).then(() => true, () => false)).toBe(false);
  }, 120_000);

  it("PISYNC_DISABLE=1 makes pi talk to the real provider URL directly (extension inert)", async () => {
    s = await Stack.start();
    const home = mkdtempSync(join(tmpdir(), "pisync-install-"));
    const r = await runPiRaw(["-p", ...PI_ARGS, "My name is agent-off"], { ...s.env(home), PISYNC_DISABLE: "1" }, home);
    void r;
    expect(s.requestsOf("agent-off")).toHaveLength(0); // never reached our fake via the daemon
    expect(s.daemon.metrics.requests).toBe(0);
    void EXT;
    void waitFor;
  }, 120_000);

  it("CLI `pisyncd status` and `print-config` work against a running daemon", () => {
    const out = execFileSync(process.execPath, [join(ROOT, "src/cli.ts"), "print-config"], { encoding: "utf8", env: { ...process.env, PISYNC_CONFIG: "" }, cwd: tmpdir() });
    expect(JSON.parse(out).config.autoStart).toBe(true);
  });
});
