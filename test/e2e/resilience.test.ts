import { afterEach, describe, expect, it } from "vitest";
import { createDaemon } from "../../src/daemon.ts";
import { resolveConfig } from "../../src/config.ts";
import { nullLogger } from "../../src/log.ts";
import { HAVE_PI, Stack, piCmd, waitFor, type Ctx } from "./stack.ts";

let s: Stack | undefined;
afterEach(async () => {
  await s?.stop();
  s = undefined;
});

describe.skipIf(!HAVE_PI)("e2e: flaky plan - retries are invisible to pi", () => {
  it("25 consecutive 529s are retried and pi still succeeds on the 26th attempt", async () => {
    let fails = 0;
    s = await Stack.start({
      behavior: (c: Ctx) => {
        if (!c.final && fails < 25) {
          fails++;
          return { kind: "status", status: 529 };
        }
        return c.final ? { kind: "text" } : { kind: "tool", command: "true" };
      },
    });
    const r = await s.runPi("agent-flaky");
    expect(r.code).toBe(0);
    expect(r.out).toContain("all done");
    expect(s.log.filter((e) => !e.final)).toHaveLength(26);
    expect(s.daemon.metrics.retries).toBe(25);
  }, 90_000);

  it("429 with Retry-After and 500/502/503 are retried; mixed failures", async () => {
    const seq = [429, 500, 502, 503, 504, 408];
    let i = 0;
    s = await Stack.start({
      behavior: (c: Ctx) => {
        if (i < seq.length) return { kind: "status", status: seq[i++]!, ...(i === 1 ? { headers: { "retry-after": "0" } } : {}) };
        return c.final ? { kind: "text" } : { kind: "tool", command: "true" };
      },
    });
    const r = await s.runPi("agent-mixed");
    expect(r.code).toBe(0);
    expect(s.daemon.metrics.retries).toBe(6);
  }, 90_000);

  it("dropped connections are retried", async () => {
    let n = 0;
    s = await Stack.start({
      behavior: (c: Ctx) => (n++ < 3 ? { kind: "reset" } : c.final ? { kind: "text" } : { kind: "tool", command: "true" }),
    });
    const r = await s.runPi("agent-reset");
    expect(r.code).toBe(0);
  }, 90_000);

  it("an overloaded_error inside an HTTP-200 stream is retried", async () => {
    let n = 0;
    s = await Stack.start({
      behavior: (c: Ctx) => (n++ < 4 ? { kind: "sse-error" } : c.final ? { kind: "text" } : { kind: "tool", command: "true" }),
    });
    const r = await s.runPi("agent-sse");
    expect(r.code).toBe(0);
    expect(r.out).toContain("all done");
    expect(s.daemon.metrics.retries).toBeGreaterThanOrEqual(4);
  }, 90_000);

  it("while one agent's request is being retried nobody else gets in between", async () => {
    let fails = 0;
    s = await Stack.start({
      behavior: (c: Ctx) => {
        if (c.who === "agent-sick" && !c.final && fails < 6) {
          fails++;
          return { kind: "status", status: 503 };
        }
        return c.final ? { kind: "text" } : { kind: "tool", command: "sleep 0.2" };
      },
    });
    const a = s.runPi("agent-sick");
    await s.waitForTask("agent-sick");
    const b = s.runPi("agent-well");
    await s.waitForTask("agent-well");
    const rs = await Promise.all([a, b]);
    expect(rs.map((r) => r.code)).toEqual([0, 0]);
    expect(s.tasks()).toEqual(["agent-sick", "agent-well"]);
  }, 90_000);

  it("a permanent 400 is not retried, pi reports it, and the slot is released for the next agent", async () => {
    s = await Stack.start({
      behavior: (c: Ctx) =>
        c.who === "agent-bad"
          ? { kind: "status", status: 400, body: JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: "nope" } }) }
          : c.final
            ? { kind: "text" }
            : { kind: "tool", command: "true" },
    });
    const bad = s.runPi("agent-bad");
    await s.waitForTask("agent-bad");
    const good = s.runPi("agent-good");
    const [rb, rg] = await Promise.all([bad, good]);
    expect(rb.code).not.toBe(0);
    expect(s.requestsOf("agent-bad")).toHaveLength(1);
    expect(rg.code).toBe(0);
  }, 90_000);

  it("exhausted retries surface the upstream error to pi (after maxRetries), then the queue moves on", async () => {
    s = await Stack.start({
      daemon: { retry: { maxRetries: 3 } },
      behavior: (c: Ctx) => (c.who === "agent-doomed" ? { kind: "status", status: 503 } : c.final ? { kind: "text" } : { kind: "tool", command: "true" }),
    });
    const a = s.runPi("agent-doomed", { args: [] });
    await s.waitForTask("agent-doomed");
    const b = s.runPi("agent-fine");
    const [ra, rb] = await Promise.all([a, b]);
    expect(rb.code).toBe(0);
    expect(ra.code).not.toBe(0);
    expect(s.requestsOf("agent-doomed").length).toBeGreaterThanOrEqual(4); // 1 + 3 retries (pi may retry again on top)
  }, 180_000);
});

describe.skipIf(!HAVE_PI)("e2e: crashes, aborts and limits", () => {
  it("kill -9 of the active agent: its lease expires and the next agent proceeds", async () => {
    s = await Stack.start({
      daemon: { agent: { leaseMs: 1500, pollMs: 500 } },
      behavior: (c: Ctx) => (c.final ? { kind: "text" } : { kind: "tool", command: c.who === "agent-victim" ? "sleep 60" : "true" }),
    });
    const victim = s.rpcPi("agent-victim");
    const vp = victim.prompt("My name is agent-victim").catch(() => "killed");
    await waitFor(() => s!.requestsOf("agent-victim").length === 1, 20_000, "victim request");
    const next = s.runPi("agent-next");
    await s.waitForTask("agent-next");
    const t0 = Date.now();
    victim.proc.kill("SIGKILL");
    const r = await next;
    expect(r.code).toBe(0);
    expect(Date.now() - t0).toBeLessThan(15_000);
    void vp;
  }, 90_000);

  it("a queued agent that dies does not block the ones behind it forever", async () => {
    s = await Stack.start({
      daemon: { agent: { leaseMs: 1500, pollMs: 500 } },
      behavior: (c: Ctx) => (c.final ? { kind: "text" } : { kind: "tool", command: c.who === "agent-first" ? "sleep 2.5" : "true" }),
    });
    const first = s.runPi("agent-first");
    await s.waitForTask("agent-first");
    const doomed = s.rpcPi("agent-doomed");
    const dp = doomed.prompt("My name is agent-doomed").catch(() => "killed");
    await s.waitForTask("agent-doomed");
    const last = s.runPi("agent-last");
    await s.waitForTask("agent-last");
    doomed.proc.kill("SIGKILL");
    const [rf, rl] = await Promise.all([first, last]);
    expect([rf.code, rl.code]).toEqual([0, 0]);
    expect(s.requestsOf("agent-doomed")).toHaveLength(0);
    void dp;
  }, 90_000);

  it("aborting a running agent (Esc / abort) ends its task and hands over the slot", async () => {
    s = await Stack.start({
      behavior: (c: Ctx) => (c.final ? { kind: "text" } : { kind: "tool", command: c.who === "agent-abort" ? "sleep 60" : "true" }),
    });
    const a = s.rpcPi("agent-abort");
    const settled = a.prompt("My name is agent-abort");
    await waitFor(() => s!.requestsOf("agent-abort").length === 1, 20_000, "first request");
    const b = s.runPi("agent-after");
    await s.waitForTask("agent-after");
    a.abort();
    const res = await settled;
    expect(res.aborted).toBe(true);
    expect((await b).code).toBe(0);
    await a.close();
  }, 90_000);

  it("graceful exit of the active agent (SIGTERM) frees the slot immediately, no lease wait", async () => {
    s = await Stack.start({
      behavior: (c: Ctx) => (c.final ? { kind: "text" } : { kind: "tool", command: c.who === "agent-term" ? "sleep 60" : "true" }),
    });
    const a = s.rpcPi("agent-term");
    void a.prompt("My name is agent-term").catch(() => {});
    await waitFor(() => s!.requestsOf("agent-term").length === 1, 20_000, "request");
    const b = s.runPi("agent-b");
    await s.waitForTask("agent-b");
    a.proc.kill("SIGTERM");
    expect((await b).code).toBe(0);
  }, 90_000);

  it("hard task limit terminates a runaway task: pi is aborted mid tool call, the next agent proceeds, the hog is NOT re-queued", async () => {
    s = await Stack.start({
      daemon: { task: { maxDurationMs: 1500 }, agent: { leaseMs: 1500, pollMs: 500 } },
      behavior: (c: Ctx) => (c.final ? { kind: "text" } : { kind: "tool", command: c.who === "agent-hog" ? "sleep 60" : "true" }),
    });
    const t0 = Date.now();
    const hog = s.runPi("agent-hog");
    await s.waitForTask("agent-hog");
    const other = s.runPi("agent-other");
    await s.waitForTask("agent-other");
    const ro = await other;
    expect(ro.code).toBe(0);
    await hog; // the 60s sleep was cut short by the abort
    expect(Date.now() - t0).toBeLessThan(30_000);
    expect(s.requestsOf("agent-hog")).toHaveLength(1); // never got another turn
    expect(s.tasks()).toEqual(["agent-hog", "agent-other"]);
    expect(s.daemon.scheduler.snapshot().agents).toHaveLength(0);
  }, 90_000);

  it("time spent waiting in the queue does not count and never expires a task", async () => {
    s = await Stack.start({
      daemon: { task: { maxDurationMs: 4000 } },
      behavior: (c: Ctx) => (c.final ? { kind: "text" } : { kind: "tool", command: c.who === "agent-first" ? "sleep 3" : "true" }),
    });
    const first = s.runPi("agent-first");
    await s.waitForTask("agent-first");
    const second = s.runPi("agent-second");
    await s.waitForTask("agent-second");
    const third = s.runPi("agent-third");
    const rs = await Promise.all([first, second, third]);
    expect(rs.map((r) => r.code)).toEqual([0, 0, 0]); // third waited > 4s in total, ran fine
  }, 90_000);

  it("a master's time suspended for its sub-agents does not count against its limit", async () => {
    const behave = (c: Ctx) =>
      c.final ? { kind: "text" as const } : { kind: "tool" as const, command: c.who === "agent-master" ? piCmd("agent-child") : "sleep 3" };
    s = await Stack.start({ behavior: behave, daemon: { task: { maxDurationMs: 4000 } } });
    const r = await s.runPi("agent-master"); // child runs 3s+ while master is suspended
    expect(r.code).toBe(0);
    expect(s.tasks()).toEqual(["agent-master", "agent-child", "agent-master"]);
  }, 90_000);

  it("daemon restart while an agent is queued and another is running: everyone still finishes", async () => {
    s = await Stack.start({
      agent: undefined,
      behavior: (c: Ctx) => (c.final ? { kind: "text" } : { kind: "tool", command: c.who === "agent-run" ? "sleep 3" : "true" }),
    } as never);
    const run = s.runPi("agent-run");
    await s.waitForTask("agent-run");
    const wait = s.runPi("agent-wait");
    await s.waitForTask("agent-wait");
    // restart the daemon on the same port
    const port = s.daemonPort;
    await s.daemon.close(50);
    await new Promise((r) => setTimeout(r, 800));
    s.daemon = createDaemon({
      config: resolveConfig({
        listen: { port },
        upstreams: { anthropic: `http://127.0.0.1:${s.upstreamPort}` },
        retry: { baseDelayMs: 5, maxDelayMs: 20, jitter: 0 },
        task: { implicitIdleMs: 0 },
        agent: { leaseMs: 30_000, pollMs: 5_000 },
        logLevel: "silent",
      }),
      logger: nullLogger,
    });
    await s.daemon.listen();
    const rs = await Promise.all([run, wait]);
    expect(rs.map((r) => r.code)).toEqual([0, 0]);
    expect(s.maxConcurrent).toBe(1);
  }, 120_000);
});
