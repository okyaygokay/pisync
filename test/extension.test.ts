import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PisyncClient } from "../src/client.ts";
import pisync from "../extension/index.ts";
import { control, fakeUpstream, sleep, startDaemon, statusOf, type FakeUpstream } from "./harness.ts";
import { probe } from "./helpers.ts";

let up: FakeUpstream;
let ctx: Awaited<ReturnType<typeof startDaemon>>;
const savedEnv = { ...process.env };

beforeEach(async () => {
  up = await fakeUpstream();
  ctx = await startDaemon(up);
  process.env.PISYNC_URL = ctx.base;
  delete process.env.PISYNC_PARENT_ID;
  delete process.env.PISYNC_SELF_ID;
  delete process.env.PISYNC_DISABLE;
  process.env.PISYNC_NO_AUTOSTART = "1";
});
afterEach(async () => {
  await ctx.daemon.close(100);
  await up.close();
  process.env = { ...savedEnv };
});

interface MockPi {
  handlers: Map<string, Array<(e: any, c: any) => any>>;
  providers: Array<{ name: string; config: any }>;
  api: any;
  fire(name: string, ev?: any, c?: any): Promise<void>;
  ui: { setStatus: ReturnType<typeof vi.fn>; notify: ReturnType<typeof vi.fn> };
  abort: ReturnType<typeof vi.fn>;
}

function mockPi(): MockPi {
  const handlers = new Map<string, Array<(e: any, c: any) => any>>();
  const providers: MockPi["providers"] = [];
  const ui = { setStatus: vi.fn(), notify: vi.fn() };
  const abort = vi.fn();
  const api = {
    on: (n: string, h: (e: any, c: any) => any) => void (handlers.get(n) ?? handlers.set(n, []).get(n)!).push(h),
    registerProvider: (name: string, config: any) => void providers.push({ name, config }),
  };
  return {
    handlers,
    providers,
    api,
    ui,
    abort,
    async fire(name, ev = {}, c = { ui, signal: undefined, abort }) {
      for (const h of handlers.get(name) ?? []) await h(ev, c);
    },
  };
}

/** Each real pi is its own process; only a child inherits PISYNC_PARENT_ID. */
async function newAgent(opts: { child?: boolean } = {}) {
  if (!opts.child) delete process.env.PISYNC_PARENT_ID;
  const pi = mockPi();
  pisync(pi.api);
  await pi.fire("session_start");
  return pi;
}

describe("extension wiring", () => {
  it("routes every configured upstream through the daemon with the agent id in the path", () => {
    const pi = mockPi();
    pisync(pi.api);
    const names = pi.providers.map((p) => p.name).sort();
    expect(names).toEqual(["anthropic", "openai", "openai-codex"]);
    for (const p of pi.providers) {
      expect(p.config.baseUrl).toMatch(new RegExp(`^${ctx.base}/a/pi-\\d+-[0-9a-f]{8}/${p.name}$`));
    }
    // no models / apiKey supplied => built-in models are preserved
    expect(pi.providers.every((p) => Object.keys(p.config).join() === "baseUrl")).toBe(true);
  });

  it("is inert when PISYNC_DISABLE is set", () => {
    process.env.PISYNC_DISABLE = "1";
    const pi = mockPi();
    pisync(pi.api);
    expect(pi.providers).toHaveLength(0);
    expect(pi.handlers.size).toBe(0);
  });

  it("registers with the daemon on session_start and unregisters on shutdown", async () => {
    const pi = await newAgent();
    let st = await statusOf(ctx.base);
    expect(st.scheduler.agents).toHaveLength(1);
    await pi.fire("session_shutdown", { reason: "quit" });
    st = await statusOf(ctx.base);
    expect(st.scheduler.agents).toHaveLength(0);
  });

  it("warns instead of crashing when the daemon is down", async () => {
    process.env.PISYNC_URL = "http://127.0.0.1:1";
    const pi = mockPi();
    pisync(pi.api);
    await pi.fire("session_start");
    expect(pi.ui.notify).toHaveBeenCalledWith(expect.stringContaining("daemon unreachable"), "warning");
  });
});

describe("task-level queueing across extension instances", () => {
  it("a, b, c: a runs, b and c wait; a's TASK ending (not its messages) hands over to b", async () => {
    const a = await newAgent();
    const b = await newAgent();
    const c = await newAgent();

    await a.fire("before_agent_start");
    const bStart = probe(b.fire("before_agent_start"));
    await sleep(20);
    const cStart = probe(c.fire("before_agent_start"));
    await sleep(20);
    expect(bStart.settled).toBe(false);
    expect(cStart.settled).toBe(false);

    // a does several turns (messages) - still a's task
    for (let i = 0; i < 3; i++) await a.fire("turn_start");
    expect(bStart.settled).toBe(false);

    await a.fire("agent_settled"); // TASK done
    await bStart.promise;
    expect(cStart.settled).toBe(false);

    // a receives another message while b runs: queued behind c
    const a2 = probe(a.fire("before_agent_start"));
    await sleep(20);
    await b.fire("agent_settled");
    await cStart.promise;
    expect(a2.settled).toBe(false);
    await c.fire("agent_settled");
    await a2.promise;
  });

  it("shows queue position in the status bar while waiting", async () => {
    const a = await newAgent();
    const b = await newAgent();
    await a.fire("before_agent_start");
    const bStart = probe(b.fire("before_agent_start"));
    await sleep(350); // > pollMs(200) so at least one "waiting" poll returned
    expect(b.ui.setStatus).toHaveBeenCalledWith("pisync", expect.stringMatching(/waiting \(#1 of 1\)/));
    await a.fire("agent_settled");
    await bStart.promise;
    expect(b.ui.setStatus).toHaveBeenLastCalledWith("pisync", undefined);
  });

  it("session_shutdown frees the slot for the next agent", async () => {
    const a = await newAgent();
    const b = await newAgent();
    await a.fire("before_agent_start");
    const bStart = probe(b.fire("before_agent_start"));
    await sleep(20);
    await a.fire("session_shutdown", { reason: "quit" });
    await bStart.promise;
  });
});

describe("hard task limit", () => {
  it("aborts the run (even mid tool call) when the daemon terminates the task, once", async () => {
    await ctx.daemon.close(50);
    ctx = await startDaemon(up, { task: { maxDurationMs: 100 }, agent: { leaseMs: 900, pollMs: 200 } });
    process.env.PISYNC_URL = ctx.base;
    const a = await newAgent();
    await a.fire("before_agent_start"); // granted; now a long tool call runs...
    await sleep(1800); // heartbeats every 500ms notice the expiry
    expect(a.abort).toHaveBeenCalledTimes(1);
    expect(a.ui.notify).toHaveBeenCalledWith(expect.stringContaining("hard time limit"), "error");
    // pi settles after the abort -> task reported finished -> flag cleared, a new task is allowed
    await a.fire("agent_settled");
    const again = probe(a.fire("before_agent_start"));
    await again.promise;
  });

  it("a refused acquire (task expired) aborts the run instead of continuing", async () => {
    await ctx.daemon.close(50);
    ctx = await startDaemon(up, { task: { maxDurationMs: 60 } });
    process.env.PISYNC_URL = ctx.base;
    const a = await newAgent();
    await a.fire("before_agent_start");
    await sleep(150);
    await expect(a.fire("turn_start")).rejects.toThrow();
    expect(a.abort).toHaveBeenCalled();
  });
});

describe("master / sub-agent discovery through the environment", () => {
  it("a child process env var links it to the master automatically", async () => {
    const master = await newAgent(); // sets PISYNC_PARENT_ID for processes it spawns
    const masterId = process.env.PISYNC_PARENT_ID!;
    expect(masterId).toMatch(/^pi-/);
    const child = await newAgent({ child: true }); // "spawned" by master: inherits env
    const st = await statusOf(ctx.base);
    const rec = st.scheduler.agents.find((x: any) => x.parent === masterId);
    expect(rec).toBeDefined();

    // master is working, then waits on the child; child needs the slot
    await master.fire("before_agent_start");
    await child.fire("before_agent_start"); // must NOT deadlock
    expect((await statusOf(ctx.base)).scheduler.active).toBe(rec.id);
    await child.fire("agent_settled");
    // master resumes at its next turn and goes first
    await master.fire("turn_start");
    expect((await statusOf(ctx.base)).scheduler.active).toBe(masterId);
  });

  it("PISYNC_SELF_ID pins a stable id and is not inherited", async () => {
    process.env.PISYNC_SELF_ID = "master-1";
    const m = mockPi();
    pisync(m.api);
    expect(m.providers[0]!.config.baseUrl).toContain("/a/master-1/");
    expect(process.env.PISYNC_SELF_ID).toBeUndefined();
    await m.fire("session_start");
    expect(process.env.PISYNC_PARENT_ID).toBe("master-1");
    await m.fire("session_shutdown", {});
    expect(process.env.PISYNC_PARENT_ID).toBeUndefined();
  });

  it("an orchestrator can declare parent via PISYNC_PARENT_ID", async () => {
    process.env.PISYNC_SELF_ID = "boss";
    const boss = mockPi();
    pisync(boss.api);
    await boss.fire("session_start");
    delete process.env.PISYNC_PARENT_ID;
    process.env.PISYNC_PARENT_ID = "boss";
    process.env.PISYNC_SELF_ID = "worker-1";
    const w = mockPi();
    pisync(w.api);
    await w.fire("session_start");
    const st = await statusOf(ctx.base);
    expect(st.scheduler.agents.find((a: any) => a.id === "worker-1").parent).toBe("boss");
  });
});

describe("client", () => {
  it("rides out a daemon outage inside acquire() and keeps polling", async () => {
    let calls = 0;
    const flaky: typeof fetch = (input, init) => {
      if (String(input).endsWith("/acquire") && ++calls <= 2) return Promise.reject(new TypeError("fetch failed"));
      return fetch(input, init);
    };
    const errors: Error[] = [];
    const c = new PisyncClient({ baseUrl: ctx.base, agentId: "flaky", fetch: flaky, retryDelayMs: 5, onError: (e) => errors.push(e) });
    await c.acquire();
    expect(errors).toHaveLength(2);
    await c.stop();
  });

  it("stop() aborts a pending acquire", async () => {
    await control(ctx.base, "acquire", { agentId: "holder" });
    const c = new PisyncClient({ baseUrl: ctx.base, agentId: "waiter" });
    const p = probe(c.acquire());
    await sleep(30);
    await c.stop();
    await sleep(30);
    expect(p.settled).toBe(true);
    expect((p.error as any).code).toBe("aborted");
  });

  it("heartbeats re-register after the daemon forgot the agent", async () => {
    const c = new PisyncClient({ baseUrl: ctx.base, agentId: "hb" });
    await c.start();
    ctx.daemon.scheduler.unregisterAgent("hb"); // simulate daemon restart
    expect(ctx.daemon.scheduler.hasAgent("hb")).toBe(false);
    await control(ctx.base, "acquire", { agentId: "hb" }); // self-heal path also works
    expect(ctx.daemon.scheduler.hasAgent("hb")).toBe(true);
    await c.stop();
  });

  it("treats a shutting-down daemon (503 shutdown) as an outage, not a failure", async () => {
    let n = 0;
    const f: typeof fetch = (input, init) => {
      if (String(input).endsWith("/acquire") && ++n <= 2)
        return Promise.resolve(new Response(JSON.stringify({ error: "shutdown", message: "scheduler closed" }), { status: 503 }));
      return fetch(input, init);
    };
    const c = new PisyncClient({ baseUrl: ctx.base, agentId: "restarting", fetch: f, retryDelayMs: 5 });
    await c.acquire();
    expect(n).toBe(3);
  });

  it("gives up with daemon_unreachable after maxOutageMs instead of hanging forever", async () => {
    const c = new PisyncClient({ baseUrl: "http://127.0.0.1:1", agentId: "lost", retryDelayMs: 10, maxOutageMs: 80 });
    await expect(c.acquire()).rejects.toMatchObject({ code: "daemon_unreachable" });
  });

  it("surfaces expiry as an AcquireError", async () => {
    await ctx.daemon.close(50);
    ctx = await startDaemon(up, { task: { maxDurationMs: 40 } });
    const c = new PisyncClient({ baseUrl: ctx.base, agentId: "greedy" });
    await c.acquire();
    await sleep(120);
    await expect(c.acquire()).rejects.toMatchObject({ code: "task_expired" });
    await c.release();
    await c.stop();
  });
});
