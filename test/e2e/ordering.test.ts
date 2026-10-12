import { afterEach, describe, expect, it } from "vitest";
import { HAVE_PI, Stack, piCmd, waitFor, type Ctx } from "./stack.ts";

let s: Stack | undefined;
afterEach(async () => {
  await s?.stop();
  s = undefined;
});

/** Start agents one by one, each only after the previous one is known to the daemon. */
async function launchInOrder(stack: Stack, names: string[], opts: Parameters<Stack["runPi"]>[1] = {}) {
  const runs = [];
  for (const n of names) {
    runs.push(stack.runPi(n, opts));
    await stack.waitForTask(n);
  }
  return Promise.all(runs);
}

describe.skipIf(!HAVE_PI)("e2e: task ordering with real pi processes", () => {
  it("a, b, c: tasks run back to back in arrival order, one request in flight", async () => {
    s = await Stack.start({ behavior: (c) => (c.final ? { kind: "text" } : { kind: "tool", command: "sleep 0.6" }) });
    const rs = await launchInOrder(s, ["agent-aaa", "agent-bbb", "agent-ccc"]);
    expect(rs.map((r) => r.code)).toEqual([0, 0, 0]);
    expect(s.tasks()).toEqual(["agent-aaa", "agent-bbb", "agent-ccc"]); // no interleaving
    expect(s.log).toHaveLength(6);
    expect(s.maxConcurrent).toBe(1);
  }, 90_000);

  it("the agent's own credentials are passed through untouched", async () => {
    s = await Stack.start();
    await s.runPi("agent-key");
    expect(s.log.every((e) => e.apiKey === "sk-fake")).toBe(true);
  }, 60_000);

  it("the active agent keeps the slot during a long tool run between its two requests", async () => {
    s = await Stack.start({ behavior: (c) => (c.final ? { kind: "text" } : { kind: "tool", command: "sleep 1.5" }) });
    const rs = await launchInOrder(s, ["agent-one", "agent-two"]);
    expect(rs.every((r) => r.code === 0)).toBe(true);
    // agent-two asked while agent-one ran its tool (idle, no request) - still had to wait
    expect(s.tasks()).toEqual(["agent-one", "agent-two"]);
  }, 60_000);

  it("a gets another message while b runs: a goes behind c (rpc mode)", async () => {
    s = await Stack.start({
      behavior: (c) => (c.final ? { kind: "text" } : { kind: "tool", command: c.who === "agent-bbb" ? "sleep 2" : "sleep 0.5" }),
    });
    const a = s.rpcPi("agent-aaa");
    const first = a.prompt("My name is agent-aaa: first");
    await s.waitForTask("agent-aaa");
    const b = s.runPi("agent-bbb");
    await s.waitForTask("agent-bbb");
    const c = s.runPi("agent-ccc");
    await s.waitForTask("agent-ccc");
    await first; // a's TASK done -> b becomes active
    await waitFor(() => s!.daemon.scheduler.snapshot().active === s!.agentByLabel("agent-bbb")?.id, 20_000, "b active");
    const second = a.prompt("My name is agent-aaa: second message");
    await s.waitForTask("agent-aaa");
    // queue must now be [c, a]
    const labels = s.daemon.scheduler.snapshot().queue.map((id) => s!.daemon.scheduler.snapshot().agents.find((x) => x.id === id)?.label);
    expect(labels).toEqual(["agent-ccc", "agent-aaa"]);
    await Promise.all([b, c, second]);
    expect(s.tasks()).toEqual(["agent-aaa", "agent-bbb", "agent-ccc", "agent-aaa"]);
    await a.close();
  }, 120_000);

  it("eight agents at once all finish; tasks never interleave", async () => {
    s = await Stack.start({ behavior: (c) => (c.final ? { kind: "text" } : { kind: "tool", command: "sleep 0.2" }) });
    const names = Array.from({ length: 8 }, (_, i) => `agent-s${i}`);
    const rs = await Promise.all(names.map((n) => s!.runPi(n)));
    expect(rs.map((r) => r.code)).toEqual(names.map(() => 0));
    const order = s.tasks();
    expect(order).toHaveLength(8);
    expect(new Set(order).size).toBe(8);
    expect(s.maxConcurrent).toBe(1);
    expect(s.daemon.scheduler.snapshot().agents).toHaveLength(0); // everybody unregistered
  }, 180_000);

  it("agents that arrive while the queue is draining are appended in order", async () => {
    s = await Stack.start({ behavior: (c) => (c.final ? { kind: "text" } : { kind: "tool", command: "sleep 0.5" }) });
    const runs = [];
    for (const n of ["agent-d0", "agent-d1", "agent-d2", "agent-d3"]) {
      runs.push(s.runPi(n));
      await s.waitForTask(n);
    }
    await Promise.all(runs);
    expect(s.tasks()).toEqual(["agent-d0", "agent-d1", "agent-d2", "agent-d3"]);
  }, 120_000);
});

describe.skipIf(!HAVE_PI)("e2e: master / sub-agent (CEO) hierarchies", () => {
  it("master spawns one sub-agent through bash and waits: no deadlock, child runs while master is suspended", async () => {
    const behave = (c: Ctx) =>
      c.final ? { kind: "text" as const } : { kind: "tool" as const, command: c.who === "agent-master" ? piCmd("agent-child") : "sleep 0.3" };
    s = await Stack.start({ behavior: behave });
    const events: string[] = [];
    s.daemon.scheduler.onEvent((e) => events.push(`${e.type}:${"agent" in e ? e.agent : ""}`));
    const rs = await Promise.all([s.runPi("agent-master")]);
    expect(rs[0]!.code).toBe(0);
    expect(s.tasks()).toEqual(["agent-master", "agent-child", "agent-master"]);
    expect(events.some((e) => e.startsWith("suspended:"))).toBe(true);
  }, 90_000);

  it("unrelated agents are served while the master waits; master resumes after its child", async () => {
    const behave = (c: Ctx) =>
      c.final ? { kind: "text" as const } : { kind: "tool" as const, command: c.who === "agent-master" ? piCmd("agent-child") : "sleep 0.3" };
    s = await Stack.start({ behavior: behave });
    const m = s.runPi("agent-master");
    await s.waitForTask("agent-master");
    const o1 = s.runPi("agent-other1");
    await s.waitForTask("agent-other1");
    const o2 = s.runPi("agent-other2");
    await s.waitForTask("agent-other2");
    const rs = await Promise.all([m, o1, o2]);
    expect(rs.map((r) => r.code)).toEqual([0, 0, 0]);
    expect(s.maxConcurrent).toBe(1);
    const t = s.tasks();
    expect(t[0]).toBe("agent-master");
    // FIFO among the rest: child registered after the others had queued
    expect(t.indexOf("agent-other1")).toBeLessThan(t.indexOf("agent-other2"));
    expect(t.lastIndexOf("agent-master")).toBeGreaterThan(t.indexOf("agent-child"));
    // A suspended master only re-queues when it asks again; agents already in line keep their turn.
    expect(t.filter((x) => x === "agent-master")).toHaveLength(2);
  }, 120_000);

  it("three levels deep: master -> manager -> worker", async () => {
    const behave = (c: Ctx) => {
      if (c.final) return { kind: "text" as const };
      if (c.who === "agent-master") return { kind: "tool" as const, command: piCmd("agent-manager") };
      if (c.who === "agent-manager") return { kind: "tool" as const, command: piCmd("agent-worker") };
      return { kind: "tool" as const, command: "sleep 0.2" };
    };
    s = await Stack.start({ behavior: behave });
    const r = await s.runPi("agent-master");
    expect(r.code).toBe(0);
    expect(s.tasks()).toEqual(["agent-master", "agent-manager", "agent-worker", "agent-manager", "agent-master"]);
  }, 120_000);

  it("master runs three workers in parallel and waits for all of them", async () => {
    const behave = (c: Ctx) => {
      if (c.final) return { kind: "text" as const };
      if (c.who === "agent-master")
        return { kind: "tool" as const, command: `(${piCmd("agent-w1")}) & (${piCmd("agent-w2")}) & (${piCmd("agent-w3")}) & wait` };
      return { kind: "tool" as const, command: "sleep 0.2" };
    };
    s = await Stack.start({ behavior: behave });
    const r = await s.runPi("agent-master");
    expect(r.code).toBe(0);
    expect(s.maxConcurrent).toBe(1);
    const t = s.tasks();
    expect(t[0]).toBe("agent-master");
    expect(t[t.length - 1]).toBe("agent-master");
    for (const w of ["agent-w1", "agent-w2", "agent-w3"]) expect(t.filter((x) => x === w)).toHaveLength(1);
    expect(s.log).toHaveLength(2 + 6); // master 2 + 3 workers x 2
  }, 150_000);

  it("two independent masters with one worker each, plus a loner", async () => {
    const behave = (c: Ctx) => {
      if (c.final) return { kind: "text" as const };
      if (c.who === "agent-m1") return { kind: "tool" as const, command: piCmd("agent-m1w") };
      if (c.who === "agent-m2") return { kind: "tool" as const, command: piCmd("agent-m2w") };
      return { kind: "tool" as const, command: "sleep 0.2" };
    };
    s = await Stack.start({ behavior: behave });
    const rs = [];
    for (const n of ["agent-m1", "agent-m2", "agent-solo"]) {
      rs.push(s.runPi(n));
      await s.waitForTask(n);
    }
    const out = await Promise.all(rs);
    expect(out.map((r) => r.code)).toEqual([0, 0, 0]);
    expect(s.maxConcurrent).toBe(1);
    expect(s.log).toHaveLength(10);
    const t = s.tasks();
    // each worker ran contiguously (its own 2 requests) and before its master's final request
    expect(t.lastIndexOf("agent-m1")).toBeGreaterThan(t.indexOf("agent-m1w"));
    expect(t.lastIndexOf("agent-m2")).toBeGreaterThan(t.indexOf("agent-m2w"));
  }, 150_000);

  it("parentResume=tail: the resumed master queues behind agents that arrived meanwhile", async () => {
    const behave = (c: Ctx) =>
      c.final ? { kind: "text" as const } : { kind: "tool" as const, command: c.who === "agent-master" ? piCmd("agent-child") : "sleep 0.3" };
    s = await Stack.start({ behavior: behave, daemon: { queue: { parentResume: "tail" } } });
    const m = s.runPi("agent-master");
    await s.waitForTask("agent-master");
    const o = s.runPi("agent-other");
    await s.waitForTask("agent-other");
    const rs = await Promise.all([m, o]);
    expect(rs.map((r) => r.code)).toEqual([0, 0]);
    expect(s.maxConcurrent).toBe(1);
  }, 120_000);
});
