import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Scheduler, SchedulerError, type SchedulerEvent } from "../src/scheduler.ts";
import { bg, flush, probe } from "./helpers.ts";

let s: Scheduler;
beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  s?.close();
  vi.useRealTimers();
});

const mk = (o = {}) => (s = new Scheduler(o));

describe("task-level exclusivity (a, b, c scenario)", () => {
  it("first agent is active, the rest queue in arrival order", async () => {
    mk();
    const a = probe(s.waitActive("a"));
    const b = probe(s.waitActive("b"));
    const c = probe(s.waitActive("c"));
    await flush();
    expect(a.value).toBe(true);
    expect(b.settled).toBe(false);
    expect(c.settled).toBe(false);
    expect(s.snapshot().queue).toEqual(["b", "c"]);
    expect(s.position("b")).toBe(1);
    expect(s.position("c")).toBe(2);
  });

  it("a keeps the slot across many requests; only ending the TASK pops b", async () => {
    mk();
    bg(s.waitActive("a"));
    const b = probe(s.waitActive("b"));
    for (let i = 0; i < 5; i++) {
      const release = await s.acquireRequest("a");
      release();
      await flush();
      expect(b.settled).toBe(false);
      expect(s.isActive("a")).toBe(true);
    }
    s.endTask("a");
    await flush();
    expect(b.value).toBe(true);
    expect(s.isActive("b")).toBe(true);
  });

  it("a re-queues at the LAST place when it gets another message", async () => {
    mk();
    bg(s.waitActive("a"));
    const b = probe(s.waitActive("b"));
    const c = probe(s.waitActive("c"));
    s.endTask("a"); // a finished its task
    await flush();
    expect(b.value).toBe(true);
    const a2 = probe(s.waitActive("a")); // a gets another message while b runs
    await flush();
    expect(s.snapshot().queue).toEqual(["c", "a"]);
    s.endTask("b");
    await flush();
    expect(c.value).toBe(true); // c, not a
    expect(a2.settled).toBe(false);
    s.endTask("c");
    await flush();
    expect(a2.value).toBe(true);
  });

  it("requests of a queued agent wait until its task is active", async () => {
    mk();
    bg(s.waitActive("a"));
    const rb = probe(s.acquireRequest("b"));
    await flush();
    expect(rb.settled).toBe(false);
    s.endTask("a");
    await flush();
    expect(typeof rb.value).toBe("function");
  });

  it("never grants two requests at once, even from the same agent", async () => {
    mk();
    bg(s.waitActive("a"));
    const r1 = probe(s.acquireRequest("a"));
    const r2 = probe(s.acquireRequest("a"));
    await flush();
    expect(r1.settled).toBe(true);
    expect(r2.settled).toBe(false);
    expect(s.snapshot().inFlight).toBe("a");
    r1.value!();
    await flush();
    expect(r2.settled).toBe(true);
  });

  it("release is idempotent", async () => {
    mk();
    const r = await s.acquireRequest("a");
    r();
    r();
    r();
    expect(s.snapshot().inFlight).toBeNull();
  });

  it("a request still in flight after endTask blocks the next task's request, not its activation", async () => {
    mk();
    bg(s.waitActive("a"));
    const ra = await s.acquireRequest("a");
    const b = probe(s.waitActive("b"));
    const rb = probe(s.acquireRequest("b"));
    s.endTask("a");
    await flush();
    expect(b.value).toBe(true);
    expect(rb.settled).toBe(false);
    ra();
    await flush();
    expect(rb.settled).toBe(true);
  });

  it("endTask on an unknown agent is a no-op", () => {
    mk();
    expect(() => s.endTask("nobody")).not.toThrow();
  });

  it("waitActive is idempotent for the same queued task (no duplicate queue entries)", async () => {
    mk();
    bg(s.waitActive("a"));
    bg(s.waitActive("b"));
    bg(s.waitActive("b"));
    bg(s.waitActive("b"));
    expect(s.snapshot().queue).toEqual(["b"]);
  });
});

describe("waiters", () => {
  it("waitActive with timeout resolves false and keeps the place in line", async () => {
    mk();
    bg(s.waitActive("a"));
    const b = probe(s.waitActive("b", { timeoutMs: 1000 }));
    await vi.advanceTimersByTimeAsync(1001);
    expect(b.value).toBe(false);
    expect(s.snapshot().queue).toEqual(["b"]);
    const b2 = probe(s.waitActive("b", { timeoutMs: 1000 }));
    s.endTask("a");
    await flush();
    expect(b2.value).toBe(true);
  });

  it("aborting a queued request removes only that request", async () => {
    mk();
    bg(s.waitActive("a"));
    const ac = new AbortController();
    const r = probe(s.acquireRequest("a", { signal: ac.signal }));
    await flush();
    const r2 = probe(s.acquireRequest("a"));
    await flush();
    expect(r.settled).toBe(true); // first is granted
    const ac2 = new AbortController();
    const r3 = probe(s.acquireRequest("a", { signal: ac2.signal }));
    ac2.abort();
    await flush();
    expect((r3.error as SchedulerError).code).toBe("aborted");
    r.value!();
    await flush();
    expect(r2.settled).toBe(true);
  });

  it("pre-aborted signals reject immediately", async () => {
    mk();
    const ac = new AbortController();
    ac.abort();
    await expect(s.acquireRequest("a", { signal: ac.signal })).rejects.toMatchObject({ code: "aborted" });
    await expect(s.waitActive("a", { signal: ac.signal })).rejects.toMatchObject({ code: "aborted" });
  });

  it("endTask rejects pending waiters and requests with task_ended", async () => {
    mk();
    bg(s.waitActive("a"));
    const w = probe(s.waitActive("b"));
    const r = probe(s.acquireRequest("b"));
    s.endTask("b");
    await flush();
    expect((w.error as SchedulerError).code).toBe("task_ended");
    expect((r.error as SchedulerError).code).toBe("task_ended");
    expect(s.snapshot().queue).toEqual([]);
  });

  it("aborting a waitActive does not dequeue the task", async () => {
    mk();
    bg(s.waitActive("a"));
    const ac = new AbortController();
    const w = probe(s.waitActive("b", { signal: ac.signal }));
    ac.abort();
    await flush();
    expect(w.error).toBeDefined();
    expect(s.snapshot().queue).toEqual(["b"]);
  });
});

describe("hierarchy: master (CEO) and sub-agents", () => {
  it("an idle master yields to its queued child instead of deadlocking", async () => {
    mk();
    s.registerAgent("master");
    s.registerAgent("child", { parentId: "master" });
    await s.waitActive("master");
    const child = probe(s.waitActive("child"));
    await flush();
    expect(child.value).toBe(true);
    expect(s.stateOf("master")).toBe("suspended");
    expect(s.isActive("child")).toBe(true);
  });

  it("master does NOT yield while it has an in-flight request, only after it completes", async () => {
    mk();
    s.registerAgent("master");
    s.registerAgent("child", { parentId: "master" });
    await s.waitActive("master");
    const rel = await s.acquireRequest("master");
    const child = probe(s.waitActive("child"));
    await flush();
    expect(child.settled).toBe(false);
    expect(s.stateOf("master")).toBe("active");
    rel();
    await flush();
    expect(child.value).toBe(true);
    expect(s.stateOf("master")).toBe("suspended");
  });

  it("master does NOT yield while it has more requests pending", async () => {
    mk();
    s.registerAgent("master");
    s.registerAgent("child", { parentId: "master" });
    await s.waitActive("master");
    const r1 = await s.acquireRequest("master");
    const r2 = probe(s.acquireRequest("master"));
    const child = probe(s.waitActive("child"));
    r1();
    await flush();
    expect(r2.settled).toBe(true); // master still goes first
    expect(child.settled).toBe(false);
    r2.value!();
    await flush();
    expect(child.value).toBe(true);
  });

  it("unrelated agents never cause a yield", async () => {
    mk();
    s.registerAgent("master");
    await s.waitActive("master");
    const other = probe(s.waitActive("other"));
    await flush();
    expect(other.settled).toBe(false);
    expect(s.stateOf("master")).toBe("active");
  });

  it("resumed master goes to the FRONT of the queue by default", async () => {
    mk();
    s.registerAgent("master");
    s.registerAgent("c1", { parentId: "master" });
    await s.waitActive("master");
    bg(s.waitActive("x")); // unrelated, queued first
    bg(s.waitActive("c1"));
    await flush();
    // x is ahead of c1; master yielded because c1 is a queued descendant.
    expect(s.isActive("x")).toBe(true);
    s.endTask("x");
    await flush();
    expect(s.isActive("c1")).toBe(true);
    bg(s.waitActive("y")); // another unrelated waiter
    const m = probe(s.acquireRequest("master")); // master resumes
    await flush();
    expect(s.snapshot().queue).toEqual(["master", "y"]);
    s.endTask("c1");
    await flush();
    expect(s.isActive("master")).toBe(true);
    expect(m.settled).toBe(true);
  });

  it("parentResume=tail puts the resumed master behind everybody", async () => {
    mk({ parentResume: "tail" });
    s.registerAgent("master");
    s.registerAgent("c1", { parentId: "master" });
    await s.waitActive("master");
    bg(s.waitActive("c1"));
    await flush();
    bg(s.waitActive("y"));
    bg(s.waitActive("master"));
    expect(s.snapshot().queue).toEqual(["y", "master"]);
  });

  it("handles grandchildren: master yields for a queued grandchild", async () => {
    mk();
    s.registerAgent("m");
    s.registerAgent("c", { parentId: "m" });
    s.registerAgent("g", { parentId: "c" });
    await s.waitActive("m");
    const g = probe(s.waitActive("g"));
    await flush();
    expect(g.value).toBe(true);
    expect(s.stateOf("m")).toBe("suspended");
  });

  it("full CEO flow: master + 3 workers, nobody starves, master resumes after workers", async () => {
    mk();
    const order: string[] = [];
    s.onEvent((e) => e.type === "activated" && order.push(e.agent));
    s.registerAgent("m");
    for (const w of ["w1", "w2", "w3"]) s.registerAgent(w, { parentId: "m" });
    await s.waitActive("m");
    for (const w of ["w1", "w2", "w3"]) s.waitActive(w);
    await flush();
    for (const w of ["w1", "w2", "w3"]) {
      expect(s.isActive(w)).toBe(true);
      s.endTask(w);
      await flush();
    }
    // all workers done; master issues its follow-up request
    const r = probe(s.acquireRequest("m"));
    await flush();
    expect(r.settled).toBe(true);
    expect(order).toEqual(["m", "w1", "w2", "w3", "m"]);
  });

  it("registering a parent link later is honoured (placeholder parent)", async () => {
    mk();
    s.registerAgent("child", { parentId: "master-not-yet-known" });
    s.registerAgent("master-not-yet-known");
    await s.waitActive("master-not-yet-known");
    const c = probe(s.waitActive("child"));
    await flush();
    expect(c.value).toBe(true);
  });

  it("parent cycles are ignored", async () => {
    mk();
    s.registerAgent("a", { parentId: "b" });
    s.registerAgent("b", { parentId: "a" }); // would be a cycle
    s.registerAgent("c", { parentId: "c" }); // self-parent
    await s.waitActive("a");
    const b = probe(s.waitActive("b"));
    await flush();
    // a is parent of nothing (b->a rejected) or b parent of a; either way no infinite loop and exactly one active
    expect(s.snapshot().agents.filter((x) => x.state === "active")).toHaveLength(1);
    void b;
  });

  it("removing a parent reattaches children to the grandparent", async () => {
    mk();
    s.registerAgent("gp");
    s.registerAgent("p", { parentId: "gp" });
    s.registerAgent("c", { parentId: "p" });
    s.unregisterAgent("p");
    expect(s.snapshot().agents.find((x) => x.id === "c")?.parent).toBe("gp");
    await s.waitActive("gp");
    const c = probe(s.waitActive("c"));
    await flush();
    expect(c.value).toBe(true);
  });

  it("a child's request (not only waitActive) also triggers the master to yield", async () => {
    mk();
    s.registerAgent("m");
    s.registerAgent("c", { parentId: "m" });
    await s.waitActive("m");
    const r = probe(s.acquireRequest("c"));
    await flush();
    expect(r.settled).toBe(true);
    expect(s.stateOf("m")).toBe("suspended");
  });

  it("ending a suspended master's task removes it cleanly", async () => {
    mk();
    s.registerAgent("m");
    s.registerAgent("c", { parentId: "m" });
    await s.waitActive("m");
    bg(s.waitActive("c"));
    await flush();
    s.endTask("m");
    expect(s.stateOf("m")).toBe("idle");
    expect(s.isActive("c")).toBe(true);
  });
});

describe("hard task duration limit (running time only)", () => {
  const H = 3 * 3600_000;

  it("default is 3 hours", () => {
    mk();
    expect(s.opts.maxTaskDurationMs).toBe(H);
  });

  it("terminates a task that runs too long: the slot passes on and the task does NOT re-queue", async () => {
    mk({ maxTaskDurationMs: 1000 });
    const events: SchedulerEvent[] = [];
    s.onEvent((e) => events.push(e));
    await s.waitActive("a");
    const b = probe(s.waitActive("b"));
    await vi.advanceTimersByTimeAsync(1001);
    expect(b.value).toBe(true);
    expect(s.snapshot().queue).toEqual([]);
    expect(s.stateOf("a")).toBe("idle");
    expect(s.isExpired("a")).toBe(true);
    expect(events.some((e) => e.type === "expired")).toBe(true);
  });

  it("long tool calls (no requests at all) count as running time and are cut at the limit", async () => {
    mk({ maxTaskDurationMs: H });
    await s.waitActive("a");
    const rel = await s.acquireRequest("a");
    rel();
    const b = probe(s.waitActive("b"));
    await vi.advanceTimersByTimeAsync(H - 1000); // a tool call that runs almost 3h
    expect(b.settled).toBe(false);
    expect(s.isActive("a")).toBe(true);
    await vi.advanceTimersByTimeAsync(2000);
    expect(b.value).toBe(true);
  });

  it("an expired task's waiters and later requests fail with task_expired until the task is reported finished", async () => {
    mk({ maxTaskDurationMs: 1000 });
    await s.waitActive("a");
    await vi.advanceTimersByTimeAsync(1001);
    await expect(s.acquireRequest("a")).rejects.toMatchObject({ code: "task_expired" });
    await expect(s.waitActive("a")).rejects.toMatchObject({ code: "task_expired" });
    s.endTask("a"); // agent aborted and settled
    expect(s.isExpired("a")).toBe(false);
    const a2 = probe(s.waitActive("a"));
    await flush();
    expect(a2.value).toBe(true); // brand-new task, fresh clock
  });

  it("a pending request of the expired task is rejected too; one already in flight finishes normally", async () => {
    mk({ maxTaskDurationMs: 1000 });
    await s.waitActive("a");
    const inflight = await s.acquireRequest("a");
    const pending = probe(s.acquireRequest("a"));
    await vi.advanceTimersByTimeAsync(1001);
    expect((pending.error as SchedulerError).code).toBe("task_expired");
    const b = probe(s.acquireRequest("b"));
    await flush();
    expect(b.settled).toBe(false); // global one-in-flight rule still holds
    inflight();
    await flush();
    expect(b.settled).toBe(true);
  });

  it("waiting in the queue is unlimited and does not eat into the task's time", async () => {
    const HR = 3600_000;
    mk({ maxTaskDurationMs: HR });
    await s.waitActive("a");
    bg(s.waitActive("b"));
    const c = probe(s.waitActive("c"));
    await vi.advanceTimersByTimeAsync(50 * 60_000);
    s.endTask("a"); // a finished after 50 min
    await vi.advanceTimersByTimeAsync(50 * 60_000);
    s.endTask("b"); // b finished after 50 min; c has now waited 100 min > limit
    await flush();
    expect(c.value).toBe(true);
    await vi.advanceTimersByTimeAsync(50 * 60_000);
    expect(s.isExpired("c")).toBe(false);
    expect(s.isActive("c")).toBe(true);
  });

  it("time suspended while sub-agents work does not count; the clock resumes with the remaining budget", async () => {
    mk({ maxTaskDurationMs: 1000 });
    s.registerAgent("m");
    s.registerAgent("c", { parentId: "m" });
    await s.waitActive("m");
    await vi.advanceTimersByTimeAsync(600); // master ran 600ms
    bg(s.waitActive("c")); // master yields
    await flush();
    expect(s.stateOf("m")).toBe("suspended");
    await vi.advanceTimersByTimeAsync(5000); // child works for ages
    expect(s.isExpired("m")).toBe(false);
    s.endTask("c");
    const m2 = probe(s.acquireRequest("m"));
    await flush();
    expect(m2.settled).toBe(true);
    await vi.advanceTimersByTimeAsync(300);
    expect(s.isExpired("m")).toBe(false); // 900ms used
    await vi.advanceTimersByTimeAsync(200);
    expect(s.isExpired("m")).toBe(true); // 1100ms > 1000ms
  });

  it("ending the task before the limit cancels the timer and resets the clock", async () => {
    mk({ maxTaskDurationMs: 1000 });
    await s.waitActive("a");
    await vi.advanceTimersByTimeAsync(800);
    s.endTask("a");
    await s.waitActive("a"); // new task: full budget again
    await vi.advanceTimersByTimeAsync(800);
    expect(s.isExpired("a")).toBe(false);
  });

  it("an active task is never demoted for being idle between requests (tool calls can take hours)", async () => {
    mk({ maxTaskDurationMs: 0 });
    await s.waitActive("a");
    const b = probe(s.waitActive("b"));
    for (let i = 0; i < 5; i++) {
      const rel = await s.acquireRequest("a");
      rel();
      await vi.advanceTimersByTimeAsync(2 * 3600_000); // two hour tool call
    }
    expect(b.settled).toBe(false);
    expect(s.isActive("a")).toBe(true);
  });

  it("limit 0 disables the timer", async () => {
    mk({ maxTaskDurationMs: 0 });
    await s.waitActive("a");
    const b = probe(s.waitActive("b"));
    await vi.advanceTimersByTimeAsync(100 * 3600_000);
    expect(b.settled).toBe(false);
    expect(s.isActive("a")).toBe(true);
  });

  it("default limit fires at exactly 3 hours", async () => {
    mk();
    await s.waitActive("a");
    const b = probe(s.waitActive("b"));
    await vi.advanceTimersByTimeAsync(H - 1000);
    expect(b.settled).toBe(false);
    await vi.advanceTimersByTimeAsync(2000);
    expect(b.value).toBe(true);
  });
});


describe("leases and implicit tasks", () => {
  it("an agent that stops heartbeating is reaped and the slot passes on", async () => {
    mk();
    s.registerAgent("a", { leaseMs: 1000 });
    await s.waitActive("a");
    const b = probe(s.waitActive("b"));
    await vi.advanceTimersByTimeAsync(1100);
    expect(b.value).toBe(true);
    expect(s.hasAgent("a")).toBe(false);
  });

  it("touch keeps the lease alive", async () => {
    mk();
    s.registerAgent("a", { leaseMs: 1000 });
    await s.waitActive("a");
    const b = probe(s.waitActive("b"));
    for (let i = 0; i < 10; i++) {
      await vi.advanceTimersByTimeAsync(500);
      s.touch("a");
    }
    expect(b.settled).toBe(false);
    expect(s.hasAgent("a")).toBe(true);
  });

  it("request-started (implicit) tasks end after the idle window", async () => {
    mk({ implicitTaskIdleMs: 500 });
    const rel = await s.acquireRequest("x");
    const y = probe(s.acquireRequest("y"));
    rel();
    await flush();
    expect(y.settled).toBe(false); // x keeps the slot for the window (burst of requests)
    const rel2 = await s.acquireRequest("x"); // x's next request arrives: still its task
    rel2();
    await vi.advanceTimersByTimeAsync(501);
    expect(y.settled).toBe(true);
  });

  it("with implicitTaskIdleMs=0 every anonymous request is its own task (FIFO)", async () => {
    mk({ implicitTaskIdleMs: 0 });
    const order: string[] = [];
    const run = async (id: string) => {
      const rel = await s.acquireRequest(id);
      order.push(id);
      rel();
    };
    await Promise.all([run("1"), run("2"), run("3"), run("4")]);
    expect(order).toEqual(["1", "2", "3", "4"]);
    expect(s.snapshot().agents).toHaveLength(0); // garbage collected
  });

  it("explicit tasks are not ended by the implicit idle timer", async () => {
    mk({ implicitTaskIdleMs: 100 });
    await s.waitActive("a");
    const b = probe(s.waitActive("b"));
    const rel = await s.acquireRequest("a");
    rel();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(b.settled).toBe(false);
  });

  it("an aborted queued implicit request lets the task end", async () => {
    mk({ implicitTaskIdleMs: 0 });
    const hold = await s.acquireRequest("hold");
    const ac = new AbortController();
    const r = probe(s.acquireRequest("x", { signal: ac.signal }));
    ac.abort();
    await flush();
    expect(r.error).toBeDefined();
    hold();
    await flush();
    expect(s.snapshot().agents.find((a) => a.id === "x")).toBeUndefined();
  });
});

describe("shutdown and introspection", () => {
  it("close() rejects everything and refuses new work", async () => {
    mk();
    await s.waitActive("a");
    const b = probe(s.waitActive("b"));
    const r = probe(s.acquireRequest("b"));
    s.close();
    await flush();
    expect((b.error as SchedulerError).code).toBe("shutdown");
    expect((r.error as SchedulerError).code).toBe("shutdown");
    await expect(s.waitActive("z")).rejects.toMatchObject({ code: "shutdown" });
  });

  it("snapshot reports queue, active and totals", async () => {
    mk();
    await s.waitActive("a");
    bg(s.waitActive("b"));
    const snap = s.snapshot();
    expect(snap.active).toBe("a");
    expect(snap.queue).toEqual(["b"]);
    expect(snap.totals.tasksStarted).toBe(2);
    expect(snap.agents.find((x) => x.id === "b")?.queuedForMs).toBeGreaterThanOrEqual(0);
  });

  it("listener exceptions never break scheduling", async () => {
    mk();
    s.onEvent(() => {
      throw new Error("boom");
    });
    await expect(s.waitActive("a")).resolves.toBe(true);
  });
});

describe("invariants under random operations", () => {
  function rng(seed: number) {
    return () => {
      seed = (seed * 1664525 + 1013904223) >>> 0;
      return seed / 2 ** 32;
    };
  }

  it.each([1, 2, 3, 4, 5, 6, 7, 8])("seed %i: one active, one in flight, queue consistent", async (seed) => {
    mk({ maxTaskDurationMs: 0, implicitTaskIdleMs: 50 });
    const rand = rng(seed);
    const ids = Array.from({ length: 12 }, (_, i) => `n${i}`);
    // random tree
    ids.forEach((id, i) => s.registerAgent(id, { parentId: i > 0 && rand() < 0.7 ? ids[Math.floor(rand() * i)]! : null }));
    const releases: Array<() => void> = [];
    let concurrent = 0;
    let maxConcurrent = 0;
    const check = () => {
      const snap = s.snapshot();
      expect(snap.agents.filter((a) => a.state === "active").length).toBeLessThanOrEqual(1);
      expect(snap.agents.filter((a) => a.inFlight).length).toBeLessThanOrEqual(1);
      for (const q of snap.queue) expect(snap.agents.find((a) => a.id === q)?.state).toBe("queued");
      expect(new Set(snap.queue).size).toBe(snap.queue.length);
      if (snap.active) expect(snap.queue).not.toContain(snap.active);
    };
    for (let step = 0; step < 600; step++) {
      const id = ids[Math.floor(rand() * ids.length)]!;
      const op = rand();
      if (op < 0.25) s.waitActive(id, { timeoutMs: 100 }).catch(() => {});
      else if (op < 0.5)
        s.acquireRequest(id)
          .then((rel) => {
            concurrent++;
            maxConcurrent = Math.max(maxConcurrent, concurrent);
            releases.push(() => {
              concurrent--;
              rel();
            });
          })
          .catch(() => {});
      else if (op < 0.7) s.endTask(id);
      else if (op < 0.9) releases.shift()?.();
      else await vi.advanceTimersByTimeAsync(30);
      await flush(3);
      check();
    }
    expect(maxConcurrent).toBeLessThanOrEqual(1);
  });

  it("liveness: with a master/worker tree and random completion order everything finishes", async () => {
    mk({ maxTaskDurationMs: 0 });
    const rand = rng(42);
    s.registerAgent("m");
    const workers = Array.from({ length: 10 }, (_, i) => `w${i}`);
    workers.forEach((w) => s.registerAgent(w, { parentId: "m" }));
    await s.waitActive("m");
    const done = new Set<string>();
    const runWorker = async (w: string) => {
      await s.waitActive(w);
      for (let i = 0; i < 3; i++) {
        const rel = await s.acquireRequest(w);
        await Promise.resolve();
        rel();
      }
      s.endTask(w);
      done.add(w);
    };
    const all = workers.sort(() => rand() - 0.5).map(runWorker);
    await Promise.all(all);
    expect(done.size).toBe(10);
    const rel = await s.acquireRequest("m");
    rel();
  });
});

describe("performance", () => {
  it("handles 20k task cycles quickly (O(1) queue ops)", async () => {
    vi.useRealTimers();
    mk({ maxTaskDurationMs: 0 });
    const N = 20_000;
    const t0 = performance.now();
    const waits: Array<Promise<boolean>> = [];
    for (let i = 0; i < N; i++) waits.push(s.waitActive(`a${i}`));
    for (let i = 0; i < N; i++) {
      await waits[i];
      s.endTask(`a${i}`);
    }
    const ms = performance.now() - t0;
    expect(s.snapshot().queue).toHaveLength(0);
    expect(ms).toBeLessThan(2000);
  });

  it("deep queues stay cheap when no hierarchy is involved", async () => {
    vi.useRealTimers();
    mk({ maxTaskDurationMs: 0 });
    for (let i = 0; i < 5000; i++) s.waitActive(`q${i}`);
    const t0 = performance.now();
    for (let i = 0; i < 5000; i++) {
      const rel = await s.acquireRequest(`q${i}`);
      rel();
      s.endTask(`q${i}`);
    }
    expect(performance.now() - t0).toBeLessThan(2000);
  });
});
