import { LinkedQueue, type QueueNode } from "./linked-queue.ts";

/**
 * Task-level scheduler.
 *
 * Model
 * -----
 * - An *agent* is one pi process (or anything that identifies itself).
 * - A *task* is everything an agent does between "user prompt received" and
 *   "agent fully settled". An agent has at most one task.
 * - Exactly ONE task is *active* at a time. Only the active task may have a
 *   request in flight upstream. Everyone else waits in a FIFO queue.
 * - A task keeps the slot across many requests (and across tool executions
 *   between them) until it explicitly ends (`endTask`), is reaped, or hits the
 *   hard duration limit. A finished agent that gets a new prompt re-enters at
 *   the TAIL of the queue.
 *
 * Hierarchy ("CEO" agents)
 * ------------------------
 * A parent that spawned sub-agents and is blocked waiting for them is *idle*
 * while holding the slot. If a descendant is queued behind it, that would be a
 * deadlock, so an active task that is idle (no pending / in-flight request)
 * with a queued descendant *yields*: it becomes `suspended` (still logically
 * running, just not holding the slot). When it makes its next request it
 * re-enters the queue (front by default, see `parentResume`).
 *
 * The class is synchronous and deterministic; only `setTimeout`/`Date.now` are
 * ambient (fake-timer friendly). All queue operations are O(1) except the
 * yield check, which is O(queued tasks x tree depth) and only runs when the
 * active task is idle.
 */

export type TaskState = "idle" | "queued" | "active" | "suspended";

export type ErrorCode =
  | "aborted"
  | "task_ended"
  | "agent_gone"
  | "queue_timeout"
  | "task_expired"
  | "shutdown";

export class SchedulerError extends Error {
  readonly code: ErrorCode;
  constructor(code: ErrorCode, message: string) {
    super(message);
    this.name = "SchedulerError";
    this.code = code;
  }
}

export interface SchedulerOptions {
  /** Where a suspended (yielded) parent re-enters when it resumes. */
  parentResume: "front" | "tail";
  /** Hard limit for how long one task may hold the slot. 0 = unlimited. */
  maxTaskDurationMs: number;
  /** What to do when the hard limit hits: send to the tail, or reject its requests. */
  onMaxDuration: "requeue" | "reject";
  /** Max time a task may wait in the queue before its waiters fail. 0 = unlimited. */
  maxQueueWaitMs: number;
  /** Implicit (request-started) tasks end this long after their last request. */
  implicitTaskIdleMs: number;
  /** An active task issuing no request for this long is demoted to the tail. 0 = off. */
  activeIdleTimeoutMs: number;
}

export const DEFAULT_SCHEDULER_OPTIONS: SchedulerOptions = {
  parentResume: "front",
  maxTaskDurationMs: 3 * 60 * 60 * 1000,
  onMaxDuration: "requeue",
  maxQueueWaitMs: 3 * 60 * 60 * 1000,
  implicitTaskIdleMs: 2000,
  activeIdleTimeoutMs: 0,
};

export type SchedulerEvent =
  | { type: "queued"; agent: string; position: number; resumed: boolean }
  | { type: "activated"; agent: string; waitedMs: number }
  | { type: "suspended"; agent: string; reason: "descendant-queued" }
  | { type: "ended"; agent: string; heldMs: number }
  | { type: "demoted"; agent: string; reason: "max-duration" | "idle" }
  | { type: "rejected"; agent: string; reason: "max-duration" | "queue-timeout" }
  | { type: "granted"; agent: string }
  | { type: "released"; agent: string; heldMs: number }
  | { type: "registered"; agent: string; parent: string | null }
  | { type: "gone"; agent: string; reason: string };

export interface RegisterOptions {
  parentId?: string | null;
  label?: string;
  pid?: number;
  /** If > 0 the agent is reaped when not touched for this long. */
  leaseMs?: number;
}

interface Waiter<T> {
  resolve: (v: T) => void;
  reject: (e: Error) => void;
  cleanup: () => void;
}

interface ReqWaiter extends Waiter<() => void> {
  node?: QueueNode<ReqWaiter>;
}

interface ActiveWaiter extends Waiter<boolean> {
  /** Identity handle for O(1) removal from the Set. */
  id: number;
}

class Agent {
  parent: Agent | null = null;
  readonly children = new Set<Agent>();
  label?: string;
  pid?: number;
  /** true once registerAgent() was called (vs. a bare placeholder / implicit agent). */
  registered = false;

  state: TaskState = "idle";
  /** Task begun through acquire/waitActive (extension) rather than a bare request. */
  explicit = false;
  expired = false;
  qNode: QueueNode<Agent> | null = null;
  /** Number of queued agents strictly below this one in the tree (O(1) yield check). */
  queuedSub = 0;

  readonly activeWaiters = new Set<ActiveWaiter>();
  readonly reqs = new LinkedQueue<ReqWaiter>();
  inFlight = false;

  activeSince = 0;
  queuedSince = 0;
  lastSeen = 0;
  leaseMs = 0;

  durationTimer: NodeJS.Timeout | null = null;
  queueTimer: NodeJS.Timeout | null = null;
  implicitTimer: NodeJS.Timeout | null = null;
  idleTimer: NodeJS.Timeout | null = null;
  leaseTimer: NodeJS.Timeout | null = null;

  tasksRun = 0;
  requestsGranted = 0;
  totalQueueWaitMs = 0;

  readonly id: string;
  constructor(id: string) {
    this.id = id;
  }
}

export interface AgentSnapshot {
  id: string;
  parent: string | null;
  label?: string;
  pid?: number;
  state: TaskState;
  explicit: boolean;
  pendingRequests: number;
  inFlight: boolean;
  queuedForMs?: number;
  activeForMs?: number;
  tasksRun: number;
  requestsGranted: number;
}

export interface SchedulerSnapshot {
  active: string | null;
  inFlight: string | null;
  queue: string[];
  suspended: string[];
  agents: AgentSnapshot[];
  totals: { tasksStarted: number; tasksEnded: number; requestsGranted: number; demotions: number };
}

let waiterSeq = 0;

export class Scheduler {
  readonly opts: SchedulerOptions;
  private readonly agents = new Map<string, Agent>();
  private readonly queue = new LinkedQueue<Agent>();
  private active: Agent | null = null;
  private inFlightAgent: Agent | null = null;
  private inFlightSince = 0;
  private dispatching = false;
  private again = false;
  private closed = false;
  private listeners: Array<(e: SchedulerEvent) => void> = [];
  private totals = { tasksStarted: 0, tasksEnded: 0, requestsGranted: 0, demotions: 0 };

  constructor(opts: Partial<SchedulerOptions> = {}) {
    this.opts = { ...DEFAULT_SCHEDULER_OPTIONS, ...opts };
  }

  onEvent(fn: (e: SchedulerEvent) => void): () => void {
    this.listeners.push(fn);
    return () => {
      this.listeners = this.listeners.filter((l) => l !== fn);
    };
  }

  private emit(e: SchedulerEvent): void {
    for (const l of this.listeners) {
      try {
        l(e);
      } catch {
        /* listeners must never break scheduling */
      }
    }
  }

  // ---------------------------------------------------------------- agents

  registerAgent(id: string, opts: RegisterOptions = {}): void {
    this.assertOpen();
    const a = this.ensure(id);
    a.registered = true;
    if (opts.label !== undefined) a.label = opts.label;
    if (opts.pid !== undefined) a.pid = opts.pid;
    if (opts.parentId !== undefined) this.setParent(a, opts.parentId);
    if (opts.leaseMs !== undefined) a.leaseMs = opts.leaseMs;
    this.touch(id);
    this.emit({ type: "registered", agent: id, parent: a.parent?.id ?? null });
    // A newly learned parent link may change who must yield.
    this.dispatch();
  }

  /** Refresh the lease. Cheap: only stores a timestamp (and arms the timer lazily). */
  touch(id: string): void {
    const a = this.agents.get(id);
    if (!a) return;
    a.lastSeen = Date.now();
    if (a.leaseMs > 0 && !a.leaseTimer) this.armLease(a);
  }

  hasAgent(id: string): boolean {
    return this.agents.has(id);
  }

  parentOf(id: string): string | null | undefined {
    const a = this.agents.get(id);
    return a ? (a.parent?.id ?? null) : undefined;
  }

  stateOf(id: string): TaskState | undefined {
    return this.agents.get(id)?.state;
  }

  isActive(id: string): boolean {
    return this.active?.id === id;
  }

  unregisterAgent(id: string, reason = "unregistered"): void {
    const a = this.agents.get(id);
    if (!a) return;
    this.removeAgent(a, reason);
    this.dispatch();
  }

  private ensure(id: string): Agent {
    let a = this.agents.get(id);
    if (!a) {
      a = new Agent(id);
      a.lastSeen = Date.now();
      this.agents.set(id, a);
    }
    return a;
  }

  private setParent(a: Agent, parentId: string | null): void {
    const k = a.queuedSub + (a.qNode ? 1 : 0);
    if (a.parent) {
      this.bump(a, -k);
      a.parent.children.delete(a);
    }
    a.parent = null;
    if (!parentId || parentId === a.id) return;
    const p = this.ensure(parentId);
    // Reject links that would create a cycle (p is a descendant of a).
    for (let x: Agent | null = p; x; x = x.parent) if (x === a) return;
    a.parent = p;
    p.children.add(a);
    this.bump(a, k);
  }

  /** Adjust the queued-descendant counter of every strict ancestor of `a`. */
  private bump(a: Agent, delta: number): void {
    if (delta === 0) return;
    for (let p = a.parent; p; p = p.parent) p.queuedSub += delta;
  }

  private queuePush(a: Agent, front: boolean): void {
    a.qNode = front ? this.queue.pushFront(a) : this.queue.pushBack(a);
    this.bump(a, 1);
  }

  private queueRemove(a: Agent): void {
    if (!a.qNode) return;
    this.queue.remove(a.qNode);
    a.qNode = null;
    this.bump(a, -1);
  }

  private armLease(a: Agent): void {
    const wait = Math.max(1, a.lastSeen + a.leaseMs - Date.now());
    a.leaseTimer = setTimeout(() => {
      a.leaseTimer = null;
      if (!this.agents.has(a.id) || a.leaseMs <= 0) return;
      if (Date.now() - a.lastSeen >= a.leaseMs) {
        this.removeAgent(a, "lease-expired");
        this.dispatch();
      } else {
        this.armLease(a);
      }
    }, wait);
    a.leaseTimer.unref?.();
  }

  private removeAgent(a: Agent, reason: string): void {
    this.endTaskInternal(a, new SchedulerError("agent_gone", `agent ${a.id} ${reason}`));
    this.clearTimers(a);
    // Re-attach children to the grandparent so the tree stays connected.
    for (const c of a.children) {
      c.parent = a.parent;
      a.parent?.children.add(c);
    }
    a.children.clear();
    a.parent?.children.delete(a);
    this.agents.delete(a.id);
    this.emit({ type: "gone", agent: a.id, reason });
    // Drop now-useless placeholder parents.
    const p = a.parent;
    if (p && !p.registered && p.state === "idle" && p.children.size === 0) this.removeAgent(p, "orphan-placeholder");
  }

  // ----------------------------------------------------------------- tasks

  /**
   * Begin (or continue) the agent's task and resolve `true` once it is the
   * active task. Resolves `false` if `timeoutMs` elapses first (the task stays
   * queued; this is a long-poll primitive). Rejects on task expiry, queue
   * timeout, agent removal, abort.
   */
  waitActive(id: string, o: { signal?: AbortSignal; timeoutMs?: number } = {}): Promise<boolean> {
    if (this.closed) return Promise.reject(new SchedulerError("shutdown", "scheduler closed"));
    if (o.signal?.aborted) return Promise.reject(new SchedulerError("aborted", "wait aborted"));
    const a = this.ensure(id);
    this.touch(id);
    if (a.expired) return Promise.reject(new SchedulerError("task_expired", `task of ${id} exceeded the hard limit`));
    a.explicit = true;
    this.enterQueue(a);
    this.dispatch();
    if (a.state === "active") return Promise.resolve(true);
    return new Promise<boolean>((resolve, reject) => {
      const w: ActiveWaiter = {
        id: ++waiterSeq,
        resolve,
        reject,
        cleanup: () => {
          a.activeWaiters.delete(w);
          if (timer) clearTimeout(timer);
          o.signal?.removeEventListener("abort", onAbort);
        },
      };
      const onAbort = () => {
        w.cleanup();
        reject(new SchedulerError("aborted", "wait aborted"));
      };
      let timer: NodeJS.Timeout | undefined;
      if (o.signal?.aborted) return onAbort();
      o.signal?.addEventListener("abort", onAbort, { once: true });
      if (o.timeoutMs !== undefined) {
        timer = setTimeout(() => {
          w.cleanup();
          resolve(false);
        }, o.timeoutMs);
        timer.unref?.();
      }
      a.activeWaiters.add(w);
    });
  }

  /** Position in the queue (1-based) or 0 when active/not queued. */
  position(id: string): number {
    const a = this.agents.get(id);
    if (!a || a.state !== "queued") return 0;
    let i = 1;
    for (const q of this.queue) {
      if (q === a) return i;
      i++;
    }
    return 0;
  }

  queueLength(): number {
    return this.queue.size;
  }

  /** The agent's task is over (agent settled / aborted / exited). */
  endTask(id: string): void {
    const a = this.agents.get(id);
    if (!a) return;
    this.endTaskInternal(a, new SchedulerError("task_ended", `task of ${id} ended`));
    // Implicit agents with nothing registered are garbage.
    this.gcImplicit(a);
    this.dispatch();
  }

  private gcImplicit(a: Agent): void {
    if (!a.registered && a.state === "idle" && a.children.size === 0 && !a.inFlight) this.removeAgent(a, "implicit-done");
  }

  private endTaskInternal(a: Agent, err: SchedulerError): void {
    const wasActive = this.active === a;
    const wasLive = a.state !== "idle";
    this.queueRemove(a);
    if (wasActive) {
      this.active = null;
      this.emit({ type: "ended", agent: a.id, heldMs: Date.now() - a.activeSince });
    }
    if (wasLive) this.totals.tasksEnded++;
    a.state = "idle";
    a.explicit = false;
    a.expired = false;
    this.clearTaskTimers(a);
    this.failWaiters(a, err);
  }

  private failWaiters(a: Agent, err: Error): void {
    for (const w of [...a.activeWaiters]) {
      w.cleanup();
      w.reject(err);
    }
    for (let n = a.reqs.shift(); n; n = a.reqs.shift()) {
      n.cleanup();
      n.reject(err);
    }
  }

  /** Put the agent's task in the queue if it is not already queued/active. */
  private enterQueue(a: Agent, continuation = false): void {
    if (a.state === "queued" || a.state === "active") return;
    const resumed = a.state === "suspended";
    a.state = "queued";
    a.queuedSince = Date.now();
    if (!resumed && !continuation) this.totals.tasksStarted++;
    this.queuePush(a, resumed && this.opts.parentResume === "front");
    if (this.opts.maxQueueWaitMs > 0) {
      a.queueTimer = setTimeout(() => {
        a.queueTimer = null;
        if (a.state !== "queued") return;
        this.emit({ type: "rejected", agent: a.id, reason: "queue-timeout" });
        this.endTaskInternal(a, new SchedulerError("queue_timeout", `waited more than ${this.opts.maxQueueWaitMs}ms`));
        this.gcImplicit(a);
        this.dispatch();
      }, this.opts.maxQueueWaitMs);
      a.queueTimer.unref?.();
    }
    this.emit({ type: "queued", agent: a.id, position: this.queue.size, resumed });
  }

  // -------------------------------------------------------------- requests

  /**
   * Wait for permission to send one upstream request. Resolves with a
   * `release` function that MUST be called when the response is fully
   * consumed (or the request failed / was aborted). Idempotent.
   */
  acquireRequest(id: string, o: { signal?: AbortSignal } = {}): Promise<() => void> {
    if (this.closed) return Promise.reject(new SchedulerError("shutdown", "scheduler closed"));
    const a = this.ensure(id);
    this.touch(id);
    if (a.expired) return Promise.reject(new SchedulerError("task_expired", `task of ${id} exceeded the hard limit`));
    if (o.signal?.aborted) return Promise.reject(new SchedulerError("aborted", "request aborted"));
    if (a.implicitTimer) {
      clearTimeout(a.implicitTimer);
      a.implicitTimer = null;
    }
    if (a.idleTimer) {
      clearTimeout(a.idleTimer);
      a.idleTimer = null;
    }
    this.enterQueue(a);
    return new Promise<() => void>((resolve, reject) => {
      const w: ReqWaiter = {
        resolve,
        reject,
        cleanup: () => o.signal?.removeEventListener("abort", onAbort),
      };
      const onAbort = () => {
        if (w.node && a.reqs.remove(w.node)) {
          w.cleanup();
          reject(new SchedulerError("aborted", "request aborted while queued"));
          this.afterReqRemoved(a);
          this.dispatch();
        }
      };
      o.signal?.addEventListener("abort", onAbort, { once: true });
      w.node = a.reqs.pushBack(w);
      this.dispatch();
    });
  }

  /** A queued request vanished; implicit tasks with nothing left should end. */
  private afterReqRemoved(a: Agent): void {
    if (!a.explicit && a.reqs.size === 0 && !a.inFlight) this.armImplicitEnd(a);
  }

  private armImplicitEnd(a: Agent): void {
    if (a.explicit || a.implicitTimer || a.state === "idle") return;
    const ms = this.opts.implicitTaskIdleMs;
    if (ms <= 0) {
      this.endTask(a.id);
      return;
    }
    a.implicitTimer = setTimeout(() => {
      a.implicitTimer = null;
      if (!a.explicit && a.reqs.size === 0 && !a.inFlight) this.endTask(a.id);
    }, ms);
    a.implicitTimer.unref?.();
  }

  private releaseFor(a: Agent): () => void {
    let done = false;
    return () => {
      if (done) return;
      done = true;
      const held = Date.now() - this.inFlightSince;
      a.inFlight = false;
      if (this.inFlightAgent === a) this.inFlightAgent = null;
      this.emit({ type: "released", agent: a.id, heldMs: held });
      if (this.agents.get(a.id) === a) {
        if (!a.explicit && a.reqs.size === 0) this.armImplicitEnd(a);
        else if (this.active === a && a.reqs.size === 0) this.armIdle(a);
        this.gcImplicit(a);
      }
      this.dispatch();
    };
  }

  // -------------------------------------------------------------- dispatch

  private dispatch(): void {
    if (this.dispatching) {
      this.again = true;
      return;
    }
    this.dispatching = true;
    try {
      do {
        this.again = false;
        this.step();
      } while (this.again);
    } finally {
      this.dispatching = false;
    }
  }

  private step(): void {
    for (;;) {
      const cur = this.active;
      if (cur) {
        if (this.shouldYield(cur)) {
          this.suspend(cur);
          continue;
        }
        break;
      }
      const next = this.queue.peek();
      if (!next) break;
      this.queueRemove(next);
      this.activate(next);
    }
    const a = this.active;
    if (a && !this.inFlightAgent) {
      const w = a.reqs.shift();
      if (w) {
        w.cleanup();
        a.inFlight = true;
        this.inFlightAgent = a;
        this.inFlightSince = Date.now();
        a.requestsGranted++;
        this.totals.requestsGranted++;
        if (a.idleTimer) {
          clearTimeout(a.idleTimer);
          a.idleTimer = null;
        }
        this.emit({ type: "granted", agent: a.id });
        w.resolve(this.releaseFor(a));
      }
    }
  }

  private shouldYield(a: Agent): boolean {
    return !a.inFlight && a.reqs.size === 0 && a.queuedSub > 0;
  }

  private suspend(a: Agent): void {
    this.active = null;
    a.state = "suspended";
    this.clearTaskTimers(a);
    this.emit({ type: "suspended", agent: a.id, reason: "descendant-queued" });
  }

  private activate(a: Agent): void {
    if (a.queueTimer) {
      clearTimeout(a.queueTimer);
      a.queueTimer = null;
    }
    const now = Date.now();
    const waited = now - a.queuedSince;
    a.totalQueueWaitMs += waited;
    a.tasksRun++;
    a.state = "active";
    a.activeSince = now;
    this.active = a;
    if (this.opts.maxTaskDurationMs > 0) {
      a.durationTimer = setTimeout(() => this.onMaxDuration(a), this.opts.maxTaskDurationMs);
      a.durationTimer.unref?.();
    }
    if (a.reqs.size === 0) this.armIdle(a);
    this.emit({ type: "activated", agent: a.id, waitedMs: waited });
    for (const w of [...a.activeWaiters]) {
      w.cleanup();
      w.resolve(true);
    }
  }

  private armIdle(a: Agent): void {
    if (this.opts.activeIdleTimeoutMs <= 0 || a.idleTimer) return;
    a.idleTimer = setTimeout(() => {
      a.idleTimer = null;
      if (this.active === a && a.reqs.size === 0 && !a.inFlight) this.demote(a, "idle");
    }, this.opts.activeIdleTimeoutMs);
    a.idleTimer.unref?.();
  }

  private onMaxDuration(a: Agent): void {
    a.durationTimer = null;
    if (this.active !== a) return;
    if (this.opts.onMaxDuration === "reject") {
      this.emit({ type: "rejected", agent: a.id, reason: "max-duration" });
      this.endTaskInternal(a, new SchedulerError("task_expired", `task of ${a.id} exceeded the hard limit`));
      a.expired = true;
      this.dispatch();
    } else {
      this.demote(a, "max-duration");
    }
  }

  /** Active -> tail of the queue (keeps waiters/requests; they resume later). */
  private demote(a: Agent, reason: "max-duration" | "idle"): void {
    if (this.active !== a) return;
    this.active = null;
    a.state = "idle"; // re-enters at the tail as a continuation of the same task
    this.clearTaskTimers(a);
    this.totals.demotions++;
    this.emit({ type: "demoted", agent: a.id, reason });
    this.enterQueue(a, true);
    this.dispatch();
  }

  // ---------------------------------------------------------------- timers

  private clearTaskTimers(a: Agent): void {
    for (const k of ["durationTimer", "queueTimer", "implicitTimer", "idleTimer"] as const) {
      const t = a[k];
      if (t) {
        clearTimeout(t);
        a[k] = null;
      }
    }
  }

  private clearTimers(a: Agent): void {
    this.clearTaskTimers(a);
    if (a.leaseTimer) {
      clearTimeout(a.leaseTimer);
      a.leaseTimer = null;
    }
  }

  // -------------------------------------------------------------- lifecycle

  /** Reject everything and stop all timers. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    const err = new SchedulerError("shutdown", "scheduler closed");
    for (const a of this.agents.values()) {
      this.failWaiters(a, err);
      this.clearTimers(a);
    }
    for (const a of this.queue.toArray()) this.queueRemove(a);
    this.active = null;
  }

  private assertOpen(): void {
    if (this.closed) throw new SchedulerError("shutdown", "scheduler closed");
  }

  // -------------------------------------------------------------- introspect

  snapshot(): SchedulerSnapshot {
    const now = Date.now();
    const agents: AgentSnapshot[] = [];
    const suspended: string[] = [];
    for (const a of this.agents.values()) {
      if (a.state === "suspended") suspended.push(a.id);
      agents.push({
        id: a.id,
        parent: a.parent?.id ?? null,
        ...(a.label !== undefined ? { label: a.label } : {}),
        ...(a.pid !== undefined ? { pid: a.pid } : {}),
        state: a.state,
        explicit: a.explicit,
        pendingRequests: a.reqs.size,
        inFlight: a.inFlight,
        ...(a.state === "queued" ? { queuedForMs: now - a.queuedSince } : {}),
        ...(a.state === "active" ? { activeForMs: now - a.activeSince } : {}),
        tasksRun: a.tasksRun,
        requestsGranted: a.requestsGranted,
      });
    }
    return {
      active: this.active?.id ?? null,
      inFlight: this.inFlightAgent?.id ?? null,
      queue: this.queue.toArray().map((a) => a.id),
      suspended,
      agents,
      totals: { ...this.totals },
    };
  }
}
