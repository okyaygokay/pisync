import { readFileSync, existsSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

const HOUR = 3_600_000;

export interface PisyncConfig {
  /** The pi extension starts a detached pisyncd when none is listening (local URLs only). */
  autoStart: boolean;
  listen: { host: string; port: number };
  /** pi provider name -> real upstream base URL. Providers listed here are routed through the daemon. */
  upstreams: Record<string, string>;
  queue: {
    /** Where a parent that yielded to its sub-agents re-enters the queue. */
    parentResume: "front" | "tail";
    /** Max buffered request body (bytes). */
    maxRequestBodyBytes: number;
  };
  task: {
    /**
     * HARD LIMIT on how long a task may RUN (hold the slot, tool calls included).
     * Waiting in the queue is unlimited and does not count. At the limit the task is
     * terminated and the agent is aborted. 0 = unlimited. Default 3h.
     */
    maxDurationMs: number;
    /** Clients without the extension: a task ends this long after its last request. */
    implicitIdleMs: number;
  };
  agent: {
    /** Extension heartbeat lease; an agent silent for this long is considered dead. */
    leaseMs: number;
    /** Long-poll window of a single /acquire call. */
    pollMs: number;
  };
  retry: {
    /** Retries after the first attempt (so attempts = maxRetries + 1). */
    maxRetries: number;
    baseDelayMs: number;
    maxDelayMs: number;
    /** +- fraction of random jitter applied to every delay (0..1). */
    jitter: number;
    /** Upper bound for honoring a Retry-After header. */
    retryAfterMaxMs: number;
    statuses: number[];
    /** Inspect the first SSE chunk for an error event (e.g. overloaded_error on HTTP 200). */
    sniffStreamErrors: boolean;
  };
  timeouts: {
    /** TCP+TLS connect. */
    connectMs: number;
    /** Until response headers arrive. */
    headersMs: number;
    /** Max silence between body chunks. */
    idleMs: number;
    /** Whole attempt, incl. streaming the body. 0 = unlimited. */
    requestMs: number;
  };
  logLevel: "debug" | "info" | "warn" | "error" | "silent";
}

export const DEFAULT_CONFIG: PisyncConfig = {
  autoStart: true,
  listen: { host: "127.0.0.1", port: 7878 },
  upstreams: {
    anthropic: "https://api.anthropic.com",
    "openai-codex": "https://chatgpt.com/backend-api",
    openai: "https://api.openai.com/v1",
  },
  queue: { parentResume: "front", maxRequestBodyBytes: 256 * 1024 * 1024 },
  task: { maxDurationMs: 3 * HOUR, implicitIdleMs: 2000 },
  agent: { leaseMs: 30_000, pollMs: 20_000 },
  retry: {
    maxRetries: 25,
    baseDelayMs: 1000,
    maxDelayMs: 30_000,
    jitter: 0.2,
    retryAfterMaxMs: 5 * 60_000,
    statuses: [408, 409, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 529],
    sniffStreamErrors: true,
  },
  timeouts: { connectMs: 30_000, headersMs: 300_000, idleMs: 120_000, requestMs: 30 * 60_000 },
  logLevel: "info",
};

export class ConfigError extends Error {
  readonly problems: string[];
  constructor(problems: string[]) {
    super(`invalid pisync config:\n  - ${problems.join("\n  - ")}`);
    this.name = "ConfigError";
    this.problems = problems;
  }
}

type Json = Record<string, unknown>;
const isObj = (v: unknown): v is Json => typeof v === "object" && v !== null && !Array.isArray(v);

function merge<T>(base: T, over: unknown, path: string, problems: string[]): T {
  if (over === undefined) return base;
  if (isObj(base)) {
    if (!isObj(over)) {
      problems.push(`${path || "config"} must be an object`);
      return base;
    }
    // upstreams is a free-form map: replace wholesale instead of merging keys
    if (path === "upstreams") return over as T;
    const out: Json = { ...(base as Json) };
    for (const [k, v] of Object.entries(over)) {
      if (!(k in (base as Json))) {
        problems.push(`unknown option "${path ? path + "." : ""}${k}"`);
        continue;
      }
      out[k] = merge((base as Json)[k], v, path ? `${path}.${k}` : k, problems);
    }
    return out as T;
  }
  if (Array.isArray(base)) {
    if (!Array.isArray(over)) problems.push(`${path} must be an array`);
    else return over as T;
    return base;
  }
  if (typeof base !== typeof over) {
    problems.push(`${path} must be a ${typeof base}`);
    return base;
  }
  return over as T;
}

export function validate(c: PisyncConfig): string[] {
  const p: string[] = [];
  const nonNeg = (name: string, v: number) => {
    if (!Number.isFinite(v) || v < 0) p.push(`${name} must be a number >= 0`);
  };
  const pos = (name: string, v: number) => {
    if (!Number.isFinite(v) || v <= 0) p.push(`${name} must be a number > 0`);
  };
  if (!Number.isInteger(c.listen.port) || c.listen.port < 0 || c.listen.port > 65535) p.push("listen.port must be 0-65535");
  if (!c.listen.host) p.push("listen.host must not be empty");
  for (const [name, url] of Object.entries(c.upstreams)) {
    if (!/^[A-Za-z0-9._-]{1,64}$/.test(name)) p.push(`upstreams."${name}": invalid name`);
    try {
      const u = new URL(String(url));
      if (u.protocol !== "http:" && u.protocol !== "https:") p.push(`upstreams.${name} must be http(s)`);
    } catch {
      p.push(`upstreams.${name} is not a valid URL`);
    }
  }
  pos("queue.maxRequestBodyBytes", c.queue.maxRequestBodyBytes);
  if (c.queue.parentResume !== "front" && c.queue.parentResume !== "tail") p.push('queue.parentResume must be "front" or "tail"');
  nonNeg("task.maxDurationMs", c.task.maxDurationMs);
  nonNeg("task.implicitIdleMs", c.task.implicitIdleMs);
  pos("agent.leaseMs", c.agent.leaseMs);
  pos("agent.pollMs", c.agent.pollMs);
  if (c.agent.pollMs >= c.agent.leaseMs) p.push("agent.pollMs should be smaller than agent.leaseMs");
  if (!Number.isInteger(c.retry.maxRetries) || c.retry.maxRetries < 0) p.push("retry.maxRetries must be an integer >= 0");
  nonNeg("retry.baseDelayMs", c.retry.baseDelayMs);
  nonNeg("retry.maxDelayMs", c.retry.maxDelayMs);
  nonNeg("retry.retryAfterMaxMs", c.retry.retryAfterMaxMs);
  if (c.retry.maxDelayMs < c.retry.baseDelayMs) p.push("retry.maxDelayMs must be >= retry.baseDelayMs");
  if (!(c.retry.jitter >= 0 && c.retry.jitter <= 1)) p.push("retry.jitter must be within 0..1");
  if (!c.retry.statuses.every((s) => Number.isInteger(s) && s >= 100 && s <= 599)) p.push("retry.statuses must be HTTP status codes");
  pos("timeouts.connectMs", c.timeouts.connectMs);
  pos("timeouts.headersMs", c.timeouts.headersMs);
  pos("timeouts.idleMs", c.timeouts.idleMs);
  nonNeg("timeouts.requestMs", c.timeouts.requestMs);
  if (!["debug", "info", "warn", "error", "silent"].includes(c.logLevel)) p.push("logLevel invalid");
  return p;
}

/** Merge a user config object over the defaults and validate it. */
export function resolveConfig(user: unknown = {}): PisyncConfig {
  const problems: string[] = [];
  const cfg = merge(structuredClone(DEFAULT_CONFIG), user, "", problems);
  problems.push(...validate(cfg));
  if (problems.length) throw new ConfigError(problems);
  return cfg;
}

/** Search order: explicit path, $PISYNC_CONFIG, ./pisync.config.json, ~/.config/pisync/config.json, ~/.pi/agent/pisync.json */
export function configSearchPaths(explicit?: string, cwd = process.cwd()): string[] {
  const list: string[] = [];
  if (explicit) list.push(resolve(cwd, explicit));
  if (process.env.PISYNC_CONFIG) list.push(resolve(cwd, process.env.PISYNC_CONFIG));
  list.push(join(cwd, "pisync.config.json"));
  list.push(join(homedir(), ".config", "pisync", "config.json"));
  list.push(join(homedir(), ".pi", "agent", "pisync.json"));
  return list;
}

export interface LoadedConfig {
  config: PisyncConfig;
  /** File the config came from, or null when defaults were used. */
  path: string | null;
}

export function loadConfig(explicit?: string, cwd = process.cwd()): LoadedConfig {
  if (explicit && !existsSync(resolve(cwd, explicit))) throw new ConfigError([`config file not found: ${explicit}`]);
  for (const p of configSearchPaths(explicit, cwd)) {
    if (!existsSync(p)) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(readFileSync(p, "utf8"));
    } catch (e) {
      throw new ConfigError([`${p}: ${(e as Error).message}`]);
    }
    try {
      return { config: resolveConfig(parsed), path: p };
    } catch (e) {
      if (e instanceof ConfigError) throw new ConfigError(e.problems.map((m) => `${p}: ${m}`));
      throw e;
    }
  }
  return { config: resolveConfig({}), path: null };
}
