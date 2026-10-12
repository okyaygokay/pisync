import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { ConfigError, DEFAULT_CONFIG, loadConfig, resolveConfig } from "../src/config.ts";
import { LinkedQueue } from "../src/linked-queue.ts";
import { backoffDelay, isRetryableError, parseRetryAfter, retryDelay, sniffRetryableStreamError } from "../src/retry.ts";

describe("config", () => {
  it("defaults: 25 retries, 3h hard limits, localhost only", () => {
    const c = resolveConfig({});
    expect(c.retry.maxRetries).toBe(25);
    expect(c.task.maxDurationMs).toBe(10_800_000);
    expect(c.queue.maxWaitMs).toBe(10_800_000);
    expect(c.listen.host).toBe("127.0.0.1");
  });

  it("merges partial overrides without losing sibling defaults", () => {
    const c = resolveConfig({ retry: { maxRetries: 40 }, listen: { port: 9000 } });
    expect(c.retry.maxRetries).toBe(40);
    expect(c.retry.baseDelayMs).toBe(DEFAULT_CONFIG.retry.baseDelayMs);
    expect(c.listen).toEqual({ host: "127.0.0.1", port: 9000 });
  });

  it("does not mutate the shared defaults", () => {
    resolveConfig({ retry: { maxRetries: 1 }, upstreams: { x: "http://x" } });
    expect(DEFAULT_CONFIG.retry.maxRetries).toBe(25);
    expect(DEFAULT_CONFIG.upstreams.x).toBeUndefined();
  });

  it("replaces upstreams wholesale", () => {
    const c = resolveConfig({ upstreams: { only: "https://example.com/v1" } });
    expect(Object.keys(c.upstreams)).toEqual(["only"]);
  });

  it.each([
    [{ retry: { maxRetries: -1 } }, /maxRetries/],
    [{ retry: { jitter: 2 } }, /jitter/],
    [{ retry: { baseDelayMs: 5000, maxDelayMs: 10 } }, /maxDelayMs/],
    [{ listen: { port: 70000 } }, /port/],
    [{ upstreams: { a: "not a url" } }, /not a valid URL/],
    [{ upstreams: { a: "ftp://x" } }, /http/],
    [{ queue: { parentResume: "middle" } }, /parentResume/],
    [{ task: { onMaxDuration: "explode" } }, /onMaxDuration/],
    [{ timeouts: { idleMs: 0 } }, /idleMs/],
    [{ agent: { leaseMs: 1000, pollMs: 5000 } }, /pollMs/],
    [{ nonsense: 1 }, /unknown option "nonsense"/],
    [{ retry: { nonsense: 1 } }, /unknown option "retry.nonsense"/],
    [{ retry: { maxRetries: "many" } }, /must be a number/],
    [{ retry: "no" }, /must be an object/],
    [{ retry: { statuses: [99] } }, /status/],
    [{ logLevel: "loud" }, /logLevel/],
  ])("rejects %j", (bad, re) => {
    expect(() => resolveConfig(bad)).toThrow(ConfigError);
    expect(() => resolveConfig(bad)).toThrow(re);
  });

  it("reports all problems at once", () => {
    try {
      resolveConfig({ retry: { maxRetries: -1, jitter: 5 } });
      expect.unreachable();
    } catch (e) {
      expect((e as ConfigError).problems.length).toBeGreaterThanOrEqual(2);
    }
  });

  it("loads from an explicit file and reports its path", () => {
    const dir = mkdtempSync(join(tmpdir(), "pisync-"));
    const file = join(dir, "c.json");
    writeFileSync(file, JSON.stringify({ retry: { maxRetries: 30 } }));
    const { config, path } = loadConfig(file, dir);
    expect(config.retry.maxRetries).toBe(30);
    expect(path).toBe(file);
  });

  it("errors on a missing explicit file, bad JSON and invalid values (with file name)", () => {
    const dir = mkdtempSync(join(tmpdir(), "pisync-"));
    expect(() => loadConfig("nope.json", dir)).toThrow(/not found/);
    writeFileSync(join(dir, "bad.json"), "{oops");
    expect(() => loadConfig("bad.json", dir)).toThrow(/bad\.json/);
    writeFileSync(join(dir, "inv.json"), JSON.stringify({ retry: { maxRetries: -3 } }));
    expect(() => loadConfig("inv.json", dir)).toThrow(/inv\.json.*maxRetries/s);
  });

  it("picks up ./pisync.config.json from the cwd", () => {
    const dir = mkdtempSync(join(tmpdir(), "pisync-"));
    writeFileSync(join(dir, "pisync.config.json"), JSON.stringify({ listen: { port: 4242 } }));
    expect(loadConfig(undefined, dir).config.listen.port).toBe(4242);
  });

  it("the shipped example config is valid", async () => {
    const { readFileSync } = await import("node:fs");
    const raw = JSON.parse(readFileSync(new URL("../pisync.config.example.json", import.meta.url), "utf8"));
    expect(() => resolveConfig(raw)).not.toThrow();
  });
});

describe("retry helpers", () => {
  const cfg = { ...DEFAULT_CONFIG.retry, jitter: 0 };

  it("backoff doubles and caps", () => {
    expect([1, 2, 3, 4, 5, 6, 7].map((n) => backoffDelay(n, cfg))).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000]);
    expect(backoffDelay(500, cfg)).toBe(30000);
  });

  it("jitter stays within bounds", () => {
    const j = { ...cfg, jitter: 0.2 };
    expect(backoffDelay(1, j, () => 0)).toBe(800);
    expect(backoffDelay(1, j, () => 1)).toBe(1200);
    expect(backoffDelay(1, j, () => 0.5)).toBe(1000);
  });

  it("parses Retry-After seconds and dates", () => {
    expect(parseRetryAfter("5")).toBe(5000);
    expect(parseRetryAfter("0")).toBe(0);
    expect(parseRetryAfter("1.5")).toBe(1500);
    expect(parseRetryAfter(undefined)).toBeUndefined();
    expect(parseRetryAfter("garbage")).toBeUndefined();
    expect(parseRetryAfter("-3")).toBeUndefined();
    const now = Date.parse("2030-01-01T00:00:00Z");
    expect(parseRetryAfter("Tue, 01 Jan 2030 00:00:10 GMT", now)).toBe(10_000);
    expect(parseRetryAfter("Mon, 31 Dec 2029 23:00:00 GMT", now)).toBe(0);
  });

  it("retryDelay = max(backoff, capped Retry-After)", () => {
    expect(retryDelay(1, cfg, "0")).toBe(1000);
    expect(retryDelay(1, cfg, "10")).toBe(10_000);
    expect(retryDelay(1, cfg, "99999")).toBe(cfg.retryAfterMaxMs);
  });

  it("classifies network errors", () => {
    expect(isRetryableError({ code: "ECONNRESET" })).toBe(true);
    expect(isRetryableError({ code: "PISYNC_TIMEOUT" })).toBe(true);
    expect(isRetryableError(new Error("socket hang up"))).toBe(true);
    expect(isRetryableError({ code: "EACCES" })).toBe(false);
    expect(isRetryableError(new Error("some bug"))).toBe(false);
    expect(isRetryableError(null)).toBe(false);
  });

  describe("stream error sniffing", () => {
    const b = (s: string) => Buffer.from(s);
    it("detects transient provider errors", () => {
      expect(sniffRetryableStreamError(b('event: error\ndata: {"type":"error","error":{"type":"overloaded_error"}}\n\n'))).toBe(true);
      expect(sniffRetryableStreamError(b('event: error\ndata: {"type":"error","error":{"type":"api_error"}}\n\n'))).toBe(true);
      expect(sniffRetryableStreamError(b('data: {"error":{"code":"server_error","message":"x"}}\n\n'))).toBe(true);
      expect(sniffRetryableStreamError(b('data: {"type":"error","error":{"type":"rate_limit_error"}}\n\n'))).toBe(true);
    });
    it("leaves normal and caller-fault streams alone", () => {
      expect(sniffRetryableStreamError(b('event: message_start\ndata: {"type":"message_start"}\n\n'))).toBe(false);
      expect(sniffRetryableStreamError(b('data: {"id":"x","choices":[]}\n\n'))).toBe(false);
      expect(sniffRetryableStreamError(b('event: error\ndata: {"type":"error","error":{"type":"invalid_request_error"}}\n\n'))).toBe(false);
      expect(sniffRetryableStreamError(b('event: error\ndata: {"type":"error","error":{"type":"authentication_error"}}\n\n'))).toBe(false);
      expect(sniffRetryableStreamError(b('data: {"error":{"code":"context_length_exceeded"}}\n\n'))).toBe(false);
      expect(sniffRetryableStreamError(b(""))).toBe(false);
      expect(sniffRetryableStreamError(Buffer.from([0xff, 0xfe, 0x00]))).toBe(false);
    });
  });
});

describe("LinkedQueue", () => {
  it("push/shift in order, front pushes, O(1) removal by node", () => {
    const q = new LinkedQueue<number>();
    const n1 = q.pushBack(1);
    q.pushBack(2);
    const n3 = q.pushBack(3);
    q.pushFront(0);
    expect(q.toArray()).toEqual([0, 1, 2, 3]);
    expect(q.remove(n1)).toBe(true);
    expect(q.remove(n1)).toBe(false); // double remove is safe
    expect(q.remove(n3)).toBe(true);
    expect(q.toArray()).toEqual([0, 2]);
    expect(q.shift()).toBe(0);
    expect(q.shift()).toBe(2);
    expect(q.shift()).toBeUndefined();
    expect(q.size).toBe(0);
    expect(q.peek()).toBeUndefined();
  });

  it("refuses to remove a node of another queue", () => {
    const a = new LinkedQueue<number>();
    const b = new LinkedQueue<number>();
    const n = a.pushBack(1);
    expect(b.remove(n)).toBe(false);
    expect(a.size).toBe(1);
  });

  it("stays consistent under random operations", () => {
    const q = new LinkedQueue<number>();
    const model: number[] = [];
    const nodes = new Map<number, ReturnType<typeof q.pushBack>>();
    let seed = 7;
    const rnd = () => (seed = (seed * 1103515245 + 12345) >>> 0) / 2 ** 32;
    for (let i = 0; i < 5000; i++) {
      const r = rnd();
      if (r < 0.35) (nodes.set(i, q.pushBack(i)), model.push(i));
      else if (r < 0.5) (nodes.set(i, q.pushFront(i)), model.unshift(i));
      else if (r < 0.7) {
        const v = q.shift();
        expect(v).toBe(model.shift());
        if (v !== undefined) nodes.delete(v);
      } else if (model.length) {
        const idx = Math.floor(rnd() * model.length);
        const v = model.splice(idx, 1)[0]!;
        expect(q.remove(nodes.get(v)!)).toBe(true);
        nodes.delete(v);
      }
      expect(q.size).toBe(model.length);
    }
    expect(q.toArray()).toEqual(model);
  });
});
