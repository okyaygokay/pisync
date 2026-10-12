import http from "node:http";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { control, fakeUpstream, sleep, startDaemon, statusOf, type FakeUpstream } from "./harness.ts";

let up: FakeUpstream;
let ctx: Awaited<ReturnType<typeof startDaemon>> | undefined;

beforeEach(async () => {
  up = await fakeUpstream();
});
afterEach(async () => {
  await ctx?.daemon.close(100);
  ctx = undefined;
  await up.close();
});

const start = async (over = {}, extra = {}) => (ctx = await startDaemon(up, over, extra));
const post = (path: string, body = "{}", headers: Record<string, string> = {}) =>
  fetch(`${ctx!.base}${path}`, { method: "POST", headers: { "content-type": "application/json", ...headers }, body });

describe("proxying", () => {
  it("forwards method, path, query, body and returns status + headers", async () => {
    up.handler = (req, res) => {
      res.writeHead(201, { "content-type": "application/json", "x-up": "yes" });
      res.end(JSON.stringify({ got: req.body.toString(), url: req.url }));
    };
    await start();
    const res = await post("/a/agent1/fake/v1/messages?beta=true", '{"hello":1}', { authorization: "Bearer tok", "x-api-key": "k" });
    expect(res.status).toBe(201);
    expect(res.headers.get("x-up")).toBe("yes");
    expect(await res.json()).toEqual({ got: '{"hello":1}', url: "/v1/messages?beta=true" });
    const seen = up.seen[0]!;
    expect(seen.headers.authorization).toBe("Bearer tok");
    expect(seen.headers["x-api-key"]).toBe("k");
    expect(seen.headers.host).toBe(new URL(up.url).host);
  });

  it("does not leak x-pisync-* headers upstream", async () => {
    await start();
    await post("/fake/x", "{}", { "x-pisync-agent": "abc", "x-pisync-other": "1" });
    expect(up.seen[0]!.headers["x-pisync-agent"]).toBeUndefined();
    expect(up.seen[0]!.headers["x-pisync-other"]).toBeUndefined();
  });

  it("supports the header form of agent identification and plain /upstream paths", async () => {
    await start();
    const r = await post("/fake/v1/x", "{}", { "x-pisync-agent": "hdr-agent" });
    expect(r.status).toBe(200);
    expect(up.seen[0]!.url).toBe("/v1/x");
  });

  it("keeps an upstream base path (e.g. /v1)", async () => {
    await ctx?.daemon.close();
    ctx = await startDaemon(up, { upstreams: { fake: up.url + "/v1" } });
    await post("/fake/chat/completions?x=1");
    expect(up.seen[0]!.url).toBe("/v1/chat/completions?x=1");
  });

  it("handles GET without body", async () => {
    await start();
    const r = await fetch(`${ctx!.base}/fake/models`);
    expect(r.status).toBe(200);
    expect(up.seen[0]!.method).toBe("GET");
  });

  it("streams SSE incrementally (first chunk arrives before upstream finishes)", async () => {
    let finish!: () => void;
    const gate = new Promise<void>((r) => (finish = r));
    up.handler = async (_req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write("data: one\n\n");
      await gate;
      res.write("data: two\n\n");
      res.end();
    };
    await start();
    const res = await post("/a/s/fake/stream");
    const reader = res.body!.getReader();
    const first = new TextDecoder().decode((await reader.read()).value);
    expect(first).toContain("one");
    finish();
    let rest = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      rest += new TextDecoder().decode(value);
    }
    expect(rest).toContain("two");
  });

  it("passes large bodies both ways intact", async () => {
    const big = Buffer.alloc(8 * 1024 * 1024, "abcdefghij");
    up.handler = (req, res) => {
      res.writeHead(200);
      res.end(req.body);
    };
    await start();
    const r = await fetch(`${ctx!.base}/a/big/fake/echo`, { method: "POST", body: big });
    const out = Buffer.from(await r.arrayBuffer());
    expect(out.equals(big)).toBe(true);
  });

  it("404 for unknown upstream, 400 for bad agent id, 413 for oversized body", async () => {
    await start({ queue: { maxRequestBodyBytes: 100 } });
    expect((await post("/nope/x")).status).toBe(404);
    expect((await post("/a/bad%20id!/fake/x")).status).toBe(400);
    expect((await post("/fake/x", "x".repeat(1000))).status).toBe(413);
    expect(up.seen).toHaveLength(0);
  });

  it("healthz and status", async () => {
    await start();
    expect(await (await fetch(`${ctx!.base}/healthz`)).text()).toBe("ok");
    const st = await statusOf(ctx!.base);
    expect(st.scheduler.active).toBeNull();
    expect(st.upstreams).toEqual(["fake"]);
  });
});

describe("single request at a time", () => {
  it("never has more than one request in flight upstream, across many agents", async () => {
    up.handler = async (_r, res) => {
      await sleep(15);
      res.writeHead(200).end("ok");
    };
    await start();
    const rs = await Promise.all(Array.from({ length: 12 }, (_, i) => post(`/a/agent${i}/fake/x`)));
    expect(rs.every((r) => r.status === 200)).toBe(true);
    expect(up.seen).toHaveLength(12);
    expect(up.maxConcurrent).toBe(1);
  });

  it("holds the slot for the whole streamed response, not just the headers", async () => {
    const order: string[] = [];
    up.handler = async (req, res) => {
      const who = req.url!.slice(1);
      order.push(`start:${who}`);
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write("data: x\n\n");
      await sleep(60);
      res.end("data: y\n\n");
      order.push(`end:${who}`);
    };
    await start();
    await Promise.all([post("/a/a/fake/a"), post("/a/b/fake/b")].map((p) => p.then((r) => r.text())));
    expect(order).toEqual(["start:a", "end:a", "start:b", "end:b"]);
  });

  it("anonymous requests are served FIFO", async () => {
    up.handler = async (req, res) => {
      await sleep(10);
      res.writeHead(200).end(req.url);
    };
    await start();
    const ps: Promise<Response>[] = [];
    for (let i = 0; i < 6; i++) {
      ps.push(post(`/fake/${i}`));
      await sleep(3);
    }
    await Promise.all(ps);
    expect(up.seen.map((s) => s.url)).toEqual(["/0", "/1", "/2", "/3", "/4", "/5"]);
  });

  it("task-level: an agent keeps the slot between its requests until the task is released", async () => {
    await start();
    const base = ctx!.base;
    expect((await control(base, "acquire", { agentId: "a" })).body.granted).toBe(true);
    // b and c ask for the slot (long poll) and queue up
    const bAcq = control(base, "acquire", { agentId: "b" });
    await sleep(10);
    const cAcq = control(base, "acquire", { agentId: "c" });
    // a makes several requests; b/c requests are blocked meanwhile
    const bReq = post("/a/b/fake/b");
    const cReq = post("/a/c/fake/c");
    for (let i = 0; i < 3; i++) {
      const r = await post("/a/a/fake/a");
      expect(r.status).toBe(200);
      await sleep(5);
    }
    expect(up.seen.map((s) => s.url)).toEqual(["/a", "/a", "/a"]);
    // a's TASK ends -> b next (not c, not a)
    await control(base, "release", { agentId: "a" });
    expect((await bAcq).body.granted).toBe(true);
    expect((await bReq).status).toBe(200);
    // a gets another message and re-queues behind c
    const aAgain = control(base, "acquire", { agentId: "a" });
    await sleep(10);
    const st = await statusOf(base);
    expect(st.scheduler.queue).toEqual(["c", "a"]);
    await control(base, "release", { agentId: "b" });
    expect((await cAcq).body.granted).toBe(true);
    expect((await cReq).status).toBe(200);
    expect(up.seen.slice(3).map((s) => s.url)).toEqual(["/b", "/c"]);
    await control(base, "release", { agentId: "c" });
    expect((await aAgain).body.granted).toBe(true);
  });
});

describe("retries", () => {
  it("retries a failing plan up to 25 times and then succeeds (26th attempt)", async () => {
    let n = 0;
    up.handler = (_r, res) => {
      n++;
      if (n <= 25) res.writeHead(500).end("boom");
      else res.writeHead(200).end("finally");
    };
    await start();
    const r = await post("/a/x/fake/x", "{}");
    expect(r.status).toBe(200);
    expect(await r.text()).toBe("finally");
    expect(up.seen).toHaveLength(26);
    expect(ctx!.daemon.metrics.retries).toBe(25);
  });

  it("gives up after maxRetries and forwards the last failure", async () => {
    up.handler = (_r, res) => res.writeHead(503, { "x-last": "1" }).end("overloaded");
    await start({ retry: { maxRetries: 3 } });
    const r = await post("/a/x/fake/x");
    expect(r.status).toBe(503);
    expect(await r.text()).toBe("overloaded");
    expect(up.seen).toHaveLength(4);
    expect(ctx!.daemon.metrics.failedAfterRetries).toBe(1);
  });

  it("the default config really retries 25 times", async () => {
    const { DEFAULT_CONFIG } = await import("../src/config.ts");
    expect(DEFAULT_CONFIG.retry.maxRetries).toBeGreaterThanOrEqual(25);
  });

  it("retries 429 / 529 / 408 but not 400 / 401 / 404", async () => {
    for (const [status, retried] of [
      [429, true],
      [529, true],
      [408, true],
      [400, false],
      [401, false],
      [404, false],
    ] as const) {
      up.seen.length = 0;
      up.handler = (_r, res) => res.writeHead(status).end("x");
      await ctx?.daemon.close(50);
      ctx = await startDaemon(up, { retry: { maxRetries: 2 } });
      const r = await post("/a/x/fake/x");
      expect(r.status).toBe(status);
      expect(up.seen.length).toBe(retried ? 3 : 1);
    }
  });

  it("honors Retry-After (capped) while keeping the slot", async () => {
    const delays: number[] = [];
    let n = 0;
    up.handler = (_r, res) => {
      n++;
      if (n === 1) res.writeHead(429, { "retry-after": "7" }).end();
      else if (n === 2) res.writeHead(429, { "retry-after": "9999" }).end();
      else res.writeHead(200).end("ok");
    };
    await start({ retry: { baseDelayMs: 10, maxDelayMs: 20, retryAfterMaxMs: 60_000 } }, { sleep: async (ms: number) => void delays.push(ms) });
    const r = await post("/a/x/fake/x");
    expect(r.status).toBe(200);
    expect(delays).toEqual([7000, 60_000]);
  });

  it("uses exponential backoff between attempts", async () => {
    const delays: number[] = [];
    let n = 0;
    up.handler = (_r, res) => (++n < 6 ? res.writeHead(500).end() : res.writeHead(200).end());
    await start({ retry: { baseDelayMs: 100, maxDelayMs: 1000, jitter: 0 } }, { sleep: async (ms: number) => void delays.push(ms) });
    await post("/a/x/fake/x");
    expect(delays).toEqual([100, 200, 400, 800, 1000]);
  });

  it("retries connection resets", async () => {
    let n = 0;
    up.handler = (_r, res) => {
      if (++n <= 3) res.destroy();
      else res.writeHead(200).end("ok");
    };
    await start();
    const r = await post("/a/x/fake/x");
    expect(r.status).toBe(200);
    expect(n).toBe(4);
  });

  it("returns 502 when upstream is unreachable after all retries", async () => {
    await start({ upstreams: { fake: "http://127.0.0.1:1" }, retry: { maxRetries: 2 } });
    const r = await post("/a/x/fake/x");
    expect(r.status).toBe(502);
    expect(((await r.json()) as any).error.message).toMatch(/unreachable after 3 attempt/);
  });

  it("retries when the first attempt hits a headers timeout", async () => {
    let n = 0;
    up.handler = async (_r, res) => {
      if (++n === 1) await sleep(400); // stall past headersMs, never answer
      else res.writeHead(200).end("ok");
    };
    await start({ timeouts: { headersMs: 100 } });
    const r = await post("/a/x/fake/x");
    expect(r.status).toBe(200);
    expect(n).toBe(2);
  });

  it("retries an HTTP-200 SSE stream that starts with an overloaded_error event", async () => {
    let n = 0;
    up.handler = (_r, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      if (++n < 3) res.end('event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}\n\n');
      else res.end("event: message_start\ndata: {}\n\n");
    };
    await start();
    const r = await post("/a/x/fake/x");
    expect(await r.text()).toContain("message_start");
    expect(n).toBe(3);
  });

  it("does not retry a stream error that is the caller's fault", async () => {
    up.handler = (_r, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end('event: error\ndata: {"type":"error","error":{"type":"invalid_request_error","message":"bad"}}\n\n');
    };
    await start();
    const r = await post("/a/x/fake/x");
    expect(await r.text()).toContain("invalid_request_error");
    expect(up.seen).toHaveLength(1);
  });

  it("sniffing can be disabled", async () => {
    up.handler = (_r, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end('event: error\ndata: {"type":"error","error":{"type":"overloaded_error"}}\n\n');
    };
    await start({ retry: { sniffStreamErrors: false } });
    await (await post("/a/x/fake/x")).text();
    expect(up.seen).toHaveLength(1);
  });

  it("a failing request keeps the slot: nobody else sneaks in between retries", async () => {
    let n = 0;
    up.handler = async (req, res) => {
      if (req.url === "/a") {
        if (++n < 4) return void res.writeHead(500).end();
      }
      res.writeHead(200).end("ok");
    };
    await start();
    const pa = post("/a/a/fake/a");
    await sleep(2);
    const pb = post("/a/b/fake/b");
    await Promise.all([pa, pb]);
    expect(up.seen.map((s) => s.url)).toEqual(["/a", "/a", "/a", "/a", "/b"]);
  });
});

describe("streaming failures and client aborts", () => {
  it("cuts the client when the upstream goes silent mid-stream (idle timeout)", async () => {
    up.handler = async (_r, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write("data: hi\n\n");
      await sleep(1000);
    };
    await start({ timeouts: { idleMs: 100 } });
    const r = await post("/a/x/fake/x");
    await expect(r.text()).rejects.toThrow();
  });

  it("frees the slot and aborts upstream when the client disconnects", async () => {
    let upstreamClosed = false;
    up.handler = async (req, res, raw) => {
      if (req.url === "/slow") {
        void raw;
        res.on("close", () => (upstreamClosed = true));
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.write("data: a\n\n");
        await sleep(3000);
        return;
      }
      res.writeHead(200).end("fast");
    };
    await start();
    const ac = new AbortController();
    const slow = fetch(`${ctx!.base}/a/slow/fake/slow`, { method: "POST", body: "{}", signal: ac.signal });
    const fastP = post("/a/other/fake/fast");
    await sleep(100);
    ac.abort();
    await slow.catch(() => {});
    const fast = await fastP;
    expect(await fast.text()).toBe("fast");
    await sleep(50);
    expect(upstreamClosed).toBe(true);
    expect((await statusOf(ctx!.base)).scheduler.inFlight).toBeNull();
  });

  it("a client that disconnects while queued never reaches upstream", async () => {
    up.handler = async (_r, res) => {
      await sleep(150);
      res.writeHead(200).end("ok");
    };
    await start();
    const first = post("/a/first/fake/1");
    await sleep(20);
    const ac = new AbortController();
    const queued = fetch(`${ctx!.base}/a/q/fake/2`, { method: "POST", body: "{}", signal: ac.signal });
    await sleep(20);
    ac.abort();
    await queued.catch(() => {});
    await first;
    await sleep(100);
    expect(up.seen.map((s) => s.url)).toEqual(["/1"]);
  });
});

describe("queue limits", () => {
  it("returns 503 + Retry-After when the queue wait limit is exceeded", async () => {
    up.handler = async (_r, res) => {
      await sleep(400);
      res.writeHead(200).end("ok");
    };
    await start({ queue: { maxWaitMs: 100 } });
    const first = post("/a/first/fake/1");
    await sleep(20);
    const second = await post("/a/second/fake/2");
    expect(second.status).toBe(503);
    expect(second.headers.get("retry-after")).toBe("30");
    await first;
  });

  it("task_expired (reject mode) yields 403 on the proxy path", async () => {
    await start({ task: { maxDurationMs: 50, onMaxDuration: "reject" } });
    await control(ctx!.base, "acquire", { agentId: "hog" });
    await control(ctx!.base, "acquire", { agentId: "next" }).then(() => {}); // blocks until poll window
    await sleep(120);
    const r = await post("/a/hog/fake/x");
    expect(r.status).toBe(403);
  });
});

describe("control API", () => {
  it("register / heartbeat / unregister", async () => {
    await start();
    const reg = await control(ctx!.base, "register", { agentId: "p1", label: "pi:proj", pid: 123 });
    expect(reg.body).toMatchObject({ ok: true, leaseMs: 5000, pollMs: 200 });
    expect((await control(ctx!.base, "heartbeat", { agentId: "p1" })).status).toBe(200);
    expect((await control(ctx!.base, "heartbeat", { agentId: "ghost" })).status).toBe(404);
    await control(ctx!.base, "unregister", { agentId: "p1" });
    expect((await control(ctx!.base, "heartbeat", { agentId: "p1" })).status).toBe(404);
  });

  it("validates input", async () => {
    await start();
    expect((await control(ctx!.base, "acquire", { agentId: "bad id" })).status).toBe(400);
    expect((await control(ctx!.base, "acquire", {})).status).toBe(400);
    expect((await control(ctx!.base, "nope", { agentId: "a" })).status).toBe(404);
    const r = await post("/_pisync/acquire", "{not json");
    expect(r.status).toBe(400);
    expect((await fetch(`${ctx!.base}/_pisync/acquire`)).status).toBe(405);
  });

  it("acquire long-polls: reports queue position, then grants", async () => {
    await start();
    await control(ctx!.base, "acquire", { agentId: "a" });
    const w = await control(ctx!.base, "acquire", { agentId: "b" }); // poll window 200ms
    expect(w.body).toMatchObject({ granted: false, position: 1, queueLength: 1 });
    const pending = control(ctx!.base, "acquire", { agentId: "b" });
    await control(ctx!.base, "release", { agentId: "a" });
    expect((await pending).body.granted).toBe(true);
  });

  it("acquire self-heals an unknown agent (daemon restarted)", async () => {
    await start();
    const r = await control(ctx!.base, "acquire", { agentId: "never-registered" });
    expect(r.body.granted).toBe(true);
  });

  it("master yields to a sub-agent through the control API (no deadlock)", async () => {
    await start();
    const b = ctx!.base;
    await control(b, "register", { agentId: "master" });
    await control(b, "register", { agentId: "worker", parentId: "master" });
    expect((await control(b, "acquire", { agentId: "master" })).body.granted).toBe(true);
    // master spawns the worker and waits on it (idle); worker asks for the slot
    expect((await control(b, "acquire", { agentId: "worker" })).body.granted).toBe(true);
    // worker works, finishes
    expect((await post("/a/worker/fake/work")).status).toBe(200);
    await control(b, "release", { agentId: "worker" });
    // master's next turn
    expect((await control(b, "acquire", { agentId: "master" })).body.granted).toBe(true);
    expect((await post("/a/master/fake/summary")).status).toBe(200);
    expect(up.seen.map((s) => s.url)).toEqual(["/work", "/summary"]);
  });

  it("a dead agent (lease expired) is reaped and the next one proceeds", async () => {
    await start({ agent: { leaseMs: 150, pollMs: 50 } });
    await control(ctx!.base, "register", { agentId: "dead" });
    await control(ctx!.base, "acquire", { agentId: "dead" });
    const next = control(ctx!.base, "acquire", { agentId: "alive" });
    const t0 = Date.now();
    let granted = false;
    while (!granted && Date.now() - t0 < 2000) {
      granted = (await control(ctx!.base, "acquire", { agentId: "alive" })).body.granted === true;
    }
    expect(granted).toBe(true);
    await next;
  });
});

describe("performance", () => {
  it("adds little latency per request", async () => {
    up.handler = (_r, res) => res.writeHead(200).end("ok");
    await start();
    const N = 300;
    const t0 = performance.now();
    for (let i = 0; i < N; i++) await (await fetch(`${up.url}/x`, { method: "POST", body: "{}" })).text();
    const direct = (performance.now() - t0) / N;
    const t1 = performance.now();
    for (let i = 0; i < N; i++) await (await post("/a/perf/fake/x")).text();
    const proxied = (performance.now() - t1) / N;
    expect(proxied - direct).toBeLessThan(5);
  });

  it("streams 64MB without buffering it in memory", async () => {
    const chunk = Buffer.alloc(64 * 1024, "z");
    up.handler = async (_r, res) => {
      res.writeHead(200, { "content-type": "application/octet-stream" });
      for (let i = 0; i < 1024; i++) if (!res.write(chunk)) await new Promise((r) => res.once("drain", r));
      res.end();
    };
    await start();
    const rss0 = process.memoryUsage().rss;
    const t0 = performance.now();
    const r = await post("/a/big/fake/x");
    let n = 0;
    for await (const c of r.body as unknown as AsyncIterable<Uint8Array>) n += c.length;
    const ms = performance.now() - t0;
    expect(n).toBe(64 * 1024 * 1024);
    expect(process.memoryUsage().rss - rss0).toBeLessThan(200 * 1024 * 1024);
    expect(ms).toBeLessThan(10_000);
  });

  it("serves 200 queued tiny requests from 20 agents quickly", async () => {
    up.handler = (_r, res) => res.writeHead(200).end("ok");
    await start();
    const t0 = performance.now();
    const rs = await Promise.all(Array.from({ length: 200 }, (_, i) => post(`/a/agent${i % 20}/fake/x`)));
    expect(rs.every((r) => r.status === 200)).toBe(true);
    expect(performance.now() - t0).toBeLessThan(5000);
    expect(up.maxConcurrent).toBe(1);
  });
});

void http;
