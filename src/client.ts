import { randomBytes } from "node:crypto";

export interface ClientOptions {
  /** Daemon base URL, e.g. http://127.0.0.1:7878 */
  baseUrl: string;
  agentId: string;
  parentId?: string | null;
  label?: string;
  pid?: number;
  fetch?: typeof fetch;
  /** Called with queue position while waiting for the slot. */
  onWaiting?: (info: { position: number; queueLength: number }) => void;
  onError?: (err: Error) => void;
  /** Delay between reconnect attempts while the daemon is unreachable. */
  retryDelayMs?: number;
  /** Give up acquire() after the daemon has been unreachable this long (default 30s). */
  maxOutageMs?: number;
}

export class AcquireError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "AcquireError";
    this.code = code;
  }
}

class TransientError extends Error {}

export function newAgentId(prefix = "pi"): string {
  return `${prefix}-${process.pid}-${randomBytes(4).toString("hex")}`;
}

/** Thin client for the daemon's /_pisync control API. */
export class PisyncClient {
  readonly agentId: string;
  private readonly base: string;
  private readonly f: typeof fetch;
  private leaseMs = 30_000;
  private hb: NodeJS.Timeout | null = null;
  private stopped = false;
  private readonly stopAc = new AbortController();
  private readonly o: ClientOptions;

  constructor(o: ClientOptions) {
    this.o = o;
    this.agentId = o.agentId;
    this.base = o.baseUrl.replace(/\/+$/, "");
    this.f = o.fetch ?? fetch;
  }

  private async post(name: string, extra: Record<string, unknown> = {}, signal?: AbortSignal): Promise<{ status: number; body: Record<string, unknown> }> {
    const sigs = [this.stopAc.signal, ...(signal ? [signal] : [])];
    const res = await this.f(`${this.base}/_pisync/${name}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        agentId: this.agentId,
        parentId: this.o.parentId ?? undefined,
        label: this.o.label,
        pid: this.o.pid,
        ...extra,
      }),
      signal: AbortSignal.any(sigs),
    });
    const body = (await res.json().catch(() => ({}))) as Record<string, unknown>;
    return { status: res.status, body };
  }

  async register(): Promise<void> {
    const { status, body } = await this.post("register");
    if (status !== 200) throw new AcquireError("register_failed", `register failed: HTTP ${status}`);
    if (typeof body.leaseMs === "number") this.leaseMs = body.leaseMs;
  }

  /** Register and keep the lease alive until stop(). */
  async start(): Promise<void> {
    this.stopped = false;
    await this.register();
    if (this.hb) clearInterval(this.hb);
    this.hb = setInterval(
      () => {
        this.post("heartbeat")
          .then((r) => (r.status === 404 ? this.register() : undefined))
          .catch((e) => this.o.onError?.(e as Error));
      },
      Math.max(500, Math.floor(this.leaseMs / 3)),
    );
    this.hb.unref?.();
  }

  /**
   * Block until this agent's task is the active one. Long-polls; survives
   * daemon restarts. Throws AcquireError on expiry/queue timeout, or if aborted.
   */
  async acquire(signal?: AbortSignal): Promise<void> {
    let downSince = 0;
    for (;;) {
      if (signal?.aborted || this.stopped) throw new AcquireError("aborted", "acquire aborted");
      try {
        const { status, body } = await this.post("acquire", {}, signal);
        downSince = 0;
        if (status === 200 && body.granted === true) return;
        if (status === 200) {
          this.o.onWaiting?.({ position: Number(body.position ?? 0), queueLength: Number(body.queueLength ?? 0) });
          continue;
        }
        const code = String(body.error ?? `http_${status}`);
        // A daemon that is shutting down / lost us is transient: ride it out like a network error.
        if (code === "shutdown" || code === "agent_gone" || (status >= 500 && !body.error)) throw new TransientError(code);
        throw new AcquireError(code, String(body.message ?? `acquire failed: HTTP ${status}`));
      } catch (e) {
        if (e instanceof AcquireError) throw e;
        if (e instanceof TransientError) await Promise.resolve();
        if (signal?.aborted || this.stopped) throw new AcquireError("aborted", "acquire aborted");
        // network error: daemon down / restarting. Wait and retry.
        this.o.onError?.(e as Error);
        downSince ||= Date.now();
        if (Date.now() - downSince >= (this.o.maxOutageMs ?? 30_000)) {
          throw new AcquireError("daemon_unreachable", `pisyncd unreachable at ${this.base} for ${Math.round((Date.now() - downSince) / 1000)}s`);
        }
        await sleep(this.o.retryDelayMs ?? 1000, signal);
      }
    }
  }

  /** The agent's task is over: hand the slot to the next one. */
  async release(): Promise<void> {
    await this.post("release").catch((e) => this.o.onError?.(e as Error));
  }

  async stop(): Promise<void> {
    if (this.hb) clearInterval(this.hb);
    this.hb = null;
    if (this.stopped) return;
    // unregister before aborting in-flight calls
    await this.post("unregister").catch(() => {});
    this.stopped = true;
    this.stopAc.abort();
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        resolve();
      },
      { once: true },
    );
  });
}
