/**
 * pisync - pi extension.
 *
 * Routes the configured providers through the local pisyncd daemon and tells
 * it when this agent's TASK starts and ends, so the daemon can run one task
 * at a time (see README).
 *
 *   before_agent_start / turn_start  -> wait until our task is the active one
 *   agent_settled                    -> task over, hand the slot on
 *   session_shutdown                 -> unregister
 */
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { PisyncClient, newAgentId } from "../src/client.ts";
import { ensureDaemon } from "../src/autostart.ts";
import { loadConfig } from "../src/config.ts";

/** Children spawned by this process inherit this and become our sub-agents. */
const PARENT_ENV = "PISYNC_PARENT_ID";
/** Orchestrators may pin a stable id for the process (removed from env so children don't inherit it). */
const SELF_ENV = "PISYNC_SELF_ID";
const URL_ENV = "PISYNC_URL";
const DISABLE_ENV = "PISYNC_DISABLE";

export default function pisync(pi: ExtensionAPI) {
  if (process.env[DISABLE_ENV]) return;

  let cfg;
  let cfgPath: string | null = null;
  try {
    const loaded = loadConfig();
    cfg = loaded.config;
    cfgPath = loaded.path;
  } catch (e) {
    // Never take pi down because of our config.
    console.error(`[pisync] disabled: ${(e as Error).message}`);
    return;
  }

  const baseUrl = (process.env[URL_ENV] ?? `http://${cfg.listen.host === "0.0.0.0" ? "127.0.0.1" : cfg.listen.host}:${cfg.listen.port}`).replace(/\/+$/, "");
  const agentId = process.env[SELF_ENV] || newAgentId();
  delete process.env[SELF_ENV];
  const parentId = process.env[PARENT_ENV] || null;

  // Route providers through the daemon. The agent id is part of the path, so
  // no header support is needed from the provider implementation.
  for (const name of Object.keys(cfg.upstreams)) {
    pi.registerProvider(name, { baseUrl: `${baseUrl}/a/${encodeURIComponent(agentId)}/${name}` });
  }

  let client: PisyncClient | null = null;
  let setStatus: ((text: string | undefined) => void) | undefined;
  // Latest pi context we can use to abort a run from a background callback.
  let abortRun: (() => void) | undefined;
  let notify: ((m: string, t?: "info" | "warning" | "error") => void) | undefined;
  let expiredHandled = false;

  const ensureClient = (): PisyncClient => {
    if (client) return client;
    client = new PisyncClient({
      baseUrl,
      agentId,
      parentId,
      pid: process.pid,
      ...(process.env.PISYNC_OUTAGE_MS ? { maxOutageMs: Number(process.env.PISYNC_OUTAGE_MS) } : {}),
      label: process.env.PISYNC_LABEL ?? `pi:${process.cwd().split("/").pop()}`,
      onWaiting: ({ position, queueLength }) => setStatus?.(`pisync: waiting (#${position} of ${queueLength})`),
      onExpired: () => {
        // Hard time limit: the daemon already gave the slot away. Stop the agent,
        // including any tool call that is still running.
        if (expiredHandled) return;
        expiredHandled = true;
        notify?.("pisync: this task exceeded the hard time limit and was terminated", "error");
        abortRun?.();
      },
      onError: () => {
        /* daemon hiccups are retried; surfaced via status only */
      },
    });
    return client;
  };

  const acquire = async (ctx: { signal?: AbortSignal; abort?: () => void; ui: { setStatus(k: string, t: string | undefined): void; notify(m: string, t?: "info" | "warning" | "error"): void } }) => {
    setStatus = (t) => ctx.ui.setStatus("pisync", t);
    notify = (m, t) => ctx.ui.notify(m, t);
    abortRun = () => ctx.abort?.();
    try {
      await ensureClient().acquire(ctx.signal);
      setStatus(undefined);
    } catch (e) {
      setStatus(undefined);
      const code = (e as { code?: string }).code;
      if (code === "aborted") return;
      ctx.ui.notify(`pisync: ${(e as Error).message}`, "error");
      // The queue refused this task (wait limit / hard limit): stop the run instead of
      // letting its requests quietly re-queue and defeat the limit.
      ctx.abort?.();
      throw e;
    }
  };

  pi.on("session_start", async (_e, ctx) => {
    setStatus = (t) => ctx.ui.setStatus("pisync", t);
    notify = (m, t) => ctx.ui.notify(m, t);
    abortRun = () => ctx.abort();
    // Sub-agents spawned from here on (bash, subprocesses, RPC children...) inherit this.
    process.env[PARENT_ENV] = agentId;
    try {
      if (cfg.autoStart && !process.env.PISYNC_NO_AUTOSTART) await ensureDaemon({ baseUrl, configPath: cfgPath });
      await ensureClient().start();
    } catch (e) {
      ctx.ui.notify(`pisync: daemon unreachable at ${baseUrl} (${(e as Error).message}). Start it with \`pisyncd\` or set "autoStart": true.`, "warning");
    }
  });

  pi.on("before_agent_start", async (_e, ctx) => {
    await acquire(ctx);
  });

  // A task spans many turns; the daemon may have suspended us while sub-agents
  // ran, or terminated the task at the hard limit. Re-check cheaply before each turn.
  pi.on("turn_start", async (_e, ctx) => {
    await acquire(ctx);
  });

  pi.on("agent_settled", async () => {
    expiredHandled = false;
    await client?.release();
  });

  pi.on("session_shutdown", async () => {
    if (parentId === null) delete process.env[PARENT_ENV];
    else process.env[PARENT_ENV] = parentId;
    const c = client;
    client = null;
    await c?.stop();
  });
}
