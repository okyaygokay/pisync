import { spawn } from "node:child_process";
import { existsSync, mkdirSync, openSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, sep } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));

export async function isUp(baseUrl: string, timeoutMs = 700): Promise<boolean> {
  try {
    const res = await fetch(`${baseUrl}/healthz`, { signal: AbortSignal.timeout(timeoutMs) });
    return res.ok && (await res.text()) === "ok";
  } catch {
    return false;
  }
}

export function isLocalUrl(baseUrl: string): boolean {
  try {
    return ["127.0.0.1", "localhost", "::1", "[::1]", "0.0.0.0"].includes(new URL(baseUrl).hostname);
  } catch {
    return false;
  }
}

/** Entry point of the daemon: TypeScript source when running from a checkout, else the compiled file. */
export function daemonEntry(): string | null {
  const ts = join(here, "cli.ts");
  const js = here.endsWith(join("dist", "src")) ? join(here, "cli.js") : join(here, "..", "dist", "src", "cli.js");
  // Node refuses to strip types below node_modules; compiled output is required there.
  if (existsSync(ts) && !ts.includes(`${sep}node_modules${sep}`)) return ts;
  if (existsSync(js)) return js;
  return null;
}

export interface EnsureOptions {
  baseUrl: string;
  configPath?: string | null;
  /** How long to wait for a freshly spawned daemon to answer. */
  startupTimeoutMs?: number;
  logFile?: string;
}

/**
 * Make sure a daemon answers on baseUrl. Starts a detached one if needed.
 * Safe to call from many pi processes at once: losers of the port race exit
 * with EADDRINUSE and everyone ends up talking to the winner.
 */
export async function ensureDaemon(o: EnsureOptions): Promise<{ started: boolean }> {
  if (await isUp(o.baseUrl)) return { started: false };
  if (!isLocalUrl(o.baseUrl)) throw new Error(`pisyncd is not reachable at ${o.baseUrl}`);
  const entry = daemonEntry();
  if (!entry) throw new Error("cannot locate pisyncd (run `npm run build` in the pisync directory)");

  const url = new URL(o.baseUrl);
  const args = [entry, "start", "--host", url.hostname === "localhost" ? "127.0.0.1" : url.hostname.replace(/^\[|\]$/g, ""), "--port", url.port || "80"];
  if (o.configPath) args.push("--config", o.configPath);
  const logFile = o.logFile ?? join(homedir(), ".pisync", "daemon.log");
  mkdirSync(dirname(logFile), { recursive: true });
  const fd = openSync(logFile, "a");
  const child = spawn(process.execPath, args, { detached: true, stdio: ["ignore", fd, fd], windowsHide: true });
  child.on("error", () => {});
  child.unref();

  const deadline = Date.now() + (o.startupTimeoutMs ?? 10_000);
  while (Date.now() < deadline) {
    if (await isUp(o.baseUrl, 300)) return { started: true };
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`pisyncd did not come up at ${o.baseUrl} (see ${logFile})`);
}
