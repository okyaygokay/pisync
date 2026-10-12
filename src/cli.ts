#!/usr/bin/env node
import { ConfigError, loadConfig } from "./config.ts";
import { createDaemon } from "./daemon.ts";
import { createLogger } from "./log.ts";

const USAGE = `pisyncd - task-level request queue daemon for pi agents

Usage:
  pisyncd [start] [--config <file>] [--port <n>] [--host <h>]
  pisyncd status [--config <file>]
  pisyncd print-config [--config <file>]

Config search order: --config, $PISYNC_CONFIG, ./pisync.config.json,
~/.config/pisync/config.json, ~/.pi/agent/pisync.json
`;

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(name);
  return i === -1 ? undefined : process.argv[i + 1];
}

async function main(): Promise<void> {
  const cmd = process.argv[2] && !process.argv[2].startsWith("-") ? process.argv[2] : "start";
  if (process.argv.includes("--help") || process.argv.includes("-h") || cmd === "help") {
    process.stdout.write(USAGE);
    return;
  }
  const { config, path } = loadConfig(arg("--config"));
  const port = arg("--port");
  const host = arg("--host");
  if (port !== undefined) config.listen.port = Number(port);
  if (host !== undefined) config.listen.host = host;

  if (cmd === "print-config") {
    process.stdout.write(JSON.stringify({ source: path, config }, null, 2) + "\n");
    return;
  }
  if (cmd === "status") {
    const res = await fetch(`http://${config.listen.host}:${config.listen.port}/_pisync/status`);
    process.stdout.write(JSON.stringify(await res.json(), null, 2) + "\n");
    return;
  }
  if (cmd !== "start") {
    process.stderr.write(USAGE);
    process.exitCode = 2;
    return;
  }

  const log = createLogger(config.logLevel);
  log.info("config loaded", { path: path ?? "(defaults)" });
  const daemon = createDaemon({ config, logger: log });
  await daemon.listen();
  let stopping = false;
  const stop = (sig: string) => {
    if (stopping) process.exit(1);
    stopping = true;
    log.info("shutting down", { signal: sig });
    daemon.close().then(() => process.exit(0));
  };
  process.on("SIGINT", () => stop("SIGINT"));
  process.on("SIGTERM", () => stop("SIGTERM"));
}

main().catch((e) => {
  if (e instanceof ConfigError) process.stderr.write(e.message + "\n");
  else process.stderr.write(`pisyncd: ${(e as Error).stack ?? e}\n`);
  process.exitCode = 1;
});
