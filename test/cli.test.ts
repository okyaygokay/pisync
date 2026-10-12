import { spawn } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const CLI = new URL("../src/cli.ts", import.meta.url).pathname;

function run(args: string[], cwd: string) {
  return new Promise<{ code: number | null; out: string; err: string }>((resolve) => {
    const p = spawn(process.execPath, [CLI, ...args], { cwd, env: { ...process.env, PISYNC_CONFIG: "" } });
    let out = "";
    let err = "";
    p.stdout.on("data", (d) => (out += d));
    p.stderr.on("data", (d) => (err += d));
    p.on("close", (code) => resolve({ code, out, err }));
  });
}

describe("pisyncd CLI", () => {
  it("print-config shows merged config", async () => {
    const dir = mkdtempSync(join(tmpdir(), "pisync-cli-"));
    writeFileSync(join(dir, "c.json"), JSON.stringify({ retry: { maxRetries: 31 } }));
    const r = await run(["print-config", "--config", "c.json"], dir);
    expect(r.code).toBe(0);
    expect(JSON.parse(r.out).config.retry.maxRetries).toBe(31);
  });

  it("fails with a readable message on an invalid config", async () => {
    const dir = mkdtempSync(join(tmpdir(), "pisync-cli-"));
    writeFileSync(join(dir, "c.json"), JSON.stringify({ retry: { jitter: 9 } }));
    const r = await run(["start", "--config", "c.json"], dir);
    expect(r.code).toBe(1);
    expect(r.err).toMatch(/jitter/);
  });

  it("starts, serves /healthz and shuts down cleanly on SIGTERM", async () => {
    const dir = mkdtempSync(join(tmpdir(), "pisync-cli-"));
    const p = spawn(process.execPath, [CLI, "start", "--port", "0"], { cwd: dir, env: { ...process.env, PISYNC_CONFIG: "" } });
    const port = await new Promise<number>((resolve, reject) => {
      let buf = "";
      p.stderr.on("data", (d) => {
        buf += d;
        const m = /"msg":"pisyncd listening".*"port":(\d+)/.exec(buf);
        if (m) resolve(Number(m[1]));
      });
      p.on("close", () => reject(new Error("exited early: " + buf)));
    });
    expect(await (await fetch(`http://127.0.0.1:${port}/healthz`)).text()).toBe("ok");
    const exit = new Promise<number | null>((r) => p.on("close", r));
    p.kill("SIGTERM");
    expect(await exit).toBe(0);
  });
});
