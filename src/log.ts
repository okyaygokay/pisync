export type Level = "debug" | "info" | "warn" | "error" | "silent";
const ORDER: Record<Level, number> = { debug: 10, info: 20, warn: 30, error: 40, silent: 99 };

export interface Logger {
  debug(msg: string, data?: Record<string, unknown>): void;
  info(msg: string, data?: Record<string, unknown>): void;
  warn(msg: string, data?: Record<string, unknown>): void;
  error(msg: string, data?: Record<string, unknown>): void;
}

/** JSON-lines logger. `sink` defaults to stderr. */
export function createLogger(level: Level, sink: (line: string) => void = (l) => process.stderr.write(l + "\n")): Logger {
  const min = ORDER[level];
  const make = (lvl: Exclude<Level, "silent">) => (msg: string, data?: Record<string, unknown>) => {
    if (ORDER[lvl] < min) return;
    sink(JSON.stringify({ t: new Date().toISOString(), level: lvl, msg, ...data }));
  };
  return { debug: make("debug"), info: make("info"), warn: make("warn"), error: make("error") };
}

export const nullLogger: Logger = { debug() {}, info() {}, warn() {}, error() {} };
