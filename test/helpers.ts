/** Let pending promise continuations run. */
export async function flush(n = 5): Promise<void> {
  for (let i = 0; i < n; i++) await Promise.resolve();
}

export interface Probe<T> {
  settled: boolean;
  value?: T;
  error?: Error;
  promise: Promise<T>;
}

/** Track a promise without awaiting it. */
export function probe<T>(p: Promise<T>): Probe<T> {
  const o: Probe<T> = { settled: false, promise: p };
  p.then(
    (v) => {
      o.settled = true;
      o.value = v;
    },
    (e) => {
      o.settled = true;
      o.error = e as Error;
    },
  );
  return o;
}

/** Fire-and-forget: attach a no-op catch so teardown rejections are not "unhandled". */
export function bg<T>(p: Promise<T>): Promise<T> {
  p.catch(() => {});
  return p;
}
