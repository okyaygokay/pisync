# pisync

A local queue daemon (`pisyncd`) plus a [pi](https://github.com/earendil-works/pi) extension, for subscription plans that allow **one request at a time** while your project runs **many pi agents at once**.

```
 pi agent a ─┐                         ┌───────────────────────────┐
 pi agent b ─┼─►  pisyncd (localhost) ─┤ one TASK active at a time ├─► your plan's API
 pi agent c ─┘    queue + retries      └───────────────────────────┘   (1 request in flight)
```

## How it works

**Tasks, not messages.** A *task* runs from "pi received a prompt" to "pi has fully settled" (`agent_settled`: no retry, compaction or queued continuation pending). It spans many LLM requests and the tool calls between them. Exactly one task is *active*; everyone else waits in a FIFO queue.

```
a requests            -> a active
b requests            -> a active, b queued
c requests            -> a active, b queued, c queued
a's TASK ends         -> b active, c queued
a gets a new message  -> b active, c queued, a queued (last)
b's TASK ends         -> c active (not a), a queued
```

Inside the active task there is still only ever **one request in flight** upstream, and the slot is held for the whole streamed response.

**The extension** does three things:

| pi event | what happens |
|---|---|
| `session_start` | registers with the daemon, starts a heartbeat |
| `before_agent_start`, `turn_start` | long-polls the daemon until this agent's task is the active one (status bar shows `pisync: waiting (#2 of 3)`) |
| `agent_settled` | tells the daemon the task is over → next in line starts |
| `session_shutdown` | unregisters (a crashed agent is reaped when its heartbeat lease expires) |

and it points the providers listed in `upstreams` at the daemon (`registerProvider(name, { baseUrl })`, which keeps pi's built-in models and your existing `/login` credentials). Waiting happens in the extension *before* the first request, so pi's own HTTP timeouts never fire while you sit in the queue.

**The daemon** is a streaming reverse proxy (no dependencies, plain `node:http`): it buffers the request body (needed for retries), waits for the agent's turn, forwards with keep-alive upstream, and pipes the response straight back.

### Master agent + sub-agents ("CEO" pattern)

A master that spawns workers and waits for them would hold the slot while idle, and the workers could never run: a deadlock. pisync solves it in two parts:

1. **Discovery.** On start the extension exports `PISYNC_PARENT_ID=<its agent id>` into its own process environment. Anything it spawns (bash tool, `child_process`, RPC children, nested `pi -p`) inherits that variable, so the child's extension registers itself as a child of the master, to any depth. If your orchestrator launches workers some other way, set the variables yourself:
   * `PISYNC_SELF_ID=master-1` — fixed id for this pi process (removed from the env so it is not inherited)
   * `PISYNC_PARENT_ID=master-1` — declare the parent of this pi process
2. **Yield.** If the active task is *idle* (no request pending or in flight, e.g. it is blocked on a tool waiting for its workers) and one of its descendants is queued, the master is **suspended**: it gives up the slot, the queue continues in FIFO order, and the master's next request puts it back in the queue (ahead of anyone who arrives later by default - at the **front** - see `queue.parentResume`; agents already waiting keep their turn) once the workers are done. A master that is mid-request never yields.

### Failures, retries, timeouts

* Up to **25 retries** (`retry.maxRetries`) per request with exponential backoff + jitter; `Retry-After` is honored (capped). Retried: network errors/resets, connect/header/idle timeouts, and HTTP 408, 409, 425, 429, 500, 502, 503, 504, 520–524, 529 (list is configurable). Also retried: an HTTP-200 event-stream whose very first event is an error such as Anthropic's `overloaded_error` (not for caller errors like `invalid_request_error`).
* The slot is kept while retrying, so nobody sneaks in between attempts and the retried request keeps its place.
* After the last attempt the final upstream response is forwarded unchanged (or a `502` JSON error if the upstream was unreachable).
* Retrying is only possible until the first response byte has been forwarded. A stream that dies half-way cannot be replayed transparently; the connection to pi is cut and pi's own retry logic takes over.
* A client that disconnects (pi aborted with Esc) cancels the upstream request and frees the slot immediately.

### Hard limits (3 hours by default)

| setting | default | meaning |
|---|---|---|
| `task.maxDurationMs` | 3 h | hard limit on how long **one task may hold the slot**. At the limit it is sent to the back of the queue (`onMaxDuration: "requeue"`) or its requests fail with HTTP 403 (`"reject"`) |
| `queue.maxWaitMs` | 3 h | hard limit on how long a task may **wait in the queue**; afterwards its requests fail with `503` + `Retry-After` |
| `agent.leaseMs` | 30 s | an agent whose extension stops heartbeating (crash/kill -9) is reaped and its slot passed on |
| `task.activeIdleTimeoutMs` | off | optional: demote an active task that issues no request for this long (hung tools) |

## Install

One command (Node ≥ 22.19, same as pi):

```bash
pi install git:github.com/okyaygokay/pisync
```

That is all. The extension starts `pisyncd` in the background the first time a pi needs it (detached, logs to `~/.pisync/daemon.log`) and every other pi on the machine shares that one daemon. No build step, no config file required: defaults route `anthropic`, `openai-codex` and `openai` through the daemon.

Make sure the extension is loaded by **every** pi that takes part, sub-agents included (a user-level install covers all of them). To remove it: `pi remove git:github.com/okyaygokay/pisync`; to stop the daemon: `pkill -f "pisyncd|cli.ts start"` or `kill $(pisyncd status | jq .pid)`.

Optional: copy `pisync.config.example.json` to `~/.config/pisync/config.json` (or `./pisync.config.json`) to change limits, retries, upstreams or the port. Set `"autoStart": false` if you prefer to run the daemon yourself (`node dist/src/cli.js start` after `npm install && npm run build`, or `npm link` for a `pisyncd` command).

Config lookup (daemon and extension, first hit wins): `--config`, `$PISYNC_CONFIG`, `./pisync.config.json`, `~/.config/pisync/config.json`, `~/.pi/agent/pisync.json`. `pisyncd print-config` shows the effective values; invalid configs are rejected with a list of every problem.

Other env vars (extension): `PISYNC_URL` (daemon URL override), `PISYNC_DISABLE=1`, `PISYNC_NO_AUTOSTART=1`, `PISYNC_LABEL`, `PISYNC_OUTAGE_MS` (how long to wait for an unreachable daemon before aborting the run; default 30000).

> If you use `HTTP_PROXY` for plain HTTP, add `127.0.0.1` to `NO_PROXY` so pi can reach the daemon.
> When spawning pi in print mode (`pi -p`) from a script, close its stdin (`< /dev/null` / `stdio: "ignore"`), otherwise pi waits for piped input.

## Configuration

All knobs live in the JSON config file; see [`pisync.config.example.json`](pisync.config.example.json) for every option with its default.

* `upstreams` — pi provider name → real base URL. **Every provider listed here is routed through the daemon**, so list only the ones on your single-request plan. Add e.g. `"my-provider": "https://…"`. Keep the same path prefix pi would use (`…/v1` for OpenAI-compatible endpoints).
* `retry.*`, `timeouts.*` (`connectMs`, `headersMs`, `idleMs`, `requestMs`), `queue.*`, `task.*`, `agent.*`, `listen.*`, `logLevel`.

## Operating it

```bash
pisyncd status        # or: curl localhost:7878/_pisync/status
```

shows the active task, the queue, suspended parents, per-agent state and retry/response counters. `GET /healthz` for liveness. Logs are JSON lines on stderr.

### Using it without the extension

Any client can use `http://127.0.0.1:7878/<upstream>/…` (e.g. `…/anthropic/v1/messages`). Requests without an agent id are served strictly FIFO, one at a time. Clients that send an `x-pisync-agent` header (or use `/a/<agentId>/<upstream>/…`) are treated as one agent whose task ends `task.implicitIdleMs` after its last request.

### Control API (used by the extension)

`POST /_pisync/{register,acquire,release,heartbeat,unregister}` with `{ "agentId", "parentId?", "label?", "pid?" }`. `acquire` long-polls (`agent.pollMs`) and answers `{granted:true}` or `{granted:false, position, queueLength}`; it also registers unknown agents, so a restarted daemon recovers on its own.

## Development

```bash
npm test            # ~180 tests: unit, daemon/proxy, and ~30 end-to-end tests that drive real pi processes
npm run typecheck
npm run build
```

The scheduler (`src/scheduler.ts`) is a synchronous, deterministic state machine with O(1) queue operations (including the "does my active task have a queued descendant" check), covered by unit tests, randomized invariant tests (never two active, never two in flight) and a 20k-task benchmark. `test/e2e-pi.test.ts` drives real `pi` processes against a fake Anthropic upstream, including a master that spawns a sub-agent through the bash tool.
