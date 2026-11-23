# Board

The Board is a shared task list for agents. Several processes, or several agents in one process, post tasks, claim them under a lease and finish them, all through one append-only log in SQL. This page is the nanocode-ts API. For how the log folds into tasks, see [nanocode's docs/board.md](https://github.com/nanocodexyz/nanocode/blob/master/docs/board.md).

A write the board refuses does not throw. It returns `{ ok: false, rejection }`, and `rejection.kind` says why (`Waiting`, `Held`, `NotHolder`, `Full`, `Steer`, `UnknownNeed` and so on). Check `ok` after every write.

## Open a board

`Board.open({ sql, scope, actor, partitions?, module?, charter? })` opens one handle over a `SqlHost`. `scope` names the board and `actor` is who writes (`agent:lead`), so give each worker its own handle. A handle charters the scope when it opens (`charter: false` skips that) and can let others in with `grant`.

Node, a SQLite file several processes can share (needs `node:sqlite`, Node 22.5+):

```ts
import { Board, Sql } from 'nanocode-ts/node'

const sql = Sql.open('board.sqlite') // WAL; a write waits up to busyTimeoutMs (10 s) for the lock
const board = await Board.open({ sql, scope: 'demo', actor: 'agent:lead' })
```

Cloudflare, inside a SQLite-backed Durable Object (pass the wasm module, as the Worker bundle imports it):

```ts
import { Board } from 'nanocode-ts'
import { Sql } from 'nanocode-ts/cloudflare'
import wasm from 'nanocode-ts/nanocode_wasm.wasm'

const board = await Board.open({
  sql: Sql.create(this.ctx.storage),
  scope: 'demo',
  actor: 'agent:lead',
  module: wasm,
})
```

Postgres, so workers on several machines share one board (needs the optional peer dependency `postgres`):

```ts
import { Board, Sql } from 'nanocode-ts/postgres'

const sql = Sql.open(process.env.DATABASE_URL!, { schema: 'board', max: 2 })
const board = await Board.open({ sql, scope: 'demo', actor: 'agent:lead' })
```

`Sql.open(url, { schema?, max?, valueLimit?, notifyMs?, backoff? })` opens a connection pool. `schema` runs every statement in that schema and creates it if missing, `max` is the pool size and `valueLimit` caps a stored value. `backoff(n)` gives the seconds to wait before the nth reconnect attempt; the default, `Sql.reconnect`, is `Math.min(0.05 * 2 ** n, 1)` (0.1, 0.2, 0.4, 0.8 s, then every second), so a pool is back within a few hundred ms of a Postgres restart, where postgres.js's own backoff grows as 3^n / 100 s with jitter up to 20 s. It also exports `Board`.

After each committed write to the board's log, a Postgres host sends `pg_notify('nanocode_log', '<schema>/<host id>')`, at most once every `notifyMs` (default 10). `Sql.wake(sql, { debounceMs? })` listens on that channel and returns a `wake` for `Board.pool`, so pools on every machine poll as soon as work lands, once per `debounceMs` burst (default 5). Boards in other schemas don't wake it, and a host skips its own notifications and wakes its own pools directly. A pool's own claims, renewals and heeds wake no pool, since they never make work ready, and its finishes and releases wake every pool but itself; posts and other writes wake them all. A lapsed lease writes nothing, so `pollMs` finds it, or with `wake` the pool's timer at the soonest lease end it sees (see [Board.pool](#boardpool)). In `test/board-pool-wake.test.ts`, two processes with `pollMs: 5000` drain 20 tasks in 74–80 ms with wake and in 4,976–5,001 ms without. With busy pools (`bench:board --sql postgres --workers 8 --wake on`), wake helps 8 partitions: task p50 / p99 go from 365 / 1,467 to 310 / 967 ms at `--poll-ms 1000`, and from 322 / 1,169 to 305 / 851 ms at 25. At 1 partition it costs about 15% of claims/s, and p50 / p99 go from 448 / 1,414 to 461 / 1,044 ms at 1000 and from 364 / 1,105 to 448 / 1,121 ms at 25. So leave it off for one partition at high load ([bench/RESULTS.md](../bench/RESULTS.md)). Skipping a pool's own writes cuts the polls the wake adds by 63–82% in three of the four bench cells. On a heavily loaded host, six rounds per cell could not resolve whether that changes claims/s or p99 (round C there).

For tests, `await Board.memorySql()` is an in-memory host (Node only). Share one host between handles to share a board. `board.close()` closes a handle, and `sql.close()` closes the Node, memory or Postgres host.

### Choosing a backend

On one host, SQLite is faster. Postgres writes take 0.36 ms (p50) against SQLite's 0.05 ms, so workers lose more append races. Measured with 8 workers on one partition, Postgres on loopback: 669 claims/s on SQLite and 454 on Postgres (task p99 519 and 1,275 ms; [DECISIONS.md](../DECISIONS.md), 'Board on Postgres'). Use Postgres when workers run on different machines. There partitions help: 8 partitions take it from 454 to 524 claims/s. On one SQLite file they cost throughput: [bench/RESULTS.md](../bench/RESULTS.md) shows 773, 742 and 625 claims/s at 1, 4 and 8 partitions with 8 workers.

## Post

```ts
await board.post(
  {
    id: 'summarize',
    spec: 'summarize both halves',
    needs: ['fetch-a', 'fetch-b'],
    terms: { retry: { maxAttempts: 2, backoffMs: 50 } },
  },
  { key: 'post:summarize' },
)
```

- `needs` are task ids that must be done first. Claiming earlier is `Waiting`, and `rejection.value` is the unmet need. A need that does not exist is `UnknownNeed` and the task is not posted.
- `terms.retry` turns a `{ failed }` finish into a rescheduled task while attempts remain, with the backoff doubling each time. The other terms are `assignee`, `dueAtMs`, `every`, `cron` and `onFail`.
- `key` makes a write safe to repeat: the same key returns the first verdict (same `seq`) instead of writing again. A repost of an identical task is accepted as a no-op, and a different `spec` under an existing id is `Exists`.

`NewTask` also takes `parent` and `labels`. `cancel(id, reason)` and `reply(id, body)` act on a task.

## Claim, renew, release, finish

```ts
const claimed = await board.claim('summarize', { leaseMs: 30_000 })
if (!claimed.ok) return // Waiting, Held, Full, ...
const { claim, untilMs, tookOver } = claimed
await board.renew('summarize', claim, 30_000)
await board.release('summarize', claim, 'giving it back')
await board.finish('summarize', claim, { done: 'two lines' }) // or { failed: 'why' }
```

A claim holds the task until `untilMs`, and the holder renews to keep it. Once the lease lapses, anyone can claim the task and gets `tookOver: true`. The old holder's renew, release and finish are then refused (`NotHolder`), so a stalled worker cannot overwrite its replacement. Every call takes an optional `atMs` for tests.

`cap` bounds concurrency. `claim(id, { leaseMs, cap: 4 })` is refused with `Full` (and `rejection.cap`) when 4 other tasks are held, counting only live leases and only within one partition. `cap: { max: 2, label: ['pool', 'lent'] }` counts only held tasks labelled `pool=lent`, whatever the claimed task's own labels, so each label value gets its own limit on one partition. `rejection.cap` has the same shape as the cap that refused the claim.

A finished task keeps the worker that finished it: `task.worker` on a `done` or `failed` task is the actor whose claim finished it.

## Steer and heed

`steer(id, text)` sends a message to a task. The holder reads it with `steers(id, claim)`, which returns `{ seq, by, body, atMs, to }`, and acknowledges it with `heed(id, claim, seq)`, up to and including `seq`. While a steer is unheeded, `finish` is refused with `Steer`, so a message sent mid-run is never skipped.

## Reads

`ready()`, `task(id)`, `tasks()` and `held()` read the local fold, the log as this handle has seen it. Your own writes show at once. Another process's show up only after `await board.refresh()`. `ready({ label: [key, value] })` filters by label.

## Board.work

```ts
const outcome = await board.work('summarize', { leaseMs: 30_000 }, async (lease, steers) => {
  // lease.signal aborts if the lease is lost
  return { done: 'two lines' }
})
```

`work` claims the task, renews every third of the lease, hands `step` the steers pending at the claim, then heeds the last one and finishes with what `step` returned. It throws `LeaseLost` if the claim, a renewal or the finish is refused.

## Board.pool

`Board.pool` is a worker pool: it refreshes, claims ready tasks, runs your function, renews, heeds and finishes.

```ts
const pool = Board.pool(board, {
  concurrency: 2, // runs at once
  leaseMs: 30_000,
  pollMs: 100,
  run: async (task, lease) => {
    for (const steer of await lease.steers()) await lease.heed(steer.seq)
    return { done: 'ok' } // or { failed: 'why' }; a throw is a failure
  },
})
```

`label: [key, value]` limits the pool to tasks with that label, and `idleMs` retires it after that long with nothing to do. `lease` has `signal`, `tookOver`, `steers()` and `heed(seq)`. A run that did not heed its steers still finishes: the pool heeds the last one and retries. `pool.idle()` waits for a poll that finds nothing running or ready, `pool.stats()` counts `claimed`, `done`, `failed`, `lost`, `released`, `delivered`, `unheard`, `retries` and `errors`, and `pool.stop()` aborts the runs and releases their claims. `cap` is a number, a `{ max, label }` cap, or a function from the task to one (or `undefined`), and the pool claims each task under it. A label cap applies only to tasks that carry that label. A refused claim is skipped. Each poll tries lapsed tasks before open ones, so when a cap slot frees, a dead holder's task gets it ahead of fresh work.

`wake: (notify) => unsubscribe` makes the pool poll as soon as `notify` fires, and `pollMs` stays the fallback. A pool with every slot running ignores a wake (a run that ends polls anyway), and the pool unsubscribes when it stops or retires. A lapse writes nothing, so no notification announces it: with `wake` set and a free slot, the pool also polls 5 ms after the soonest lease it sees on another holder's task ends, when that comes before `pollMs`. With `pollMs: 5000`, a SIGKILLed holder's task is taken over within about its lease plus 100 ms (`test/board-pool-wake.test.ts`). On Postgres pass `wake: Sql.wake(sql)` (see [Choosing a backend](#choosing-a-backend)). node:sqlite has no cross-process notify, so leave `wake` unset there and the pool keeps polling.

## Partitions

`partitions: 4` splits the log into 4 (the default is 1). A task lives in one partition: its `thread` or else `project` label picks it, a child follows its parent, and otherwise its id does. `needs` resolve inside the task's partition, so a need on a task in another partition is `UnknownNeed`. Give tasks that depend on each other a shared `project` label or a parent. A task cannot move between partitions.

## Examples

Run each with `npm run example:<name>` after `npm install`. The [board-pool](../examples/board-pool/index.ts) lead posts a four-task graph while two `Board.pool` workers drain it on one SQLite file, with one steer. The others add:

- [board-agents](../examples/board-agents/README.md): two pools run a six-task graph with `cap: { max: 2, label: ['kind', 'agent'] }` and prints each task's `Task.worker`.
- [board-durable-object](../examples/board-durable-object/README.md): a Worker whose Durable Object holds a `Board` over `Sql.create(ctx.storage)`, served by workerd, with a takeover and a label cap.
- [board-postgres](../examples/board-postgres/README.md): a lead and three worker processes share one board over Postgres with `nanocode-ts/postgres`. A killed worker's task is taken over once its lease lapses. It needs `DATABASE_URL`.

