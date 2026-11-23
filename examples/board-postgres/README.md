# Board over Postgres, across processes

[index.ts](index.ts) is a lead, and [worker.ts](worker.ts) is a worker. They share one board in Postgres through `nanocode-ts/postgres`, as processes on separate machines would. Every process opens its own connection pool with `Sql.open(DATABASE_URL, { schema, max: 2 })` and its own actor (`agent:lead`, `agent:w1` and so on), and they meet only in the database.

The lead charters the board, lets every `agent:` actor in, and posts a small graph: `plan`, then three fetches labelled `agent=llm`, then `summarize`. It starts three worker processes. Each runs one `Board.pool` with `concurrency: 1`, a 1.5 s lease and `cap: { max: 2, label: ['agent', 'llm'] }`, so at most two fetches are held at once across all three processes.

When a worker starts `fetch-b`, the lead waits 300 ms and kills it with SIGKILL. The process gets no chance to release its claim, so `fetch-b` stays held until its lease lapses. Another worker then claims it and its lease reports `tookOver: true`. Once `summarize` is held, the lead steers it, and its worker reads the steer and heeds it before finishing. The lead then stops the surviving workers and prints a timeline, the worker each task finished under (`Task.worker`) and each pool's counts. It works in a fresh schema and drops it at the end.

## Run it

It needs `DATABASE_URL` pointing at a Postgres database it may create a schema in. Start a throwaway Postgres 16 in a temp directory from its binaries (the path is Debian and Ubuntu's; Homebrew puts them under `$(brew --prefix postgresql@16)/bin`):

```sh
BIN=/usr/lib/postgresql/16/bin
PG=$(mktemp -d)
$BIN/initdb -D "$PG/data" -U postgres -A trust
$BIN/pg_ctl -D "$PG/data" -l "$PG/log" -w start \
  -o "-c listen_addresses=127.0.0.1 -c port=54329 -c unix_socket_directories=$PG"
DATABASE_URL=postgres://postgres@127.0.0.1:54329/postgres npm run example:board-postgres
$BIN/pg_ctl -D "$PG/data" -m fast stop && rm -rf "$PG"
```

Or with Docker:

```sh
docker run --rm -d --name board-pg -e POSTGRES_HOST_AUTH_METHOD=trust -p 54329:5432 postgres:16
DATABASE_URL=postgres://postgres@127.0.0.1:54329/postgres npm run example:board-postgres
docker stop board-pg
```

Run `npm install` first, which fills `dist/`. worker.ts takes its URL, schema, scope and name as one JSON argument, so the same file can run on another machine against the same server.

## Output

This output is from Node 22.23 and Postgres 16.15 on Linux:

```text
+  113ms  lead  post   plan
+  115ms  lead  post   fetch-a
+  116ms  lead  post   fetch-b
+  117ms  lead  post   fetch-c
+  118ms  lead  post   summarize
+  379ms  w1    start  plan
+  580ms  w1    done   plan
+  586ms  w1    start  fetch-a
+  618ms  w2    start  fetch-b
+  917ms  lead  kill   w2 (SIGKILL) while it holds fetch-b
+ 1587ms  w1    done   fetch-a
+ 1590ms  w1    start  fetch-c
+ 2122ms  lead  lapse  fetch-b: its lease ran out
+ 2145ms  w3    start  fetch-b (took over)
+ 2591ms  w1    done   fetch-c
+ 3145ms  w3    done   fetch-b
+ 3151ms  w3    start  summarize
+ 3160ms  lead  steer  summarize
+ 3952ms  w3    steer  summarize: keep it to two lines
+ 3954ms  w3    done   summarize
+ 3959ms  lead  peak   2 agent=llm tasks held at once (cap 2)

plan       done  agent:w1  {"done":"w1 finished plan"}
fetch-a    done  agent:w1  {"done":"w1 finished fetch-a"}
fetch-b    done  agent:w3  {"done":"w3 finished fetch-b"}
fetch-c    done  agent:w1  {"done":"w1 finished fetch-c"}
summarize  done  agent:w3  {"done":"w3 finished summarize"}

w1 claimed 3, done 3, lost 0, steers 0, errors 0
w2 killed
w3 claimed 2, done 2, lost 0, steers 1, errors 0
```

w2 is killed 300 ms into `fetch-b` and its lease runs out about 1.2 s later. w3 takes the task over and finishes it, and `fetch-b` ends up with `agent:w3` as its worker. The cap shows while w2's claim is still live: w1 and the dead w2 hold two `agent=llm` tasks, so w3 sits idle with `fetch-c` ready until `fetch-a` is done. The steer is sent while w3 holds `summarize`, and it is heeded before the finish. Which worker gets which task changes from run to run, and the `lapse` line shows only when one of the lead's polls lands between the lapse and the takeover.

## Soak

[soak.ts](soak.ts) runs the same setup under takeover chaos until a time limit. Each swarm posts 30 tasks to a fresh schema: a layered graph where each task needs up to two of the eight before it, about 40% labelled `agent=llm` (cap 2) and 20% `tool=browser` (cap 1). Six worker processes drain it, each a `Board.pool` with `concurrency: 2` and a 2 s lease. Swarms rotate between every worker woken by `Sql.wake`, every worker polling, and half of each. While a swarm runs, the driver:

- SIGKILLs a random worker every 2-8 s and starts a new one,
- SIGSTOPs a worker that holds a task for 2.5-5 s, longer than its lease, then SIGCONTs it, so it wakes after losing the lease,
- restarts Postgres with `pg_ctl restart -m fast` in about 30% of swarms,
- steers a held task now and then.

After each swarm it audits from a fresh connection. It replays the raw log and checks that every task finished exactly once with `done`, that no claim came before its needs were done or while another lease was live, that no claim took a label past its cap, and that every renew and finish carried the live claim's id, so a resumed holder never finished on a claim another worker had taken over. It also checks that each task's `Task.worker` is the actor whose finish landed, that nothing is still claimed after the leases lapse, and that no `soak_` schema is left. On a violation it stops, keeps the schema, writes the timeline, raw log and violations to `output/board-postgres-soak/`, and prints a `SOAK_SEED=… SOAK_SWARMS=1` line that replays the same graph and chaos choices (the timing still varies).

```sh
PATH=$BIN:$PATH DATABASE_URL=postgres://postgres@127.0.0.1:54329/postgres \
  SOAK_PGDATA="$PG/data" SOAK_MINUTES=45 npm run example:board-postgres:soak
```

`SOAK_PGDATA` turns on the Postgres restarts and needs `pg_ctl` on `PATH` (or `SOAK_PG_CTL`). Each swarm prints one JSON line, and the run ends with a `totals` line.

Two runs on Node 22.23 and Postgres 16.15, pinned to 4 cores of a 48-core Linux box:

| | Run 1 | Run 2 |
| --- | --- | --- |
| Minutes | 25.0 | 25.2 |
| Swarms (30 tasks each) | 102 | 103 |
| Worker SIGKILLs | 214 | 192 |
| SIGSTOPs past the lease | 153 | 157 |
| Postgres restarts | 34 | 38 |
| Steers | 342 | 328 |
| Takeovers (after a Postgres restart) | 309 (31) | 326 (53) |
| Takeover latency p50 / p99 / max | 1778 / 4433 / 5854 ms | 1773 / 4356 / 6074 ms |
| Board violations | 0 | 0 |

Takeover latency runs from the SIGKILL or SIGSTOP of the holder to another worker's claim, so it starts with what was left of the 2 s lease. In run 2, 157 renews or finishes by resumed workers on the claims they held before the pause were refused (`NotHolder` while the new holder ran, `Terminal` once it had finished); 7 resumed workers finished their task because nobody had claimed it while they slept, and 1 claimed its task again after the next holder's lease lapsed too and finished on that new claim. The 725 pool errors were calls that failed while Postgres restarted; the pools retried them.

Run 1 stopped at swarm 102 on a false alarm from an earlier version of the resumed-holder check. A paused worker's renew of `t10` was refused with `NotHolder`; the worker that took `t10` over lost its own lease during a Postgres restart; the resumed worker then claimed `t10` afresh and finished on that new claim. The log audit had passed. The check now compares claim ids, and run 2 used it.

Where the takeover tail comes from, from a 20-minute run of an instrumented copy of soak.ts (90 swarms, 24 Postgres restarts, 204 takeovers after a kill or stop; p50 / p99 / max 1738 / 3272 / 4317 ms) that logged each lapsed lease's end and every pool's `ready` and `claim` calls. Latency is the lease left when the holder died plus the wait after it lapsed. The lease left is 1.33 to 2 s by construction, since a claim renews every `leaseMs / 3` (p50 1693 ms, max 1999 ms), and SIGKILL and SIGSTOP look the same (p50 1689 and 1695 ms): a stopped worker is dead to the board from its last renew. The wait after the lapse was at most one poll for 185 takeovers (91%; p50 20 ms, max 93 ms). In every case a pool with a free slot listed the task within 80 ms, so no takeover waited on all slots being busy. `Sql.wake` made no difference (p50 39 ms woken, 19 ms polling), because a lapse writes nothing and so sends no notification. The other 19 (9%) waited 0.5 to 2.4 s behind a label cap: the lapse freed a cap slot, a fresh `agent=llm` or `tool=browser` task took it, and every claim on the lapsed task was refused `Full` until a capped run finished. Those 19 are every takeover above p95 and all three at p99; cut to one poll, the p99 would be 2076 ms. Restarts kept Postgres down for about 200 ms (p50 206, max 307 ms), postgres.js reconnected within its first backoff steps, and none of the 35 takeovers that overlapped one waited longer for it. A 14-minute run with the pool claiming `lapsed` tasks before others (not in this code) had p99 2215 ms and max 2365 ms, with 5 of 159 takeovers waiting on a cap. Both runs stopped early on a false alarm from the resumed-holder check: a worker paused twice in one swarm retook its first task over a later lapse and finished it on the new claim, but the check compares that retake with the second pause's time.
