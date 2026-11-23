# Agents on a Board

In [index.ts](index.ts), a planner posts a six-task graph on one SQLite file (`Sql.open` from `nanocode-ts/node`) and two `Board.pool` workers drain it. `plan` runs first, then three `notes-*` tasks beside a `check-links` task, then `assemble`.

When `ANTHROPIC_API_KEY` is set, each `kind=agent` task runs a nanocode-ts agent turn: an `AgentRuntime` session in memory per task, given the outcomes of the tasks it needs, on the model in `NANOCODE_MODEL` (default `claude-haiku-5-5`). Without a key those tasks run a deterministic stand-in, so the example needs no keys. `kind=tool` tasks always run the stand-in.

A label cap of `{ max: 2, label: ['kind', 'agent'] }` allows at most two `kind=agent` tasks held at once across both pools. Both pools pass it as `Board.pool`'s `cap` option, which applies it only to `kind=agent` tasks. The board counts every live lease on such tasks, whichever pool holds it. Since `check-links` is `kind=tool`, it runs beside two agent tasks.

At the end it prints each task's phase, the worker that finished it (`Task.worker`) and its outcome, then each pool's `stats()`.

Run it with `npm run example:board-agents` after `npm install`, which fills `dist/`. This output is from Node 22 with no key. Node also prints an ExperimentalWarning for `node:sqlite`, left out here:

```text
+   74ms  planner  no ANTHROPIC_API_KEY: kind=agent tasks run the fallback
+   75ms  planner  post   plan
+   76ms  planner  post   notes-claim
+   76ms  planner  post   notes-pool
+   76ms  planner  post   notes-worker
+   77ms  planner  post   check-links
+   77ms  planner  post   assemble
+   97ms  w1       start  plan         kind=agent  agents held 1/2
+  248ms  w1       done   plan
+  250ms  w1       start  check-links  kind=tool
+  250ms  w1       start  notes-claim  kind=agent  agents held 1/2
+  263ms  w2       start  notes-pool   kind=agent  agents held 2/2
+  450ms  w1       done   check-links
+  550ms  w1       done   notes-claim
+  551ms  w1       start  notes-worker kind=agent  agents held 2/2
+  563ms  w2       done   notes-pool
+  851ms  w1       done   notes-worker
+  852ms  w1       start  assemble     kind=agent  agents held 1/2
+ 1002ms  w1       done   assemble
+ 1017ms  planner  done   assemble     worker agent:w1  {"done":"fallback: Join the three lines into a short changelog entry."}
+ 1017ms  planner  done   check-links  worker agent:w1  {"done":"tool: Check the doc links resolve."}
+ 1017ms  planner  done   notes-claim  worker agent:w1  {"done":"fallback: Write one line on claim caps."}
+ 1017ms  planner  done   notes-pool   worker agent:w2  {"done":"fallback: Write one line on Board.pool."}
+ 1017ms  planner  done   notes-worker worker agent:w1  {"done":"fallback: Write one line on Task.worker."}
+ 1017ms  planner  done   plan         worker agent:w1  {"done":"fallback: Split release notes for the Board into sections."}
+ 1017ms  planner  at most 2 kind=agent tasks held at once
w1 {"state":"stopped","running":0,"claimed":5,"done":5,"failed":0,"lost":0,"released":0,"delivered":0,"unheard":0,"retries":0,"polls":47,"errors":0}
w2 {"state":"stopped","running":0,"claimed":1,"done":1,"failed":0,"lost":0,"released":0,"delivered":0,"unheard":0,"retries":0,"polls":46,"errors":0}
```
