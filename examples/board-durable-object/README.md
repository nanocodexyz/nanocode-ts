# Board in a Durable Object

[worker.ts](worker.ts) is a Cloudflare Worker whose Durable Object holds a `Board` over its own SQLite storage, through `Sql.create(ctx.storage)` from `nanocode-ts/cloudflare`. Each client is an actor with its own handle inside the object. The Worker serves four routes:

| Route | Body | Does |
| --- | --- | --- |
| `POST /tasks` | `{ actor, task, key? }` | posts a task |
| `POST /claim` | `{ actor, id, leaseMs, cap? }` | claims it under a lease; `cap` is a number or `{ max, label: [key, value] }` |
| `POST /finish` | `{ actor, id, claim, outcome }` | finishes it with `{ done }` or `{ failed }` |
| `GET /tasks` | | lists every task with its stage and `worker` |

A refused write comes back as a reply, `{ ok: false, rejection }`, rather than an error.

[index.ts](index.ts) bundles the Worker with rolldown and serves it with workerd on a local Durable Object store, as `test/board-cloudflare.test.ts` does. It then drives the Worker as two clients. alice claims `index` for 300 ms and stalls. bob is refused while her lease is live and takes the task over once it lapses, so alice's late finish is refused. Then a cap of `{ max: 2, label: ['agent', 'llm'] }` holds at most two `agent=llm` tasks at once, whoever holds them. The run ends with `GET /tasks`, showing the worker of each finished task.

Run it with `npm run example:board-durable-object` after `npm install`, which fills `dist/`. This output is from Node 22:

```text
+  305ms  alice  post   index
+  310ms  alice  post   draft-a
+  316ms  alice  post   draft-b
+  319ms  alice  post   draft-c
+  322ms  alice  claim  index for 300ms
+  333ms  bob    claim  index refused: Held
+  737ms  bob    claim  index (took over) for 5000ms
+  742ms  alice  finish index refused: NotHolder
+  746ms  bob    finish index
+  750ms  alice  claim  draft-a for 5000ms
+  753ms  bob    claim  draft-b for 5000ms
+  754ms  bob    claim  draft-c refused: Full {"label":["agent","llm"],"max":2}
+  756ms  alice  finish draft-a
+  759ms  bob    claim  draft-c for 5000ms
+  762ms  bob    finish draft-b
+  765ms  bob    finish draft-c
+  778ms  GET    draft-a  done  worker agent:alice
+  779ms  GET    draft-b  done  worker agent:bob
+  779ms  GET    draft-c  done  worker agent:bob
+  779ms  GET    index    done  worker agent:bob
```
