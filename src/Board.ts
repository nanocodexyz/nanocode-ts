import * as AgentRuntime from './AgentRuntime.ts'
import type * as Contracts from './Contracts.ts'
import * as SqlReach from './SqlReach.ts'

export type Retry = { maxAttempts: number; backoffMs: number }

export type Terms = {
  assignee?: string
  dueAtMs?: number
  retry?: Retry
  every?: number
  cron?: { cron: string; offsetMinutes?: number }
  onFail?: 'cascade' | 'wait'
}

export type NewTask = {
  id: string
  parent?: string
  spec: string
  labels?: Record<string, string>
  needs?: string[]
  terms?: Terms
}

export type RejectionKind =
  | 'Exists'
  | 'UnknownTask'
  | 'UnknownParent'
  | 'UnknownNeed'
  | 'Waiting'
  | 'Held'
  | 'NotHolder'
  | 'Terminal'
  | 'Access'
  | 'Tasks'
  | 'Sync'
  | 'Intent'
  | 'Said'
  | 'Watch'
  | 'Steer'
  | 'Illegal'
  | 'Stale'
  | 'Full'
  | 'Unknown'

export type Rejection = {
  kind: RejectionKind
  claim?: string
  untilMs?: number
  cap?: Cap
  from?: string
  to?: string
  fence?: { partition: number; epoch: number }
  value?: unknown
}

export type Verdict =
  | { ok: true; event: string; found: boolean; seq: number; change: unknown }
  | { ok: false; rejection: Rejection }

export type Claimed =
  | { ok: true; claim: string; untilMs: number; tookOver: boolean; seq: number }
  | { ok: false; rejection: Rejection }

// A claim cap: at most max live leases on the task's partition, or only those whose task
// carries the label, so each label value is capped on its own.
export type Cap = number | { max: number; label: [string, string] }

export type Phase = 'open' | 'claimed' | 'done' | 'failed' | 'cancelled' | 'blocked'

export type Stage =
  | 'scheduled'
  | 'open'
  | 'held'
  | 'lapsed'
  | 'blocked'
  | 'stalled'
  | 'done'
  | 'failed'
  | 'cancelled'

export type Outcome = { done: string } | { failed: string }

export type Task = {
  id: string
  parent: string | null
  spec: string
  labels: Record<string, string>
  needs: string[]
  phase: Phase
  stage: Stage
  claim?: string
  claimSeq?: number
  // The lease holder while claimed; on done or failed, the worker that finished it.
  worker?: string
  untilMs?: number
  attempts: number
  outcome?: Outcome
  reason?: string
  blocked?: { kind: string; reason: string }
  assignee: string | null
  dueAtMs: number | null
  rank: number | null
  replies: { by: string; body: string }[]
}

export type Steer = { seq: number; by: string; body: string; atMs: number; to: string | null }

export type RawOp = { op: string; [field: string]: unknown }

export type Write = { op: RawOp; atMs?: number; key?: string; by?: string }

export type Lease = {
  readonly id: string
  readonly claim: string
  readonly untilMs: number
  readonly signal: AbortSignal
}

export type Can = 'read' | 'post' | 'react' | 'label' | 'sync' | 'rank' | 'manage' | 'plan'

export type Who =
  | { kind: 'principal'; id: string }
  | { kind: 'kind'; id: string }
  | { kind: 'everyone' }

export type Grant = { who: Who[]; can: Record<string, Can[]> }

export type Options = {
  sql: Contracts.SqlHost
  scope: string
  actor: string
  partitions?: number
  module?: WebAssembly.Module
  charter?: boolean
  // Events a partition folds past its last fold checkpoint before it saves another (4096 by
  // default); 0 saves none on its own, though checkpoint() and close() still save one.
  checkpointEvery?: number
  retain?: Retain
}

export type Retain = {
  settled: number
  entries: number
  only?: [string, string]
  events?: number
  gone?: number
  labelEvents?: [string, string, number]
}

export type MemorySql = Contracts.SqlHost & { close(): void }

// How one partition came to its fold when the board opened: from its scope's newest fold
// checkpoint and the tail rows after it, or from a full replay and why the checkpoint was not used.
export type Opened =
  | { from: 'checkpoint'; seq: number; tail: number }
  | { from: 'resumed' }
  | {
      from: 'replay'
      fallback: 'unsupported' | 'missing' | 'unread' | 'version' | 'corrupt' | 'tail'
      detail?: string
    }

export class LeaseLost extends Error {
  readonly task: string
  readonly rejection: Rejection | undefined

  constructor(task: string, rejection?: Rejection) {
    super('lost the lease on ' + task + (rejection ? ': ' + rejection.kind : ''))
    this.name = 'LeaseLost'
    this.task = task
    this.rejection = rejection
  }
}

export type Board = {
  readonly scope: string
  readonly actor: string
  post(task: NewTask, options?: { key?: string; atMs?: number }): Promise<Verdict>
  claim(
    id: string,
    options: { leaseMs: number; cap?: Cap; atMs?: number; waitMs?: number },
  ): Promise<Claimed>
  renew(id: string, claim: string, leaseMs: number, atMs?: number): Promise<Verdict>
  release(id: string, claim: string, note?: string, atMs?: number): Promise<Verdict>
  finish(id: string, claim: string, outcome: Outcome, atMs?: number): Promise<Verdict>
  cancel(id: string, reason: string, atMs?: number): Promise<Verdict>
  reply(
    id: string,
    body: string,
    options?: number | { atMs?: number; key?: string },
  ): Promise<Verdict>
  steer(
    id: string,
    text: string,
    options?: { key?: string; claim?: string; atMs?: number },
  ): Promise<Verdict>
  steers(id: string, claim?: string): Promise<Steer[]>
  heed(id: string, claim: string, upto: number, atMs?: number): Promise<Verdict>
  ready(options?: { atMs?: number; label?: [string, string] }): Promise<Task[]>
  task(id: string, atMs?: number): Promise<Task | null>
  tasks(atMs?: number): Promise<Task[]>
  held(atMs?: number): Promise<Task[]>
  refresh(): Promise<void>
  // How each partition opened; see Opened.
  opened(): Promise<Opened[]>
  // Saves each partition's fold as its scope's checkpoint, so the next open folds only the rows
  // after it. close() does this too. Resolves whether any partition saved one.
  checkpoint(): Promise<boolean>
  charter(atMs?: number): Promise<Verdict>
  grant(grant: Grant, options?: { key?: string; atMs?: number }): Promise<Verdict>
  revoke(event: string, options?: { key?: string; atMs?: number }): Promise<Verdict>
  submit(op: RawOp, options?: { key?: string; by?: string; atMs?: number }): Promise<Verdict>
  group(writes: Write[]): Promise<Verdict[]>
  work(
    id: string,
    options: { leaseMs: number; cap?: Cap },
    step: (lease: Lease, steers: Steer[]) => Promise<Outcome>,
  ): Promise<Outcome>
  close(): Promise<void>
}

export async function open(options: Options): Promise<Board> {
  const module = options.module ?? (await bundled())
  // The board runs one request at a time, so the reach its current request carries is its own.
  const reach: { current?: Contracts.SqlReach | undefined } = {}
  const sql: Contracts.SqlHost = {
    get value_limit() {
      return options.sql.value_limit
    },
    transaction: (kind, operation, statements, access) =>
      options.sql.transaction(kind, operation, statements, access, reach.current),
  }
  const runtime = await AgentRuntime.instantiate(module, sql, { published() {} })
  try {
    const board = (await runtime.board({
      open: {
        scope: options.scope,
        actor: options.actor,
        partitions: options.partitions ?? 1,
        ...(options.checkpointEvery === undefined
          ? {}
          : { checkpoint_every: options.checkpointEvery }),
        ...(options.retain === undefined ? {} : { retain: retention(options.retain) }),
      },
    })) as number
    const opened = wrap(runtime, board, options, reach)
    if ((options.charter ?? true) && !options.actor.includes('+')) {
      const chartered = await opened.charter()
      if (!chartered.ok && chartered.rejection.kind !== 'Exists') {
        await opened.close()
        throw new Error(
          'could not charter ' + options.scope + ': ' + JSON.stringify(chartered.rejection),
        )
      }
    }
    return opened
  } catch (error) {
    await runtime.dispose()
    throw error
  }
}

function retention(retain: Retain) {
  return {
    settled: retain.settled,
    entries: retain.entries,
    only: retain.only ?? null,
    events: retain.events ?? 0,
    gone: retain.gone ?? 0,
    label_events: retain.labelEvents ?? null,
  }
}

async function bundled(): Promise<WebAssembly.Module> {
  const specifier = './vercel/Wasm.js'
  const wasm = (await import(specifier)) as { load(): Promise<WebAssembly.Module> }
  return wasm.load()
}

type Sqlite = {
  DatabaseSync: new (path: string) => {
    prepare(query: string): {
      setReturnArrays?(enabled: boolean): void
      all(...values: unknown[]): unknown[]
    }
    exec(query: string): void
    close(): void
  }
}

export async function memorySql(): Promise<MemorySql> {
  const specifier = 'node:sqlite'
  const sqlite = (await import(specifier)) as Sqlite
  const db = new sqlite.DatabaseSync(':memory:')
  const prepared = new Map<string, ReturnType<typeof db.prepare>>()
  const prepare = (query: string) => {
    let statement = prepared.get(query)
    if (!statement) {
      statement = db.prepare(query)
      statement.setReturnArrays?.(true)
      prepared.set(query, statement)
    }
    return statement
  }
  const all = (statements: Contracts.Statement[]) =>
    statements.map(
      ([query, ...values]) =>
        prepare(query)
          .all(
            ...values.map((value) =>
              value instanceof ArrayBuffer ? new Uint8Array(value) : value,
            ),
          )
          .map((row) =>
            Array.isArray(row) ? row : Object.values(row as Record<string, Contracts.SqlValue>),
          ) as Contracts.SqlValue[][],
    )
  return {
    transaction(_kind, _operation, statements, access = 'write') {
      if (access === 'read') return all(statements)
      db.exec('BEGIN IMMEDIATE')
      try {
        const rows = all(statements)
        db.exec('COMMIT')
        return rows
      } catch (error) {
        db.exec('ROLLBACK')
        throw error
      }
    },
    close() {
      db.close()
    },
  }
}

function wrap(
  runtime: AgentRuntime.AgentRuntime,
  board: number,
  options: Options,
  reach: { current?: Contracts.SqlReach | undefined },
): Board {
  const now = (atMs?: number) => atMs ?? Date.now()
  let queue: Promise<unknown> = Promise.resolve()
  const call = (request: object) => {
    const carried = SqlReach.current()
    const next = queue.then(async () => {
      reach.current = carried
      try {
        return await runtime.board(request)
      } finally {
        reach.current = undefined
      }
    })
    queue = next.catch(() => undefined)
    return next
  }
  const wire = (op: RawOp, at?: { atMs?: number; key?: string; by?: string }) => ({
    op,
    at_ms: now(at?.atMs),
    key: at?.key ?? null,
    by: at?.by ?? null,
  })
  const write = (op: RawOp, at?: { atMs?: number; key?: string; by?: string }) =>
    call({ write: { board, write: wire(op, at) } }) as Promise<Verdict>
  const claimOnce = async (
    id: string,
    options: { leaseMs: number; cap?: Cap; atMs?: number },
  ): Promise<Claimed> => {
    const verdict = await write(
      {
        op: 'claim',
        task: id,
        lease_ms: options.leaseMs,
        ...(options.cap === undefined ? {} : { cap: wireCap(options.cap) }),
      },
      { atMs: options.atMs },
    )
    if (!verdict.ok) return verdict
    const claimed = (verdict.change as { Claimed?: Record<string, unknown> } | null)?.Claimed
    if (!claimed) throw new Error('claim of ' + id + ' applied without a claim')
    return {
      ok: true,
      claim: claimed.claim as string,
      untilMs: claimed.until_ms as number,
      tookOver: claimed.took_over != null,
      seq: verdict.seq,
    }
  }

  const self: Board = {
    scope: options.scope,
    actor: options.actor,

    post(task, at) {
      return write({ op: 'post', task: newTask(task) }, at)
    },

    async claim(id, options) {
      const deadline = options.waitMs === undefined ? undefined : Date.now() + options.waitMs
      for (;;) {
        const claimed = await claimOnce(id, options)
        if (claimed.ok || deadline === undefined || claimed.rejection.kind !== 'Held') {
          return claimed
        }

        const now = Date.now()
        if (now >= deadline) return claimed
        const wake = Math.min(deadline, claimed.rejection.untilMs ?? deadline)
        await new Promise((resolve) => setTimeout(resolve, wake - now))
        await self.refresh()
      }
    },

    renew(id, claim, leaseMs, atMs) {
      return write({ op: 'renew', task: id, claim, lease_ms: leaseMs }, { atMs })
    },

    release(id, claim, note = '', atMs) {
      return write({ op: 'release', task: id, claim, note }, { atMs })
    },

    finish(id, claim, outcome, atMs) {
      const detail =
        'done' in outcome
          ? { kind: 'done', detail: outcome.done }
          : { kind: 'failed', detail: outcome.failed }
      return write({ op: 'finish', task: id, claim, outcome: detail }, { atMs })
    },

    cancel(id, reason, atMs) {
      return write({ op: 'cancel', task: id, reason }, { atMs })
    },

    reply(id, body, options) {
      return write(
        { op: 'reply', task: id, body },
        typeof options === 'number' ? { atMs: options } : options,
      )
    },

    steer(id, text, at) {
      return write(
        {
          op: 'steer',
          act: 'send',
          task: id,
          body: text,
          ...(at?.claim === undefined ? {} : { claim: at.claim }),
        },
        at,
      )
    },

    steers(id, claim) {
      return call({ steers: { board, task: id, claim: claim ?? null } }) as Promise<Steer[]>
    },

    heed(id, claim, upto, atMs) {
      return write({ op: 'steer', act: 'heed', task: id, claim, upto }, { atMs })
    },

    ready(at) {
      return call({
        ready: { board, at_ms: now(at?.atMs), label: at?.label ?? null },
      }) as Promise<Task[]>
    },

    async task(id, atMs) {
      return (await call({ task: { board, task: id, at_ms: now(atMs) } })) as Task | null
    },

    tasks(atMs) {
      return call({ tasks: { board, at_ms: now(atMs) } }) as Promise<Task[]>
    },

    async held(atMs) {
      return (await self.tasks(atMs)).filter((task) => task.stage === 'held')
    },

    async refresh() {
      await call({ refresh: { board } })
    },

    charter(atMs) {
      return write({ op: 'charter' }, { atMs, key: 'charter' })
    },

    grant(grant, at) {
      return write({ op: 'grant', grant }, at)
    },

    revoke(event, at) {
      return write({ op: 'revoke', grant: event }, at)
    },

    submit(op, at) {
      return write(op, at)
    },

    group(writes) {
      return call({
        group: { board, writes: writes.map((entry) => wire(entry.op, entry)) },
      }) as Promise<Verdict[]>
    },

    async work(id, { leaseMs, cap }, step) {
      const claimed = await self.claim(id, { leaseMs, cap })
      if (!claimed.ok) throw new LeaseLost(id, claimed.rejection)
      const abort = new AbortController()
      const lease = { id, claim: claimed.claim, untilMs: claimed.untilMs, signal: abort.signal }
      let lost: LeaseLost | undefined
      let renewing = Promise.resolve()
      const renew = async () => {
        if (lost) return
        const renewed: Verdict = await self
          .renew(id, lease.claim, leaseMs)
          .catch((error: unknown) => ({
            ok: false,
            rejection: { kind: 'Unknown', value: String(error) },
          }))
        if (renewed.ok) {
          lease.untilMs = Date.now() + leaseMs
          return
        }
        lost = new LeaseLost(id, renewed.rejection)
        abort.abort(lost)
      }
      const timer = setInterval(
        () => {
          renewing = renewing.then(renew)
        },
        Math.max(1, Math.floor(leaseMs / 3)),
      )
      try {
        const steers = await self.steers(id, lease.claim)
        const outcome = await step(lease, steers)
        clearInterval(timer)
        await renewing
        if (lost) throw lost
        const last = steers.at(-1)
        if (last) {
          const heeded = await self.heed(id, lease.claim, last.seq)
          if (!heeded.ok) throw new LeaseLost(id, heeded.rejection)
        }
        const finished = await self.finish(id, lease.claim, outcome)
        if (!finished.ok) throw new LeaseLost(id, finished.rejection)
        return outcome
      } finally {
        clearInterval(timer)
      }
    },

    opened() {
      return call({ opened: { board } }) as Promise<Opened[]>
    },

    checkpoint() {
      return call({ checkpoint: { board } }) as Promise<boolean>
    },

    async close() {
      await call({ close: { board } })
      await runtime.dispose()
    },
  }
  return self
}

function newTask(task: NewTask) {
  const terms = task.terms ?? {}
  const recur =
    terms.every !== undefined
      ? { kind: 'every', every_ms: terms.every }
      : terms.cron
        ? { kind: 'cron', cron: terms.cron.cron, offset_minutes: terms.cron.offsetMinutes ?? 0 }
        : undefined
  return {
    id: task.id,
    parent: task.parent ?? null,
    spec: task.spec,
    labels: task.labels ?? {},
    needs: task.needs ?? [],
    terms: {
      ...(terms.assignee === undefined ? {} : { assignee: terms.assignee }),
      ...(terms.onFail === undefined ? {} : { on_fail: terms.onFail }),
      ...(terms.dueAtMs === undefined ? {} : { due_at_ms: terms.dueAtMs }),
      ...(recur === undefined ? {} : { recur }),
      ...(terms.retry === undefined
        ? {}
        : { retry: { max_attempts: terms.retry.maxAttempts, backoff_ms: terms.retry.backoffMs } }),
    },
  }
}

function wireCap(cap: Cap) {
  if (typeof cap === 'number') return cap
  const [key, value] = cap.label
  return { max: cap.max, key, value }
}
