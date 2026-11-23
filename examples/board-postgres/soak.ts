// A soak of the Postgres board under takeover chaos. Each swarm posts about 30 tasks with needs and
// label caps to a fresh schema and drains it with six Board.pool worker processes, some woken by
// Sql.wake and some polling. Meanwhile the driver SIGKILLs a random worker every 2-8 s and starts a
// new one, SIGSTOPs a holder for longer than its lease and then SIGCONTs it, restarts the Postgres
// server (pg_ctl restart -m fast) and steers held tasks. After each swarm it audits the raw log and
// the fold from a fresh connection, drops the schema and checks that no soak schema is left.
// Run it with: DATABASE_URL=postgres://... SOAK_PGDATA=<data dir> npm run example:board-postgres:soak
import { spawn, type ChildProcess } from 'node:child_process'
import * as Fs from 'node:fs'
import * as Path from 'node:path'
import { createInterface } from 'node:readline'
import { fileURLToPath } from 'node:url'
import { Board, Sql } from 'nanocode-ts/postgres'

type Mode = 'wake' | 'poll' | 'mixed'

type Role = {
  name: string
  schema: string
  seed: number
  url: string
  wake: boolean
}

type Line = {
  e: string
  at: number
  who?: string
  task?: string
  ok?: boolean
  kind?: string
} & Record<string, unknown>

type Event = {
  id: string
  at_ms: number
  by: string
  op: {
    op: string
    task?: unknown
    claim?: string
    lease_ms?: number
    outcome?: { kind?: string }
  }
}

type Takeover = {
  at: number
  from: string
  to: string
  task: string
  lapsedMs: number
}

type Slot = {
  child: ChildProcess
  held: Set<string>
  name: string
  stopped: boolean
}

const scope = 'soak'
const tasksPerSwarm = 30
const workerCount = 6
const concurrency = 2
const leaseMs = 2_000
const pollMs = 100
const drainMs = 120_000
const caps: readonly { label: [string, string]; max: number }[] = [
  { label: ['agent', 'llm'], max: 2 },
  { label: ['tool', 'browser'], max: 1 },
]
const modes: readonly Mode[] = ['wake', 'poll', 'mixed']

const self = fileURLToPath(import.meta.url)
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const emit = (e: string, fields: Record<string, unknown> = {}) =>
  process.stdout.write(JSON.stringify({ e, at: Date.now(), ...fields }) + '\n')

function random(seed: number) {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let value = Math.imul(state ^ (state >>> 15), 1 | state)
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296
  }
}

const capOf = (labels: Record<string, string> | undefined) =>
  caps.find((entry) => labels?.[entry.label[0]] === entry.label[1])

// A worker process: one Board.pool that reports what it starts, finishes and loses on stdout.
async function worker(role: Role) {
  const sql = Sql.open(role.url, { schema: role.schema, max: 2 })
  const board = await Board.open({
    sql,
    scope,
    actor: 'agent:' + role.name,
    charter: false,
  })
  const draw = random(role.seed)
  const observed: Board.Board = {
    ...board,
    async finish(id, claim, outcome, atMs) {
      const finished = await board.finish(id, claim, outcome, atMs)
      emit('finish', {
        task: id,
        ok: finished.ok,
        ...(finished.ok ? {} : { kind: finished.rejection.kind }),
      })
      return finished
    },
    async renew(id, claim, ms, atMs) {
      const renewed = await board.renew(id, claim, ms, atMs)
      if (!renewed.ok) emit('renew', { task: id, ok: false, kind: renewed.rejection.kind })
      return renewed
    },
  }
  const pool = Board.pool(observed, {
    concurrency,
    leaseMs,
    pollMs,
    cap: (task) => capOf(task.labels),
    wake: role.wake ? Sql.wake(sql) : undefined,
    async run(task, lease) {
      emit('start', { task: task.id, tookOver: lease.tookOver })
      await sleep(300 + draw() * 1_200)
      const steers = await lease.steers()
      const last = steers.at(-1)
      if (last) {
        await lease.heed(last.seq)
        emit('heed', { task: task.id, seq: last.seq })
      }
      return { done: role.name }
    },
  })
  emit('up')

  let stopping = false
  const stop = async () => {
    if (stopping) return
    stopping = true
    await pool.stop()
    emit('stats', { stats: pool.stats() })
    await board.close()
    await sql.close()
    process.exit(0)
  }
  process.stdin.on('data', stop)
  process.stdin.on('end', stop)
}

// A layered graph: each task after the first few needs up to two of the eight before it, and
// about 40% carry agent=llm (cap 2) and 20% tool=browser (cap 1).
function graph(draw: () => number): Board.Write[] {
  const writes: Board.Write[] = []
  for (let index = 0; index < tasksPerSwarm; index++) {
    const id = 't' + String(index).padStart(2, '0')
    const needs = new Set<string>()
    if (index > 0) {
      const from = Math.max(0, index - 8)
      const count = index < 4 ? 1 : Math.floor(draw() * 3)
      for (let pick = 0; pick < count; pick++)
        needs.add('t' + String(from + Math.floor(draw() * (index - from))).padStart(2, '0'))
    }
    const roll = draw()
    const labels: Record<string, string> =
      roll < 0.4 ? { agent: 'llm' } : roll < 0.6 ? { tool: 'browser' } : {}
    const task = {
      id,
      parent: null,
      spec: 'soak ' + id,
      labels,
      needs: [...needs],
      terms: {},
    }
    writes.push({ op: { op: 'post', task }, key: id })
  }
  return writes
}

function events(value: unknown, into: Event[]) {
  if (typeof value === 'string' && value.startsWith('{')) return events(JSON.parse(value), into)
  if (Array.isArray(value)) {
    for (const item of value) events(item, into)
    return
  }
  if (!value || typeof value !== 'object') return
  const record = value as Record<string, unknown>
  const op = record.op as { op?: unknown } | undefined
  if (typeof record.at_ms === 'number' && op && typeof op.op === 'string') {
    into.push(record as Event)
    return
  }
  for (const item of Object.values(record)) events(item, into)
}

// Replays the raw log: claims only after needs are done and no live lease, label caps held,
// renews and finishes only by the current holder, every task finished once with done, and the
// fold's Task.worker equal to the actor whose finish landed.
function audit(rows: readonly (readonly unknown[])[], fold: readonly Board.Task[]) {
  const violations: string[] = []
  const finishes = new Map<string, Event[]>()
  const labels = new Map<string, Record<string, string>>()
  const leases = new Map<string, { by: string; claim: string; untilMs: number }>()
  const needsOf = new Map<string, readonly string[]>()
  const takeovers: Takeover[] = []
  let clock = 0

  for (const [, seq, body] of rows) {
    const found: Event[] = []
    events(String(body), found)
    for (const event of found) {
      clock = Math.max(clock, event.at_ms)
      const op = event.op
      const where = '#' + String(seq)
      if (op.op === 'post') {
        const task = op.task as {
          id: string
          labels?: Record<string, string>
          needs?: string[]
        }
        labels.set(task.id, task.labels ?? {})
        needsOf.set(task.id, task.needs ?? [])
        continue
      }
      const id = typeof op.task === 'string' ? op.task : undefined
      if (id === undefined) continue
      const live = leases.get(id)
      if (op.op === 'claim') {
        const unmet = (needsOf.get(id) ?? []).filter((need) => !finishes.has(need))
        if (unmet.length > 0)
          violations.push('claim of ' + id + ' at ' + where + ' before needs ' + unmet.join(' '))
        if (finishes.has(id))
          violations.push('claim of ' + id + ' at ' + where + ' after its finish')
        if (live && live.untilMs > clock)
          violations.push(
            'claim of ' +
              id +
              ' at ' +
              where +
              ' by ' +
              event.by +
              ' while ' +
              live.by +
              ' held it ' +
              (live.untilMs - clock) +
              ' ms more',
          )
        if (live)
          takeovers.push({
            at: event.at_ms,
            from: live.by,
            to: event.by,
            task: id,
            lapsedMs: clock - live.untilMs,
          })
        const limit = capOf(labels.get(id))
        if (limit) {
          const held = [...leases].filter(
            ([other, lease]) =>
              other !== id && lease.untilMs > clock && capOf(labels.get(other)) === limit,
          )
          if (held.length >= limit.max)
            violations.push(
              'claim of ' +
                id +
                ' at ' +
                where +
                ' exceeds cap ' +
                limit.max +
                ' on ' +
                limit.label.join('=') +
                ': ' +
                held.map(([other, lease]) => other + ' by ' + lease.by).join(', '),
            )
        }
        leases.set(id, { by: event.by, claim: event.id, untilMs: clock + (op.lease_ms ?? 0) })
      } else if (op.op === 'renew') {
        if (!live || live.by !== event.by || live.claim !== op.claim)
          violations.push(
            'renew of ' +
              id +
              ' at ' +
              where +
              ' with claim ' +
              op.claim +
              ', not the live claim ' +
              live?.claim,
          )
        else live.untilMs = clock + (op.lease_ms ?? 0)
      } else if (op.op === 'release') leases.delete(id)
      else if (op.op === 'finish') {
        if (!live || live.by !== event.by || live.claim !== op.claim)
          violations.push(
            'finish of ' +
              id +
              ' at ' +
              where +
              ' with claim ' +
              op.claim +
              ', not the live claim ' +
              live?.claim,
          )
        if (op.outcome?.kind !== 'done')
          violations.push('finish of ' + id + ' at ' + where + ' was ' + JSON.stringify(op.outcome))
        finishes.set(id, [...(finishes.get(id) ?? []), event])
        leases.delete(id)
      }
    }
  }

  if (labels.size !== tasksPerSwarm)
    violations.push('the log holds ' + labels.size + ' posts, not ' + tasksPerSwarm)
  for (const id of labels.keys()) {
    const count = finishes.get(id)?.length ?? 0
    if (count !== 1) violations.push(id + ' finished ' + count + ' times')
  }
  if (fold.length !== labels.size)
    violations.push('the fold holds ' + fold.length + ' tasks, the log ' + labels.size)
  for (const task of fold) {
    if (task.phase !== 'done')
      violations.push(task.id + ' is ' + task.phase + ' (' + task.stage + ') after the drain')
    const by = finishes.get(task.id)?.[0]?.by
    if (task.worker !== by)
      violations.push(task.id + ' has worker ' + task.worker + ', but ' + by + ' finished it')
  }
  return { takeovers, violations }
}

function quantile(values: readonly number[], q: number) {
  if (values.length === 0) return null
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1))]!
}

function exited(child: ChildProcess) {
  return new Promise<void>((resolve) =>
    child.exitCode !== null || child.signalCode !== null
      ? resolve()
      : child.once('exit', () => resolve()),
  )
}

async function restartPostgres(pgdata: string) {
  const child = spawn(
    process.env.SOAK_PG_CTL ?? 'pg_ctl',
    ['restart', '-m', 'fast', '-w', '-D', pgdata, '-l', Path.join(pgdata, 'soak.log')],
    {
      stdio: 'ignore',
    },
  )
  const code = await new Promise<number | null>((resolve) => child.once('exit', resolve))
  if (code !== 0) throw new Error('pg_ctl restart exited ' + code)
}

async function retry<T>(attempt: () => Promise<T>, ms: number): Promise<T> {
  const until = Date.now() + ms
  for (;;) {
    try {
      return await attempt()
    } catch (error) {
      if (Date.now() > until) throw error
      await sleep(250)
    }
  }
}

async function swarm(url: string, seed: number, out: string) {
  const draw = random(seed)
  const mode = modes[seed % modes.length]!
  const schema = 'soak_' + seed
  const pgdata = process.env.SOAK_PGDATA
  const timeline: Line[] = []
  const note = (who: string, e: string, fields: Record<string, unknown> = {}) =>
    timeline.push({ e, at: Date.now(), who, ...fields })
  const counts = {
    kills: 0,
    stops: 0,
    restarts: 0,
    steers: 0,
    exits: 0,
    resumedRefused: 0,
    resumedKept: 0,
    resumedRetook: 0,
  }
  const refusals: Record<string, number> = {}
  const chaos = new Map<string, number[]>()
  // What each SIGSTOPped worker held when it was paused, and when it was let go again.
  const pausedWith = new Map<string, { at: number; tasks: Set<string> }>()
  const resumedAt = new Map<string, number>()
  const resumedFinishes: { actor: string; task: string; at: number; startedAt: number }[] = []
  // When each worker last started each task, that is, took the claim a later finish lands on.
  const startedAt = new Map<string, number>()
  const slots: Slot[] = []
  const incarnations: number[] = []
  const stats: Board.PoolStats[] = []
  let draining = false

  const sql = Sql.open(url, { schema, max: 2 })
  const lead = await Board.open({ sql, scope, actor: 'agent:lead' })
  const granted = await lead.grant(
    { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
    { key: 'agents' },
  )
  if (!granted.ok) throw new Error('grant refused: ' + JSON.stringify(granted.rejection))
  const posted = await lead.group(graph(draw))
  const refused = posted.find((verdict) => !verdict.ok)
  if (refused) throw new Error('post refused: ' + JSON.stringify(refused))

  const start = (index: number) => {
    const incarnation = (incarnations[index] = (incarnations[index] ?? 0) + 1)
    const name = 'w' + index + '-' + incarnation
    const wake = mode === 'wake' || (mode === 'mixed' && index % 2 === 0)
    const role: Role = {
      name,
      schema,
      seed: seed * 1009 + index * 31 + incarnation,
      url,
      wake,
    }
    const child = spawn(process.execPath, [self, 'worker', JSON.stringify(role)], {
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    const slot: Slot = { child, held: new Set(), name, stopped: false }
    slots[index] = slot
    createInterface({ input: child.stdout! }).on('line', (text) => {
      let line: Line
      try {
        line = JSON.parse(text) as Line
      } catch {
        return void note(name, 'stdout', { text })
      }
      timeline.push({ ...line, who: name })
      if (line.e === 'start' && line.task) {
        slot.held.add(line.task)
        startedAt.set(name + ' ' + line.task, line.at)
      }
      if ((line.e === 'finish' || line.e === 'renew') && line.task) {
        slot.held.delete(line.task)
        const resumed = resumedAt.get(name)
        if (line.ok === false) refusals[line.kind ?? '?'] = (refusals[line.kind ?? '?'] ?? 0) + 1
        if (resumed !== undefined && line.at >= resumed) {
          const paused = pausedWith.get(name)?.tasks.has(line.task) ?? false
          if (paused && line.ok === false) counts.resumedRefused++
          else if (paused && line.e === 'finish')
            resumedFinishes.push({
              actor: 'agent:' + name,
              task: line.task,
              at: line.at,
              startedAt: startedAt.get(name + ' ' + line.task) ?? 0,
            })
        }
      }
      if (line.e === 'stats') stats.push(line.stats as Board.PoolStats)
    })
    createInterface({ input: child.stderr! }).on('line', (text) => note(name, 'stderr', { text }))
    child.once('exit', (code, signal) => {
      note(name, 'exit', { code, signal })
      if (draining || signal === 'SIGKILL' || slots[index] !== slot) return
      counts.exits++
      setTimeout(() => {
        if (!draining && slots[index] === slot) start(index)
      }, 250)
    })
  }
  for (let index = 0; index < workerCount; index++) start(index)

  const began = Date.now()
  let nextKill = began + 2_000 + draw() * 6_000
  let nextStop = began + 1_500 + draw() * 3_000
  const restarts: number[] = []
  const restartAt = pgdata && draw() < 0.3 ? began + 1_000 + draw() * 5_000 : undefined
  let restarting: Promise<void> | undefined
  let drained = false
  let fold: Board.Task[] = []
  while (Date.now() - began < drainMs) {
    const now = Date.now()
    if (now >= nextKill) {
      nextKill = now + 2_000 + draw() * 6_000
      const running = slots.flatMap((slot, index) => (slot.stopped ? [] : [index]))
      const index = running[Math.floor(draw() * running.length)]
      if (index !== undefined) {
        const slot = slots[index]!
        note('lead', 'kill', { worker: slot.name, held: [...slot.held] })
        chaos.set('agent:' + slot.name, [...(chaos.get('agent:' + slot.name) ?? []), now])
        slot.child.kill('SIGKILL')
        counts.kills++
        start(index)
      }
    }
    if (now >= nextStop) {
      nextStop = now + 2_000 + draw() * 5_000
      const holders = slots.filter((slot) => !slot.stopped && slot.held.size > 0)
      const paused = holders[Math.floor(draw() * holders.length)]
      if (paused && draw() < 0.6) {
        const ms = leaseMs + 500 + Math.floor(draw() * 2_500)
        note('lead', 'stop', {
          worker: paused.name,
          held: [...paused.held],
          ms,
        })
        chaos.set('agent:' + paused.name, [...(chaos.get('agent:' + paused.name) ?? []), now])
        paused.stopped = true
        pausedWith.set(paused.name, { at: now, tasks: new Set(paused.held) })
        paused.child.kill('SIGSTOP')
        counts.stops++
        setTimeout(() => {
          resumedAt.set(paused.name, Date.now())
          paused.stopped = false
          paused.child.kill('SIGCONT')
          note('lead', 'cont', { worker: paused.name })
        }, ms)
      }
    }
    if (restartAt !== undefined && pgdata && !restarting && now >= restartAt) {
      note('lead', 'pg-restart')
      restarts.push(now)
      counts.restarts++
      restarting = restartPostgres(pgdata).then(() => void note('lead', 'pg-up'))
    }
    try {
      await lead.refresh()
      fold = await lead.tasks()
      if (fold.length > 0 && fold.every((task) => task.phase === 'done')) {
        drained = true
        break
      }
      const held = fold.filter((task) => task.stage === 'held')
      const target = held[Math.floor(draw() * held.length)]
      if (target && draw() < 0.03) {
        const sent = await lead.steer(target.id, 'steer ' + counts.steers, {
          key: 'steer-' + counts.steers,
        })
        note('lead', 'steer', { task: target.id, ok: sent.ok })
        counts.steers++
      }
    } catch (error) {
      note('lead', 'error', { message: String(error) })
    }
    await sleep(pollMs)
  }
  await restarting

  draining = true
  for (const slot of slots) {
    slot.child.kill('SIGCONT')
    slot.child.stdin!.end('stop\n')
  }
  await Promise.race([Promise.all(slots.map((slot) => exited(slot.child))), sleep(15_000)])
  for (const slot of slots) slot.child.kill('SIGKILL')
  await lead.close().catch(() => undefined)
  await sql.close()

  // Audit from a fresh connection, after every lease has lapsed.
  await sleep(leaseMs)
  const fresh = Sql.open(url, { schema, max: 1 })
  const result = await retry(async () => {
    const [rows = []] = await fresh.transaction(
      'log',
      'audit',
      [['SELECT scope, seq, body FROM nanocode_log ORDER BY scope, seq']],
      'read',
    )
    const auditor = await Board.open({
      sql: fresh,
      scope,
      actor: 'agent:auditor',
      charter: false,
    })
    try {
      await auditor.refresh()
      return { ...audit(rows, await auditor.tasks()), rows }
    } finally {
      await auditor.close()
    }
  }, 30_000)
  await fresh.close()
  const violations = [...result.violations]
  if (!drained)
    violations.unshift(
      'the board did not drain in ' +
        drainMs +
        ' ms: ' +
        fold
          .filter((t) => t.phase !== 'done')
          .map((t) => t.id + '=' + t.stage)
          .join(' '),
    )
  // A paused holder may still finish the task it held if nobody took it over while it slept, or
  // after claiming it afresh once a later holder's lease lapsed too. A finish on the claim it held
  // before the pause, after a takeover, is the bug this hunts: the claim-id audit above catches it,
  // and this flags a resumed finish that neither case explains. The claim a finish lands on is the
  // one its run started on, recorded when the finish was seen: a worker paused more than once may
  // have lost the task at its first pause and retaken it before a later one.
  for (const finish of resumedFinishes) {
    const taken = result.takeovers.filter(
      (takeover) => takeover.task === finish.task && takeover.from === finish.actor,
    )
    const retook = taken.length > 0 && taken.every((takeover) => takeover.at < finish.startedAt)
    if (retook) counts.resumedRetook++
    else if (taken.length > 0)
      violations.push(
        finish.actor +
          ' finished ' +
          finish.task +
          ' after resuming, though another worker took it over',
      )
    else counts.resumedKept++
  }

  // Takeover latency: from the kill or stop of the holder to another worker's claim. A takeover
  // whose holder was neither killed nor stopped lost its lease while Postgres restarted, or was
  // too slow to renew (unexplained); the audit already checks that the takeover came after the lapse.
  const latency: number[] = []
  let afterRestart = 0
  let unexplained = 0
  for (const takeover of result.takeovers) {
    const cause = (chaos.get(takeover.from) ?? []).filter((at) => at <= takeover.at).at(-1)
    if (cause !== undefined) latency.push(takeover.at - cause)
    else if (restarts.some((at) => at <= takeover.at)) afterRestart++
    else unexplained++
  }

  const admin = Sql.open(url, { max: 1 })
  if (violations.length === 0)
    await admin.transaction('log', 'drop', [['DROP SCHEMA ' + schema + ' CASCADE']], 'schema')
  else {
    Fs.writeFileSync(
      Path.join(out, schema + '.timeline.jsonl'),
      timeline.map((line) => JSON.stringify(line)).join('\n') + '\n',
    )
    Fs.writeFileSync(
      Path.join(out, schema + '.log.jsonl'),
      result.rows.map((row) => JSON.stringify(row)).join('\n') + '\n',
    )
    Fs.writeFileSync(Path.join(out, schema + '.violations.txt'), violations.join('\n') + '\n')
  }
  const [schemas = []] = await admin.transaction(
    'log',
    'schemas',
    [["SELECT nspname FROM pg_namespace WHERE nspname LIKE 'soak!_%' ESCAPE '!'"]],
    'read',
  )
  await admin.close()

  return {
    seed,
    mode,
    ms: Date.now() - began,
    ...counts,
    takeovers: result.takeovers.length,
    afterRestart,
    unexplained,
    latency,
    refusals,
    poolErrors: stats.reduce((sum, entry) => sum + entry.errors, 0),
    schemas: schemas.map((row) => String(row[0])),
    violations,
  }
}

async function main() {
  const url = process.env.DATABASE_URL
  if (!url) {
    console.error(
      'Set DATABASE_URL to a Postgres database, such as postgres://postgres@127.0.0.1:5432/postgres',
    )
    process.exit(1)
  }
  const base = Number(process.env.SOAK_SEED ?? (Date.now() % 1_000_000) * 1000)
  const minutes = Number(process.env.SOAK_MINUTES ?? 45)
  const limit = Number(process.env.SOAK_SWARMS ?? Number.POSITIVE_INFINITY)
  const out =
    process.env.SOAK_OUT ??
    fileURLToPath(new URL('../../output/board-postgres-soak/', import.meta.url))
  Fs.mkdirSync(out, { recursive: true })
  const until = Date.now() + minutes * 60_000
  const totals = {
    swarms: 0,
    tasks: 0,
    kills: 0,
    stops: 0,
    restarts: 0,
    steers: 0,
    exits: 0,
    takeovers: 0,
    afterRestart: 0,
    unexplained: 0,
    resumedRefused: 0,
    resumedKept: 0,
    resumedRetook: 0,
    poolErrors: 0,
  }
  const refusals: Record<string, number> = {}
  const latency: number[] = []
  const began = Date.now()
  console.log(JSON.stringify({ e: 'soak', seed: base, minutes, out }))

  for (let index = 0; index < limit && Date.now() < until; index++) {
    const seed = base + index
    const result = await swarm(url, seed, out)
    totals.swarms++
    totals.tasks += tasksPerSwarm
    for (const key of [
      'kills',
      'stops',
      'restarts',
      'steers',
      'exits',
      'takeovers',
      'afterRestart',
      'unexplained',
      'resumedRefused',
      'resumedKept',
      'resumedRetook',
      'poolErrors',
    ] as const)
      totals[key] += result[key]
    for (const [kind, count] of Object.entries(result.refusals))
      refusals[kind] = (refusals[kind] ?? 0) + count
    latency.push(...result.latency)
    const leaked =
      result.violations.length > 0
        ? result.schemas.filter((name) => name !== 'soak_' + seed)
        : result.schemas
    const { latency: _, schemas: __, ...shown } = result
    console.log(JSON.stringify({ e: 'swarm', ...shown, leaked }))
    if (result.violations.length > 0 || leaked.length > 0) {
      console.error(
        'violation: repro with SOAK_SEED=' +
          seed +
          ' SOAK_SWARMS=1; schema ' +
          (result.violations.length > 0 ? 'soak_' + seed + ' kept' : 'dropped') +
          ', logs in ' +
          out,
      )
      process.exitCode = 1
      break
    }
  }

  console.log(
    JSON.stringify({
      e: 'totals',
      minutes: Math.round((Date.now() - began) / 6_000) / 10,
      ...totals,
      refusals,
      takeoverP50Ms: quantile(latency, 0.5),
      takeoverP99Ms: quantile(latency, 0.99),
      takeoverMaxMs: quantile(latency, 1),
    }),
  )
}

if (process.argv[2] === 'worker')
  await worker(JSON.parse(process.argv[3] ?? '{}') as Role).catch((error: unknown) => {
    emit('error', { message: String(error) })
    process.exit(1)
  })
else await main()
