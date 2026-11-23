import { execFileSync, spawn } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import * as Fs from 'node:fs'
import * as Os from 'node:os'
import * as Path from 'node:path'
import * as Readline from 'node:readline'
import { DatabaseSync } from 'node:sqlite'
import { fileURLToPath } from 'node:url'
import { Board, Sql } from 'nanocode-ts/node'
import { Sql as Postgres } from 'nanocode-ts/postgres'

type Config = {
  children: number
  concurrency: number
  drainMs: number
  inflight: number | undefined
  killEveryMs: number
  leaseMs: number
  partitions: readonly number[]
  pollMs: number
  wake: boolean
  postMs: number
  seed: number
  sql: 'sqlite' | 'postgres'
  steerShare: number
  workers: readonly number[]
  workMaxMs: number
  workMinMs: number
}

type Cell = Config & { partition: number; worker: number }

type Histogram = Record<string, number>

type Stats = {
  metrics: {
    errors: Record<string, number>
    lastError: string
    read: Histogram
    refresh: Histogram
    refreshMs: number
    refreshes: number
    rejected: Record<string, number>
    steered: number
    write: Histogram
  }
  pool: Board.PoolStats
}

type Event = {
  at_ms: number
  by: string
  id: string
  op: { op: string; task?: unknown; lease_ms?: number; outcome?: { kind?: string } }
}

type Slot = { child: ChildProcess; name: string; ready: Promise<number> }

const here = Path.dirname(fileURLToPath(import.meta.url))
const output = Path.resolve(here, '../output/bench/board-load')
const workerFile = Path.join(here, 'board-load.worker.ts')
const scope = 'load'
const shape = { firstWave: 4, maxNeeds: 2, window: 6 }

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

function random(seed: number) {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let value = Math.imul(state ^ (state >>> 15), 1 | state)
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296
  }
}

function parse(argv: readonly string[]): Config {
  const flags = new Map<string, string>()
  for (let index = 0; index < argv.length; index += 2) {
    const name = argv[index] ?? ''
    const value = argv[index + 1]
    if (!name.startsWith('--') || value === undefined)
      throw new Error('expected --name value pairs, got ' + name)
    flags.set(name.slice(2), value)
  }

  const number = (name: string, fallback: number) => {
    const value = Number(flags.get(name) ?? fallback)
    if (!Number.isFinite(value) || value < 0)
      throw new Error('--' + name + ' must be a non-negative number')
    return value
  }
  const list = (name: string, fallback: string) =>
    (flags.get(name) ?? fallback).split(',').map((value) => {
      const parsed = Number(value)
      if (!Number.isSafeInteger(parsed) || parsed < 1)
        throw new Error('--' + name + ' takes positive integers')
      return parsed
    })
  const inflight = flags.get('inflight')
  const wake = flags.get('wake') ?? 'off'
  if (wake !== 'on' && wake !== 'off') throw new Error('--wake takes on or off')
  if (wake === 'on' && (flags.get('sql') ?? 'sqlite') !== 'postgres')
    throw new Error('--wake on needs --sql postgres: node:sqlite has no cross-process notify')

  return {
    children: number('children', 20),
    concurrency: number('concurrency', 4),
    drainMs: number('drain-ms', 30_000),
    inflight: inflight === undefined ? undefined : number('inflight', 0),
    killEveryMs: number('kill-every-ms', 10_000),
    leaseMs: number('lease-ms', 3_000),
    partitions: list('partitions', '1,4,8'),
    pollMs: number('poll-ms', 25),
    wake: wake === 'on',
    postMs: number('post-ms', 45_000),
    seed: number('seed', 1),
    sql: backend(flags.get('sql') ?? 'sqlite'),
    steerShare: number('steer', 0.02),
    workers: list('workers', '4,8,16'),
    workMaxMs: number('work-max-ms', 50),
    workMinMs: number('work-min-ms', 5),
  }
}

function backend(name: string) {
  if (name !== 'sqlite' && name !== 'postgres') throw new Error('--sql takes sqlite or postgres')
  if (name === 'postgres') databaseUrl()
  return name
}

function databaseUrl() {
  const url = process.env.DATABASE_URL
  if (!url) throw new Error('--sql postgres needs DATABASE_URL, a postgres:// URL')
  return url
}

function swarm(cell: Cell, draw: () => number, name: string): Board.NewTask[] {
  const labels = { project: name }
  const tasks: Board.NewTask[] = [{ id: name, labels, needs: [], spec: 'root of ' + name }]
  for (let index = 0; index < cell.children; index++) {
    const needs = new Set<string>()
    if (index < shape.firstWave) needs.add(name)
    else {
      const from = Math.max(0, index - shape.window)
      const count = 1 + Math.floor(draw() * shape.maxNeeds)
      while (needs.size < count)
        needs.add(name + '.c' + (from + Math.floor(draw() * (index - from))))
    }
    tasks.push({
      id: name + '.c' + index,
      labels,
      needs: [...needs],
      parent: name,
      spec: 'child ' + index + ' of ' + name,
    })
  }
  return tasks
}

const post = (task: Board.NewTask): Board.Write => ({
  op: {
    op: 'post',
    task: {
      id: task.id,
      labels: task.labels ?? {},
      needs: task.needs ?? [],
      parent: task.parent ?? null,
      spec: task.spec,
      terms: {},
    },
  },
})

function quantile(values: readonly number[], q: number) {
  if (values.length === 0) return null
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? null
}

function merge(histograms: readonly Histogram[]) {
  const merged: Histogram = {}
  for (const histogram of histograms)
    for (const [key, count] of Object.entries(histogram)) merged[key] = (merged[key] ?? 0) + count
  return merged
}

function histogramQuantile(histogram: Histogram, q: number) {
  const entries = Object.entries(histogram)
    .map(([key, count]) => [Number(key), count] as const)
    .sort((a, b) => a[0] - b[0])
  const total = entries.reduce((sum, entry) => sum + entry[1], 0)
  if (total === 0) return null
  let seen = 0
  for (const [key, count] of entries) {
    seen += count
    if (seen >= q * total) return Number((10 ** (key / 20)).toPrecision(3))
  }
  return null
}

const total = (histogram: Histogram) =>
  Object.values(histogram).reduce((sum, count) => sum + count, 0)

const round = (value: number | null) => (value === null ? null : Math.round(value * 10) / 10)

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

const auditQuery = 'SELECT scope, seq, body FROM nanocode_log ORDER BY scope, seq'

async function logRows(where: Where) {
  if (where.sql === 'sqlite') {
    const db = new DatabaseSync(where.path, { readOnly: true })
    const rows = db.prepare(auditQuery).all() as { body: string; scope: string; seq: number }[]
    db.close()
    return rows
  }
  const host = Postgres.open(where.url, { schema: where.schema })
  try {
    const [rows = []] = await host.transaction('log', 'audit', [[auditQuery]], 'read')
    await host.transaction('log', 'drop', [['DROP SCHEMA ' + where.schema + ' CASCADE']], 'schema')
    return rows.map(([scope, seq, body]) => ({
      scope: String(scope),
      seq: Number(seq),
      body: String(body),
    }))
  } finally {
    await host.close()
  }
}

type Where = { sql: 'sqlite'; path: string } | { sql: 'postgres'; url: string; schema: string }

function audit(
  rows: readonly { body: string; scope: string; seq: number }[],
  cell: Cell,
  kills: ReadonlyMap<string, number>,
) {
  const violations: string[] = []
  const claimed = new Map<string, number>()
  const finishes = new Map<string, number>()
  const finishedAt = new Map<string, number>()
  const homes = new Map<string, Set<string>>()
  const needsOf = new Map<string, readonly string[]>()
  const postedAt = new Map<string, number>()
  const takeovers: { at: number; from: string; id: string }[] = []
  let claims = 0
  let current = ''
  let clock = 0
  let finished = new Set<string>()
  let leases = new Map<string, { by: string; untilMs: number }>()

  for (const row of rows) {
    if (row.scope !== current) {
      current = row.scope
      clock = 0
      finished = new Set()
      leases = new Map()
    }
    const found: Event[] = []
    events(row.body, found)
    for (const event of found) {
      clock = Math.max(clock, event.at_ms)
      const op = event.op
      const where = row.scope + '#' + row.seq
      if (op.op === 'post') {
        const task = op.task as { id: string; needs?: string[] }
        const swarmName = task.id.split('.')[0] ?? task.id
        postedAt.set(task.id, event.at_ms)
        needsOf.set(task.id, task.needs ?? [])
        const set = homes.get(swarmName) ?? new Set<string>()
        set.add(row.scope)
        homes.set(swarmName, set)
        continue
      }
      const id = typeof op.task === 'string' ? op.task : undefined
      if (id === undefined) continue
      if (op.op === 'claim') {
        claims++
        claimed.set(id, (claimed.get(id) ?? 0) + 1)
        const unmet = (needsOf.get(id) ?? []).filter((need) => !finished.has(need))
        if (unmet.length > 0)
          violations.push('claim of ' + id + ' at ' + where + ' before needs ' + unmet.join(' '))
        if (finishes.has(id))
          violations.push('claim of ' + id + ' at ' + where + ' after its finish')
        const live = leases.get(id)
        const left = live ? live.untilMs - clock : 0
        if (live && left > 0)
          violations.push(
            `claim of ${id} at ${where} by ${event.by} while ${live.by} held it ${left} ms more`,
          )
        if (live) takeovers.push({ at: event.at_ms, from: live.by, id })
        leases.set(id, { by: event.by, untilMs: clock + (op.lease_ms ?? 0) })
      } else if (op.op === 'renew') {
        const lease = leases.get(id)
        if (lease) lease.untilMs = clock + (op.lease_ms ?? 0)
      } else if (op.op === 'release') leases.delete(id)
      else if (op.op === 'finish') {
        finishes.set(id, (finishes.get(id) ?? 0) + 1)
        finishedAt.set(id, event.at_ms)
        if (op.outcome?.kind === 'done') finished.add(id)
        else
          violations.push('finish of ' + id + ' at ' + where + ' was ' + JSON.stringify(op.outcome))
        leases.delete(id)
      }
    }
  }

  for (const id of postedAt.keys()) {
    const count = finishes.get(id) ?? 0
    if (count !== 1) violations.push(id + ' finished ' + count + ' times')
  }
  for (const [name, set] of homes)
    if (set.size !== 1) violations.push('swarm ' + name + ' spans ' + [...set].join(' '))

  const perScope = new Map<string, number>()
  for (const set of homes.values())
    for (const home of set) perScope.set(home, (perScope.get(home) ?? 0) + 1)

  const latency = [...postedAt].flatMap(([id, at]) => {
    const done = finishedAt.get(id)
    return done === undefined ? [] : [done - at]
  })
  const swarms = [...homes.keys()].flatMap((name) => {
    const started = postedAt.get(name)
    const ends = [...postedAt.keys()].filter((id) => id === name || id.startsWith(name + '.'))
    const last = Math.max(...ends.map((id) => finishedAt.get(id) ?? Number.NaN))
    return started === undefined || Number.isNaN(last) ? [] : [last - started]
  })
  const takeover = takeovers.flatMap((entry) => {
    const killed = kills.get(entry.from.replace(/^agent:/, ''))
    return killed === undefined ? [] : [entry.at - killed]
  })
  const firstPost = Math.min(...postedAt.values())
  const lastFinish = Math.max(...finishedAt.values())

  return {
    claims,
    elapsedMs: lastFinish - firstPost,
    latency,
    perScope: Object.fromEntries([...perScope].sort()),
    reclaimed: [...claimed.values()].filter((count) => count > 1).length,
    rows: rows.length,
    swarms,
    takeover,
    takeoversWithoutKill: takeovers.length - takeover.length,
    tasks: postedAt.size,
    violations,
  }
}

async function run(cell: Cell) {
  Fs.mkdirSync(output, { recursive: true })
  const dir = Fs.mkdtempSync(Path.join(output, 'p' + cell.partition + '-w' + cell.worker + '-'))
  const where: Where =
    cell.sql === 'sqlite'
      ? { sql: 'sqlite', path: Path.join(dir, 'board.sqlite') }
      : {
          sql: 'postgres',
          url: databaseUrl(),
          schema:
            'bench_' +
            Path.basename(dir)
              .replace(/[^A-Za-z0-9]/g, '_')
              .toLowerCase(),
        }
  const sql =
    where.sql === 'sqlite'
      ? Sql.open(where.path)
      : Postgres.open(where.url, { schema: where.schema })
  const board = await Board.open({ actor: 'agent:bench', partitions: cell.partition, scope, sql })
  const granted = await board.grant(
    { can: { '*': ['manage'] }, who: [{ id: 'agent', kind: 'kind' }] },
    { key: 'grant-agents' },
  )
  if (!granted.ok) throw new Error('grant refused: ' + JSON.stringify(granted.rejection))

  const draw = random(cell.seed)
  const inflight = cell.inflight ?? 2 * cell.worker
  const violations: string[] = []
  const doneReports = new Map<string, number>()
  const exits: string[] = []
  const incarnations: number[] = []
  const killed = new Set<string>()
  const kills = new Map<string, number>()
  const open = new Map<string, { postedAt: number; remaining: Set<string> }>()
  const restarts: number[] = []
  const slots: Slot[] = []
  const stats = new Map<string, Stats>()
  const steerTargets = new Set<string>()
  const steered = new Set<string>()
  const steerVerdicts: Record<string, number> = {}
  let stopping = false

  const settle = (id: string) => {
    const name = id.split('.')[0] ?? id
    const entry = open.get(name)
    if (!entry) return
    entry.remaining.delete(id)
    if (entry.remaining.size === 0) open.delete(name)
  }

  const handle = (name: string, event: { e: string; id?: string } & Partial<Stats>) => {
    if (event.e === 'stats' && event.metrics && event.pool)
      stats.set(name, { metrics: event.metrics, pool: event.pool })
    const id = event.id
    if (id === undefined) return
    if (event.e === 'done') {
      doneReports.set(id, (doneReports.get(id) ?? 0) + 1)
      settle(id)
    }
    if (event.e === 'claim' && steerTargets.has(id) && !steered.has(id)) {
      steered.add(id)
      void board
        .steer(id, 'redirect ' + id, { key: 'steer-' + id })
        .then((verdict) => (verdict.ok ? 'ok' : verdict.rejection.kind))
        .catch((error: unknown) => 'threw ' + String(error))
        .then((kind) => {
          steerVerdicts[kind] = (steerVerdicts[kind] ?? 0) + 1
        })
    }
  }

  const start = (index: number) => {
    const incarnation = (incarnations[index] = (incarnations[index] ?? 0) + 1)
    const name = 'w' + index + '-' + incarnation
    const role = {
      concurrency: cell.concurrency,
      leaseMs: cell.leaseMs,
      name,
      partitions: cell.partition,
      where,
      pollMs: cell.pollMs,
      wake: cell.wake,
      scope,
      seed: cell.seed * 1_000_003 + index * 1009 + incarnation,
      workMaxMs: cell.workMaxMs,
      workMinMs: cell.workMinMs,
    }
    const started = Date.now()
    const child = spawn(process.execPath, [workerFile, JSON.stringify(role)], {
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let stderr = ''
    child.stderr?.on('data', (chunk: Buffer) => {
      if (stderr.length < 4000) stderr += chunk.toString()
    })
    const ready = new Promise<number>((resolve) => {
      Readline.createInterface({ input: child.stdout! }).on('line', (line) => {
        const event = (() => {
          try {
            return JSON.parse(line) as { e: string; id?: string } & Partial<Stats>
          } catch {
            return undefined
          }
        })()
        if (!event) return
        handle(name, event)
        if (event.e === 'ready') resolve(Date.now() - started)
      })
    })
    child.on('exit', (code, signal) => {
      if (killed.has(name) || (stopping && (code === 0 || signal === 'SIGTERM'))) return
      const head = stderr.trim().split('\n').slice(0, 6).join(' | ')
      exits.push(name + ' exited ' + (code ?? signal) + ': ' + head)
    })
    slots[index] = { child, name, ready }
  }

  for (let index = 0; index < cell.worker; index++) start(index)
  const startup = await Promise.race([
    Promise.all(slots.map((slot) => slot.ready)),
    sleep(30_000).then(() => undefined),
  ])
  if (!startup) throw new Error('workers did not start: ' + exits.join('; '))

  let next = 0
  let lastReconcile = Date.now()
  const reconcile = async () => {
    lastReconcile = Date.now()
    await board.refresh()
    for (const entry of [...open.values()])
      for (const id of [...entry.remaining])
        if ((await board.task(id))?.phase === 'done') settle(id)
  }

  const began = Date.now()
  const killer = setInterval(() => {
    const index = kills.size % cell.worker
    const slot = slots[index]
    if (!slot) return
    killed.add(slot.name)
    kills.set(slot.name, Date.now())
    slot.child.kill('SIGKILL')
    start(index)
    void slots[index]!.ready.then((ms) => restarts.push(ms))
  }, cell.killEveryMs)

  while (Date.now() - began < cell.postMs) {
    while (open.size < inflight && Date.now() - began < cell.postMs) {
      const name = 's' + next++
      const tasks = swarm(cell, draw, name)
      for (const task of tasks) if (draw() < cell.steerShare) steerTargets.add(task.id)
      open.set(name, { postedAt: Date.now(), remaining: new Set(tasks.map((task) => task.id)) })
      const verdicts = await board.group(tasks.map(post))
      const refused = verdicts.filter((verdict) => !verdict.ok)
      if (refused.length > 0)
        throw new Error('post of ' + name + ' refused: ' + JSON.stringify(refused[0]))
      for (const [id, count] of doneReports) if (count > 0) settle(id)
    }
    await sleep(10)
    if (Date.now() - lastReconcile > 2_000) await reconcile()
  }
  const postedSwarms = next
  const drainStarted = Date.now()
  while (open.size > 0 && Date.now() - drainStarted < cell.drainMs) {
    await sleep(50)
    if (Date.now() - lastReconcile > 1_000) await reconcile()
  }
  const drained = open.size === 0
  clearInterval(killer)

  stopping = true
  await Promise.all(
    slots.map(
      (slot) =>
        new Promise<void>((resolve) => {
          if (slot.child.exitCode !== null) return resolve()
          const force = setTimeout(() => slot.child.kill('SIGKILL'), 10_000)
          slot.child.on('exit', () => {
            clearTimeout(force)
            resolve()
          })
          slot.child.kill('SIGTERM')
        }),
    ),
  )
  const wall = Date.now() - began

  await board.refresh()
  const notDone = (await board.tasks()).filter((task) => task.phase !== 'done')
  await board.close()
  await sql.close()

  const audited = audit(await logRows(where), cell, kills)
  violations.push(...audited.violations)
  if (!drained) violations.push(open.size + ' swarms not done after the drain')
  if (notDone.length > 0)
    violations.push(notDone.length + ' tasks not done, first ' + JSON.stringify(notDone[0]))
  for (const [id, count] of doneReports)
    if (count > 1) violations.push(id + ' reported done ' + count + ' times')
  violations.push(...exits)

  const all = [...stats.values()]
  const sum = (pick: (entry: Stats) => number) => all.reduce((acc, entry) => acc + pick(entry), 0)
  const errors = { busy: 0, constraint: 0, other: 0 } as Record<string, number>
  const rejected: Record<string, number> = {}
  for (const entry of all) {
    for (const [kind, count] of Object.entries(entry.metrics.errors))
      errors[kind] = (errors[kind] ?? 0) + count
    for (const [kind, count] of Object.entries(entry.metrics.rejected))
      rejected[kind] = (rejected[kind] ?? 0) + count
  }
  const writes = merge(all.map((entry) => entry.metrics.write))
  const reads = merge(all.map((entry) => entry.metrics.read))
  const refresh = merge(all.map((entry) => entry.metrics.refresh))
  const writeCount = total(writes)
  const errorCount = Object.values(errors).reduce((acc, count) => acc + count, 0)
  const seconds = audited.elapsedMs / 1000
  const distinct = (values: (string | undefined)[]) =>
    [...new Set(values.filter(Boolean))].slice(0, 3)
  const lastErrors = distinct(all.map((entry) => entry.metrics.lastError))
  const refreshMs = sum((entry) => entry.metrics.refreshMs)
  const refreshes = sum((entry) => entry.metrics.refreshes)

  const result = {
    cell: {
      concurrency: cell.concurrency,
      inflight,
      leaseMs: cell.leaseMs,
      partitions: cell.partition,
      pollMs: cell.pollMs,
      wake: cell.wake,
      workers: cell.worker,
    },
    claimRejections: rejected,
    claims: audited.claims,
    claimsPerSecond: round(audited.claims / seconds),
    dir,
    donePerSecond: round(audited.tasks / seconds),
    elapsedMs: audited.elapsedMs,
    kills: kills.size,
    latencyMs: {
      p50: quantile(audited.latency, 0.5),
      p95: quantile(audited.latency, 0.95),
      p99: quantile(audited.latency, 0.99),
    },
    perPartition: audited.perScope,
    pool: {
      errors: sum((entry) => entry.pool.errors),
      lastErrors: distinct(all.map((entry) => entry.pool.lastError)),
      lost: sum((entry) => entry.pool.lost),
      polls: sum((entry) => entry.pool.polls),
      retries: sum((entry) => entry.pool.retries),
    },
    reclaimed: audited.reclaimed,
    refresh: {
      count: total(refresh),
      meanMs: round(refreshMs / Math.max(1, refreshes)),
      p50Ms: histogramQuantile(refresh, 0.5),
      p99Ms: histogramQuantile(refresh, 0.99),
      shareOfWorkerTime: round((100 * refreshMs) / (cell.worker * wall)),
    },
    restartReadyMs: { p50: quantile(restarts, 0.5), max: quantile(restarts, 1) },
    retries: {
      errors,
      lastErrors,
      perThousandWrites: round((1000 * errorCount) / Math.max(1, writeCount)),
      perSecond: round(errorCount / seconds),
    },
    rows: audited.rows,
    sql: {
      readP99Ms: histogramQuantile(reads, 0.99),
      reads: total(reads),
      writeP50Ms: histogramQuantile(writes, 0.5),
      writeP99Ms: histogramQuantile(writes, 0.99),
      writes: writeCount,
    },
    steers: {
      delivered: sum((entry) => entry.metrics.steered),
      sent: steered.size,
      targets: steerTargets.size,
      verdicts: steerVerdicts,
    },
    swarmLatencyMs: { p50: quantile(audited.swarms, 0.5), p99: quantile(audited.swarms, 0.99) },
    swarms: postedSwarms,
    takeoverMs: {
      count: audited.takeover.length,
      max: quantile(audited.takeover, 1),
      p50: quantile(audited.takeover, 0.5),
      p99: quantile(audited.takeover, 0.99),
      withoutKill: audited.takeoversWithoutKill,
    },
    tasks: audited.tasks,
    violations: violations.slice(0, 50),
    violationCount: violations.length,
    wallMs: wall,
  }
  return result
}

const sha = (() => {
  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], { cwd: here, encoding: 'utf8' }).trim()
  } catch {
    return 'unknown'
  }
})()

const config = parse(process.argv.slice(2))
const machine = {
  cores: Os.availableParallelism(),
  cpu: Os.cpus()[0]?.model ?? 'unknown',
  lease: process.env.NC_BUILD_LEASE_CORES ?? 'none',
  node: process.version,
  sha,
}
console.log('MACHINE ' + JSON.stringify(machine))
console.log('CONFIG ' + JSON.stringify(config))

const results = []
for (const partition of config.partitions)
  for (const worker of config.workers) {
    const result = await run({ ...config, partition, worker })
    results.push(result)
    console.log('RESULT ' + JSON.stringify(result))
  }

const rows = results.map((result) =>
  [
    result.cell.partitions,
    result.cell.workers,
    result.tasks,
    result.claimsPerSecond,
    result.latencyMs.p50 + ' / ' + result.latencyMs.p95 + ' / ' + result.latencyMs.p99,
    result.takeoverMs.p50 + ' / ' + result.takeoverMs.max + ' (' + result.takeoverMs.count + ')',
    result.retries.perThousandWrites,
    result.refresh.p50Ms + ' / ' + result.refresh.p99Ms,
    result.violationCount,
  ].join(' | '),
)
console.log(
  [
    '| partitions | workers | tasks | claims/s | latency p50 / p95 / p99 ms | ' +
      'takeover p50 / max ms (n) | retries per 1k writes | refresh p50 / p99 ms | violations |',
    '|---|---|---|---|---|---|---|---|---|',
    ...rows.map((row) => '| ' + row + ' |'),
  ].join('\n'),
)
const saved = Path.join(output, 'results-' + Date.now() + '.json')
Fs.writeFileSync(saved, JSON.stringify({ config, machine, results }, null, 2))
process.exitCode = results.some((result) => result.violationCount > 0) ? 1 : 0
