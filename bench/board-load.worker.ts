import { Board, Sql } from 'nanocode-ts/node'
import { Sql as Postgres } from 'nanocode-ts/postgres'

type Role = {
  concurrency: number
  leaseMs: number
  name: string
  partitions: number
  where: { sql: 'sqlite'; path: string } | { sql: 'postgres'; url: string; schema: string }
  pollMs: number
  wake: boolean
  scope: string
  seed: number
  workMaxMs: number
  workMinMs: number
}

type Histogram = Record<string, number>

type Operation = { count: number; ms: number }

const role = JSON.parse(process.argv[2] ?? '{}') as Role

const emit = (line: object) => process.stdout.write(JSON.stringify(line) + '\n')

const record = (histogram: Histogram, ms: number) => {
  const key = String(Math.round(Math.log10(Math.max(ms, 0.001)) * 20))
  histogram[key] = (histogram[key] ?? 0) + 1
}

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

function classify(error: unknown) {
  const fields = error as { code?: unknown; errcode?: unknown; message?: unknown } | null
  const text = String(fields?.message ?? error)
  if (fields?.code === '40001' || fields?.code === '40P01' || fields?.code === '55P03')
    return 'busy'
  if (fields?.errcode === 5 || fields?.errcode === 6 || /locked|busy/i.test(text)) return 'busy'
  if (fields?.code === '23505' || fields?.errcode === 19 || /constraint/i.test(text))
    return 'constraint'
  return 'other'
}

const metrics = {
  errors: { busy: 0, constraint: 0, other: 0 } as Record<string, number>,
  lastError: '',
  operations: {} as Record<string, Operation>,
  read: {} as Histogram,
  refresh: {} as Histogram,
  refreshMs: 0,
  refreshes: 0,
  rejected: {} as Record<string, number>,
  steered: 0,
  write: {} as Histogram,
}

const where = role.where
const postgres =
  where.sql === 'postgres' ? Postgres.open(where.url, { schema: where.schema, max: 2 }) : undefined
const host: Board.Options['sql'] & { close(): unknown } =
  where.sql === 'sqlite' ? Sql.open(where.path) : postgres!

const failed = (error: unknown) => {
  const type = classify(error)
  metrics.errors[type] = (metrics.errors[type] ?? 0) + 1
  metrics.lastError = String((error as Error | null)?.message ?? error).slice(0, 200)
}

const timed: Board.Options['sql'] = {
  ...host,
  transaction(kind, operation, statements, access) {
    const started = performance.now()
    const done = () => {
      const ms = performance.now() - started
      const entry = (metrics.operations[operation] ??= { count: 0, ms: 0 })
      entry.count++
      entry.ms += ms
      const reads = access === 'read' || /load|scan|read/.test(operation)
      record(reads ? metrics.read : metrics.write, ms)
    }
    let result: ReturnType<Board.Options['sql']['transaction']>
    try {
      result = host.transaction(kind, operation, statements, access)
    } catch (error) {
      failed(error)
      done()
      throw error
    }
    if (!(result instanceof Promise)) {
      done()
      return result
    }
    return result.then(
      (rows) => {
        done()
        return rows
      },
      (error: unknown) => {
        failed(error)
        done()
        throw error
      },
    )
  },
}

const board = await Board.open({
  actor: 'agent:' + role.name,
  charter: false,
  partitions: role.partitions,
  scope: role.scope,
  sql: timed,
})

const draw = random(role.seed)

const observed: Board.Board = {
  ...board,

  async claim(id, options) {
    const claimed = await board.claim(id, options)
    if (claimed.ok) emit({ at: Date.now(), e: 'claim', id, tookOver: claimed.tookOver })
    else {
      const kind = claimed.rejection.kind
      metrics.rejected[kind] = (metrics.rejected[kind] ?? 0) + 1
    }
    return claimed
  },

  async finish(id, claim, outcome, atMs) {
    const finished = await board.finish(id, claim, outcome, atMs)
    if (finished.ok) emit({ at: Date.now(), e: 'done', id })
    return finished
  },

  async refresh() {
    const started = performance.now()
    await board.refresh()
    const ms = performance.now() - started
    metrics.refreshes++
    metrics.refreshMs += ms
    record(metrics.refresh, ms)
  },
}

const pool = Board.pool(observed, {
  concurrency: role.concurrency,
  leaseMs: role.leaseMs,
  pollMs: role.pollMs,
  wake: role.wake && postgres ? Postgres.wake(postgres) : undefined,
  async run(_task, lease) {
    await sleep(role.workMinMs + draw() * (role.workMaxMs - role.workMinMs))
    const steers = await lease.steers()
    const last = steers.at(-1)
    if (last) {
      await lease.heed(last.seq)
      metrics.steered++
    }
    return { done: role.name }
  },
})

const report = () => emit({ e: 'stats', metrics, pool: pool.stats() })

const ticker = setInterval(report, 500)

emit({ at: Date.now(), e: 'ready' })

process.on('SIGTERM', () => {
  clearInterval(ticker)
  void pool
    .stop()
    .then(() => board.close())
    .then(() => host.close())
    .then(() => {
      report()
      process.exit(0)
    })
})
