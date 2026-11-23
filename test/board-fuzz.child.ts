import * as fs from 'node:fs'
import { Board } from 'nanocode-ts/node'
import {
  generator,
  type Lane,
  lanes,
  openSql,
  type Placed,
  placer,
  projects,
} from './board-fuzz.shared.ts'

type Config = {
  child: number
  generation: number
  journal: string
  partitions: number
  path: string
  pool: boolean
  scope: string
  seed: number
  untilMs: number
}

type Held = { claim: string; id: string }

const config = JSON.parse(process.argv[2]!) as Config
const actor = 'agent:c' + config.child
const random = generator(config.seed)
const sql = openSql(config.path)
const board = await Board.open({ sql, scope: config.scope, actor, partitions: config.partitions })
const held = new Map<string, Held>()
const placed = new Map<string, Placed>()
const place = placer(config.partitions, (id) => placed.get(id))
let known: string[] = []
let live: string[] = []
let count = 0

function below(n: number) {
  return Math.floor(random() * n)
}

function pick<T>(items: readonly T[]): T | undefined {
  return items.length === 0 ? undefined : items[below(items.length)]
}

function record(entry: object) {
  fs.appendFileSync(
    config.journal,
    JSON.stringify({ child: config.child, generation: config.generation, ...entry }) + '\n',
  )
}

async function timed<T>(call: () => Promise<T>) {
  const t0 = Date.now()
  const result = await call()
  return { result, t0, t1: Date.now() }
}

function verdict(
  kind: string,
  fields: object,
  { result, t0, t1 }: { result: Board.Verdict; t0: number; t1: number },
) {
  record(
    result.ok
      ? { kind, ok: true, event: result.event, seq: result.seq, t0, t1, ...fields }
      : { kind, ok: false, rejection: result.rejection.kind, t0, t1, ...fields },
  )
  return result
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function learn(task: Board.Task) {
  placed.set(task.id, { id: task.id, labels: task.labels, parent: task.parent })
}

async function look() {
  await board.refresh()
  const tasks = await board.tasks()
  for (const task of tasks) learn(task)
  known = tasks.map((task) => task.id)
  live = tasks
    .filter((task) => task.phase === 'open' || task.phase === 'claimed')
    .map((task) => task.id)
  for (let i = 0; i < 4; i++) {
    const task = pick(tasks)
    if (task && (task.phase === 'done' || task.phase === 'failed'))
      record({
        kind: 'seen',
        id: task.id,
        outcome: task.outcome,
        phase: task.phase,
        worker: task.worker ?? null,
      })
  }
  return tasks
}

async function post() {
  const id = 'c' + config.child + 'g' + config.generation + 'n' + count
  const slot = below(12)
  const key = random() < 0.35 ? 'post-' + slot : undefined
  const shape = random()
  const parent = key === undefined && shape < 0.2 ? pick(known.slice(-30)) : undefined
  const project =
    key !== undefined
      ? projects[slot % projects.length]
      : parent !== undefined
        ? random() < 0.3
          ? pick(projects)
          : undefined
        : shape < 0.8
          ? pick(projects)
          : undefined
  const roll = random()
  const lane: Lane | 'pool' | undefined =
    roll < 0.15 ? 'capA' : roll < 0.3 ? 'capB' : roll < 0.5 ? 'pool' : undefined
  const labels: Record<string, string> = {
    ...(key === undefined ? {} : { key: actor + '#' + key }),
    ...(project === undefined ? {} : { project }),
    ...(lane === undefined ? {} : { lane }),
  }
  const task: Placed = { id, labels, parent: parent ?? null }
  const partition = place(task)
  const nearby = known.slice(-60).filter((other) => {
    const where = placed.get(other)
    return where !== undefined && place(where) === partition
  })
  const needs = [
    ...new Set(
      Array.from({ length: below(4) }, () =>
        random() < 0.85 ? pick(nearby) : pick(known.slice(-30)),
      ),
    ),
  ].filter((need) => need !== undefined)
  const result = await timed(() =>
    board.post(
      { id, labels, needs, spec: 'fuzz ' + id, ...(parent === undefined ? {} : { parent }) },
      key === undefined ? {} : { key },
    ),
  )
  verdict(
    'post',
    {
      id,
      key: key === undefined ? null : actor + '#' + key,
      labels,
      needs,
      parent: parent ?? null,
    },
    result,
  )
  if (result.result.ok && key === undefined) {
    placed.set(id, task)
    known.push(id)
    live.push(id)
  }
}

function capFor(id: string): Board.Cap | undefined {
  const lane = placed.get(id)?.labels.lane
  if (lane === 'capA' || lane === 'capB') return { max: lanes[lane], label: ['lane', lane] }
  const roll = random()
  if (roll < 0.1) return 1 + below(4)
  if (roll < 0.2) return { max: 1 + below(2), label: ['lane', random() < 0.5 ? 'capA' : 'capB'] }
  return undefined
}

async function claimed(
  target: Board.Board,
  id: string,
  options: { cap?: Board.Cap | undefined; leaseMs: number },
  pool: boolean,
) {
  const { cap, leaseMs } = options
  const { result, t0, t1 } = await timed(() =>
    target.claim(id, cap === undefined ? { leaseMs } : { cap, leaseMs }),
  )
  record(
    result.ok
      ? {
          kind: 'claim',
          ok: true,
          id,
          cap: cap ?? null,
          claim: result.claim,
          leaseMs,
          pool,
          seq: result.seq,
          t0,
          t1,
          tookOver: result.tookOver,
          untilMs: result.untilMs,
        }
      : {
          kind: 'claim',
          ok: false,
          id,
          cap: cap ?? null,
          leaseMs,
          pool,
          rejection: result.rejection.kind,
          rejectionCap: (result.rejection as { cap?: unknown }).cap ?? null,
          t0,
          t1,
        },
  )
  return result
}

async function claim() {
  const ready = random() < 0.6 ? await board.ready() : []
  for (const task of ready) learn(task)
  const id = pick(ready)?.id ?? (random() < 0.8 ? pick(live) : pick(known))
  if (id === undefined || held.has(id) || !placed.has(id)) return
  const result = await claimed(board, id, { cap: capFor(id), leaseMs: 50 + below(251) }, false)
  if (!result.ok) {
    if (result.rejection.kind === 'Terminal') live = live.filter((other) => other !== id)
    return
  }
  held.set(id, { claim: result.claim, id })
}

async function pending(lease: Held) {
  return board.steers(lease.id, lease.claim).catch((error: unknown) => {
    record({ kind: 'steers', ok: false, id: lease.id, message: String(error) })
    return null
  })
}

async function heed(lease: Held) {
  const steers = await pending(lease)
  if (!steers) return false
  const upto = random() < 0.15 ? 1 + below(Math.max(1, steers[0]?.seq ?? 1)) : pick(steers)?.seq
  if (upto === undefined) return true
  const result = verdict(
    'heed',
    { id: lease.id, claim: lease.claim, upto },
    await timed(() => board.heed(lease.id, lease.claim, upto)),
  )
  return result.ok || result.rejection.kind === 'Steer'
}

async function work() {
  const lease = pick([...held.values()])
  if (!lease) return claim()
  const roll = random()
  if (roll < 0.2) {
    const leaseMs = 50 + below(251)
    const result = verdict(
      'renew',
      { id: lease.id, claim: lease.claim, leaseMs },
      await timed(() => board.renew(lease.id, lease.claim, leaseMs)),
    )
    if (!result.ok) held.delete(lease.id)
  } else if (roll < 0.3) {
    verdict(
      'release',
      { id: lease.id, claim: lease.claim },
      await timed(() => board.release(lease.id, lease.claim, 'fuzz')),
    )
    held.delete(lease.id)
  } else if (roll < 0.55) {
    if (!(await heed(lease))) held.delete(lease.id)
  } else {
    const steers = await pending(lease)
    const last = steers?.at(-1)
    if (last && random() < 0.9)
      verdict(
        'heed',
        { id: lease.id, claim: lease.claim, upto: last.seq },
        await timed(() => board.heed(lease.id, lease.claim, last.seq)),
      )
    const tag = 'c' + config.child + 'g' + config.generation + 'o' + count
    const outcome = random() < 0.7 ? { done: tag } : { failed: tag }
    const result = verdict(
      'finish',
      { id: lease.id, claim: lease.claim, outcome },
      await timed(() => board.finish(lease.id, lease.claim, outcome)),
    )
    if (result.ok || result.rejection.kind !== 'Steer') held.delete(lease.id)
    if (result.ok) live = live.filter((other) => other !== lease.id)
  }
}

async function steer() {
  const id =
    (random() < 0.5 ? pick([...held.keys()]) : undefined) ??
    (random() < 0.8 ? pick(live) : pick(known))
  if (id === undefined) return
  const key = random() < 0.4 ? 'steer-' + below(10) : undefined
  const body =
    key === undefined
      ? 'c' + config.child + 'g' + config.generation + 's' + count
      : actor + '#' + key
  const lease = held.get(id)
  const options = {
    ...(key === undefined ? {} : { key }),
    ...(lease && random() < 0.5 ? { claim: lease.claim } : {}),
  }
  verdict(
    'steer',
    { id, body, key: key === undefined ? null : actor + '#' + key },
    await timed(() => board.steer(id, body, options)),
  )
}

function poolCap(lane: Lane | 'pool', poolRandom: () => number): Board.PoolOptions['cap'] {
  const roll = poolRandom()
  if (lane !== 'pool') {
    const shared: Board.Cap = { max: lanes[lane], label: ['lane', lane] }
    return roll < 0.5 ? shared : (task) => (task.labels.lane === lane ? shared : 1)
  }
  if (roll < 0.3) return undefined
  if (roll < 0.6) return 2 + Math.floor(poolRandom() * 3)
  return () => {
    const chosen = poolRandom()
    if (chosen < 0.4) return undefined
    if (chosen < 0.7) return 2 + Math.floor(poolRandom() * 3)
    return { max: 1, label: ['lane', 'capA'] }
  }
}

async function startPool() {
  const poolRandom = generator(config.seed ^ 0x5bd1e995)
  const poolSql = openSql(config.path)
  const poolBoard = await Board.open({
    sql: poolSql,
    scope: config.scope,
    actor: actor + 'p',
    partitions: config.partitions,
  })
  const lane = (['pool', 'capA', 'capB'] as const)[Math.floor(poolRandom() * 3)]!
  const cap = poolCap(lane, poolRandom)
  record({
    kind: 'pool-start',
    lane,
    capKind:
      typeof cap === 'function'
        ? 'function'
        : typeof cap === 'number'
          ? 'number'
          : cap === undefined
            ? 'none'
            : 'label',
  })
  const journaled: Board.Board = {
    ...poolBoard,
    claim: (id, options) => claimed(poolBoard, id, options, true),
    finish: async (id, claim, outcome) =>
      verdict(
        'finish',
        { id, claim, outcome, pool: true },
        await timed(() => poolBoard.finish(id, claim, outcome)),
      ),
    heed: async (id, claim, upto) =>
      verdict(
        'heed',
        { id, claim, upto, pool: true },
        await timed(() => poolBoard.heed(id, claim, upto)),
      ),
    release: async (id, claim, reason) =>
      verdict(
        'release',
        { id, claim, pool: true },
        await timed(() => poolBoard.release(id, claim, reason)),
      ),
    renew: async (id, claim, leaseMs) =>
      verdict(
        'renew',
        { id, claim, leaseMs, pool: true },
        await timed(() => poolBoard.renew(id, claim, leaseMs)),
      ),
  }
  let runs = 0
  const pool = Board.pool(journaled, {
    cap,
    concurrency: 1 + Math.floor(poolRandom() * 2),
    label: ['lane', lane],
    leaseMs: 100 + Math.floor(poolRandom() * 200),
    pollMs: 20 + Math.floor(poolRandom() * 60),
    run: async (task, lease) => {
      const tag = 'c' + config.child + 'g' + config.generation + 'p' + ++runs
      record({ kind: 'pool-run', id: task.id, tag, tookOver: lease.tookOver })
      await sleep(Math.floor(poolRandom() * 60))
      if (poolRandom() < 0.6) for (const steer of await lease.steers()) await lease.heed(steer.seq)
      return poolRandom() < 0.75 ? { done: tag } : { failed: tag }
    },
  })
  return {
    async stop() {
      await pool.stop()
      record({ kind: 'pool-stats', stats: pool.stats() })
      await poolBoard.close()
      await poolSql.close()
    },
  }
}

try {
  const pool = config.pool ? await startPool() : undefined
  await look()
  while (Date.now() < config.untilMs) {
    count++
    const roll = random()
    if (roll < 0.25) await post()
    else if (roll < 0.4) await claim()
    else if (roll < 0.7) await work()
    else if (roll < 0.9) await steer()
    else await look()
    const pause = random()
    if (pause < 0.03) await sleep(100 + below(300))
    else if (pause < 0.5) await sleep(below(15))
  }
  await pool?.stop()
  await board.close()
  await sql.close()
  record({ kind: 'end' })
} catch (error) {
  const message = String((error as Error)?.stack ?? error)
  record({ kind: 'error', message })
  process.stderr.write(message + '\n')
  process.exit(1)
}
