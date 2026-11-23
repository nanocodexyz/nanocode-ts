// Benchmarks the Board the way a swarm deploys it: one Board per Cloudflare Durable Object, over
// nanocode-ts/cloudflare's Sql.create(ctx.storage), served locally by workerd. bench/board-do.worker.ts
// is bundled with rolldown as test/board-cloudflare.test.ts bundles its worker.
//
// load: one object serves swarms (a root and 20 children in a DAG, about 2% of tasks steered) to K
// concurrent clients, each claiming, renewing and finishing over fetch. Reports claims/s, the
// per-call latency seen by the client and inside the object, and the refresh share.
// replay: one object's log grows to each size; at each, the object restarts (ctx.abort) several
// times and the wake is timed: Board.open, the first read, and a first post, claim and finish.
// Every run ends with an audit of the log: every task finished exactly once.
//
// npm run bench:board-do [-- --clients 1,4,16 --modes one,actors --swarms 120
//   --sizes 1000,2500,5000,10000,25000,50000 --restarts 5 --phases load,replay --checkpoints on,off]
// --checkpoints on wakes from the board's fold checkpoints; off drops them before every restart
// and opens with checkpointEvery 0, so each wake replays the whole log.
// Needs dist/ (npm run build:ts and the board WASM), like the test.
import { execFileSync, spawn, spawnSync, type ChildProcess } from 'node:child_process'
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs'
import * as Os from 'node:os'
import { basename, join } from 'node:path'
import type { Readable } from 'node:stream'
import { fileURLToPath } from 'node:url'
import { rolldown } from 'rolldown'
import workerdModule from 'workerd'

// workerd is CommonJS with a default export, which Node's ESM interop wraps once more.
const workerd: string =
  typeof workerdModule === 'string'
    ? workerdModule
    : (workerdModule as unknown as { default: string }).default
const root = fileURLToPath(new URL('..', import.meta.url))
const output = join(root, 'output', 'bench', 'board-do')
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

type Mode = 'one' | 'actors'

type Config = {
  checkpoints: ('on' | 'off')[]
  children: number
  clients: number[]
  keep: boolean
  leaseMs: number
  modes: Mode[]
  phases: string[]
  restarts: number
  seed: number
  sizes: number[]
  steer: number
  swarms: number
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
  const modes = (flags.get('modes') ?? 'one,actors').split(',')
  for (const mode of modes)
    if (mode !== 'one' && mode !== 'actors') throw new Error('--modes takes one and actors')
  const checkpoints = (flags.get('checkpoints') ?? 'on,off').split(',')
  for (const value of checkpoints)
    if (value !== 'on' && value !== 'off') throw new Error('--checkpoints takes on and off')
  return {
    checkpoints: checkpoints as ('on' | 'off')[],
    children: number('children', 20),
    clients: list('clients', '1,4,16'),
    keep: flags.get('keep') === 'true',
    leaseMs: number('lease-ms', 30_000),
    modes: modes as Mode[],
    phases: (flags.get('phases') ?? 'load,replay').split(','),
    restarts: number('restarts', 5),
    seed: number('seed', 1),
    sizes: list('sizes', '1000,2500,5000,10000,25000,50000'),
    steer: number('steer', 0.02),
    swarms: number('swarms', 120),
  }
}

function random(seed: number) {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let value = Math.imul(state ^ (state >>> 15), 1 | state)
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296
  }
}

type NewTask = {
  id: string
  labels: Record<string, string>
  needs: string[]
  parent?: string
  spec: string
}

// The swarm shape of bench/board-load.ts: the first four children need the root, each later
// child one or two of the six before it.
function swarm(name: string, children: number, draw: () => number): NewTask[] {
  const labels = { project: name }
  const tasks: NewTask[] = [{ id: name, labels, needs: [], spec: 'root of ' + name }]
  for (let index = 0; index < children; index++) {
    const needs = new Set<string>()
    if (index < 4) needs.add(name)
    else {
      const from = Math.max(0, index - 6)
      const count = 1 + Math.floor(draw() * 2)
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

function quantile(values: readonly number[], q: number) {
  if (values.length === 0) return null
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? null
}

const round = (value: number | null | undefined, places = 2) =>
  value === null || value === undefined ? null : Number(value.toFixed(places))

const median = (values: readonly number[]) => quantile(values, 0.5)

// In-object times come from workerd's clock, which runs in real time at 1 ms resolution: their
// means are sound, their percentiles are whole milliseconds.
function spread(values: readonly number[]) {
  return {
    n: values.length,
    mean: round(values.reduce((sum, value) => sum + value, 0) / Math.max(1, values.length), 3),
    p50: round(quantile(values, 0.5)),
    p95: round(quantile(values, 0.95)),
    p99: round(quantile(values, 0.99)),
    max: round(quantile(values, 1)),
  }
}

const dir = mkdtempSync(join(Os.tmpdir(), 'nanocode-board-do-'))
const storage = join(dir, 'do')

async function bundle(): Promise<string> {
  const build = await rolldown({
    input: join(root, 'bench', 'board-do.worker.ts'),
    platform: 'neutral',
    external: [/^cloudflare:/, /^node:/, /\.wasm$/],
    resolve: { conditionNames: ['workerd', 'worker', 'import', 'default'] },
    logLevel: 'silent',
  })
  const { output: chunks } = await build.generate({ format: 'esm' })
  await build.close()
  const modules: string[] = []
  const wasm = new Set<string>()
  for (const chunk of chunks) {
    if (chunk.type !== 'chunk') continue
    writeFileSync(join(dir, chunk.fileName), chunk.code)
    const name = JSON.stringify(chunk.fileName)
    const entry = '(name = ' + name + ', esModule = embed ' + name + ')'
    if (chunk.isEntry) modules.unshift(entry)
    else modules.push(entry)
    for (const one of [...chunk.imports, ...chunk.dynamicImports])
      if (one.endsWith('.wasm')) wasm.add(one)
  }
  for (const specifier of wasm) {
    const file = join(root, 'dist', basename(specifier))
    if (!existsSync(file)) continue // only a dynamic import (python.wasm) may be missing
    copyFileSync(file, join(dir, basename(specifier)))
    const wasmName = JSON.stringify(basename(specifier))
    modules.push('(name = ' + JSON.stringify(specifier) + ', wasm = embed ' + wasmName + ')')
  }
  if (!wasm.has('nanocode-ts/nanocode_wasm.wasm'))
    throw new Error('the bundle does not import the board WASM')
  mkdirSync(storage, { recursive: true })
  const config = join(dir, 'config.capnp')
  writeFileSync(
    config,
    [
      'using Workerd = import "/workerd/workerd.capnp";',
      'const config :Workerd.Config = (',
      '  services = [',
      '    (name = "main", worker = .worker),',
      '    (name = "storage", disk = (path = ' + JSON.stringify(storage) + ', writable = true)),',
      '  ],',
      '  sockets = [(name = "http", address = "127.0.0.1:0", http = (), service = "main")],',
      ');',
      'const worker :Workerd.Worker = (',
      '  modules = [' + modules.join(', ') + '],',
      '  compatibilityDate = "2026-10-01",',
      '  compatibilityFlags = ["nodejs_compat"],',
      '  bindings = [(name = "BOARD", durableObjectNamespace = "BoardObject")],',
      '  durableObjectNamespaces = [',
      '    (className = "BoardObject", uniqueKey = "nanocode-board-do-bench", enableSql = true),',
      '  ],',
      '  durableObjectStorage = (localDisk = "storage"),',
      ');',
      '',
    ].join('\n'),
  )
  return config
}

// Starts workerd and resolves with its URL once it reports its port (--control-fd).
function start(config: string): Promise<{ url: string; proc: ChildProcess }> {
  const proc = spawn(workerd, ['serve', config, '--experimental', '--control-fd=3'], {
    cwd: dir,
    stdio: ['ignore', 'pipe', 'pipe', 'pipe'],
  })
  let log = ''
  proc.stdout!.on('data', (chunk) => (log = (log + chunk).slice(-8000)))
  proc.stderr!.on('data', (chunk) => (log = (log + chunk).slice(-8000)))
  return new Promise((resolve, reject) => {
    let control = ''
    ;(proc.stdio[3] as Readable).on('data', (chunk) => {
      control += chunk
      for (const line of control.split('\n')) {
        if (!line.trim()) continue
        try {
          const event = JSON.parse(line) as { event?: string; socket?: string; port?: number }
          if (event.event === 'listen' && event.socket === 'http')
            resolve({ url: 'http://127.0.0.1:' + event.port, proc })
        } catch {}
      }
    })
    proc.on('error', reject)
    proc.on('exit', (code) => reject(new Error('workerd exited ' + code + '\n' + log)))
  })
}

type Reply<T> = { boot: string; ms: number; value: T; wallMs: number }

let base = ''

async function send<T = Record<string, unknown>>(object: string, body: object): Promise<Reply<T>> {
  const started = performance.now()
  const response = await fetch(base + '/' + object, { method: 'POST', body: JSON.stringify(body) })
  const reply = (await response.json()) as Reply<T> & { error?: string; aborted?: string }
  if (reply.error) throw new Error(JSON.stringify(body).slice(0, 200) + ' failed:\n' + reply.error)
  reply.wallMs = performance.now() - started
  return reply
}

// ctx.abort() fails the call that asked for it; the next call constructs a new instance.
async function restart(object: string) {
  const before = (await send<{ boot: string }>(object, { method: 'boot' })).value.boot
  const started = performance.now()
  const response = await fetch(base + '/' + object, {
    method: 'POST',
    body: JSON.stringify({ method: 'abort' }),
  })
  const reply = (await response.json()) as { aborted?: string }
  if (!reply.aborted) throw new Error('abort did not restart ' + object)
  const booted = await send<{ boot: string }>(object, { method: 'boot' })
  if (booted.value.boot === before) throw new Error(object + ' did not restart')
  return { abortMs: performance.now() - started, bootMs: booted.wallMs }
}

type Claim = {
  attempts: number
  claim?: string
  claimMs?: number
  id: string | null
  readyCount?: number
  readyMs?: number
  refreshMs: number
  rejection?: string
}

type Audit = {
  bodyBytes: number
  databaseSize: number
  events: number
  ops: Record<string, number>
  rows: number
  tasks: number
  violationCount: number
  violations: string[]
}

type Operations = Record<string, { count: number; ms: number; errors: number }>

async function load(config: Config, mode: Mode, clients: number) {
  const object = 'load-' + mode + '-k' + clients + '-' + Date.now()
  await send(object, { method: 'setup', mode })
  await send(object, { method: 'operations' })
  const draw = random(config.seed)
  const inflight = Math.max(4, 2 * clients)
  const open = new Map<string, Set<string>>()
  const steerTargets = new Set<string>()
  const finishedBy = new Map<string, number>()
  const wall = { claim: [] as number[], renew: [] as number[], finish: [] as number[] }
  const inside = { claim: [] as number[], renew: [] as number[], finish: [] as number[] }
  const refresh: number[] = []
  const ready: number[] = []
  const misses = { empty: 0, raced: 0 }
  const rejections: Record<string, number> = {}
  const steers: Record<string, number> = {}
  let posted = 0
  let claims = 0
  let heeded = 0
  let busyMs = 0

  const settle = (id: string) => {
    const name = id.split('.')[0]!
    const remaining = open.get(name)
    if (!remaining) return
    remaining.delete(id)
    if (remaining.size === 0) open.delete(name)
  }

  const poster = async () => {
    while (posted < config.swarms) {
      if (open.size >= inflight) {
        await sleep(1)
        continue
      }
      const name = 's' + posted++
      const tasks = swarm(name, config.children, draw)
      for (const task of tasks) if (draw() < config.steer) steerTargets.add(task.id)
      open.set(name, new Set(tasks.map((task) => task.id)))
      await send(object, { method: 'post', tasks })
    }
  }

  const client = async (index: number) => {
    const name = 'c' + index
    for (;;) {
      if (posted >= config.swarms && open.size === 0) return
      const claimed = await send<Claim>(object, {
        client: name,
        leaseMs: config.leaseMs,
        method: 'claim',
      })
      refresh.push(claimed.value.refreshMs)
      if (claimed.value.readyMs !== undefined) ready.push(claimed.value.readyMs)
      busyMs += claimed.ms
      if (!claimed.value.id || !claimed.value.claim) {
        if (claimed.value.rejection)
          rejections[claimed.value.rejection] = (rejections[claimed.value.rejection] ?? 0) + 1
        if (claimed.value.attempts > 0) misses.raced++
        else misses.empty++
        await sleep(1)
        continue
      }
      claims++
      wall.claim.push(claimed.wallMs)
      inside.claim.push(claimed.ms)
      const { id, claim } = claimed.value
      if (steerTargets.has(id)) {
        const steered = await send<{ ok: boolean; rejection?: string }>(object, {
          id,
          method: 'steer',
        })
        const kind = steered.value.ok ? 'ok' : steered.value.rejection!
        steers[kind] = (steers[kind] ?? 0) + 1
      }
      const renewed = await send<{ ok: boolean; rejection?: string }>(object, {
        claim,
        client: name,
        id,
        leaseMs: config.leaseMs,
        method: 'renew',
      })
      wall.renew.push(renewed.wallMs)
      inside.renew.push(renewed.ms)
      busyMs += renewed.ms
      if (!renewed.value.ok) throw new Error('renew of ' + id + ': ' + renewed.value.rejection)
      const finished = await send<{
        heeded?: number
        ok: boolean
        refreshMs: number
        rejection?: string
      }>(object, { claim, client: name, id, method: 'finish' })
      wall.finish.push(finished.wallMs)
      inside.finish.push(finished.ms)
      refresh.push(finished.value.refreshMs)
      busyMs += finished.ms
      if (!finished.value.ok) throw new Error('finish of ' + id + ': ' + finished.value.rejection)
      heeded += finished.value.heeded ?? 0
      finishedBy.set(id, (finishedBy.get(id) ?? 0) + 1)
      settle(id)
    }
  }

  const began = performance.now()
  await Promise.all([poster(), ...Array.from({ length: clients }, (_, index) => client(index))])
  const seconds = (performance.now() - began) / 1000
  const operations = (await send<Operations>(object, { method: 'operations' })).value
  const audit = (await send<Audit>(object, { method: 'audit' })).value
  const violations = [...audit.violations]
  for (const [id, count] of finishedBy)
    if (count !== 1) violations.push(id + ' finished ok ' + count + ' times by the clients')
  if (finishedBy.size !== audit.tasks)
    violations.push(finishedBy.size + ' finishes for ' + audit.tasks + ' tasks')
  const refreshMs = refresh.reduce((sum, ms) => sum + ms, 0)
  const sqlMs = Object.values(operations).reduce((sum, entry) => sum + entry.ms, 0)
  return {
    audit: { events: audit.events, rows: audit.rows, tasks: audit.tasks, ops: audit.ops },
    claims,
    claimsPerSecond: round(claims / seconds, 1),
    clients,
    heeded,
    inside: {
      claim: spread(inside.claim),
      finish: spread(inside.finish),
      renew: spread(inside.renew),
    },
    misses,
    mode,
    operations,
    readyMs: spread(ready),
    refresh: {
      ...spread(refresh),
      meanMs: round(refreshMs / Math.max(1, refresh.length), 3),
      shareOfObjectTime: round((100 * refreshMs) / Math.max(1e-9, busyMs), 1),
    },
    rejections,
    seconds: round(seconds, 2),
    sqlShareOfObjectTime: round((100 * sqlMs) / Math.max(1e-9, busyMs), 1),
    steers,
    steerTargets: steerTargets.size,
    violationCount: violations.length + audit.violationCount - audit.violations.length,
    violations: violations.slice(0, 20),
    wall: { claim: spread(wall.claim), finish: spread(wall.finish), renew: spread(wall.renew) },
  }
}

function diskBytes(path: string): number {
  if (!existsSync(path)) return 0
  const stat = statSync(path)
  if (!stat.isDirectory()) return stat.size
  return readdirSync(path).reduce((sum, name) => sum + diskBytes(join(path, name)), 0)
}

async function replay(config: Config) {
  const object = 'replay-' + Date.now()
  await send(object, { method: 'setup', mode: 'one' })
  const diskBefore = diskBytes(storage)
  const points = []
  let written = 0
  let prefix = 0
  let audit = (await send<Audit>(object, { method: 'audit' })).value
  for (const size of config.sizes) {
    const fillStarted = performance.now()
    while (audit.events < size) {
      const chunk = Math.min(5000, size - audit.events)
      const filled = await send<{ swarms: number; written: number }>(object, {
        children: config.children,
        events: chunk,
        method: 'fill',
        prefix: 'f' + prefix++ + '-',
        seed: config.seed + prefix,
        steer: config.steer,
      })
      written += filled.value.written
      audit = (await send<Audit>(object, { method: 'audit' })).value
    }
    const fillMs = performance.now() - fillStarted
    for (const checkpoints of config.checkpoints) {
      const checkpointEvery = checkpoints === 'off' ? 0 : undefined
      if (checkpoints === 'on') {
        // One uncounted wake that saves a checkpoint, as a closing board does, so each timed wake
        // folds only the few events the wakes before it wrote. An aborted object closes nothing
        // and saves one only every 4,096 events, so a live wake folds up to that many more.
        await restart(object)
        await send(object, { method: 'checkpoint' })
      }
      let opened: unknown = null
      const wake = { abort: [] as number[], boot: [] as number[], open: [] as number[] }
      const steps = {
        claimInside: [] as number[],
        first: [] as number[],
        firstInside: [] as number[],
        readSql: [] as number[],
        readyInside: [] as number[],
        openInside: [] as number[],
        warm: [] as number[],
        write: [] as number[],
        total: [] as number[],
      }
      let tasks = 0
      for (let index = 0; index < config.restarts; index++) {
        if (checkpoints === 'off') await send(object, { method: 'drop_checkpoints' })
        const restarted = await restart(object)
        wake.abort.push(restarted.abortMs)
        wake.boot.push(restarted.bootMs)
        const t0 = performance.now()
        const open = await send<{ openMs: number; opened: unknown }>(object, {
          method: 'open',
          checkpointEvery,
        })
        wake.open.push(open.wallMs)
        steps.openInside.push(open.value.openMs)
        opened = open.value.opened
        // The new instance's SQL time so far is the open's log read; the rest of the open is the fold.
        const read = (await send<Operations>(object, { method: 'operations' })).value
        steps.readSql.push(Object.values(read).reduce((sum, entry) => sum + entry.ms, 0))
        const first = await send<{ count: number; tasksMs: number }>(object, { method: 'tasks' })
        steps.first.push(first.wallMs)
        steps.firstInside.push(first.value.tasksMs)
        tasks = first.value.count
        const warm = await send<{ count: number; tasksMs: number }>(object, { method: 'tasks' })
        steps.warm.push(warm.wallMs)
        // A first write after the wake: post one task, claim it, finish it.
        const w0 = performance.now()
        const id = 'w' + size + '-' + checkpoints + '-' + index
        await send(object, { method: 'post', tasks: [{ id, labels: {}, needs: [], spec: 'wake' }] })
        const claimed = await send<Claim>(object, {
          client: 'lead',
          leaseMs: config.leaseMs,
          method: 'claim',
        })
        if (claimed.value.id !== id) throw new Error('the wake claim took ' + claimed.value.id)
        steps.readyInside.push(claimed.value.readyMs ?? 0)
        steps.claimInside.push(claimed.ms)
        const finished = await send<{ ok: boolean; rejection?: string }>(object, {
          claim: claimed.value.claim,
          client: 'lead',
          id,
          method: 'finish',
        })
        if (!finished.value.ok) throw new Error('the wake finish: ' + finished.value.rejection)
        steps.write.push(performance.now() - w0)
        steps.total.push(performance.now() - t0)
      }
      audit = (await send<Audit>(object, { method: 'audit' })).value
      const disk = diskBytes(storage) - diskBefore
      const point = {
        checkpoints,
        opened,
        bodyBytes: audit.bodyBytes,
        databaseSize: audit.databaseSize,
        diskBytes: disk,
        events: audit.events,
        fillMs: round(fillMs, 0),
        bytesPer1kEvents: Math.round((1000 * audit.databaseSize) / audit.events),
        logBytesPer1kEvents: Math.round((1000 * audit.bodyBytes) / audit.events),
        ops: audit.ops,
        rows: audit.rows,
        size,
        tasks,
        violationCount: audit.violationCount,
        violations: audit.violations,
        wake: {
          abortMs: round(median(wake.abort)),
          bootMs: round(median(wake.boot)),
          firstReadInsideMs: round(median(steps.firstInside)),
          firstReadMs: round(median(steps.first)),
          openInsideMs: round(median(steps.openInside)),
          openReadSqlMs: round(median(steps.readSql)),
          wakeClaimInsideMs: round(median(steps.claimInside)),
          wakeReadyInsideMs: round(median(steps.readyInside)),
          openMs: round(median(wake.open)),
          openSamples: wake.open.map((ms) => round(ms, 1)),
          totalMs: round(median(steps.total)),
          warmReadMs: round(median(steps.warm)),
          writeMs: round(median(steps.write)),
        },
      }
      console.log('REPLAY ' + JSON.stringify(point))
      points.push(point)
    }
  }
  // Replay cost against log size: ms per 1k events at each point, and the growth between points
  // relative to the growth in events (1.0 is linear).
  const growth = points.flatMap((point, index) => {
    const before = points
      .slice(0, index)
      .reverse()
      .find((one) => one.checkpoints === point.checkpoints)
    if (!before) return []
    const events = point.events / before.events
    const ratio = (pick: (p: typeof point) => number | null) => {
      const a = pick(before)
      const b = pick(point)
      return a && b ? round(b / a / events) : null
    }
    return [
      {
        checkpoints: point.checkpoints,
        from: before.size,
        to: point.size,
        open: ratio((p) => p.wake.openMs),
        firstRead: ratio((p) => p.wake.firstReadMs),
        total: ratio((p) => p.wake.totalMs),
      },
    ]
  })
  return { growth, points, written }
}

const config = parse(process.argv.slice(2))
const sha = (() => {
  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim()
  } catch {
    return 'unknown'
  }
})()
const workerdVersion = spawnSync(workerd, ['--version'], { encoding: 'utf8' }).stdout.trim()
const machine = {
  cores: Os.availableParallelism(),
  cpu: Os.cpus()[0]?.model ?? 'unknown',
  lease: process.env.NC_BUILD_LEASE_CORES ?? 'none',
  node: process.version,
  sha,
  workerd: workerdVersion,
}
console.log('MACHINE ' + JSON.stringify(machine))
console.log('CONFIG ' + JSON.stringify(config))

const server = await start(await bundle())
base = server.url
let failed = false
const results: { load: unknown[]; replay?: unknown; clock?: unknown } = { load: [] }
try {
  results.clock = (await send('clock', { method: 'clock' })).value
  console.log('CLOCK ' + JSON.stringify(results.clock))
  if (config.phases.includes('load'))
    for (const mode of config.modes)
      for (const clients of config.clients) {
        const result = await load(config, mode, clients)
        if (result.violationCount > 0) failed = true
        results.load.push(result)
        console.log('LOAD ' + JSON.stringify(result))
      }
  if (config.phases.includes('replay')) {
    const result = await replay(config)
    if (result.points.some((point) => point.violationCount > 0)) failed = true
    results.replay = result
    console.log('GROWTH ' + JSON.stringify(result.growth))
  }
} finally {
  server.proc.kill('SIGTERM')
  mkdirSync(output, { recursive: true })
  const saved = join(output, 'results-' + Date.now() + '.json')
  writeFileSync(saved, JSON.stringify({ config, machine, results }, null, 2))
  console.log('SAVED ' + saved)
  if (!config.keep) rmSync(dir, { force: true, recursive: true })
}
process.exitCode = failed ? 1 : 0
