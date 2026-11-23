// Worker that bench/board-do.ts bundles and serves with workerd. One Durable Object per URL path
// holds a Board over Sql.create(ctx.storage), the way a swarm's object opens its board. The bench
// drives it with POST bodies { method, ... }: claims, renews and finishes from its clients, posts
// and steers from a lead, and fill, audit and the restart steps for the replay measurements.
// Every reply carries ms, the time the call took inside the object.

import { DurableObject } from 'cloudflare:workers'
import { Board } from 'nanocode-ts'
import { Sql } from 'nanocode-ts/cloudflare'
import wasm from 'nanocode-ts/nanocode_wasm.wasm'

type Env = { BOARD: DurableObjectNamespace<BoardObject> }

type Mode = 'one' | 'actors'

type Call = {
  method: string
  mode?: Mode
  client?: string
  id?: string
  claim?: string
  leaseMs?: number
  tasks?: Board.NewTask[]
  events?: number
  steer?: number
  seed?: number
  children?: number
  prefix?: string
  checkpointEvery?: number
}

type Operation = { count: number; ms: number; errors: number }

const scope = 'swarm'
const lead = 'lead'
const shape = { firstWave: 4, maxNeeds: 2, window: 6 }

function random(seed: number) {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let value = Math.imul(state ^ (state >>> 15), 1 | state)
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296
  }
}

// A root and its children in a DAG: the first wave needs the root, each later child needs one or
// two of the six children before it. bench/board-load.ts posts the same shape.
function swarm(name: string, children: number, draw: () => number): Board.NewTask[] {
  const labels = { project: name }
  const tasks: Board.NewTask[] = [{ id: name, labels, needs: [], spec: 'root of ' + name }]
  for (let index = 0; index < children; index++) {
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

type Event = { at_ms: number; op: { op: string; task?: unknown; outcome?: unknown } }

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

export class BoardObject extends DurableObject<Env> {
  readonly boot = crypto.randomUUID()
  readonly boards = new Map<string, Promise<Board.Board>>()
  readonly operations: Record<string, Operation> = {}
  mode: Mode = 'one'

  // The object's SQL host, timing every transaction by its operation name.
  host() {
    const host = Sql.create(this.ctx.storage)
    const operations = this.operations
    return {
      ...host,
      transaction(...args: Parameters<typeof host.transaction>) {
        const entry = (operations[args[1]] ??= { count: 0, ms: 0, errors: 0 })
        const started = performance.now()
        try {
          return host.transaction(...args)
        } catch (error) {
          entry.errors++
          throw error
        } finally {
          entry.count++
          entry.ms += performance.now() - started
        }
      },
    }
  }

  // One handle for the whole object (mode one, as a swarm's object holds its board), or one per
  // client actor (mode actors, as test/board-cloudflare.test.ts does), each with its own fold.
  board(who: string, checkpointEvery?: number): Promise<Board.Board> {
    const actor = this.mode === 'one' ? lead : who
    let opened = this.boards.get(actor)
    if (!opened) {
      opened = Board.open({
        sql: this.host(),
        scope,
        actor: 'agent:' + actor,
        module: wasm,
        ...(checkpointEvery === undefined ? {} : { checkpointEvery }),
      })
      opened.catch(() => this.boards.delete(actor))
      this.boards.set(actor, opened)
    }
    return opened
  }

  async claim(client: string, leaseMs: number) {
    const board = await this.board(client)
    const t0 = performance.now()
    await board.refresh()
    const t1 = performance.now()
    const ready = await board.ready()
    const t2 = performance.now()
    let attempts = 0
    // Clients race for the same ready tasks: start at a random one and take the first that holds.
    const start = Math.floor(Math.random() * ready.length)
    for (let index = 0; index < ready.length && attempts < 4; index++) {
      const task = ready[(start + index) % ready.length]!
      attempts++
      const claimed = await board.claim(task.id, { leaseMs })
      if (claimed.ok)
        return {
          attempts,
          claim: claimed.claim,
          claimMs: performance.now() - t2,
          id: task.id,
          readyCount: ready.length,
          readyMs: t2 - t1,
          refreshMs: t1 - t0,
        }
      if (claimed.rejection.kind !== 'Held')
        return { attempts, id: null, rejection: claimed.rejection.kind, refreshMs: t1 - t0 }
    }
    return { attempts, id: null, readyCount: ready.length, readyMs: t2 - t1, refreshMs: t1 - t0 }
  }

  async finish(client: string, id: string, claim: string) {
    const board = await this.board(client)
    const t0 = performance.now()
    await board.refresh()
    const refreshMs = performance.now() - t0
    let heeded = 0
    for (let attempt = 0; attempt < 4; attempt++) {
      const last = (await board.steers(id, claim)).at(-1)
      if (last) {
        const verdict = await board.heed(id, claim, last.seq)
        if (!verdict.ok)
          return { ok: false, refreshMs, rejection: 'heed ' + verdict.rejection.kind }
        heeded++
      }
      const verdict = await board.finish(id, claim, { done: client })
      if (verdict.ok) return { heeded, ok: true, refreshMs }
      if (verdict.rejection.kind !== 'Steer')
        return { ok: false, refreshMs, rejection: verdict.rejection.kind }
      await board.refresh()
    }
    return { ok: false, refreshMs, rejection: 'Steer after 4 heeds' }
  }

  // Grows the log inside the object with the same ops the clients send, swarm after swarm, until
  // at least target writes landed.
  async fill(target: number, children: number, steerShare: number, seed: number, prefix: string) {
    const board = await this.board(lead)
    const draw = random(seed)
    let written = 0
    let swarms = 0
    while (written < target) {
      const tasks = swarm(prefix + swarms++, children, draw)
      const verdicts = await board.group(tasks.map(post))
      const refused = verdicts.find((verdict) => !verdict.ok)
      if (refused) throw new Error('fill post refused: ' + JSON.stringify(refused))
      written += tasks.length
      // Children come after their needs, so this order is a topological one.
      for (const task of tasks) {
        const claimed = await board.claim(task.id, { leaseMs: 60_000 })
        if (!claimed.ok) throw new Error('fill claim of ' + task.id + ': ' + claimed.rejection.kind)
        const renewed = await board.renew(task.id, claimed.claim, 60_000)
        if (!renewed.ok) throw new Error('fill renew of ' + task.id + ': ' + renewed.rejection.kind)
        written += 2
        if (draw() < steerShare) {
          const steered = await board.steer(task.id, 'redirect ' + task.id)
          if (!steered.ok) throw new Error('fill steer: ' + steered.rejection.kind)
          const last = (await board.steers(task.id, claimed.claim)).at(-1)
          if (last) {
            const heeded = await board.heed(task.id, claimed.claim, last.seq)
            if (!heeded.ok) throw new Error('fill heed: ' + heeded.rejection.kind)
          }
          written += 2
        }
        const finished = await board.finish(task.id, claimed.claim, { done: 'fill' })
        if (!finished.ok)
          throw new Error('fill finish of ' + task.id + ': ' + finished.rejection.kind)
        written++
      }
    }
    return { swarms, written }
  }

  // Reads the whole log: every posted task must be finished exactly once, as done, and the
  // board's own view must agree. Also reports the log's size and the database's.
  async audit() {
    const rows = [
      ...this.ctx.storage.sql.exec('SELECT scope, seq, body FROM nanocode_log ORDER BY scope, seq'),
    ] as { scope: string; seq: number; body: unknown }[]
    const posted = new Set<string>()
    const finishes = new Map<string, number>()
    const ops: Record<string, number> = {}
    const violations: string[] = []
    let count = 0
    let bodyBytes = 0
    for (const row of rows) {
      const body =
        typeof row.body === 'string' ? row.body : new TextDecoder().decode(row.body as ArrayBuffer)
      bodyBytes += body.length
      const found: Event[] = []
      events(body, found)
      for (const event of found) {
        count++
        ops[event.op.op] = (ops[event.op.op] ?? 0) + 1
        if (event.op.op === 'post') posted.add((event.op.task as { id: string }).id)
        if (event.op.op === 'finish' && typeof event.op.task === 'string') {
          finishes.set(event.op.task, (finishes.get(event.op.task) ?? 0) + 1)
          const kind = (event.op.outcome as { kind?: string } | undefined)?.kind
          if (kind !== 'done')
            violations.push(event.op.task + ' finished ' + JSON.stringify(event.op.outcome))
        }
      }
    }
    for (const id of posted) {
      const times = finishes.get(id) ?? 0
      if (times !== 1) violations.push(id + ' finished ' + times + ' times')
    }
    const board = await this.board(lead)
    await board.refresh()
    const notDone = (await board.tasks()).filter((task) => task.phase !== 'done')
    if (notDone.length > 0)
      violations.push(notDone.length + ' tasks not done, first ' + notDone[0]!.id)
    return {
      bodyBytes,
      databaseSize: this.ctx.storage.sql.databaseSize,
      events: count,
      ops,
      rows: rows.length,
      tasks: posted.size,
      violationCount: violations.length,
      violations: violations.slice(0, 20),
    }
  }

  async call(call: Call): Promise<unknown> {
    switch (call.method) {
      case 'boot':
        return { boot: this.boot }
      case 'clock': {
        // workerd may hold the clock still during synchronous work; this says whether the
        // in-object times are real.
        const t0 = performance.now()
        const d0 = Date.now()
        let spin = 0
        for (let index = 0; index < 3e7; index++) spin = (spin + index) % 7919
        return { dateMs: Date.now() - d0, perfMs: performance.now() - t0, spin }
      }
      case 'setup': {
        this.mode = call.mode ?? 'one'
        const board = await this.board(lead)
        const granted = await board.grant(
          { can: { '*': ['manage'] }, who: [{ id: 'agent', kind: 'kind' }] },
          { key: 'grant-agents' },
        )
        if (!granted.ok) throw new Error('grant refused: ' + JSON.stringify(granted.rejection))
        return { mode: this.mode }
      }
      case 'open': {
        // The first board call after a restart: Board.open over the stored log, from its newest
        // fold checkpoint unless the bench dropped them.
        const t0 = performance.now()
        const board = await this.board(lead, call.checkpointEvery)
        const openMs = performance.now() - t0
        return { openMs, opened: await board.opened() }
      }
      case 'drop_checkpoints':
        this.ctx.storage.sql.exec('DELETE FROM nanocode_checkpoint')
        return null
      case 'checkpoint':
        return (await this.board(lead)).checkpoint()
      case 'tasks': {
        const board = await this.board(lead)
        const t0 = performance.now()
        const tasks = await board.tasks()
        return { count: tasks.length, tasksMs: performance.now() - t0 }
      }
      case 'post': {
        const board = await this.board(lead)
        const verdicts = await board.group((call.tasks ?? []).map(post))
        const refused = verdicts.find((verdict) => !verdict.ok)
        if (refused) throw new Error('post refused: ' + JSON.stringify(refused))
        return { posted: verdicts.length }
      }
      case 'claim':
        return this.claim(call.client!, call.leaseMs!)
      case 'renew': {
        const board = await this.board(call.client!)
        const verdict = await board.renew(call.id!, call.claim!, call.leaseMs!)
        return verdict.ok ? { ok: true } : { ok: false, rejection: verdict.rejection.kind }
      }
      case 'steer': {
        const board = await this.board(lead)
        const verdict = await board.steer(call.id!, 'redirect ' + call.id, {
          key: 'steer-' + call.id,
        })
        return verdict.ok ? { ok: true } : { ok: false, rejection: verdict.rejection.kind }
      }
      case 'finish':
        return this.finish(call.client!, call.id!, call.claim!)
      case 'fill':
        return this.fill(
          call.events ?? 1000,
          call.children ?? 20,
          call.steer ?? 0.02,
          call.seed ?? 1,
          call.prefix ?? 'f',
        )
      case 'audit':
        return this.audit()
      case 'operations': {
        const copy = structuredClone(this.operations)
        for (const key of Object.keys(this.operations)) delete this.operations[key]
        return copy
      }
      default:
        throw new Error('unknown method ' + call.method)
    }
  }

  override async fetch(request: Request): Promise<Response> {
    const call = (await request.json()) as Call
    // Drops this instance; the next request constructs a new one over the same storage.
    if (call.method === 'abort') this.ctx.abort('board-do bench restart')
    const started = performance.now()
    try {
      const value = await this.call(call)
      return Response.json({ boot: this.boot, ms: performance.now() - started, value })
    } catch (error) {
      const message = error instanceof Error ? (error.stack ?? error.message) : String(error)
      return Response.json({ boot: this.boot, error: message }, { status: 500 })
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const stub = env.BOARD.get(env.BOARD.idFromName(new URL(request.url).pathname))
    try {
      return await stub.fetch(request)
    } catch (error) {
      // ctx.abort() fails the request that called it; report that as the restart it was.
      return Response.json({ aborted: String(error) })
    }
  },
}
