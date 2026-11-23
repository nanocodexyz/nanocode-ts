import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { basename, join } from 'node:path'
import { Readable } from 'node:stream'
import { after, before, describe, test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { rolldown } from 'rolldown'
import workerdModule from 'workerd'

// The Board binding inside a Cloudflare Durable Object, the way a swarm's object runs it:
// test/board-cloudflare.worker.ts is bundled with rolldown and served by workerd, and every
// Board.open there runs over nanocode-ts/cloudflare's Sql.create(ctx.storage) with SQLite-backed
// storage on disk. Each test uses its own object (named by the URL path). A handle here is one
// actor's Board in that object; two handles share the object's storage. A restart is either
// ctx.abort(), which makes the next request construct a new instance, or a new workerd process
// over the same storage directory.

// workerd is CommonJS with a default export, which Node's ESM interop wraps once more.
const workerd: string =
  typeof workerdModule === 'string'
    ? workerdModule
    : (workerdModule as unknown as { default: string }).default
const root = fileURLToPath(new URL('..', import.meta.url))
const dir = mkdtempSync(join(tmpdir(), 'nanocode-board-cf-'))
const storage = join(dir, 'do')
let server: { url: string; proc: ChildProcess } | undefined

type Reply = { boot?: string; value?: unknown; error?: string; aborted?: string }

async function bundle(): Promise<string> {
  const build = await rolldown({
    input: join(root, 'test', 'board-cloudflare.worker.ts'),
    platform: 'neutral',
    external: [/^cloudflare:/, /^node:/, /\.wasm$/],
    resolve: { conditionNames: ['workerd', 'worker', 'import', 'default'] },
    logLevel: 'silent',
  })
  const { output } = await build.generate({ format: 'esm' })
  await build.close()
  const modules: string[] = []
  const wasm = new Set<string>()
  for (const chunk of output) {
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
    // Only a dynamic import (python.wasm) may be missing; the board never loads it.
    if (!existsSync(file)) continue
    copyFileSync(file, join(dir, basename(specifier)))
    modules.push(
      '(name = ' +
        JSON.stringify(specifier) +
        ', wasm = embed ' +
        JSON.stringify(basename(specifier)) +
        ')',
    )
  }
  assert.ok(wasm.has('nanocode-ts/nanocode_wasm.wasm'), 'the bundle does not import the board WASM')
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
      '    (className = "BoardObject", uniqueKey = "nanocode-board-cloudflare", enableSql = true),',
      '  ],',
      '  durableObjectStorage = (localDisk = "storage"),',
      ');',
      '',
    ].join('\n'),
  )
  return config
}

// Starts workerd and resolves with its URL once it reports the port it listens on (--control-fd).
function start(config: string): Promise<{ url: string; proc: ChildProcess }> {
  const proc = spawn(workerd, ['serve', config, '--experimental', '--control-fd=3'], {
    cwd: dir,
    stdio: ['ignore', 'pipe', 'pipe', 'pipe'],
  })
  let log = ''
  proc.stdout!.on('data', (chunk) => (log += chunk))
  proc.stderr!.on('data', (chunk) => (log += chunk))
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

async function stop() {
  const proc = server?.proc
  server = undefined
  if (!proc || proc.exitCode !== null) return
  const exited = new Promise((resolve) => proc.once('exit', resolve))
  proc.kill('SIGTERM')
  await exited
}

async function send(object: string, body: object): Promise<Reply> {
  assert.ok(server, 'workerd is not running')
  const response = await fetch(server.url + '/' + object, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  const reply = (await response.json()) as Reply
  if (reply.error)
    assert.fail(JSON.stringify(body) + ' failed in the Durable Object:\n' + reply.error)
  return reply
}

type Handle = {
  <T = unknown>(method: string, ...args: unknown[]): Promise<T>
  boot(): Promise<string>
}

// One actor's Board in one object; the object opens it on first use.
function handle(object: string, actor: string): Handle {
  const call = async <T>(method: string, ...args: unknown[]) =>
    (await send(object, { actor, method, args })).value as T
  return Object.assign(call, {
    boot: async () => (await send(object, { actor, method: 'boot' })).boot!,
  })
}

async function abort(object: string) {
  const reply = await send(object, { actor: 'none', method: 'abort' })
  assert.ok(reply.aborted, 'abort did not reset the object: ' + JSON.stringify(reply))
}

function ok<T extends { ok: boolean }>(verdict: T): Extract<T, { ok: true }> {
  if (!verdict.ok)
    assert.fail(
      'expected ok, got ' + JSON.stringify((verdict as { rejection?: unknown }).rejection),
    )
  return verdict as Extract<T, { ok: true }>
}

function rejected(verdict: { ok: boolean }, kind: string) {
  assert.equal(verdict.ok, false, 'expected ' + kind + ', got ' + JSON.stringify(verdict))
  const { rejection } = verdict as unknown as { rejection: { kind: string; claim?: string } }
  assert.equal(rejection.kind, kind, JSON.stringify(rejection))
  return rejection
}

type Verdict = { ok: boolean; seq: number; rejection?: { kind: string } }
type Claimed = Verdict & { claim: string; untilMs: number; tookOver: boolean }
type Task = {
  id: string
  phase: string
  stage: string
  claim: string | null
  worker: string | null
  outcome: unknown
}
type Steer = { seq: number; by: string; body: string }

// The board's first opener charters it; like the conformance suite, it lets every agent manage it.
async function charter(alice: Handle) {
  ok(
    await alice<Verdict>(
      'grant',
      { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
      { key: 'grant-agents' },
    ),
  )
}

const tasks = async (board: Handle) =>
  new Map((await board<Task[]>('tasks')).map((one) => [one.id, one]))

describe('Board in a Cloudflare Durable Object', { timeout: 120_000 }, () => {
  let config = ''

  before(async () => {
    config = await bundle()
    server = await start(config)
  })

  after(async () => {
    await stop()
    rmSync(dir, { recursive: true, force: true })
  })

  test('post, claim, renew, steer, heed and finish over ctx.storage with two handles', async () => {
    const alice = handle('flow', 'alice')
    const bob = handle('flow', 'bob')
    await charter(alice)

    const posted = ok(await alice<Verdict>('post', { id: 'a', spec: 'first' }, { key: 'post-a' }))
    const again = ok(await alice<Verdict>('post', { id: 'a', spec: 'first' }, { key: 'post-a' }))
    assert.equal(again.seq, posted.seq)
    await bob('refresh')
    assert.deepEqual(
      (await bob<Task[]>('ready')).map((one) => one.id),
      ['a'],
    )

    const claimed = ok(await alice<Claimed>('claim', 'a', { leaseMs: 60_000 }))
    assert.equal(claimed.tookOver, false)
    const held = rejected(await bob<Claimed>('claim', 'a', { leaseMs: 60_000 }), 'Held')
    assert.equal(held.claim, claimed.claim)
    ok(await alice<Verdict>('renew', 'a', claimed.claim, 120_000))
    assert.deepEqual(await alice<Steer[]>('steers', 'a', claimed.claim), [])

    const sent = ok(await bob<Verdict>('steer', 'a', 'use the blue one'))
    rejected(await alice<Verdict>('finish', 'a', claimed.claim, { done: 'red' }), 'Steer')
    const steers = await alice<Steer[]>('steers', 'a', claimed.claim)
    assert.equal(steers.length, 1)
    assert.equal(steers[0]!.body, 'use the blue one')
    assert.equal(steers[0]!.by, 'agent:bob')
    assert.equal(steers[0]!.seq, sent.seq)
    rejected(await bob<Verdict>('heed', 'a', 'not-a-claim', sent.seq), 'NotHolder')
    ok(await alice<Verdict>('heed', 'a', claimed.claim, sent.seq))
    ok(await alice<Verdict>('finish', 'a', claimed.claim, { done: 'blue' }))
    rejected(await bob<Verdict>('steer', 'a', 'too late'), 'Terminal')

    await bob('refresh')
    const done = (await tasks(bob)).get('a')!
    assert.equal(done.phase, 'done')
    assert.deepEqual(done.outcome, { done: 'blue' })
  })

  test('a restarted object resumes claims, keys and steers from ctx.storage', async () => {
    const alice = handle('restart', 'alice')
    const bob = handle('restart', 'bob')
    await charter(alice)
    const posts: number[] = []
    for (const id of ['s1', 's2', 's3'])
      posts.push(
        ok(await alice<Verdict>('post', { id, spec: 'seeded ' + id }, { key: 'seed-' + id })).seq,
      )
    const first = ok(await alice<Claimed>('claim', 's1', { leaseMs: 600_000 }))
    const second = ok(await alice<Claimed>('claim', 's2', { leaseMs: 600_000 }))
    ok(await alice<Verdict>('finish', 's2', second.claim, { done: 'seeded' }))
    const steered = ok(await bob<Verdict>('steer', 's1', 'seeded steer'))

    // A new instance over the same storage, with no board open in memory.
    const before = await alice.boot()
    await abort('restart')
    assert.notEqual(await alice.boot(), before)

    const resumed = await tasks(alice)
    assert.deepEqual([...resumed.keys()].sort(), ['s1', 's2', 's3'])
    assert.equal(resumed.get('s1')!.stage, 'held')
    assert.equal(resumed.get('s1')!.claim, first.claim)
    assert.equal(resumed.get('s2')!.phase, 'done')
    assert.equal(resumed.get('s3')!.stage, 'open')
    const reposted = ok(
      await alice<Verdict>('post', { id: 's1', spec: 'seeded s1' }, { key: 'seed-s1' }),
    )
    assert.equal(reposted.seq, posts[0])
    ok(await alice<Verdict>('post', { id: 's4', spec: 'after restart' }))
    ok(await alice<Verdict>('renew', 's1', first.claim, 600_000))
    rejected(await alice<Verdict>('finish', 's1', first.claim, { done: 'resumed' }), 'Steer')
    const steers = await alice<Steer[]>('steers', 's1', first.claim)
    assert.deepEqual(
      steers.map((steer) => [steer.seq, steer.body]),
      [[steered.seq, 'seeded steer']],
    )
    ok(await alice<Verdict>('heed', 's1', first.claim, steered.seq))
    ok(await alice<Verdict>('finish', 's1', first.claim, { done: 'resumed' }))

    // A new workerd process over the same storage directory, which holds the objects' SQLite files.
    await stop()
    const files = readdirSync(storage, { recursive: true }).map(String)
    assert.ok(
      files.some((file) => file.endsWith('.sqlite')),
      files.join(', '),
    )
    server = await start(config)
    const reopened = await tasks(bob)
    assert.equal(reopened.size, 4)
    assert.deepEqual(reopened.get('s1')!.outcome, { done: 'resumed' })
    assert.equal(reopened.get('s4')!.stage, 'open')
    const s3 = ok(await bob<Claimed>('claim', 's3', { leaseMs: 60_000 }))
    ok(await bob<Verdict>('finish', 's3', s3.claim, { done: 'after two restarts' }))
  })

  test('a restarted object opens from its fold checkpoint to the state a full replay folds', async () => {
    const alice = handle('checkpoint', 'alice')
    await charter(alice)
    const events = 10_000
    for (let from = 0; from < events; from += 1_000) {
      const writes = Array.from({ length: 1_000 }, (_, index) => {
        const n = from + index
        return n % 4 === 3
          ? { op: { op: 'cancel', task: 't' + (n - 1), reason: 'gone' } }
          : { op: { op: 'post', task: { id: 't' + n, spec: 'task ' + n, labels: {}, needs: [] } } }
      })
      for (const verdict of await alice<Verdict[]>('group', writes)) ok(verdict)
    }
    const live = await alice<Task[]>('tasks')
    assert.equal(await alice<boolean>('checkpoint'), true)

    await abort('checkpoint')
    const bob = handle('checkpoint', 'bob')
    const restored = await bob<Task[]>('tasks')
    const [opened] = await bob<{ from: string; seq?: number; tail?: number }[]>('opened')
    assert.equal(opened!.from, 'checkpoint', JSON.stringify(opened))
    assert.equal(opened!.tail, 0)
    assert.deepEqual(restored, live)

    await send('checkpoint', { actor: 'none', method: 'drop_checkpoints' })
    await abort('checkpoint')
    const carol = handle('checkpoint', 'carol')
    assert.deepEqual(await carol('opened'), [{ from: 'replay', fallback: 'missing' }])
    assert.deepEqual(await carol<Task[]>('tasks'), live)
  })

  test('a lease that lapses on the wall clock is taken over by the other handle', async () => {
    const alice = handle('takeover', 'alice')
    const bob = handle('takeover', 'bob')
    await charter(alice)
    ok(await alice<Verdict>('post', { id: 't', spec: 'contended' }))
    const first = ok(await alice<Claimed>('claim', 't', { leaseMs: 600 }))
    assert.equal(first.tookOver, false)
    const held = rejected(await bob<Claimed>('claim', 't', { leaseMs: 5_000 }), 'Held')
    assert.equal(held.claim, first.claim)

    await new Promise((resolve) =>
      setTimeout(resolve, Math.max(0, first.untilMs - Date.now()) + 300),
    )
    await bob('refresh')
    assert.equal((await tasks(bob)).get('t')!.stage, 'lapsed')
    const second = ok(await bob<Claimed>('claim', 't', { leaseMs: 60_000 }))
    assert.equal(second.tookOver, true)
    assert.notEqual(second.claim, first.claim)
    assert.equal((await tasks(bob)).get('t')!.worker, 'agent:bob')

    rejected(await alice<Verdict>('renew', 't', first.claim, 60_000), 'NotHolder')
    rejected(await alice<Verdict>('finish', 't', first.claim, { done: 'late' }), 'NotHolder')
    ok(await bob<Verdict>('finish', 't', second.claim, { done: 'bob' }))
    await alice('refresh')
    assert.deepEqual((await tasks(alice)).get('t')!.outcome, { done: 'bob' })
  })
})
