// Serves worker.ts with workerd (bundled with rolldown, as test/board-cloudflare.test.ts does)
// and drives it over HTTP as two clients, alice and bob: bob takes over a task whose lease
// alice let lapse, and a label cap keeps at most two agent=llm tasks held at once.
// Run it with: npm run example:board-durable-object (needs dist/, from npm run build).
import { spawn, type ChildProcess } from 'node:child_process'
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
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
const root = fileURLToPath(new URL('../..', import.meta.url))
const dir = mkdtempSync(join(tmpdir(), 'board-durable-object-'))
const t0 = Date.now()
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const log = (who: string, what: string) =>
  console.log(`+${String(Date.now() - t0).padStart(5)}ms  ${who.padEnd(5)}  ${what}`)

// Bundles the Worker and writes a workerd config with one SQLite-backed Durable Object class.
async function bundle(): Promise<string> {
  const build = await rolldown({
    input: join(root, 'examples', 'board-durable-object', 'worker.ts'),
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
    if (!existsSync(file)) continue // only a dynamic import (python.wasm) may be missing
    copyFileSync(file, join(dir, basename(specifier)))
    const wasmName = JSON.stringify(basename(specifier))
    modules.push('(name = ' + JSON.stringify(specifier) + ', wasm = embed ' + wasmName + ')')
  }
  const storage = join(dir, 'do')
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
      '    (className = "BoardObject", uniqueKey = "board-durable-object", enableSql = true),',
      '  ],',
      '  durableObjectStorage = (localDisk = "storage"),',
      ');',
      '',
    ].join('\n'),
  )
  return config
}

// Starts workerd and resolves with its URL once it reports its port on --control-fd.
function serve(config: string): Promise<{ url: string; proc: ChildProcess }> {
  const proc = spawn(workerd, ['serve', config, '--experimental', '--control-fd=3'], {
    cwd: dir,
    stdio: ['ignore', 'pipe', 'pipe', 'pipe'],
  })
  let output = ''
  proc.stdout!.on('data', (chunk) => (output += chunk))
  proc.stderr!.on('data', (chunk) => (output += chunk))
  return new Promise((resolve, reject) => {
    let control = ''
    ;(proc.stdio[3] as Readable).on('data', (chunk) => {
      control += chunk
      for (const line of control.split('\n')) {
        try {
          const event = JSON.parse(line) as { event?: string; socket?: string; port?: number }
          if (event.event === 'listen' && event.socket === 'http')
            resolve({ url: 'http://127.0.0.1:' + event.port, proc })
        } catch {}
      }
    })
    proc.on('error', reject)
    proc.on('exit', (code) => reject(new Error('workerd exited ' + code + '\n' + output)))
  })
}

type Rejection = { kind: string; cap?: unknown }
type Verdict = { ok: boolean; rejection?: Rejection }
type Claimed = Verdict & { claim?: string; tookOver?: boolean }
type Row = { id: string; labels: Record<string, string>; stage: string; worker: string | null }

const server = await serve(await bundle())
async function call<T>(method: 'GET' | 'POST', path: string, body?: object): Promise<T> {
  const response = await fetch(server.url + path, {
    method,
    ...(body ? { body: JSON.stringify(body) } : {}),
  })
  const reply = (await response.json()) as T & { error?: string }
  if (reply.error) throw new Error(method + ' ' + path + ': ' + reply.error)
  return reply
}

// A client is an actor with its own Board handle inside the object.
const client = (actor: string) => ({
  async post(id: string, labels: Record<string, string> = {}) {
    const verdict = await call<Verdict>('POST', '/tasks', {
      actor,
      task: { id, spec: 'do ' + id, labels },
      key: 'post:' + id,
    })
    log(actor, `post   ${id}${verdict.ok ? '' : ' refused: ' + verdict.rejection!.kind}`)
  },
  async claim(id: string, leaseMs: number, cap?: unknown) {
    const claimed = await call<Claimed>('POST', '/claim', { actor, id, leaseMs, cap })
    const note = claimed.ok
      ? (claimed.tookOver ? ' (took over)' : '') + ' for ' + leaseMs + 'ms'
      : ' refused: ' +
        claimed.rejection!.kind +
        (claimed.rejection!.cap ? ' ' + JSON.stringify(claimed.rejection!.cap) : '')
    log(actor, `claim  ${id}${note}`)
    return claimed
  },
  async finish(id: string, claimed: Claimed) {
    const verdict = await call<Verdict>('POST', '/finish', {
      actor,
      id,
      claim: claimed.claim,
      outcome: { done: actor + ' finished ' + id },
    })
    log(actor, `finish ${id}${verdict.ok ? '' : ' refused: ' + verdict.rejection!.kind}`)
  },
})

try {
  const alice = client('alice')
  const bob = client('bob')
  const cap = { max: 2, label: ['agent', 'llm'] }

  await alice.post('index')
  for (const id of ['draft-a', 'draft-b', 'draft-c']) await alice.post(id, { agent: 'llm' })

  // alice claims index for 300ms and stalls; bob is refused while her lease is live, then
  // takes the task over once it lapses, and alice's late finish is refused.
  const stalled = await alice.claim('index', 300)
  await bob.claim('index', 5_000)
  await sleep(400)
  const index = await bob.claim('index', 5_000)
  await alice.finish('index', stalled)
  await bob.finish('index', index)

  // At most two agent=llm tasks held at once, whoever holds them.
  const a = await alice.claim('draft-a', 5_000, cap)
  const b = await bob.claim('draft-b', 5_000, cap)
  await bob.claim('draft-c', 5_000, cap)
  await alice.finish('draft-a', a)
  const c = await bob.claim('draft-c', 5_000, cap)
  await bob.finish('draft-b', b)
  await bob.finish('draft-c', c)

  for (const task of await call<Row[]>('GET', '/tasks'))
    log('GET', `${task.id.padEnd(8)} ${task.stage.padEnd(5)} worker ${task.worker}`)
} finally {
  const exited = new Promise((resolve) => server.proc.once('exit', resolve))
  server.proc.kill('SIGTERM')
  await exited
  rmSync(dir, { recursive: true, force: true })
}
