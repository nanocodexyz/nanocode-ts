// A planner posts a small task graph and two Board.pool workers drain it on one SQLite file.
// Tasks labelled kind=agent run a nanocode-ts agent turn when ANTHROPIC_API_KEY is set, and a
// deterministic stand-in otherwise, so the example runs with no keys. A label cap keeps at
// most two kind=agent tasks held at once across both pools; kind=tool tasks do not count.
// Run it with: npm run example:board-agents (needs dist/, from npm run build).
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { AgentRuntime, Contracts } from 'nanocode-ts'
import { Board, Sql } from 'nanocode-ts/node'

const key = process.env.ANTHROPIC_API_KEY
const model = process.env.NANOCODE_MODEL ?? 'claude-haiku-5-5'
const dir = mkdtempSync(join(tmpdir(), 'board-agents-'))
const path = join(dir, 'board.sqlite')
const t0 = Date.now()
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const log = (who: string, what: string) =>
  console.log(`+${String(Date.now() - t0).padStart(5)}ms  ${who.padEnd(7)}  ${what}`)

// Each actor opens its own handle on the one file; the first to open also charters the board.
async function connect(name: string) {
  const sql = Sql.open(path)
  const board = await Board.open({ sql, scope: 'agents', actor: 'agent:' + name })
  const who = [{ kind: 'kind', id: 'agent' } as const]
  await board.grant({ who, can: { '*': ['manage'] } }, { key: 'agents' }) // refused after the first
  return { board, sql }
}

// One agent turn in its own in-memory session: submit the prompt, drive it, read the answer.
let wasm: Promise<WebAssembly.Module> | undefined
async function agentTurn(apiKey: string, prompt: string): Promise<string> {
  wasm ??= WebAssembly.compile(
    readFileSync(fileURLToPath(import.meta.resolve('nanocode-ts/nanocode_wasm.wasm'))),
  )
  const sql = await Board.memorySql()
  const runtime = await AgentRuntime.instantiate(await wasm, sql, { published() {} })
  try {
    await runtime.open(
      {
        scope: 'turn',
        session_id: Contracts.sessionUuid(),
        provider: 'claude',
        model,
        subscription: false,
        endpoint: 'https://api.anthropic.com/v1/messages',
        workspace_tools: 'none',
        instructions: 'Answer in one or two sentences, without tools.',
      },
      {
        dispatch: (request, signal) => {
          const headers = new Headers(request.headers)
          headers.set('x-api-key', apiKey)
          return fetch(request.endpoint, {
            method: request.method,
            headers,
            body: request.body as Uint8Array<ArrayBuffer>,
            signal,
          })
        },
      },
    )
    const id = 'turn-' + Contracts.sessionUuid()
    await runtime.submit('turn', { request_id: id, prompt, metadata: {} })
    for (;;) {
      const next = (await runtime.pending('turn'))[0]
      if (!next) break
      const driven = await runtime.drive(next.report.request_id)
      if (driven.waiting) await sleep(5)
    }
    const turn = await runtime.turn('turn', id)
    if (turn?.report.status !== 'completed')
      throw new Error('agent turn ended ' + JSON.stringify(turn?.report ?? null))
    return String(turn.report.final_message ?? '').trim()
  } finally {
    await runtime.dispose()
    sql.close()
  }
}

// plan -> three sections and a link check -> assemble. Durations stand in for work offline.
const graph: (Board.NewTask & { ms: number })[] = [
  {
    id: 'plan',
    spec: 'Split release notes for the Board into sections.',
    labels: { kind: 'agent' },
    ms: 150,
  },
  {
    id: 'notes-claim',
    spec: 'Write one line on claim caps.',
    needs: ['plan'],
    labels: { kind: 'agent' },
    ms: 300,
  },
  {
    id: 'notes-pool',
    spec: 'Write one line on Board.pool.',
    needs: ['plan'],
    labels: { kind: 'agent' },
    ms: 300,
  },
  {
    id: 'notes-worker',
    spec: 'Write one line on Task.worker.',
    needs: ['plan'],
    labels: { kind: 'agent' },
    ms: 300,
  },
  {
    id: 'check-links',
    spec: 'Check the doc links resolve.',
    needs: ['plan'],
    labels: { kind: 'tool' },
    ms: 200,
  },
  {
    id: 'assemble',
    spec: 'Join the three lines into a short changelog entry.',
    needs: ['notes-claim', 'notes-pool', 'notes-worker', 'check-links'],
    labels: { kind: 'agent' },
    ms: 150,
  },
]
const durations = new Map(graph.map(({ id, ms }) => [id, ms]))

const planner = await connect('planner')
log(
  'planner',
  key
    ? 'ANTHROPIC_API_KEY set: kind=agent tasks run ' + model
    : 'no ANTHROPIC_API_KEY: kind=agent tasks run the fallback',
)
for (const { ms: _, ...task } of graph) {
  const posted = await planner.board.post(task, { key: 'post:' + task.id })
  log('planner', `post   ${task.id}${posted.ok ? '' : ' refused: ' + posted.rejection.kind}`)
}

let agentsHeld = 0
let mostAgentsHeld = 0
const worker = (name: string, board: Board.Board) => async (task: Board.Task) => {
  const agent = task.labels.kind === 'agent'
  if (agent) mostAgentsHeld = Math.max(mostAgentsHeld, ++agentsHeld)
  log(
    name,
    `start  ${task.id.padEnd(12)} kind=${task.labels.kind}${agent ? '  agents held ' + agentsHeld + '/2' : ''}`,
  )
  try {
    let done: string
    if (agent && key) {
      const inputs = await Promise.all(task.needs.map((id) => board.task(id)))
      const context = inputs
        .map((one) => one && one.id + ': ' + JSON.stringify(one.outcome))
        .join('\n')
      done = await agentTurn(key, task.spec + (context ? '\n\nEarlier results:\n' + context : ''))
    } else {
      await sleep(durations.get(task.id) ?? 100)
      done = (agent ? 'fallback: ' : 'tool: ') + task.spec
    }
    log(name, `done   ${task.id}`)
    return { done }
  } finally {
    if (agent) agentsHeld--
  }
}

const members = await Promise.all([connect('w1'), connect('w2')])
const pools = members.map(({ board }, index) =>
  Board.pool(board, {
    cap: { max: 2, label: ['kind', 'agent'] },
    concurrency: 2,
    leaseMs: key ? 60_000 : 5_000,
    pollMs: 20,
    run: worker('w' + (index + 1), board),
  }),
)

// Reads come from the local fold, so refresh before each look.
for (;;) {
  await planner.board.refresh()
  const tasks = await planner.board.tasks()
  if (tasks.every((task) => task.phase === 'done' || task.phase === 'failed')) break
  await sleep(20)
}
await Promise.all(pools.map((pool) => pool.stop()))

for (const task of await planner.board.tasks())
  log(
    'planner',
    `${task.phase.padEnd(6)} ${task.id.padEnd(12)} worker ${task.worker}  ${JSON.stringify(task.outcome)}`,
  )
log('planner', 'at most ' + mostAgentsHeld + ' kind=agent tasks held at once')
pools.forEach((pool, index) => console.log('w' + (index + 1), JSON.stringify(pool.stats())))

for (const { board, sql } of [planner, ...members]) {
  await board.close()
  sql.close()
}
rmSync(dir, { recursive: true, force: true })
