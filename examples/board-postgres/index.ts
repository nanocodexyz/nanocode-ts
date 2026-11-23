// A lead and three Board.pool workers share one board in Postgres. Each worker is its own process
// with its own connection pool and actor, standing in for a separate machine. One worker is killed
// mid-task and another takes the task over once its lease lapses.
// Run it with: DATABASE_URL=postgres://... npm run example:board-postgres
import { spawn, type ChildProcess } from 'node:child_process'
import { createInterface } from 'node:readline'
import { fileURLToPath } from 'node:url'
import { Board, Sql } from 'nanocode-ts/postgres'

const url = process.env.DATABASE_URL
if (!url) {
  console.error(
    'Set DATABASE_URL to a Postgres database, such as postgres://postgres@127.0.0.1:5432/postgres',
  )
  process.exit(1)
}

const scope = 'swarm'
const schema = 'board_example_' + process.pid + '_' + Date.now().toString(36) // dropped at the end
const workerFile = fileURLToPath(new URL('./worker.ts', import.meta.url))
const t0 = Date.now()
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const timeline: { at: number; who: string; what: string }[] = []
const log = (who: string, what: string, at = Date.now()) => timeline.push({ at, who, what })

type Line = { e: string; at: number; task?: string; tookOver?: boolean; body?: string } & Record<
  string,
  unknown
>
type Worker = { name: string; child: ChildProcess; exited: Promise<void> }

const sql = Sql.open(url, { schema, max: 2 })
const workers: Worker[] = []
const stats = new Map<string, Board.PoolStats>()
let killed: string | undefined
let tookOver: string | undefined
let failure: string | undefined

try {
  const lead = await Board.open({ sql, scope, actor: 'agent:lead' })
  const agents = {
    who: [{ kind: 'kind', id: 'agent' } as const],
    can: { '*': ['manage' as const] },
  }
  const granted = await lead.grant(agents, { key: 'agents' }) // lets every agent: actor work here
  if (!granted.ok) throw new Error('grant refused: ' + JSON.stringify(granted.rejection))

  // plan -> fetch-a, fetch-b, fetch-c -> summarize. The fetches carry agent=llm, which the workers cap at 2.
  const llm = { agent: 'llm' }
  const graph: Board.NewTask[] = [
    { id: 'plan', spec: 'split the work' },
    { id: 'fetch-a', spec: 'fetch the first third', needs: ['plan'], labels: llm },
    { id: 'fetch-b', spec: 'fetch the second third', needs: ['plan'], labels: llm },
    { id: 'fetch-c', spec: 'fetch the last third', needs: ['plan'], labels: llm },
    {
      id: 'summarize',
      spec: 'summarize the three thirds',
      needs: ['fetch-a', 'fetch-b', 'fetch-c'],
      terms: { retry: { maxAttempts: 2, backoffMs: 50 } },
    },
  ]
  for (const task of graph) {
    const posted = await lead.post(task, { key: 'post:' + task.id })
    log('lead', `post   ${task.id}${posted.ok ? '' : ' refused: ' + posted.rejection.kind}`)
  }

  // Each worker prints one JSON line per event; the lead keeps them for the timeline.
  const heard = (worker: Worker, line: Line) => {
    const { name, child } = worker
    if (line.e === 'error') failure ??= name + ' failed: ' + String(line.message)
    if (line.e === 'stats') stats.set(name, line.stats as Board.PoolStats)
    if (line.e === 'start') {
      log(name, `start  ${line.task}${line.tookOver ? ' (took over)' : ''}`, line.at)
      if (line.tookOver) tookOver = name
      // Kill whoever first starts fetch-b, 300 ms into its 1000 ms of work, without letting it release.
      if (line.task === 'fetch-b' && !line.tookOver && !killed) {
        killed = name
        setTimeout(() => {
          child.kill('SIGKILL')
          log('lead', `kill   ${name} (SIGKILL) while it holds fetch-b`)
        }, 300)
      }
    }
    if (line.e === 'steer') log(name, `steer  ${line.task}: ${line.body}`, line.at)
    if (line.e === 'done') log(name, `done   ${line.task}`, line.at)
  }
  for (const name of ['w1', 'w2', 'w3']) {
    const args = [workerFile, JSON.stringify({ url, schema, scope, name })]
    const child = spawn(process.execPath, args, { stdio: ['pipe', 'pipe', 'inherit'] })
    const exited = new Promise<void>((resolve) => child.on('exit', () => resolve()))
    const worker = { name, child, exited }
    workers.push(worker)
    const lines = createInterface({ input: child.stdout! })
    lines.on('line', (text) => heard(worker, JSON.parse(text) as Line))
  }

  // Reads come from the local fold, so refresh before each look.
  let steered = false
  let lapsed = false
  let peak = 0
  const deadline = Date.now() + 60_000
  for (;;) {
    if (failure) throw new Error(failure)
    await lead.refresh()
    const tasks = await lead.tasks()
    const held = tasks.filter((task) => task.stage === 'held' && task.labels.agent === 'llm')
    peak = Math.max(peak, held.length)
    if (!lapsed && tasks.find((task) => task.id === 'fetch-b')?.stage === 'lapsed') {
      lapsed = true
      log('lead', 'lapse  fetch-b: its lease ran out')
    }
    if (!steered && tasks.find((task) => task.id === 'summarize')?.phase === 'claimed') {
      const sent = await lead.steer('summarize', 'keep it to two lines')
      log('lead', `steer  summarize${sent.ok ? '' : ' refused: ' + sent.rejection.kind}`)
      steered = true
    }
    if (tasks.every((task) => task.phase === 'done')) break
    if (Date.now() > deadline)
      throw new Error(
        'the board did not drain: ' + tasks.map((t) => t.id + '=' + t.stage).join(' '),
      )
    await sleep(25)
  }
  log('lead', `peak   ${peak} agent=llm tasks held at once (cap 2)`)

  // Stop the workers still alive; each stops its pool, reports its stats and exits.
  for (const { name, child } of workers) if (name !== killed) child.stdin!.end('stop\n')
  await Promise.all(workers.map((worker) => worker.exited))

  for (const { at, who, what } of timeline.sort((a, b) => a.at - b.at))
    console.log(`+${String(at - t0).padStart(5)}ms  ${who.padEnd(4)}  ${what}`)
  console.log()
  // Each finished task keeps the worker whose claim finished it.
  for (const { id } of graph) {
    const task = (await lead.task(id))!
    const outcome = JSON.stringify(task.outcome)
    console.log(`${id.padEnd(10)} ${task.phase}  ${String(task.worker).padEnd(9)} ${outcome}`)
  }
  console.log()
  for (const { name } of workers) {
    const s = stats.get(name)
    console.log(
      name,
      s
        ? `claimed ${s.claimed}, done ${s.done}, lost ${s.lost}, steers ${s.delivered}, errors ${s.errors}`
        : 'killed',
    )
  }
  if (!tookOver) {
    console.error('no worker took over the killed task')
    process.exitCode = 1
  }
  await lead.close()
} catch (error) {
  console.error(error)
  process.exitCode = 1
} finally {
  for (const { child } of workers) child.kill('SIGKILL')
  await sql.transaction('log', 'drop', [['DROP SCHEMA IF EXISTS ' + schema + ' CASCADE']], 'schema')
  await sql.close()
}
