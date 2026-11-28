// A lead posts a small task graph and two Board.pool workers drain it on one SQLite file,
// with one steer along the way. Run it with: npm run example:board-pool
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Board, Sql } from 'nanocode-ts/node'

const dir = mkdtempSync(join(tmpdir(), 'board-pool-'))
const path = join(dir, 'board.sqlite')
const t0 = Date.now()
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const log = (who: string, what: string) =>
  console.log(`+${String(Date.now() - t0).padStart(4)}ms  ${who.padEnd(4)}  ${what}`)

// Each actor opens its own handle on the one file; the first to open also charters the board.
async function connect(name: string) {
  const sql = Sql.open(path)
  const board = await Board.open({ sql, scope: 'demo', actor: 'agent:' + name })
  const agents = {
    who: [{ kind: 'kind', id: 'agent' } as const],
    can: { '*': ['manage' as const] },
  }
  await board.grant(agents, { key: 'agents' }) // refused for everyone but the chartering lead
  return { board, sql }
}

const lead = await connect('lead')
const worker = (name: string) => async (task: Board.Task, lease: Board.PoolLease) => {
  log(name, `start  ${task.id}${lease.tookOver ? ' (took over)' : ''}`)
  await sleep(task.id === 'summarize' ? 400 : 150) // stands in for the real work
  const steers = await lease.steers()
  for (const steer of steers) log(name, `steer  ${task.id}: ${steer.body}`)
  const last = steers.at(-1)
  if (last) await lease.heed(last.seq) // a finish is refused while a steer is unheeded
  log(name, `done   ${task.id}`)
  return { done: name + ' finished ' + task.id }
}

// plan -> fetch-a, fetch-b -> summarize. A key makes a post safe to repeat.
const graph: Board.NewTask[] = [
  { id: 'plan', spec: 'split the work' },
  { id: 'fetch-a', spec: 'fetch the first half', needs: ['plan'] },
  { id: 'fetch-b', spec: 'fetch the second half', needs: ['plan'] },
  {
    id: 'summarize',
    spec: 'summarize both halves',
    needs: ['fetch-a', 'fetch-b'],
    terms: { retry: { maxAttempts: 2, backoffMs: 50 } },
  },
]
await lead.board.setTextLimit(200, { key: 'text-limit' })
log('lead', `limit  ${JSON.stringify(await lead.board.maxText())}`)
const long = await lead.board.post({ id: 'long', spec: 'x'.repeat(201) })
if (!long.ok && long.rejection.reason === 'Text')
  log('lead', `post   long refused: ${long.rejection.chars} of ${long.rejection.max} chars`)

for (const task of graph) {
  const posted = await lead.board.post(task, { key: 'post:' + task.id })
  log('lead', `post   ${task.id}${posted.ok ? '' : ' refused: ' + posted.rejection.kind}`)
}

const members = await Promise.all([connect('w1'), connect('w2')])
const pools = members.map(({ board }, index) =>
  Board.pool(board, { concurrency: 1, leaseMs: 2_000, pollMs: 20, run: worker('w' + (index + 1)) }),
)

// Reads come from the local fold, so refresh before each look. Steer summarize once it is held.
let steered = false
for (;;) {
  await lead.board.refresh()
  const tasks = await lead.board.tasks()
  if (!steered && tasks.find((task) => task.id === 'summarize')?.phase === 'claimed') {
    await lead.board.steer('summarize', 'keep it to two lines')
    log('lead', 'steer  summarize')
    steered = true
  }
  if (tasks.every((task) => task.phase === 'done')) break
  await sleep(20)
}

await Promise.all(pools.map((pool) => pool.stop()))
for (const task of await lead.board.tasks())
  log('lead', `result ${task.id}: ${JSON.stringify(task.outcome)}`)
pools.forEach((pool, index) => console.log('w' + (index + 1), JSON.stringify(pool.stats())))

for (const { board, sql } of [lead, ...members]) {
  await board.close()
  sql.close()
}
rmSync(dir, { recursive: true, force: true })
