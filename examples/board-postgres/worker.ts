// One Board.pool worker in its own process, with its own Postgres connection pool and actor.
// examples/board-postgres/index.ts starts three of these; it is not meant to be run alone.
import { Board, Sql } from 'nanocode-ts/postgres'

type Role = { url: string; schema: string; scope: string; name: string }

const { url, schema, scope, name } = JSON.parse(process.argv[2] ?? '{}') as Role
const emit = (e: string, fields: Record<string, unknown> = {}) =>
  process.stdout.write(JSON.stringify({ e, at: Date.now(), ...fields }) + '\n')
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
const work: Record<string, number> = { plan: 200, summarize: 800 } // a fetch takes 1000 ms

try {
  // Each worker opens its own pool of two connections; the lead already chartered the board.
  const sql = Sql.open(url, { schema, max: 2 })
  const board = await Board.open({ sql, scope, actor: 'agent:' + name, charter: false })
  const pool = Board.pool(board, {
    concurrency: 1,
    leaseMs: 1_500, // renewed every 500 ms while the process lives
    pollMs: 50,
    cap: { max: 2, label: ['agent', 'llm'] }, // at most two agent=llm tasks held across all workers
    run: async (task, lease) => {
      emit('start', { task: task.id, tookOver: lease.tookOver })
      await sleep(work[task.id] ?? 1_000) // stands in for the real work
      const steers = await lease.steers()
      for (const steer of steers) emit('steer', { task: task.id, body: steer.body })
      const last = steers.at(-1)
      if (last) await lease.heed(last.seq) // a finish is refused while a steer is unheeded
      emit('done', { task: task.id })
      return { done: name + ' finished ' + task.id }
    },
  })
  emit('up')

  // The lead writes a line (or closes stdin) to stop this worker.
  let stopping = false
  const stop = async () => {
    if (stopping) return
    stopping = true
    await pool.stop()
    emit('stats', { stats: pool.stats() })
    await board.close()
    await sql.close()
    process.stdin.destroy()
  }
  process.stdin.on('data', stop).on('end', stop)
} catch (error) {
  emit('error', { message: error instanceof Error ? error.message : String(error) })
  process.exit(1)
}
