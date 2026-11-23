import { Board } from 'nanocode-ts/vercel'
import { Sql } from 'nanocode-ts/postgres'

type Role = { url: string; schema: string; scope: string; actor: string } & (
  | { role: 'hold'; task: string; leaseMs: number }
  | { role: 'rescue'; task: string; leaseMs: number }
  | { role: 'race'; tasks: string[]; atMs: number }
)

const role = JSON.parse(process.argv[2] ?? '{}') as Role
const emit = (line: object) => process.stdout.write(JSON.stringify(line) + '\n')
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

try {
  const sql = Sql.open(role.url, { schema: role.schema })
  const board = await Board.open({
    sql,
    scope: role.scope,
    actor: 'agent:' + role.actor,
    charter: false,
  })
  if (role.role === 'hold') {
    const claimed = await board.claim(role.task, { leaseMs: role.leaseMs })
    emit({ e: 'claimed', ...claimed })
    setInterval(() => {}, 60_000)
  } else if (role.role === 'rescue') {
    for (;;) {
      await board.refresh()
      const claimed = await board.claim(role.task, { leaseMs: role.leaseMs })
      if (claimed.ok) {
        const claimedAt = Date.now()
        const finished = await board.finish(role.task, claimed.claim, { done: role.actor })
        if (!finished.ok) throw new Error('finish refused: ' + JSON.stringify(finished.rejection))
        emit({ e: 'finished', tookOver: claimed.tookOver, claimedAt })
        break
      }
      if (claimed.rejection.kind !== 'Held')
        throw new Error('claim refused: ' + JSON.stringify(claimed.rejection))
      await sleep(50)
    }
    await board.close()
    await sql.close()
  } else {
    await board.refresh()
    await sleep(Math.max(0, role.atMs - Date.now()))
    const won: string[] = []
    const lost: Record<string, number> = {}
    for (const id of role.tasks) {
      const claimed = await board.claim(id, { leaseMs: 600_000 })
      if (claimed.ok) won.push(id)
      else lost[claimed.rejection.kind] = (lost[claimed.rejection.kind] ?? 0) + 1
    }
    emit({ e: 'raced', won, lost })
    await board.close()
    await sql.close()
  }
} catch (error) {
  emit({ e: 'error', message: String((error as Error | null)?.stack ?? error) })
  process.exit(1)
}
