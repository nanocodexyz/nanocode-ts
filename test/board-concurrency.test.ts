import assert from 'node:assert/strict'
import { test } from 'node:test'
import { Board } from 'nanocode-ts/node'

test('one handle takes concurrent posts and claims', async () => {
  const sql = await Board.memorySql()
  const board = await Board.open({ sql, scope: 'concurrent', actor: 'agent:alice' })
  try {
    const ids = Array.from({ length: 20 }, (_, index) => 'task-' + index)
    const posted = await Promise.all(ids.map((id) => board.post({ id, spec: id })))
    assert.ok(
      posted.every((verdict) => verdict.ok),
      JSON.stringify(posted),
    )
    const [claims, more, tasks] = await Promise.all([
      Promise.all(ids.map((id) => board.claim(id, { leaseMs: 60_000 }))),
      Promise.all(ids.map((id) => board.post({ id: 'more-' + id, spec: id }))),
      board.tasks(),
    ])
    assert.ok(
      claims.every((claim) => claim.ok),
      JSON.stringify(claims),
    )
    assert.ok(
      more.every((verdict) => verdict.ok),
      JSON.stringify(more),
    )
    assert.ok(tasks.length >= 20)
    assert.equal((await board.held()).length, 20)
    assert.equal((await board.tasks()).length, 40)
  } finally {
    await board.close()
    sql.close()
  }
})
