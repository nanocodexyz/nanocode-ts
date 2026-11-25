import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, test } from 'node:test'
import { Board, Sql } from 'nanocode-ts/node'

const root = mkdtempSync(join(tmpdir(), 'nc-board-module-'))
after(() => rmSync(root, { recursive: true, force: true }))

test('the board-only module opens a board on a SqlHost with a value_limit', async () => {
  const module = await WebAssembly.compile(
    readFileSync(new URL(import.meta.resolve('nanocode-ts/nanocode_board.wasm'))),
  )
  const sql = Sql.open(join(root, 'board.sqlite'), { valueLimit: 1 << 20 })
  const board = await Board.open({ sql, scope: 'limited', actor: 'agent:alice', module })
  try {
    assert.ok((await board.post({ id: 'a', spec: 'first' })).ok)
    assert.equal((await board.task('a'))?.spec, 'first')
  } finally {
    await board.close()
    sql.close()
  }
})
