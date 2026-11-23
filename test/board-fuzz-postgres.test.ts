import { test } from 'node:test'

// The Board fuzz (board-fuzz.test.ts) over Postgres: one schema per seed, each process on its own
// connection pool. Runs when NC_BOARD_POSTGRES or DATABASE_URL names a database, like the
// conformance suite in board-postgres.test.ts; FUZZ_SEED, FUZZ_MS and FUZZ_PARTITIONS apply as
// there.
const url = process.env.NC_BOARD_POSTGRES ?? process.env.DATABASE_URL

if (!url) {
  test(
    'Board fuzz over Postgres',
    { skip: 'NC_BOARD_POSTGRES and DATABASE_URL are not set' },
    () => {},
  )
} else {
  process.env.NC_BOARD_POSTGRES = url
  await import('./board-fuzz.test.ts')
}
