import * as ChildProcess from 'node:child_process'
import * as Fs from 'node:fs'
import * as Os from 'node:os'
import * as Path from 'node:path'
import postgres from 'postgres'
import * as Fixtures from './test-fixtures.ts'
import * as Wasm from './wasm.mjs'

function execute(command: string, args: readonly string[]): void {
  const result = ChildProcess.spawnSync(command, args, { cwd: Wasm.root, stdio: 'inherit' })
  if (result.error) throw result.error
  if (result.status !== 0) throw new Error(command + ' exited ' + result.status)
}

try {
  Fixtures.validate({ receipt: process.env.NANOCODE_TEST_WASM_RECEIPT })
} catch (cause) {
  throw new Error(
    'Tests require the exact local WASM module and its receipt. Acquire assets separately with node scripts/prepare.mjs, or set NANOCODE_TEST_WASM_RECEIPT for an explicit local build. Tests never download or build Rust.',
    { cause },
  )
}

execute(process.platform === 'win32' ? 'npm.cmd' : 'npm', ['run', 'build:ts'])

let directory: string | undefined
let pgCtl: string | undefined
let runner: ChildProcess.ChildProcess | undefined
let url = process.env.DATABASE_URL
let interrupted = false
const signal = (value: NodeJS.Signals) => {
  interrupted = true
  runner?.kill(value)
}
process.on('SIGINT', signal)
process.on('SIGTERM', signal)

try {
  if (!url) {
    const configured = process.env.PG_BINDIR
    const result = configured
      ? undefined
      : ChildProcess.spawnSync('pg_config', ['--bindir'], { encoding: 'utf8' })
    const bin = configured ?? (result?.status === 0 ? result.stdout.trim() : undefined)
    if (!bin || !Fs.existsSync(Path.join(bin, 'initdb')))
      throw new Error('Tests require DATABASE_URL or local PostgreSQL server binaries in PG_BINDIR')

    pgCtl = Path.join(bin, 'pg_ctl')
    directory = Fs.mkdtempSync(Path.join(Os.tmpdir(), 'nc-test-pg-'))
    const data = Path.join(directory, 'data')
    execute(Path.join(bin, 'initdb'), [
      '-D',
      data,
      '-A',
      'trust',
      '-U',
      'postgres',
      '--no-locale',
      '--encoding=UTF8',
    ])
    execute(pgCtl, [
      '-D',
      data,
      '-l',
      Path.join(directory, 'postgres.log'),
      '-o',
      "-h '' -k '" + directory.replaceAll("'", "'\\''") + "'",
      '-w',
      'start',
    ])
    url = 'postgres://postgres@localhost/postgres?host=' + encodeURIComponent(directory)
  }

  const database = postgres(url, { max: 1 })
  try {
    await database.unsafe('SELECT 1')
  } finally {
    await database.end()
  }

  if (interrupted) throw new Error('The test run was interrupted')

  process.exitCode = await new Promise<number>((resolve, reject) => {
    runner = ChildProcess.spawn(
      process.execPath,
      [
        '--test',
        ...process.argv.slice(2),
        'test/bash-jq.test.ts',
        'test/board*.test.ts',
        'test/session-lifecycle.test.ts',
        'test/test-fixtures.test.ts',
      ],
      {
        cwd: Wasm.root,
        env: { ...process.env, DATABASE_URL: url, NC_BOARD_POSTGRES: undefined },
        stdio: 'inherit',
      },
    )
    runner.once('error', reject)
    runner.once('exit', (code) => resolve(code ?? 1))
  })
} finally {
  process.off('SIGINT', signal)
  process.off('SIGTERM', signal)
  if (directory && pgCtl) {
    const result = ChildProcess.spawnSync(
      pgCtl,
      ['-D', Path.join(directory, 'data'), '-m', 'immediate', '-w', 'stop'],
      { stdio: 'inherit' },
    )
    if (result.status === 0 || !Fs.existsSync(Path.join(directory, 'data', 'postmaster.pid')))
      Fs.rmSync(directory, { force: true, recursive: true })
    else {
      console.error('Could not stop the test PostgreSQL server at ' + directory)
      process.exitCode = 1
    }
  }
}
