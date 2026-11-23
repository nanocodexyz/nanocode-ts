import * as ChildProcess from 'node:child_process'
import * as Crypto from 'node:crypto'
import * as Fs from 'node:fs'
import * as Path from 'node:path'
import { Bash, MemoryFileSystem } from '../../src/bash/index.ts'
import { cases } from './probes.ts'

const digest = (bytes: Uint8Array | string) =>
  Crypto.createHash('sha256').update(bytes).digest('hex')

type Entry = {
  bytes: number
  headers: [string, string][]
  sha256: string
  status: number
  url: string
}

function entry(value: unknown): Entry {
  if (
    typeof value !== 'object' ||
    value === null ||
    !('url' in value) ||
    typeof value.url !== 'string' ||
    !('bytes' in value) ||
    !Number.isSafeInteger(value.bytes) ||
    Number(value.bytes) < 0 ||
    !('sha256' in value) ||
    typeof value.sha256 !== 'string' ||
    !/^[a-f0-9]{64}$/.test(value.sha256) ||
    !('status' in value) ||
    !Number.isInteger(value.status) ||
    !('headers' in value) ||
    !Array.isArray(value.headers) ||
    !value.headers.every(
      (header: unknown) =>
        Array.isArray(header) &&
        header.length === 2 &&
        header.every((part: unknown) => typeof part === 'string'),
    )
  )
    throw new Error('Invalid replay response metadata')

  return value as Entry
}

export function replay(directory: string) {
  const errors: string[] = []
  const requests: { sha256: string; url: string }[] = []
  const fetch: typeof globalThis.fetch = async (input, options) => {
    try {
      const url = new URL(
        typeof input === 'string' ? input : input instanceof URL ? input.href : input.url,
      )
      const method = options?.method ?? (input instanceof Request ? input.method : 'GET')
      if (
        method !== 'GET' ||
        options?.body != null ||
        url.protocol !== 'https:' ||
        url.username ||
        url.password ||
        url.port ||
        url.search ||
        url.hash ||
        !(
          (url.hostname === 'pypi.org' &&
            url.pathname.startsWith('/pypi/') &&
            url.pathname.endsWith('/json')) ||
          (url.hostname === 'files.pythonhosted.org' && url.pathname.startsWith('/packages/'))
        )
      )
        throw new Error('Request outside offline PyPI replay scope: ' + url.origin + url.pathname)

      const key = digest(url.href)
      const metadata = entry(
        JSON.parse(Fs.readFileSync(Path.join(directory, key + '.json'), 'utf8')),
      )
      const bytes = Fs.readFileSync(Path.join(directory, key + '.body'))
      if (
        metadata.url !== url.href ||
        metadata.bytes !== bytes.length ||
        metadata.sha256 !== digest(bytes)
      )
        throw new Error('Changed replay response: ' + url.href)

      requests.push({ sha256: metadata.sha256, url: metadata.url })
      return new Response(bytes, { headers: metadata.headers, status: metadata.status })
    } catch (error) {
      errors.push(String(error))
      throw error
    }
  }

  return { errors, fetch, requests }
}

type Bundle = {
  files: { bytes: number; path: string; sha256: string }[]
  matrixSha256: string
  revision: string
  version: 1
}

function revision() {
  return ChildProcess.execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim()
}

function outputPath(path: string) {
  if (!Path.resolve(path).startsWith(Path.resolve('output') + Path.sep))
    throw new Error('Replay artifacts must be under output/')

  return path
}

function bundle(directory: string) {
  const raw = Fs.readFileSync(Path.join(directory, 'manifest.json'))
  const value: unknown = JSON.parse(raw.toString('utf8'))
  if (
    typeof value !== 'object' ||
    value === null ||
    !('version' in value) ||
    value.version !== 1 ||
    !('matrixSha256' in value) ||
    value.matrixSha256 !== digest(JSON.stringify(cases)) ||
    !('revision' in value) ||
    typeof value.revision !== 'string' ||
    !('files' in value) ||
    !Array.isArray(value.files) ||
    !value.files.every(
      (file: unknown) =>
        typeof file === 'object' &&
        file !== null &&
        'path' in file &&
        typeof file.path === 'string' &&
        /^(http\/[a-f0-9]{64}\.(json|body)|dist\/(python\.wasm|python\.data|psh\.wasm))$/.test(
          file.path,
        ) &&
        'bytes' in file &&
        Number.isSafeInteger(file.bytes) &&
        Number(file.bytes) >= 0 &&
        'sha256' in file &&
        typeof file.sha256 === 'string' &&
        /^[a-f0-9]{64}$/.test(file.sha256),
    )
  )
    throw new Error('Invalid replay bundle manifest')

  const manifest = value as Bundle
  const paths = new Set(manifest.files.map((file) => file.path))
  if (
    paths.size !== manifest.files.length ||
    !['python.wasm', 'python.data', 'psh.wasm'].every((name) => paths.has('dist/' + name)) ||
    !manifest.files.some((file) => file.path.startsWith('http/')) ||
    !manifest.files.every(
      (file) =>
        !file.path.startsWith('http/') ||
        paths.has(
          file.path.replace(/\.(json|body)$/, file.path.endsWith('.json') ? '.body' : '.json'),
        ),
    )
  )
    throw new Error('Incomplete replay bundle manifest')

  for (const name of ['dist', 'http']) {
    const actual = Fs.readdirSync(Path.join(directory, name)).map((file) => name + '/' + file)
    const expected = manifest.files.filter((file) => file.path.startsWith(name + '/'))
    if (actual.length !== expected.length || actual.some((path) => !paths.has(path)))
      throw new Error('Unlisted replay bundle files: ' + name)
  }

  for (const file of manifest.files) {
    const path = Path.join(directory, file.path)
    if (!Fs.lstatSync(path).isFile()) throw new Error('Replay bundle file required: ' + file.path)

    const bytes = Fs.readFileSync(path)
    if (bytes.length !== file.bytes || digest(bytes) !== file.sha256)
      throw new Error('Changed replay bundle file: ' + file.path)
  }

  return { manifest, sha256: digest(raw) }
}

async function snapshot(fixtures: string, options: { dist: string; output: string }) {
  const files = Fs.readdirSync(fixtures).sort()
  if (
    !files.length ||
    files.some((name) => !/^[a-f0-9]{64}\.(json|body)$/.test(name)) ||
    files.some(
      (name) =>
        !files.includes(name.replace(/\.(json|body)$/, name.endsWith('.json') ? '.body' : '.json')),
    )
  )
    throw new Error('Snapshot requires complete SHA256(URL).json/.body pairs')

  const cached = replay(fixtures)
  for (const name of files.filter((name) => name.endsWith('.json'))) {
    const metadata = entry(JSON.parse(Fs.readFileSync(Path.join(fixtures, name), 'utf8')))
    if (name !== digest(metadata.url) + '.json')
      throw new Error('Changed replay response key: ' + name)

    await cached.fetch(metadata.url)
  }

  const target = outputPath(options.output)
  Fs.mkdirSync(target)
  Fs.mkdirSync(Path.join(target, 'dist'))
  Fs.mkdirSync(Path.join(target, 'http'))
  const manifest: Bundle = {
    files: [],
    matrixSha256: digest(JSON.stringify(cases)),
    revision: revision(),
    version: 1,
  }
  const sources = [
    ...files.map((name) => ({ path: 'http/' + name, source: Path.join(fixtures, name) })),
    ...['python.wasm', 'python.data', 'psh.wasm'].map((name) => ({
      path: 'dist/' + name,
      source: Path.join(options.dist, name),
    })),
  ].sort((a, b) => a.path.localeCompare(b.path))
  for (const file of sources) {
    if (!Fs.lstatSync(file.source).isFile())
      throw new Error('Snapshot file required: ' + file.source)

    const bytes = Fs.readFileSync(file.source)
    Fs.writeFileSync(Path.join(target, file.path), bytes, { flag: 'wx' })
    manifest.files.push({ bytes: bytes.length, path: file.path, sha256: digest(bytes) })
  }
  Fs.writeFileSync(Path.join(target, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n', {
    flag: 'wx',
  })
  const verified = bundle(target)
  console.log(
    JSON.stringify({
      type: 'snapshot',
      files: manifest.files.length,
      responses: files.length / 2,
      sha256: verified.sha256,
      output: Path.resolve(target),
    }),
  )
}

async function run() {
  const args = process.argv.slice(2)
  if (args[0] === '--snapshot' && args.length === 4) {
    await snapshot(args[1]!, { dist: args[2]!, output: args[3]! })
    return
  }

  const bundled = args[0] === '--bundle'
  const [input, output, ...extra] = bundled ? args.slice(1) : args
  if (!input || !output || extra.length || input.startsWith('--')) {
    console.log(
      'Usage: node bench/python/replay.ts FIXTURE_DIRECTORY NEW_OUTPUT_JSONL\n' +
        '       node bench/python/replay.ts --snapshot FIXTURE_DIRECTORY MATCHED_DIST NEW_BUNDLE_DIRECTORY\n' +
        '       node bench/python/replay.ts --bundle BUNDLE_DIRECTORY NEW_OUTPUT_JSONL\n' +
        'Snapshot copies retained responses and matched python.wasm/python.data/psh.wasm into a portable hash-verified bundle under output/. It never captures or downloads.\n' +
        'Bundle replay verifies every file before running and rejects NANOCODE_DIST/NANOCODE_PYTHON_DIST overrides. Copy the bundle into a clean checkout and run three fresh network-denied processes.\n' +
        'Legacy mode requires SHA256(URL).json metadata (url, status, headers, sha256, bytes) and .body files. No capture or network fallback.\n' +
        'NANOCODE_DIST selects psh.wasm and the default Python pair (default dist). NANOCODE_PYTHON_DIST optionally selects another matched python.wasm/python.data pair for every probe.\n' +
        'Example: unshare -Urn node bench/python/replay.ts --bundle output/python-replay output/trial-1.jsonl',
    )
    if (input !== '--help') process.exitCode = 1
    return
  }

  outputPath(output)
  if (
    bundled &&
    (process.env.NANOCODE_DIST !== undefined || process.env.NANOCODE_PYTHON_DIST !== undefined)
  )
    throw new Error('Bundle replay rejects asset directory overrides')

  const verified = bundled ? bundle(input) : undefined
  const fixtures = bundled ? Path.join(input, 'http') : input
  const dist = Path.resolve(
    bundled ? Path.join(input, 'dist') : (process.env.NANOCODE_DIST ?? 'dist'),
  )
  const pythonDist = Path.resolve(process.env.NANOCODE_PYTHON_DIST ?? dist)
  const assets = {
    data: Fs.readFileSync(Path.join(pythonDist, 'python.data')),
    module: Fs.readFileSync(Path.join(pythonDist, 'python.wasm')),
    psh: Fs.readFileSync(Path.join(dist, 'psh.wasm')),
  }
  const module = new WebAssembly.Module(assets.module)
  const psh = new WebAssembly.Module(assets.psh)
  const cached = replay(fixtures)
  const fd = Fs.openSync(output, 'wx')
  const write = (value: unknown) => Fs.writeSync(fd, JSON.stringify(value) + '\n')
  const startedAt = new Date().toISOString()
  let passed = 0

  try {
    write({
      type: 'configuration',
      assets: Object.fromEntries(
        Object.entries(assets).map(([name, bytes]) => [name, digest(bytes)]),
      ),
      ...(verified
        ? { bundle: { sha256: verified.sha256, revision: verified.manifest.revision } }
        : {}),
      dist,
      fixtures: Path.resolve(fixtures),
      matrixSha256: digest(JSON.stringify(cases)),
      node: process.version,
      policies: {
        maxMemoryBytes: 512 << 20,
        pythonCpuMs: 300000,
        stepsPerMs: 42000,
        timeoutMs: 300000,
      },
      pythonDist,
      revision: revision(),
      sources: Object.fromEntries(
        ['../../src/bash/internal/commands/pip.ts', './probes.ts', './replay.ts'].map((path) => [
          path,
          digest(Fs.readFileSync(new URL(path, import.meta.url))),
        ]),
      ),
      startedAt,
      total: cases.length,
    })
    for (const [name, code] of cases) {
      const before = cached.requests.length
      const fs = MemoryFileSystem.create()
      const bash = await Bash.create({
        cwd: '/workspace',
        fetch: cached.fetch,
        fs,
        limits: { maxMemoryBytes: 512 << 20, timeoutMs: 300000 },
        psh,
        python: { cpuMs: 300000, data: assets.data, module, stepsPerMs: 42000 },
      })
      const start = performance.now()
      const result = await (async () => {
        try {
          const installed = await bash.exec('pip install -q ' + name)
          await bash.writeFile('/workspace/probe.py', code)
          const ran = installed.exitCode === 0 ? await bash.exec('python3 probe.py') : undefined
          return { installed, pass: ran?.exitCode === 0, ran }
        } catch (error) {
          return { error: String(error), pass: false }
        }
      })()
      if (cached.errors.length) throw new Error(cached.errors.join('\n'))

      const metadata = []
      const site = '/usr/local/lib/python3.14/site-packages'
      for (const name of await fs.list(site).catch(() => [])) {
        if (!name.endsWith('.dist-info')) continue
        const bytes = await fs.read(site + '/' + name + '/METADATA')
        metadata.push({
          directory: name,
          headers: new TextDecoder().decode(bytes).split(/\r?\n\r?\n/)[0],
          sha256: digest(bytes),
        })
      }
      const elapsedMs = Math.round(performance.now() - start)
      write({
        type: 'case',
        ...result,
        elapsedMs,
        metadata,
        name,
        requests: cached.requests.slice(before),
      })
      console.log(JSON.stringify({ name, pass: result.pass, elapsedMs }))
      passed += Number(result.pass)
    }
    const summary = { type: 'summary', passed, total: cases.length }
    write(summary)
    console.log(JSON.stringify(summary))
  } finally {
    Fs.closeSync(fd)
  }
}

if (import.meta.main) await run()
