import * as assert from 'node:assert/strict'
import { test } from 'node:test'
import type * as AgentRuntime from '../src/AgentRuntime.ts'
import * as Session from '../src/Session.ts'

function deferred() {
  return Promise.withResolvers<void>()
}

async function fixture(
  options: {
    disposal?: Promise<void>
    submit?: Promise<unknown>
    retry?: boolean
    closed?: boolean
    reopen?: Promise<void>
    pending?: boolean
    steered?: boolean
  } = {},
) {
  const calls: string[] = []
  let closed = options.closed ?? false
  let opens = 0
  let pending = options.pending ?? false
  const runtime = {
    get closed() {
      return closed
    },
    async dispose() {
      calls.push('dispose')
      await options.disposal
      closed = true
    },
    async pending() {
      calls.push('pending')
      return pending ? [{ report: { request_id: 'request' } }] : []
    },
    async submit() {
      calls.push('submit')
      await options.submit
      pending = true
      return { submitted: 'accepted' }
    },
    async steer() {
      calls.push('steer')
      return options.steered ?? true
    },
    async drive() {
      calls.push('drive')
      pending = false
      return { retry: options.retry ?? false }
    },
    async turn() {
      calls.push('turn')
      return { report: { request_id: 'request' } }
    },
  } as unknown as AgentRuntime.AgentRuntime
  const session = await Session.open({
    scope: 'scope',
    async open() {
      calls.push('open')
      if (opens++ > 0) {
        await options.reopen
        closed = false
      }
      return runtime
    },
  })
  return { calls, runtime, session }
}

for (const operation of ['ask', 'steer', 'resume'] as const) {
  test(`open permits ${operation}`, async () => {
    const { calls, session } = await fixture()
    if (operation === 'resume') assert.equal(await session.resume(), undefined)
    else if (operation === 'steer') assert.equal(await session.steer('prompt'), 'steered')
    else assert.deepEqual(await session.ask('prompt'), { request_id: 'request' })
    assert.equal(calls.filter((call) => call === 'open').length, 1)
    await session.dispose()
  })
  for (const phase of ['closing', 'closed'] as const) {
    test(`${phase} rejects ${operation} without dispatch or reopen`, async () => {
      const disposal = deferred()
      const { calls, session } = await fixture({ disposal: disposal.promise })
      const completion = session.dispose()
      await Promise.resolve()
      if (phase === 'closed') {
        disposal.resolve()
        await completion
      }
      const before = [...calls]
      const attempt = operation === 'resume' ? session.resume() : session[operation]('prompt')
      await assert.rejects(attempt, /session is closed/)
      assert.deepEqual(calls, before)
      assert.equal(session.dispose(), completion)
      disposal.resolve()
      await completion
      assert.equal(calls.filter((call) => call === 'dispose').length, 1)
    })
  }
}

for (const phase of ['closing', 'closed'] as const) {
  test(`${phase} rejects hand connection`, async () => {
    const disposal = deferred()
    const { session } = await fixture({ disposal: disposal.promise })
    const completion = session.dispose()
    if (phase === 'closed') {
      disposal.resolve()
      await completion
    }
    assert.throws(() => session.hand(), /session is closed/)
    disposal.resolve()
    await completion
  })
}

test('disposal failure stays terminal and is not retried', async () => {
  const disposal = deferred()
  const { calls, session } = await fixture({ disposal: disposal.promise })
  const completion = session.dispose()
  disposal.reject(new Error('dispose failed'))
  await assert.rejects(completion, /dispose failed/)
  assert.equal(session.dispose(), completion)
  await assert.rejects(session.dispose(), /dispose failed/)
  await assert.rejects(session.ask('prompt'), /session is closed/)
  assert.equal(calls.filter((call) => call === 'dispose').length, 1)
})

test('a dispatched failure is not replayed after disposal', async () => {
  const submit = deferred()
  const { calls, session } = await fixture({ submit: submit.promise })
  const work = session.ask('charge')
  await Promise.resolve()
  const completion = session.dispose()
  submit.reject(new Error('outcome unknown'))
  await assert.rejects(work, /outcome unknown/)
  await completion
  await assert.rejects(session.ask('charge'), /session is closed/)
  assert.equal(calls.filter((call) => call === 'submit').length, 1)
  assert.equal(calls.filter((call) => call === 'drive').length, 0)
  assert.equal(calls.filter((call) => call === 'open').length, 1)
})

test('a retry result surfaces without replay', async () => {
  const { calls, session } = await fixture({ retry: true })
  await assert.rejects(session.ask('charge'), /session must reopen/)
  assert.equal(calls.filter((call) => call === 'submit').length, 1)
  assert.equal(calls.filter((call) => call === 'drive').length, 1)
  assert.equal(calls.filter((call) => call === 'open').length, 1)
  await session.dispose()
})

test('a lease waiting for reopen cannot dispatch after closing', async () => {
  const reopen = deferred()
  const { calls, session } = await fixture({ closed: true, reopen: reopen.promise })
  const work = session.ask('charge')
  const completion = session.dispose()
  reopen.resolve()
  await assert.rejects(work, /session is closed/)
  await completion
  assert.equal(calls.filter((call) => call === 'submit').length, 0)
  assert.equal(calls.filter((call) => call === 'drive').length, 0)
})

test('disposal synchronously closes the session and shares one completion', async () => {
  const disposal = deferred()
  const { calls, session } = await fixture({ disposal: disposal.promise })
  const completion = session.dispose()
  assert.equal(session.dispose(), completion)
  assert.throws(() => session.hand(), /session is closed/)
  await assert.rejects(session.ask('prompt'), /session is closed/)
  await assert.rejects(session.steer('prompt'), /session is closed/)
  await assert.rejects(session.resume(), /session is closed/)
  disposal.resolve()
  await completion
  assert.equal(session.dispose(), completion)
  assert.equal(calls.filter((call) => call === 'dispose').length, 1)
  assert.equal(calls.filter((call) => call === 'submit').length, 0)
})

test('a closed runtime disposes without reopening or another disposal', async () => {
  const { calls, session } = await fixture({ closed: true })
  await session.dispose()
  assert.equal(calls.filter((call) => call === 'dispose').length, 0)
  assert.equal(calls.filter((call) => call === 'open').length, 1)
})

test('an open lease reopens a closed runtime before dispatch', async () => {
  const { calls, session } = await fixture({ closed: true })
  assert.deepEqual(await session.ask('prompt'), { request_id: 'request' })
  assert.equal(calls.filter((call) => call === 'open').length, 2)
  assert.equal(calls.filter((call) => call === 'submit').length, 1)
  await session.dispose()
})

test('concurrent leases share a reopen and cannot dispatch after closing', async () => {
  const reopen = deferred()
  const { calls, session } = await fixture({ closed: true, reopen: reopen.promise })
  const first = assert.rejects(session.ask('first'), /session is closed/)
  const second = assert.rejects(session.steer('second'), /session is closed/)
  const completion = session.dispose()
  assert.equal(session.dispose(), completion)
  reopen.resolve()
  await Promise.all([first, second, completion])
  assert.equal(calls.filter((call) => call === 'open').length, 2)
  assert.equal(calls.filter((call) => call === 'submit').length, 0)
  assert.equal(calls.filter((call) => call === 'steer').length, 0)
  assert.equal(calls.filter((call) => call === 'dispose').length, 1)
})

test('a submission accepted during closing is not driven or replayed', async () => {
  const submit = deferred()
  const { calls, session } = await fixture({ submit: submit.promise })
  const work = session.ask('charge')
  const completion = session.dispose()
  submit.resolve()
  await assert.rejects(work, /session is closed/)
  await completion
  assert.equal(calls.filter((call) => call === 'submit').length, 1)
  assert.equal(calls.filter((call) => call === 'drive').length, 0)
  assert.equal(calls.filter((call) => call === 'open').length, 1)
})

test('resume drains interrupted work without submitting another prompt', async () => {
  const { calls, session } = await fixture({ pending: true })
  assert.equal(session.interrupted, true)
  assert.deepEqual(await session.resume(), { request_id: 'request' })
  assert.equal(calls.filter((call) => call === 'submit').length, 0)
  assert.equal(calls.filter((call) => call === 'drive').length, 1)
  await session.dispose()
})

test('an unhandled steer queues one prompt and drains it once', async () => {
  const { calls, session } = await fixture({ steered: false })
  assert.equal(await session.steer('prompt'), 'queued')
  await session.resume()
  assert.equal(calls.filter((call) => call === 'steer').length, 1)
  assert.equal(calls.filter((call) => call === 'submit').length, 1)
  assert.equal(calls.filter((call) => call === 'drive').length, 1)
  await session.dispose()
})

test('read-only session access remains available after disposal', async () => {
  const { runtime, session } = await fixture()
  assert.equal(session.hand(), undefined)
  await session.dispose()
  assert.equal(session.runtime(), runtime)
  assert.deepEqual(session.hands(), [])
  assert.deepEqual(session.listing(), { hands: [], toolsOnly: [] })
  assert.equal(session.interrupted, false)
})
