// Browser e2e of pyramid2-web's live feed against a running pyramid2 (web on WEB, API on NODES[0]); run it with
// test/e2e/pyramid2-web.run.sh, which takes the lease and drives bench/pyramid2-swarm.py beside it. Phases (PHASES):
// auth, basic, fanout8, fanout32, blip, reload, subagents, soak. auth tries the web's own same-origin /live (WEB/live)
// first: a token login (sessionStorage, then ?token=), then the session cookie set on WEB's origin. The first mode
// that syncs (LIVE=token|query|cookie|relay forces one) carries the other phases; relay, a local relay that adds the
// bearer header (a browser WebSocket cannot), is the fallback, and blip always uses it so it can cut a session off.
// Writes OUT/result.json (latencies p50/p99 from API post to DOM appearance, console errors and failed requests per
// phase) and screenshots. The token is read from PYRAMID2_TOKEN_FILE and never printed.
import { chromium } from 'playwright'
import { WebSocketServer, WebSocket } from 'ws'
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs'
import { randomUUID } from 'node:crypto'

const WEB = process.env.WEB ?? 'http://127.0.0.1:18470'
const NODES = (process.env.NODES ?? 'http://127.0.0.1:18480').split(',')
const OUT = process.env.OUT ?? './out'
const PHASES = (process.env.PHASES ?? 'auth,basic,fanout8,fanout32,blip,reload,subagents,soak').split(',')
const PROBES = Number(process.env.PROBES ?? '8')
const SOAK_MS = Number(process.env.SOAK_MIN ?? '10') * 60_000
const SEEN_MS = Number(process.env.SEEN_MS ?? '30000')
const DEADLINE = Number(process.env.DEADLINE_MS ?? String(Date.now() + 3_600_000))
const token = readFileSync(process.env.PYRAMID2_TOKEN_FILE ?? '', 'utf8').trim()
type Mode = 'token' | 'query' | 'cookie' | 'relay'
let mode: Mode = (process.env.LIVE as Mode) || 'relay'
const SESSION_COOKIE = '__Host-pyramid-session'
mkdirSync(OUT, { recursive: true })

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const pct = (xs: number[], p: number) => {
  if (!xs.length) return null
  const s = [...xs].sort((a, b) => a - b)
  return s[Math.min(s.length - 1, Math.max(0, Math.ceil((p / 100) * s.length) - 1))]
}
const dist = (xs: number[]) => ({ n: xs.length, p50: pct(xs, 50), p99: pct(xs, 99), max: xs.length ? Math.max(...xs) : null })
const result: any = { started: new Date().toISOString(), web: WEB, nodes: NODES, phases: {}, logs: {} }
let scenario = 'setup'
const log = (kind: string, text: string) => {
  const l = (result.logs[scenario] ??= { console: [], pageerror: [], requestfailed: [], http: [] })
  if (l[kind].length < 200) l[kind].push(text.slice(0, 400))
}
const step = (s: string) => console.log(new Date().toISOString() + ' [' + scenario + '] ' + s)
const save = () => writeFileSync(OUT + '/result.json', JSON.stringify(result, null, 2))

// A 503 (pyramid2's 'Try again shortly' under load) is retried up to 5 times with backoff; result.retries_503 counts them.
async function v1(path: string, init: any = {}) {
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(NODES[0] + '/v1' + path, {
      ...init,
      headers: { authorization: 'Bearer ' + token, 'content-type': 'application/json', ...(init.headers ?? {}) },
    }).catch((e: any) => {
      if (refused(e)) noteDown(NODES[0], 'ECONNREFUSED ' + path)
      throw e
    })
    if (res.status === 503 && attempt < 5) {
      await res.text().catch(() => '')
      result.retries_503 = (result.retries_503 ?? 0) + 1
      ;(result.retries_by_phase ??= {})[scenario] = (result.retries_by_phase[scenario] ?? 0) + 1
      await sleep(250 * 2 ** attempt)
      continue
    }
    if (!res.ok) throw new Error(path + ': ' + res.status + ' ' + (await res.text()).slice(0, 300))
    return res.status === 204 ? null : res.json()
  }
}
async function until(what: string, ok: () => Promise<boolean>, timeout = 45_000, every = 200) {
  const t = Date.now()
  while (Date.now() - t < timeout) {
    if (await ok().catch(() => false)) return Date.now() - t
    await sleep(every)
  }
  throw new Error('timed out: ' + what)
}
async function newThread() {
  return (await v1('/threads', { method: 'POST', body: JSON.stringify({ request_id: randomUUID() }) })).thread.id as string
}
async function turn(thread: string, input: string) {
  const sent = Date.now()
  await v1('/threads/' + thread + '/turns', { method: 'POST', body: JSON.stringify({ turn_id: randomUUID(), input }) })
  return sent
}
async function answered(thread: string, needle: string) {
  const snap = await v1('/threads/' + thread + '/snapshot')
  return snap.messages.some((m: any) => m.role === 'assistant' && JSON.stringify(m).includes(needle))
}

// Node restarts: each node's /readyz is polled every second (and a fetch that fails with ECONNREFUSED counts too); a
// down period is recorded with timestamps, and a phase that overlaps one is confounded (and not ok).
type Restart = { node: string; down_at: string; up_at: string | null; down_ms: number; up_ms: number | null; reason: string }
const restarts: Restart[] = []
const downNow = new Map<string, Restart>()
function noteDown(node: string, reason: string) {
  if (downNow.has(node)) return
  const r: Restart = { node, down_at: new Date().toISOString(), up_at: null, down_ms: Date.now(), up_ms: null, reason }
  downNow.set(node, r)
  restarts.push(r)
}
function noteUp(node: string) {
  const r = downNow.get(node)
  if (!r) return
  r.up_at = new Date().toISOString()
  r.up_ms = Date.now()
  downNow.delete(node)
}
const refused = (e: any) => /ECONNREFUSED/.test(String(e?.cause?.code ?? e?.code ?? '') + String(e?.cause?.message ?? e?.message ?? ''))
let polling = true
const poller = (async () => {
  while (polling) {
    await Promise.all(
      NODES.map(async (n) => {
        try {
          const res = await fetch(n + '/readyz', { signal: AbortSignal.timeout(2000) })
          await res.text().catch(() => '')
          res.ok ? noteUp(n) : noteDown(n, 'readyz ' + res.status)
        } catch (e: any) {
          noteDown(n, refused(e) ? 'ECONNREFUSED' : 'readyz ' + String(e?.cause?.code ?? e?.message ?? e).slice(0, 80))
        }
      }),
    )
    await sleep(1000)
  }
})()
const blocked = new Set<string>()
const sockets = new Map<string, Set<WebSocket>>()
const frames = new Map<string, number>()
const nodeWs = NODES[0].replace(/^http/, 'ws') + '/live'
const wss = new WebSocketServer({ host: '127.0.0.1', port: 0 })
wss.on('connection', (client, req) => {
  const s = new URL(req.url ?? '/', 'http://x').searchParams.get('s') ?? 'anon'
  if (blocked.has(s)) return client.close(1011, 'blip')
  const up = new WebSocket(nodeWs, { headers: { authorization: 'Bearer ' + token } })
  const queue: any[] = []
  const set = sockets.get(s) ?? new Set()
  sockets.set(s, set)
  set.add(client)
  up.on('open', () => queue.splice(0).forEach((m) => up.send(m)))
  up.on('message', (m, binary) => {
    frames.set(s, (frames.get(s) ?? 0) + 1)
    if (client.readyState === 1) client.send(m, { binary })
  })
  client.on('message', (m, binary) => (up.readyState === 1 ? up.send(m, { binary }) : queue.push(binary ? m : String(m))))
  const end = () => {
    set.delete(client)
    if (up.readyState <= 1) up.close()
    if (client.readyState <= 1) client.close()
  }
  up.on('close', end)
  up.on('error', (e) => (log('requestfailed', s + ' upstream /live: ' + e.message), end()))
  client.on('close', end)
  client.on('error', end)
})
await new Promise((r) => wss.once('listening', r))
const proxy = 'ws://127.0.0.1:' + (wss.address() as any).port + '/live'

const browser = await chromium.launch({ args: ['--enable-precise-memory-info'] })
type S = { name: string; context: any; page: any; mode: Mode; first: boolean }
// token: the web's token login in sessionStorage and its default same-origin /live; query: the same login through
// ?token= on the first load (open adds it); cookie: the session cookie on WEB's origin, no token; relay: the token plus
// ?live= pointing at the local relay.
async function session(name: string, m: Mode = mode): Promise<S> {
  const context = await browser.newContext({ viewport: { width: 1280, height: 860 } })
  // __Host- needs Secure, path / and no Domain; Chromium treats a loopback http origin as secure.
  if (m === 'cookie') await context.addCookies([{ name: SESSION_COOKIE, value: token, domain: new URL(WEB).hostname, path: '/', secure: true, httpOnly: true, sameSite: 'Lax' }])
  await context.addInitScript(
    ([t, l]: string[]) => {
      try {
        if (t) sessionStorage.setItem('pyramid2-web.token', t)
        if (l) sessionStorage.setItem('pyramid2-web.live', l)
        else sessionStorage.removeItem('pyramid2-web.live')
      } catch {}
    },
    [m === 'token' || m === 'relay' ? token : '', m === 'relay' ? proxy + '?s=' + name : ''],
  )
  const page = await context.newPage()
  page.on('console', (m: any) => m.type() === 'error' && log('console', name + ': ' + m.text()))
  page.on('pageerror', (e: any) => log('pageerror', name + ': ' + e))
  page.on('requestfailed', (r: any) => log('requestfailed', name + ': ' + r.method() + ' ' + r.url().split('?')[0] + ' ' + r.failure()?.errorText))
  page.on('response', (r: any) => r.status() >= 400 && log('http', name + ': ' + r.status() + ' ' + r.request().method() + ' ' + r.url().split('?')[0]))
  const s = { name, context, page, mode: m, first: true }
  opened.add(s)
  context.on('close', () => opened.delete(s))
  return s
}
const opened = new Set<S>()
async function evidence(tag: string) {
  const out: any[] = []
  for (const s of [...opened].slice(0, 3)) {
    await shot(s.page, 'fail-' + tag + '-' + s.name)
    const text = (await s.page.locator('body').innerText({ timeout: 3000 }).catch(() => '')) as string
    out.push({ session: s.name, url: s.page.url().replace(/token=[^&]*/, 'token=x'), chip: await chip(s.page), body: text.replace(/\s+/g, ' ').slice(0, 700) })
  }
  return out
}
const chip = async (p: any) => ((await p.locator('[aria-label^="Connection:"]').first().innerText({ timeout: 5000 }).catch(() => '?')) as string).trim().replace(/\s+/g, ' ')
const shot = (p: any, n: string) => p.screenshot({ path: OUT + '/' + n + '.png' }).catch(() => {})
async function open(s: S, path: string) {
  let url = WEB + path
  if (s.mode === 'query' && s.first) url += (path.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(token)
  s.first = false
  await s.page.goto(url, { waitUntil: 'load' })
  await until(s.name + ' synced', async () => /^Synced/.test(await chip(s.page)), 45_000, 250)
}
async function watch(p: any) {
  await p.evaluate(() => {
    const w = window as any
    if (w.__obs) return
    w.__want = new Set()
    w.__seen = {}
    w.__streaming = 0
    const hit = (s: string) => {
      for (const m of w.__want) if (s.includes(m)) (w.__seen[m] = Date.now()), w.__want.delete(m)
    }
    w.__obs = new MutationObserver((records) => {
      if (document.querySelector('[data-streaming]')) w.__streaming++
      if (!w.__want.size) return
      for (const r of records) {
        if (r.type === 'characterData') hit(r.target.textContent ?? '')
        else if (r.type === 'attributes') hit((r.target as Element).outerHTML ?? '')
        else r.addedNodes.forEach((n) => hit(n.nodeType === 1 ? (n as Element).outerHTML : n.textContent ?? ''))
      }
    })
    w.__obs.observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: ['href'] })
  })
}
const want = (p: any, marks: string[]) =>
  p.evaluate((ms: string[]) => {
    const w = window as any
    const html = document.body.innerHTML
    for (const m of ms) html.includes(m) ? (w.__seen[m] = Date.now()) : w.__want.add(m)
  }, marks)
const seen = (p: any) => p.evaluate(() => (window as any).__seen)
const count = (p: any, mark: string) =>
  p.evaluate((m: string) => [...document.querySelectorAll('[role="log"] article')].filter((a) => (a.textContent ?? '').includes(m)).length, mark)
const stuck = (p: any) =>
  p.evaluate(() => ({
    pending: document.querySelectorAll('[data-pending]').length,
    writing: document.querySelectorAll('[aria-label="Writing"],[data-streaming]').length,
  }))
async function metrics(s: S) {
  const cdp = await s.context.newCDPSession(s.page)
  await cdp.send('Performance.enable')
  return async () => {
    await cdp.send('HeapProfiler.collectGarbage').catch(() => {})
    const m = Object.fromEntries((await cdp.send('Performance.getMetrics')).metrics.map((x: any) => [x.name, x.value]))
    const dom = await s.page.evaluate(() => document.getElementsByTagName('*').length)
    return { t: Date.now(), heapMB: Math.round(m.JSHeapUsedSize / 1e5) / 10, nodes: m.Nodes, listeners: m.JSEventListeners, dom }
  }
}
async function pane(p: any, timeout = 20_000) {
  const t = Date.now()
  const ok = await p.locator('[role="log"] article').first().waitFor({ timeout }).then(() => true, () => false)
  const ms = Date.now() - t
  const dialog = (await p.locator('[role="dialog"]').first().innerText({ timeout: 2000 }).catch(() => '<no dialog>')) as string
  return { rendered: ok, ms: ok ? ms : null, dialog: dialog.replace(/\s+/g, ' ').slice(0, 300) }
}
async function closeAll(ss: S[]) {
  await Promise.all(ss.map((s) => s.context.close().catch(() => {})))
}

let watched = ''
async function seed() {
  watched = await newThread()
  await turn(watched, 'e2e:hello seed ' + watched.slice(0, 8))
  await until('seed answers', () => answered(watched, 'e2e hello'), 60_000, 500)
  result.watched = watched
}

// Each mode in turn on the web's own origin: does the chip reach Synced, is it not Fixture, and does the board list the
// API-created seed thread? The first mode that passes all three carries the other phases (unless LIVE forces one).
async function auth() {
  const tried: any = {}
  const order: Mode[] = process.env.LIVE ? [mode] : ['token', 'query', 'cookie', 'relay']
  let picked: Mode | null = null
  for (const m of order) {
    let s: S
    try {
      s = await session('auth-' + m, m)
    } catch (e: any) {
      tried[m] = { ok: false, error: String(e?.message ?? e).split('\n')[0].slice(0, 200) }
      step(m + ' ' + JSON.stringify(tried[m]))
      continue
    }
    const t0 = Date.now()
    let synced: number | null = null
    try {
      await open(s, '/')
      synced = Date.now() - t0
    } catch {}
    const c = await chip(s.page)
    const board = await until('seed thread on the board', async () => (await s.page.locator('a[href*="' + watched + '"]').count()) > 0, synced == null ? 3000 : 20_000, 250).catch(() => null)
    const links = await s.page.locator('a[href^="/t/"],a[href*="open="]').count()
    const http = (result.logs.auth?.http ?? []).filter((x: string) => x.startsWith('auth-' + m + ':')).slice(0, 5)
    const live = await s.page.evaluate(() => sessionStorage.getItem('pyramid2-web.live')).catch(() => null)
    await shot(s.page, 'auth-' + m)
    await s.context.close()
    const reachedSynced = synced != null && /^Synced/.test(c)
    const syncedWithoutData = reachedSynced && board == null
    const ok = reachedSynced && !/Fixture/i.test(c) && board != null
    tried[m] = { ok, synced_without_data: syncedWithoutData, chip: c, synced_ms: synced, fixture: /Fixture/i.test(c), seed_thread_on_board_ms: board, thread_links: links, live_override: live ? 'relay' : 'same-origin', http }
    step(m + ' ' + JSON.stringify(tried[m]))
    if (ok && !picked) {
      picked = m
      if (!process.env.ALL_MODES) break
    }
  }
  if (!picked) throw new Error('no live mode synced: ' + JSON.stringify(tried))
  mode = picked
  result.mode = mode
  return { mode, tried, synced_without_data: Object.keys(tried).filter((k) => tried[k].synced_without_data), token_login_authenticates_live: !!(tried.token?.ok || tried.query?.ok) }
}

async function basic() {
  const s = await session('basic')
  await open(s, '/')
  await watch(s.page)
  const threadLinks = await s.page.locator('a[href^="/t/"]').count()
  await shot(s.page, 'basic-1-root')
  await s.page.goto(WEB + '/?open=' + encodeURIComponent(watched), { waitUntil: 'load' })
  const pv = await pane(s.page, 30_000)
  await shot(s.page, 'basic-2-thread')
  if (!pv.rendered) {
    await s.context.close()
    return { thread_links_on_root: threadLinks, thread_pane: pv }
  }
  await watch(s.page)
  const mark = 'ui' + randomUUID().slice(0, 8)
  await want(s.page, [mark])
  const composer = s.page.getByLabel('Steer this thread')
  await composer.fill('e2e:slow 3000 ' + mark)
  const t0 = Date.now()
  await composer.press('Control+Enter')
  await until('own steer shows', async () => (await seen(s.page))[mark] != null, 10_000, 20)
  const ownMs = (await seen(s.page))[mark] - t0
  const running = await s.page.getByText(/running|queued/i).first().isVisible().catch(() => false)
  await shot(s.page, 'basic-3-running')
  await s.page.locator('[role="log"] article', { hasText: 'e2e slow done' }).last().waitFor({ timeout: 60_000 })
  const doneMs = Date.now() - t0
  await sleep(1500)
  const after = await stuck(s.page)
  const streamingSeen = await s.page.evaluate(() => (window as any).__streaming)
  const copies = await count(s.page, mark)
  await shot(s.page, 'basic-4-answered')
  await s.context.close()
  return { thread_links_on_root: threadLinks, steer_echo_ms: ownMs, running_badge_seen: running, answer_ms: doneMs, streaming_mutations: streamingSeen, after, user_copies: copies }
}

async function fanout(n: number) {
  const ss = await Promise.all(Array.from({ length: n }, (_, i) => session('f' + n + '-' + i)))
  const t0 = Date.now()
  await Promise.all(ss.map((s) => open(s, '/?open=' + encodeURIComponent(watched))))
  const panes = await Promise.all(ss.map((s) => pane(s.page, 30_000)))
  const loadMs = Date.now() - t0
  await Promise.all(ss.map((s) => watch(s.page)))
  const thr: number[] = [], trn: number[] = [], miss: string[] = []
  const marks: string[] = []
  for (let k = 0; k < PROBES; k++) {
    const mark = 'tn' + randomUUID().slice(0, 8)
    marks.push(mark)
    const created = Date.now()
    const id = await newThread()
    await Promise.all(ss.map((s) => want(s.page, [id, mark])))
    const sentTurn = await turn(watched, 'e2e:hello ' + mark)
    await turn(id, 'e2e:hello first ' + mark)
    const t = Date.now()
    await until('probe ' + k, async () => {
      const all = await Promise.all(ss.map((s) => seen(s.page)))
      return all.every((x) => x[id] != null && x[mark] != null) || Date.now() - t > SEEN_MS
    }, SEEN_MS + 5000, 100).catch(() => {})
    const all = await Promise.all(ss.map((s) => seen(s.page)))
    all.forEach((x, i) => {
      x[id] != null ? thr.push(x[id] - created) : miss.push(ss[i].name + ' thread ' + id)
      x[mark] != null ? trn.push(x[mark] - sentTurn) : miss.push(ss[i].name + ' turn ' + mark)
    })
    step('probe ' + k + ' done, misses so far ' + miss.length)
  }
  await sleep(2000)
  const dups: string[] = []
  for (const s of ss) for (const m of marks) {
    const c = await count(s.page, m)
    if (c !== 1) dups.push(s.name + ' ' + m + ' x' + c)
  }
  await shot(ss[0].page, 'fanout' + n + '-s0')
  const chips = await Promise.all(ss.map((s) => chip(s.page)))
  await closeAll(ss)
  return { sessions: n, mode, panes_rendered: panes.filter((x) => x.rendered).length, deep_link_pane_ms: dist(panes.flatMap((x) => (x.ms == null ? [] : [x.ms]))), pane_sample: panes[0], initial_load_ms: loadMs, new_thread_ms: dist(thr), new_turn_ms: dist(trn), misses: miss.slice(0, 40), miss_count: miss.length, user_message_not_exactly_once: dups.slice(0, 40), chips_not_synced: chips.filter((c) => !/^Synced/.test(c)) }
}

async function blip() {
  const ss = await Promise.all(Array.from({ length: 4 }, (_, i) => session('blip-' + i, 'relay')))
  await Promise.all(ss.map((s) => open(s, '/?open=' + encodeURIComponent(watched))))
  const panes = await Promise.all(ss.map((s) => pane(s.page, 30_000)))
  await Promise.all(ss.map((s) => watch(s.page)))
  for (const s of ss) {
    blocked.add(s.name)
    for (const c of sockets.get(s.name) ?? []) c.close(1011, 'blip')
    await s.context.setOffline(true)
  }
  const tOff = Date.now()
  await sleep(1500)
  const offChips = await Promise.all(ss.map((s) => chip(s.page)))
  await shot(ss[0].page, 'blip-1-offline')
  const marks: string[] = []
  const thr: string[] = []
  for (let k = 0; k < 3; k++) {
    const m = 'bl' + randomUUID().slice(0, 8)
    marks.push(m)
    await turn(watched, 'e2e:hello ' + m)
    const id = await newThread()
    thr.push(id)
    await turn(id, 'e2e:hello ' + m)
    await sleep(2000)
  }
  await Promise.all(ss.map((s) => want(s.page, [...marks, ...thr])))
  await sleep(Math.max(0, 10_000 - (Date.now() - tOff)))
  for (const s of ss) {
    blocked.delete(s.name)
    await s.context.setOffline(false)
  }
  const tOn = Date.now()
  await until('blip catch-up', async () => {
    const all = await Promise.all(ss.map((s) => seen(s.page)))
    return all.every((x) => [...marks, ...thr].every((m) => x[m] != null))
  }, 60_000, 100).catch(() => {})
  const all = await Promise.all(ss.map((s) => seen(s.page)))
  const catchUp = all.map((x) => {
    const v = Math.max(...[...marks, ...thr].map((m) => (x[m] ?? NaN) - tOn))
    return Number.isFinite(v) ? v : null
  })
  for (const m of marks) await until('answer ' + m, () => answered(watched, m), 60_000, 500).catch(() => {})
  await sleep(4000)
  const counts: string[] = []
  for (const s of ss) for (const m of marks) {
    const c = await count(s.page, m)
    if (c !== 1) counts.push(s.name + ' ' + m + ' x' + c)
  }
  const st = await Promise.all(ss.map((s) => stuck(s.page)))
  const chips = await Promise.all(ss.map((s) => chip(s.page)))
  await shot(ss[0].page, 'blip-2-back')
  await closeAll(ss)
  const missing = all.map((x) => [...marks, ...thr].filter((m) => x[m] == null).length)
  const failures: string[] = []
  ss.forEach((s, i) => {
    if (catchUp[i] == null) failures.push(s.name + ' catch_up_ms is null')
    if (missing[i] > 0) failures.push(s.name + ' missing ' + missing[i] + ' posted item(s) after reconnect')
  })
  if (counts.length) failures.push('duplicated or missing after reconnect: ' + counts.slice(0, 10).join(', '))
  return { panes_rendered: panes.filter((x) => x.rendered).length, offline_chips: offChips, catch_up_ms: catchUp, missing, user_message_not_exactly_once: counts, stuck: st, chips, failures }
}

async function reload() {
  const s = await session('reload')
  await open(s, '/?open=' + encodeURIComponent(watched))
  const pv = await pane(s.page, 30_000)
  if (!pv.rendered) throw new Error('thread pane did not render: ' + JSON.stringify(pv))
  const m = 'rl' + randomUUID().slice(0, 8)
  await watch(s.page)
  await want(s.page, [m])
  await turn(watched, 'e2e:slow 8000 ' + m)
  await until('reload mark', async () => (await seen(s.page))[m] != null, 20_000, 50)
  await sleep(1500)
  await shot(s.page, 'reload-1-before')
  const r0 = Date.now()
  await s.page.reload({ waitUntil: 'load' })
  await s.page.locator('[role="log"] article', { hasText: m }).first().waitFor({ timeout: 20_000 })
  const repaintMs = Date.now() - r0
  const during = await stuck(s.page)
  await shot(s.page, 'reload-2-after')
  let doneMs: number | null = null
  try {
    await until('answer after the mark', async () =>
      s.page.evaluate((mk: string) => {
        const a = [...document.querySelectorAll('[role="log"] article')]
        const i = a.findIndex((x) => (x.textContent ?? '').includes(mk))
        return i >= 0 && a.slice(i + 1).some((x) => (x.textContent ?? '').includes('e2e slow done'))
      }, m), 30_000, 100)
    doneMs = Date.now() - r0
  } catch {}
  await sleep(2000)
  const res = { repaint_ms: repaintMs, during, answer_after_reload_ms: doneMs, user_copies: await count(s.page, m), after: await stuck(s.page), chip: await chip(s.page) }
  await shot(s.page, 'reload-3-done')
  await s.context.close()
  return res
}

async function subagents() {
  const id = await newThread()
  const tag = 'sub' + randomUUID().slice(0, 6)
  const SWARM = 4
  await turn(id, 'e2e:swarm ' + SWARM + ' ' + tag)
  // The API first: the snapshot's thread.subagents lists the running children.
  let apiMax = 0, apiFirst: number | null = null
  const t0 = Date.now()
  await until('subagent rows in the API', async () => {
    const snap = await v1('/threads/' + id + '/snapshot')
    const n = (snap.thread?.subagents ?? []).length
    if (n > apiMax) apiMax = n
    if (n > 0 && apiFirst == null) apiFirst = Date.now() - t0
    return apiMax >= SWARM
  }, 60_000, 300).catch(() => {})
  const api = { max_running_subagents: apiMax, first_row_ms: apiFirst, requested: SWARM }
  step('api ' + JSON.stringify(api))
  const s = await session('subagents')
  await open(s, '/t/' + encodeURIComponent(id))
  await shot(s.page, 'subagents-1-board-early')
  let children = 0
  await until('children on the board', async () => {
    children = await s.page.locator('section[aria-labelledby="s-threads"] a[href^="/t/"]').count()
    return children >= SWARM
  }, 90_000, 500).catch(() => {})
  await shot(s.page, 'subagents-2-board')
  const sections = await s.page.locator('main h2').allInnerTexts()
  await s.page.goto(WEB + '/t/' + encodeURIComponent(id) + '?open=' + encodeURIComponent(id), { waitUntil: 'load' })
  await s.page.locator('[role="log"] article').first().waitFor({ timeout: 30_000 }).catch(() => {})
  await until('swarm answered', () => answered(id, 'child'), 120_000, 1000).catch(() => {})
  await sleep(3000)
  const articles = await s.page.locator('[role="log"] article').count()
  const text = (await s.page.locator('[role="log"]').innerText().catch(() => '')) as string
  await shot(s.page, 'subagents-3-thread')
  const st = await stuck(s.page)
  await s.context.close()
  const failures: string[] = []
  if (apiMax < SWARM) failures.push('max_running_subagents ' + apiMax + ' < requested ' + SWARM)
  if (children === 0) failures.push('no child shows on the board')
  return { thread: id, api, child_links_on_board: children, sections, articles, shows_children_done: /child \d+ done/.test(text), stuck: st, failures }
}

async function soak() {
  const s = await session('soak')
  await open(s, '/?open=' + encodeURIComponent(watched))
  const pv = await pane(s.page, 30_000)
  const sample = await metrics(s)
  const samples = [await sample()]
  const end = Date.now() + Math.min(SOAK_MS, DEADLINE - Date.now() - 120_000)
  let k = 0
  while (Date.now() < end) {
    await turn(watched, 'e2e:hello soak ' + k++).catch(() => {})
    await sleep(30_000)
    samples.push(await sample())
    step('soak ' + JSON.stringify(samples[samples.length - 1]))
  }
  await shot(s.page, 'soak-end')
  const res = { pane: pv, minutes: Math.round((samples[samples.length - 1].t - samples[0].t) / 600) / 100, first: samples[0], last: samples[samples.length - 1], max_heap_mb: Math.max(...samples.map((x) => x.heapMB)), max_dom: Math.max(...samples.map((x) => x.dom)), samples, chip: await chip(s.page), after: await stuck(s.page) }
  await s.context.close()
  return res
}

const run: Record<string, () => Promise<any>> = { auth, basic, fanout8: () => fanout(8), fanout32: () => fanout(32), blip, reload, subagents, soak }
const guard = setTimeout(() => {
  result.deadline_hit = true
  save()
  process.exit(3)
}, Math.max(0, DEADLINE - Date.now()))
let failed = false
try {
  await seed()
  step('watched thread ' + watched)
  for (const p of PHASES) {
    scenario = p
    step('start')
    const t = Date.now()
    const confound = () => {
      const end = Date.now()
      const hit = restarts.filter((r) => r.down_ms <= end && (r.up_ms ?? end) >= t)
      return { confounded: hit.length > 0, ...(hit.length ? { restarts: hit.map(({ node, down_at, up_at, reason }) => ({ node, down_at, up_at, reason })) } : {}) }
    }
    try {
      const out = await run[p]()
      const c = confound()
      const bad = (out.failures?.length ?? 0) > 0 || c.confounded
      if (bad) failed = true
      result.phases[p] = { ok: !bad, ...out, ...c, ms: Date.now() - t }
    } catch (e: any) {
      failed = true
      result.phases[p] = { ok: false, error: String(e?.stack ?? e).slice(0, 1500), ms: Date.now() - t, ...confound(), evidence: await evidence(p).catch(() => []) }
    }
    await closeAll([...opened])
    for (const [k, v] of frames) frames.delete(k), ((result.frames ??= {})[p + ':' + k] = v)
    step(JSON.stringify(result.phases[p]).slice(0, 1500))
    save()
  }
} finally {
  clearTimeout(guard)
  polling = false
  await poller
  result.node_restarts = restarts.map(({ node, down_at, up_at, reason }) => ({ node, down_at, up_at, reason }))
  await browser.close()
  wss.close()
  result.ended = new Date().toISOString()
  save()
}
process.exit(failed ? 1 : 0)
