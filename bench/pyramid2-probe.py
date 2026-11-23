#!/usr/bin/env python3
"""Minimal repros against a live pyramid2 over its public HTTP API (stdlib only).

reuse:   one thread, K sequential e2e:swarm C turns; counts subagent rows and tool.updated frames each turn's
         live SSE window carries. Expect C rows per turn; re-emission shows C*n rows on turn n. Also counts
         /v1/board/events per turn.
spread:  N threads, one turn each; per-partition appended-records deltas from every node's /metrics.
active:  pyramid2_turns_active scraped from every node every 0.5 s through the whole run.
sse:     continuous stream vs a fresh full replay vs a resume from a mid-run cursor (?after=, and Last-Event-ID).

Usage: PYRAMID2_TOKEN_FILE=... pyramid2-probe.py --nodes http://127.0.0.1:18480,... --out DIR
"""
import argparse, json, os, re, threading, time, urllib.parse, urllib.request, uuid

ENDED = ("succeeded", "failed", "cancelled", "interrupted", "errored")


def http(method, url, token, body=None, timeout=30, headers=None):
    data = None if body is None else json.dumps(body).encode()
    h = {"authorization": "Bearer " + token, "content-type": "application/json"}
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw[:300].decode(errors="replace")


def ops(node):
    u = urllib.parse.urlparse(node)
    return "%s://%s:%d" % (u.scheme, u.hostname, u.port + 2)


def scrape(node):
    try:
        with urllib.request.urlopen(ops(node) + "/metrics", timeout=5) as r:
            text = r.read().decode()
    except OSError as e:
        return {"error": str(e)}
    out = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        m = re.match(r"^(\S+?)(\{[^}]*\})?\s+(\S+)$", line)
        if m and (m.group(1).startswith("pyramid2_turns") or m.group(1).startswith("pyramid2_partition_appended_records")):
            out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
    return out


class Stream:
    """One SSE connection; records (t, id, event, data) until stop() or idle_s with nothing."""

    def __init__(self, node, token, thread, after=None, last_event_id=None, idle_s=None, snapshot=False):
        self.frames, self.comments, self.error, self.done = [], 0, None, threading.Event()
        self.cond = threading.Condition()
        q = {}
        if after:
            q["after"] = after
        if snapshot:
            q["snapshot"] = "true"
        url = "%s/v1/threads/%s/events%s" % (node, thread, ("?" + urllib.parse.urlencode(q)) if q else "")
        h = {"authorization": "Bearer " + token, "accept": "text/event-stream"}
        if last_event_id:
            h["last-event-id"] = last_event_id
        self.req, self.idle_s, self.stopped, self.resp, self.status = urllib.request.Request(url, headers=h), idle_s, False, None, None
        self.t = threading.Thread(target=self.run, daemon=True)
        self.t.start()

    def run(self):
        try:
            with urllib.request.urlopen(self.req, timeout=self.idle_s or 60) as r:
                self.resp, self.status = r, r.status
                cur, ev, data = None, None, []
                for raw in r:
                    if self.stopped:
                        break
                    line = raw.decode().rstrip("\r\n")
                    if line.startswith(":"):
                        self.comments += 1
                    elif line.startswith("id:"):
                        cur = line[3:].strip()
                    elif line.startswith("event:"):
                        ev = line[6:].strip()
                    elif line.startswith("data:"):
                        data.append(line[5:].lstrip())
                    elif not line and data:
                        try:
                            body = json.loads("\n".join(data))
                        except ValueError:
                            body = {"raw": "\n".join(data)}
                        with self.cond:
                            self.frames.append((time.monotonic(), cur, ev, body))
                            self.cond.notify_all()
                        cur, ev, data = None, None, []
        except urllib.error.HTTPError as e:
            self.status, self.error = e.code, "HTTP %d" % e.code
        except Exception as e:  # an idle timeout is the normal end of a replay
            self.error = type(e).__name__ + ": " + str(e)[:200]
        self.done.set()
        with self.cond:
            self.cond.notify_all()

    def stop(self):
        self.stopped = True
        try:
            self.resp.close()
        except Exception:
            pass


def key(f):
    return (f[1], json.dumps(f[3], sort_keys=True))


def tool_rows(frames):
    rows, n = {}, 0
    for _, _, _, body in frames:
        if body.get("type") != "tool.updated":
            continue
        d = body.get("data") or {}
        if d.get("name") == "subagent":
            n += 1
            rows.setdefault(str(d.get("id")), []).append(d.get("status"))
    return n, rows


def turn_status(frames):
    st = {}
    for _, _, _, body in frames:
        d = body.get("data") or {}
        if body.get("type") == "turn.updated" and d.get("id"):
            st[d["id"]] = d.get("status")
    return st


def tnum(t):
    tail = str(t).rsplit(".", 1)[-1]
    return int(tail) if tail.isdigit() else 0


def board_last(node, token):
    after, last = 0, 0
    while True:
        s, b = http("GET", node + "/v1/board/events?after=%d&limit=500" % after, token)
        if s != 200 or not isinstance(b, dict) or not b.get("events"):
            return last
        last = b.get("last") or last
        after = last


def board_count(node, token, after):
    n, kinds, cur = 0, {}, after
    while True:
        s, b = http("GET", node + "/v1/board/events?after=%d&limit=500" % cur, token)
        if s != 200 or not isinstance(b, dict) or not b.get("events"):
            return n, kinds
        for e in b["events"]:
            n += 1
            k = (e.get("change") or {}).get("kind")
            kinds[k] = kinds.get(k, 0) + 1
        cur = b.get("last") or cur


def wait_turn(stream, start, timeout, known=()):
    """Wait for a turn not in known to end in frames[start:]; returns (turn_id, status)."""
    deadline = time.monotonic() + timeout
    with stream.cond:
        while True:
            st = turn_status(stream.frames[start:])
            fin = sorted(((t, s) for t, s in st.items() if s in ENDED and t not in known), key=lambda x: tnum(x[0]))
            if fin:
                return fin[-1]
            left = deadline - time.monotonic()
            if left <= 0 or stream.done.is_set():
                return (None, None)
            stream.cond.wait(min(left, 1.0))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--turns", type=int, default=5)
    ap.add_argument("--children", type=int, default=3)
    ap.add_argument("--spread", type=int, default=16)
    ap.add_argument("--wait-s", type=float, default=90)
    a = ap.parse_args()
    token = open(os.environ["PYRAMID2_TOKEN_FILE"]).read().strip()
    nodes = a.nodes.split(",")
    node = nodes[0]
    os.makedirs(a.out, exist_ok=True)
    res = {"nodes": nodes, "t0": time.time()}

    active, stop = [], threading.Event()

    def sampler():
        while not stop.is_set():
            row = {"t": round(time.time(), 2)}
            for n in nodes:
                m = scrape(n)
                row[n] = m.get("pyramid2_turns_active", m.get("error"))
            active.append(row)
            stop.wait(0.5)

    threading.Thread(target=sampler, daemon=True).start()
    before = {n: scrape(n) for n in nodes}

    def dump_active():
        vals = [v for row in active for k, v in row.items() if k != "t" and isinstance(v, (int, float))]
        res["active"] = {"samples": len(active), "min": min(vals) if vals else None, "max": max(vals) if vals else None,
                         "negative_or_wrapped": [row for row in active if any(isinstance(v, (int, float)) and (v < 0 or v > 1e12) for k, v in row.items() if k != "t")][:10],
                         "missing": sum(1 for row in active for k, v in row.items() if k != "t" and v is None),
                         "before": {n: {k: v for k, v in m.items() if not k.startswith("pyramid2_partition")} for n, m in before.items()}}
        with open(os.path.join(a.out, "active.jsonl"), "w") as f:
            for row in active:
                f.write(json.dumps(row) + "\n")

    def save():
        with open(os.path.join(a.out, "probe.json"), "w") as f:
            json.dump(res, f, indent=1)

    # ---- reuse: one thread, K turns
    s, b = http("POST", node + "/v1/threads", token, {"request_id": str(uuid.uuid4())})
    if s not in (200, 201):
        res["reuse"] = {"create": s, "body": b}
    else:
        thread = b["thread"]["id"]
        live = Stream(node, token, thread)
        time.sleep(0.5)
        turns, mid_cursor, known = [], None, set()
        for k in range(a.turns):
            with live.cond:
                start = len(live.frames)
            board0 = board_last(node, token)
            t0 = time.monotonic()
            s, rb = http("POST", node + "/v1/threads/%s/turns" % thread, token,
                         {"turn_id": str(uuid.uuid4()), "input": "e2e:swarm %d probe-reuse-%d" % (a.children, k)})
            tid, status = wait_turn(live, start, a.wait_s, known)
            known.add(tid)
            time.sleep(1.5)
            with live.cond:
                window = live.frames[start:]
            nframes, rows = tool_rows(window)
            bn, bk = board_count(node, token, board0)
            prefixes = {}
            for r in rows:
                p = r.rsplit(":", 1)[0]
                prefixes[p] = prefixes.get(p, 0) + 1
            types = {}
            for f in window:
                types[f[3].get("type")] = types.get(f[3].get("type"), 0) + 1
            turns.append({"k": k, "post": s, "post_body": rb if s not in (200, 201, 202) else None,
                          "turn_id": tid, "status": status, "ms": round((time.monotonic() - t0) * 1000),
                          "frames": len(window), "types": types, "subagent_frames": nframes, "subagent_rows": len(rows),
                          "rows_by_prefix": prefixes, "row_ids": sorted(rows)[:40],
                          "board_events": bn, "board_kinds": bk})
            res["reuse"] = {"thread": thread, "turns": turns}
            save()
            if k == a.turns // 2 - 1:
                with live.cond:
                    ids = [f[1] for f in live.frames if f[1]]
                mid_cursor = ids[-1] if ids else None
        time.sleep(2)
        live.stop()
        full = Stream(node, token, thread, idle_s=6)
        rs_after = Stream(nodes[1 % len(nodes)], token, thread, after=mid_cursor, idle_s=6) if mid_cursor else None
        rs_lei = Stream(nodes[2 % len(nodes)], token, thread, last_event_id=mid_cursor, idle_s=6) if mid_cursor else None
        for st in (full, rs_after, rs_lei):
            if st:
                st.done.wait(60)
        lv = [key(f) for f in live.frames if f[1]]
        fl = [key(f) for f in full.frames if f[1]]
        ids = [f[1] for f in live.frames if f[1]]
        cut = ids.index(mid_cursor) + 1 if mid_cursor in ids else None
        exp = lv[cut:] if cut is not None else None

        def cmp(got, want):
            if want is None or got is None:
                return None
            return {"got": len(got), "want": len(want), "equal": got == want,
                    "first_diff": next((i for i, (x, y) in enumerate(zip(got, want)) if x != y), None),
                    "dup_ids": len(got) - len(set(x[0] for x in got))}

        hist_s, hist_b = http("GET", node + "/v1/threads/%s" % thread, token)
        res["reuse"].update({
            "live": {"frames": len(live.frames), "with_id": len(lv), "dup_ids": len(ids) - len(set(ids)),
                     "error": live.error, "first_ids": ids[:3], "last_ids": ids[-3:]},
            "history_vs_live": cmp(fl, lv), "history_error": full.error, "history_frames": len(full.frames),
            "mid_cursor": mid_cursor,
            "resume_after_vs_live": cmp([key(f) for f in rs_after.frames if f[1]], exp) if rs_after else None,
            "resume_last_event_id_vs_live": cmp([key(f) for f in rs_lei.frames if f[1]], exp) if rs_lei else None,
            "last_event_id_frames": len(rs_lei.frames) if rs_lei else None,
            "thread_get": hist_s,
            "thread_get_keys": sorted(hist_b.keys()) if isinstance(hist_b, dict) else str(hist_b)[:200],
        })
        for name, st in (("live", live), ("history", full), ("resume-after", rs_after), ("resume-lei", rs_lei)):
            if st:
                with open(os.path.join(a.out, "reuse-%s.jsonl" % name), "w") as f:
                    for fr in st.frames:
                        f.write(json.dumps({"t": fr[0], "id": fr[1], "ev": fr[2], "frame": fr[3]}) + "\n")
        with open(os.path.join(a.out, "thread.json"), "w") as f:
            json.dump(hist_b, f)
        save()

    # ---- spread: N threads, one turn each, per-partition deltas
    mid = {n: scrape(n) for n in nodes}
    threads, created_on = [], {}
    for i in range(a.spread):
        n = nodes[i % len(nodes)]
        s, b = http("POST", n + "/v1/threads", token, {"request_id": str(uuid.uuid4())})
        if s in (200, 201):
            threads.append(b["thread"]["id"])
            created_on[n] = created_on.get(n, 0) + 1
    streams = [Stream(node, token, t) for t in threads]
    time.sleep(0.5)
    for t in threads:
        http("POST", node + "/v1/threads/%s/turns" % t, token, {"turn_id": str(uuid.uuid4()), "input": "e2e:swarm 1 probe-spread"})
    ends = [wait_turn(st, 0, a.wait_s) for st in streams]
    for st in streams:
        st.stop()
    after = {n: scrape(n) for n in nodes}

    def parts(snap):
        out = {}
        for n, m in snap.items():
            for k, v in (m.items() if isinstance(m, dict) else []):
                mm = re.match(r'pyramid2_partition_appended_records_total\{partition="(\d+)"\}', k)
                if mm:
                    out.setdefault(n, {})[int(mm.group(1))] = v
        return out

    pm, pa = parts(mid), parts(after)
    deltas = {n: {p: pa[n].get(p, 0) - pm.get(n, {}).get(p, 0) for p in pa[n]} for n in pa}
    res["spread"] = {"threads": threads, "created_on": created_on, "ended": [e[1] for e in ends],
                     "partition_deltas": deltas,
                     "partitions_nonzero_per_node": {n: sorted(p for p, v in d.items() if v > 0) for n, d in deltas.items()}}
    stop.set()
    time.sleep(0.6)
    dump_active()
    res["active"]["after"] = {n: {k: v for k, v in m.items() if not k.startswith("pyramid2_partition")} for n, m in after.items()}
    save()
    print(json.dumps(res)[:20000])


if __name__ == "__main__":
    main()
