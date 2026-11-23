import argparse, json, os, re, socket, threading, time, urllib.error, urllib.parse, urllib.request, uuid

from http.client import HTTPConnection

ENDED_BADLY = ("failed", "cancelled", "interrupted", "errored")
PARTITIONED = re.compile(r"^pyramid2_(partition|node|turns|lease|board)")


def http(method, url, token, body=None, timeout=30):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method,
                                     headers={"authorization": "Bearer " + token, "content-type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload, status = response.read(), response.status
    except urllib.error.HTTPError as error:
        payload, status = error.read(), error.code
    try:
        return status, json.loads(payload or b"null")
    except ValueError:
        return status, payload.decode(errors="replace")


def is_timeout(error):
    reason = getattr(error, "reason", error)
    return isinstance(error, (socket.timeout, TimeoutError)) or isinstance(reason, (socket.timeout, TimeoutError)) \
        or "timed out" in str(error)


def pct(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[min(len(ordered) - 1, int(len(ordered) * p / 100))], 1)


def dist(values):
    return {"n": len(values), "p50": pct(values, 50), "p95": pct(values, 95), "p99": pct(values, 99),
            "max": round(max(values), 1) if values else None}


def scrape(ops):
    try:
        with urllib.request.urlopen(ops + "/metrics", timeout=10) as response:
            text = response.read().decode(errors="replace")
    except OSError as error:
        return {"error": str(error)}
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        key, _, value = line.rpartition(" ")
        if PARTITIONED.match(key.split("{", 1)[0]):
            try:
                out[key] = float(value)
            except ValueError:
                pass
    return out


GAUGES = re.compile(r"^pyramid2_(turns_active|lease_epoch|lease_phase)")


def delta(before, after):
    # Counters as after - before; gauges as [before, after] (a gauge delta such as turns_active 7 -> 0 is not
    # the gauge going negative).
    out = {}
    for node in after:
        b, a = before.get(node, {}), after[node]
        out[node] = {k: ([b.get(k), v] if GAUGES.match(k.split("{", 1)[0]) else round(v - b.get(k, 0), 3))
                     for k, v in a.items() if k != "error" and v != b.get(k, 0)}
        if "error" in a:
            out[node]["error"] = a["error"]
    return out


def sse_lines(node, path, token, timeout, holder=None, headers=None):
    """Yield (status, line) from one SSE GET; holder["sock"] lets another thread shut the socket down."""
    u = urllib.parse.urlparse(node)
    conn = HTTPConnection(u.hostname, u.port, timeout=timeout)
    conn.connect()
    if holder is not None:
        holder["sock"] = conn.sock
    h = {"authorization": "Bearer " + token, "accept": "text/event-stream"}
    h.update(headers or {})
    try:
        conn.request("GET", path, headers=h)
        response = conn.getresponse()
        if response.status != 200:
            yield response.status, None
            return
        while True:
            raw = response.readline()  # decodes chunked transfer; response.fp would not
            if not raw:
                return
            yield 200, raw.decode(errors="replace").rstrip("\r\n")
    finally:
        conn.close()


class Run:
    def __init__(self, t0_holder):
        self.lock = threading.Lock()
        self.t0 = t0_holder
        self.errors, self.samples = {}, []
        self.ack, self.claim, self.root, self.spawn_to_child, self.child_run, self.child_total = [], [], [], [], [], []
        self.threads_by_node, self.turns_done, self.children_done = {}, 0, 0
        self.ends = []
        self.http5xx, self.retries, self.sse = {}, {}, {"opened": 0, "dropped": 0, "drop_kinds": {}, "http": {}}
        self.minutes = {}  # minute -> {"done": n, "claim": [], "root": [], "errors": n}
        self.unsettled = []  # (node, thread, request turn_id, sent_rel)

    def rel(self, t=None):
        return (t or time.monotonic()) - self.t0[0] if self.t0[0] else 0.0

    def minute(self, t):
        return self.minutes.setdefault(max(0, int(self.rel(t) // 60)), {"done": 0, "claim": [], "root": [], "errors": 0})

    def error(self, kind, detail):
        with self.lock:
            self.errors[kind] = self.errors.get(kind, 0) + 1
            self.minute(None)["errors"] += 1
            if len(self.samples) < 20:
                self.samples.append("%s: %s" % (kind, str(detail)[:300]))

    def count(self, table, kind):
        with self.lock:
            table[kind] = table.get(kind, 0) + 1


class Swarm:
    def __init__(self, run, index, nodes, args):
        self.run, self.index, self.args = run, index, args
        self.order = nodes[index % len(nodes):] + nodes[:index % len(nodes)]
        self.cond = threading.Condition()
        self.frames, self.cursor, self.thread, self.turn = [], None, None, None
        self.gen, self.holder = 0, {}
        self.dump = open(os.path.join(args.dump, "swarm-%d.jsonl" % index), "w") if args.dump else None

    def call(self, method, path, body=None):
        last = None
        for node in self.order:
            try:
                status, reply = http(method, node + "/v1" + path, self.args.token, body)
            except OSError as error:
                kind = "timeout" if is_timeout(error) else "connect"
                self.run.count(self.run.retries, "%s %s" % (method, kind))
                last = (kind, error)
                continue
            if status >= 500:
                self.run.count(self.run.http5xx, "%s %s %d" % (method, re.sub(r"[0-9a-f-]{36}", "{id}", path), status))
                last = ("http_%d" % status, reply)
                continue
            return node, status, reply
        raise ConnectionError(last)

    def create(self):
        try:
            node, status, body = self.call("POST", "/threads", {"request_id": str(uuid.uuid4())})
        except ConnectionError as error:
            self.run.error("create_unreachable", error)
            return None
        if status not in (200, 201) or not isinstance(body, dict):
            self.run.error("create_http_%d" % status, body)
            return None
        with self.run.lock:
            self.run.threads_by_node[node] = self.run.threads_by_node.get(node, 0) + 1
        return body["thread"]["id"]

    def attach(self, thread):
        with self.cond:
            self.gen += 1
            self.frames, self.cursor, self.thread = [], None, thread
            gen, self.holder = self.gen, {}
        threading.Thread(target=self.follow, args=(gen, self.holder), daemon=True).start()

    def detach(self):
        with self.cond:
            self.gen += 1
            holder = self.holder
        sock = holder.get("sock")
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def live(self, gen):
        return gen == self.gen

    def follow(self, gen, holder):
        attempt = 0
        while self.live(gen):
            node = self.order[attempt % len(self.order)]
            attempt += 1
            query = "?" + urllib.parse.urlencode({"after": self.cursor}) if self.cursor else ""
            kind = "eof"
            try:
                cursor, data, opened = None, [], False
                for status, line in sse_lines(node, "/v1/threads/%s/events%s" % (self.thread, query), self.args.token,
                                              self.args.sse_timeout, holder):
                    if not self.live(gen):
                        break
                    if status != 200:
                        self.run.count(self.run.sse["http"], str(status))
                        kind = "http_%d" % status
                        break
                    if not opened:
                        opened = True
                        with self.run.lock:
                            self.run.sse["opened"] += 1
                    if line.startswith("id:"):
                        cursor = line[3:].strip()
                    elif line.startswith("data:"):
                        data.append(line[5:].lstrip())
                    elif not line and data:
                        self.apply(gen, cursor, json.loads("\n".join(data)))
                        cursor, data = None, []
            except (OSError, ValueError) as error:
                kind = "timeout" if is_timeout(error) else type(error).__name__
            if not self.live(gen):
                return
            with self.run.lock:
                self.run.sse["dropped"] += 1
                self.run.sse["drop_kinds"][kind] = self.run.sse["drop_kinds"].get(kind, 0) + 1
            time.sleep(min(1.0, 0.05 * attempt))

    def apply(self, gen, cursor, frame):
        now = time.monotonic()
        with self.cond:
            if gen != self.gen:
                return
            self.frames.append((now, frame))
            if cursor:
                self.cursor = cursor
            self.cond.notify_all()
        if self.dump:
            self.dump.write(json.dumps({"t": now, "frame": frame}) + "\n")
            self.dump.flush()

    def root_end(self, start=0):
        status, answered = {}, set()
        for _, frame in self.frames[start:]:
            body = frame.get("data") or {}
            if frame.get("type") == "turn.updated" and body.get("id"):
                status[body["id"]] = body.get("status")
            elif frame.get("type") == "message.updated" and body.get("role") == "assistant" and body.get("turn_id"):
                answered.add(body["turn_id"])
        turn = next(iter(status), None)
        state = status.get(turn)
        if state in ENDED_BADLY or state == "succeeded" and turn in answered:
            return state
        return None

    def go(self, start):
        if not self.args.fresh:
            thread = self.create()
            if not thread:
                return
            self.attach(thread)
        start.wait()
        deadline = time.monotonic() + self.args.duration
        while True:
            if self.args.fresh:
                thread = self.create()
                if thread:
                    self.attach(thread)
                    self.once()
                    self.detach()
                else:
                    time.sleep(0.5)
            else:
                self.once()
            if time.monotonic() >= deadline:
                break
        if not self.args.fresh:
            self.detach()

    def once(self):
        self.turn = str(uuid.uuid4())  # request id; the server names the turn <thread>.<n>
        prompt = "e2e:swarm %d swarm-%s-%d" % (self.args.children, self.args.tag, self.index)
        with self.cond:
            first = len(self.frames)
        sent = time.monotonic()
        try:
            node, status, body = self.call("POST", "/threads/%s/turns" % self.thread, {"turn_id": self.turn, "input": prompt})
        except ConnectionError as error:
            self.run.error("send_unreachable", error)
            return
        if status not in (200, 201, 202):
            self.run.error("send_http_%d" % status, body)
            return
        acked = time.monotonic()
        with self.cond:
            self.cond.wait_for(lambda: self.root_end(first) or time.monotonic() > acked + self.args.wait_s,
                               timeout=self.args.wait_s + 1)
            ended = self.root_end(first)
            frames = self.frames[first:]
        if ended is None:
            with self.run.lock:
                self.run.unsettled.append((node, self.thread, self.turn, round(self.run.rel(sent), 1)))
            self.run.error("turn_timeout", self.thread)
            return
        if ended != "succeeded":
            self.run.error("turn_" + ended, self.thread)
            return
        self.measure(sent, acked, frames)

    def measure(self, sent, acked, frames):
        # Server times (frame data "at", ms) where present; client receive times otherwise.
        running, posted, seen, settled = None, [], {}, {}
        for t, frame in frames:
            body = frame.get("data") or {}
            at = body.get("at") / 1000.0 if isinstance(body.get("at"), (int, float)) else None
            if frame.get("type") == "turn.updated" and body.get("status") == "running":
                running = running or t
            if frame.get("type") != "tool.updated":
                continue
            name, row, state = body.get("name"), str(body.get("id") or ""), body.get("status")
            if name == "spawn_agent" and state == "running":
                posted.append(at or t)
            if name == "subagent":
                if state == "running":
                    seen.setdefault(row, at or t)
                elif state == "succeeded":
                    settled.setdefault(row, at or t)
        ended = frames[-1][0]
        order = sorted(seen, key=lambda r: int(r.rsplit(":", 1)[1]) if r.rsplit(":", 1)[1].isdigit() else 0)
        with self.run.lock:
            self.run.ack.append((acked - sent) * 1000)
            bucket = self.run.minute(sent)
            if running:
                self.run.claim.append((running - sent) * 1000)
                bucket["claim"].append((running - sent) * 1000)
            self.run.root.append((ended - sent) * 1000)
            bucket["root"].append((ended - sent) * 1000)
            self.run.minute(ended)["done"] += 1
            for i, row in enumerate(order):
                if i < len(posted):
                    self.run.spawn_to_child.append(max(0.0, seen[row] - posted[i]) * 1000)
                if row in settled:
                    self.run.child_run.append((settled[row] - seen[row]) * 1000)
            self.run.turns_done += 1
            self.run.children_done += len(settled)
            self.run.ends.append(ended)


def recheck(node, thread, token, timeout=5.0):
    """Replay a thread from 0 and return its turns' last statuses (the stream idles out at timeout)."""
    status, data = {}, []
    try:
        for code, line in sse_lines(node, "/v1/threads/%s/events" % thread, token, timeout):
            if code != 200:
                return {"http": code}
            status.setdefault("_reached", True)
            if line.startswith("data:"):
                data.append(line[5:].lstrip())
            elif not line and data:
                frame = json.loads("\n".join(data))
                data = []
                body = frame.get("data") or {}
                if frame.get("type") == "turn.updated" and body.get("id"):
                    status[body["id"]] = body.get("status")
    except (OSError, ValueError) as error:
        if "_reached" not in status:
            return {"unreachable": type(error).__name__}
    status.pop("_reached", None)
    return status


def board_poll(node, token, stop, seen):
    after = 0
    try:
        status, body = http("GET", node + "/v1/board/events?after=0&limit=500", token)
        while status == 200 and isinstance(body, dict) and body.get("events"):
            after = body.get("last") or after
            status, body = http("GET", node + "/v1/board/events?after=%d&limit=500" % after, token)
    except OSError as error:
        seen["start_error"] = str(error)[:200]
    seen["start_seq"] = after
    while not stop.is_set():
        try:
            status, body = http("GET", node + "/v1/board/events?after=%d&limit=500" % after, token)
        except OSError:
            status, body = 0, None
        if status != 200 or not isinstance(body, dict):
            seen["errors"] = seen.get("errors", 0) + 1
            time.sleep(0.5)
            continue
        for event in body.get("events", []):
            kind = (event.get("change") or {}).get("kind")
            seen.setdefault("kinds", {})[kind] = seen.get("kinds", {}).get(kind, 0) + 1
        after = body.get("last") or after
        if not body.get("events"):
            time.sleep(0.25)
    seen["end_seq"] = after


def active_sampler(ops, stop, series, t0):
    # Sum of pyramid2_turns_active over nodes, once a second; None while any node fails to answer.
    while not stop.is_set():
        total, per = 0, []
        for o in ops.values():
            v = scrape(o).get("pyramid2_turns_active")
            per.append(v)
            total = None if v is None or total is None else total + v
        now = time.monotonic()
        series.append((round(now - t0[0], 1) if t0[0] else None, now, total, per))
        stop.wait(1.0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", required=True)
    parser.add_argument("--ops-offset", type=int, default=2)
    parser.add_argument("--swarms", "--slots", type=int, required=True, dest="swarms")
    parser.add_argument("--children", type=int, default=8)
    parser.add_argument("--wait-s", type=float, default=120)
    parser.add_argument("--duration", type=float, default=0, help="keep each slot running swarms for this many seconds")
    parser.add_argument("--fresh", action="store_true", help="a new thread (and SSE stream) for every swarm")
    parser.add_argument("--sse-timeout", type=float, default=30, help="SSE socket idle timeout; counts as a drop")
    parser.add_argument("--drain-s", type=float, default=0,
                        help="after the last slot stops, sample turns_active up to this long for the drain to idle")
    parser.add_argument("--tag", default=uuid.uuid4().hex[:8])
    parser.add_argument("--dump")
    args = parser.parse_args()
    with open(os.environ["PYRAMID2_TOKEN_FILE"]) as f:
        args.token = f.read().strip()
    nodes = args.nodes.split(",")
    ops = {n: re.sub(r":(\d+)$", lambda m: ":%d" % (int(m.group(1)) + args.ops_offset), n) for n in nodes}
    t0_holder = [None]
    run, start, stop, board = Run(t0_holder), threading.Event(), threading.Event(), {}
    sampling, series = threading.Event(), []
    threading.Thread(target=active_sampler, args=(ops, sampling, series, t0_holder), daemon=True).start()
    swarms = [Swarm(run, i, nodes, args) for i in range(args.swarms)]
    workers = [threading.Thread(target=s.go, args=(start,), daemon=True) for s in swarms]
    for w in workers:
        w.start()
    poller = threading.Thread(target=board_poll, args=(nodes[0], args.token, stop, board), daemon=True)
    poller.start()
    time.sleep(5.0)
    baseline = [s[2] for s in series if s[2] is not None]
    before = {n: scrape(o) for n, o in ops.items()}
    t0 = time.monotonic()
    t0_holder[0] = t0
    start.set()
    for w in workers:
        w.join(max(0, t0 + args.duration + args.wait_s + 60 - time.monotonic()))
    stuck_slots = sum(1 for w in workers if w.is_alive())
    stopped = time.monotonic()
    elapsed = (max(run.ends) if run.ends else stopped) - t0
    # Drain: the first sample at or after the last slot stopped where turns_active (all nodes) is back at or under
    # the pre-run baseline's max (the box's own p2live-traffic keeps it above 0).
    drain = None
    if args.drain_s:
        floor = max(baseline) if baseline else 0
        end = stopped + args.drain_s
        while time.monotonic() < end:
            last = series[-1] if series else None
            if last and last[1] >= stopped and last[2] is not None and last[2] <= floor:
                drain = {"baseline_max": floor, "baseline_p50": pct(baseline, 50),
                         "idle_after_last_slot_s": round(last[1] - stopped, 1),
                         "idle_after_load_end_s": round(last[1] - (t0 + args.duration), 1)}
                break
            time.sleep(0.5)
        if drain is None:
            drain = {"baseline_max": floor, "not_idle_within_s": args.drain_s}
    time.sleep(1.0)
    stop.set()
    poller.join(5)
    after = {n: scrape(o) for n, o in ops.items()}
    sampling.set()
    never = []
    for node, thread, turn, sent_rel in run.unsettled:
        st = recheck(node, thread, args.token)
        never.append({"thread": thread, "sent_s": sent_rel, "statuses": st})
    ended_states = ("succeeded",) + ENDED_BADLY
    unknown = sum(1 for n in never if "unreachable" in n["statuses"] or "http" in n["statuses"])
    still = sum(1 for n in never if "unreachable" not in n["statuses"] and "http" not in n["statuses"]
                and not any(v in ended_states for v in n["statuses"].values()))
    turns = run.turns_done * (1 + args.children)
    minutes = []
    for m in sorted(run.minutes):
        b = run.minutes[m]
        act = [s[2] for s in series if s[0] is not None and s[0] >= 0 and int(s[0] // 60) == m and s[2] is not None]
        minutes.append({"min": m, "swarms_done": b["done"], "turns_per_s": round(b["done"] * (1 + args.children) / 60.0, 1),
                        "claim_p50": pct(b["claim"], 50), "claim_p99": pct(b["claim"], 99),
                        "root_p50": pct(b["root"], 50), "root_p99": pct(b["root"], 99), "errors": b["errors"],
                        "active_max": max(act) if act else None})
    print(json.dumps({
        "tag": args.tag, "swarms": args.swarms, "children": args.children, "nodes": nodes, "fresh": args.fresh,
        "duration": args.duration, "elapsed_s": round(elapsed, 2), "swarms_done": run.turns_done,
        "children_settled": run.children_done,
        "turns_per_s": round(turns / elapsed, 2) if elapsed > 0 else None,
        "swarms_per_s": round(run.turns_done / elapsed, 3) if elapsed > 0 else None,
        "ack_ms": dist(run.ack), "root_claim_ms": dist(run.claim), "spawn_to_child_ms": dist(run.spawn_to_child),
        "child_run_ms": dist(run.child_run), "root_turn_ms": dist(run.root),
        "threads_by_node": run.threads_by_node, "errors": run.errors, "error_samples": run.samples,
        "http_5xx": run.http5xx, "retries": run.retries, "sse": run.sse, "stuck_slots": stuck_slots,
        "unsettled": {"timed_out": len(run.unsettled), "never_settled_after_run": still, "recheck_unreachable": unknown, "rechecked": never[:20]},
        "client_drain_s": round(max(run.ends) - (t0 + args.duration), 2) if run.ends else None,
        "drain": drain, "active_baseline": {"n": len(baseline), "max": max(baseline) if baseline else None},
        "per_minute": minutes, "board": board, "metrics_delta": delta(before, after)}), flush=True)
    if args.dump:
        with open(os.path.join(args.dump, "active.jsonl"), "w") as f:
            for s in series:
                f.write(json.dumps({"rel": s[0], "total": s[2], "per": s[3]}) + "\n")


if __name__ == "__main__":
    main()
