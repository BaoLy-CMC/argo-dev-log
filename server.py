#!/usr/bin/env python3
"""Local log viewer backend: streams `argocd app logs` for many dev services over SSE.

No Loki, no docker. The only data tap is `argocd app logs` (ArgoCD read-only SSO token).
Token lives in memory only (set via POST /api/token), never written to disk, never logged.
A short buffer window re-orders lines by their own UTC timestamp so the merged stream is
roughly time-sorted across services.

ponytail: single-process, in-memory heap with a fixed reorder window. Good for a handful of
services; for many high-throughput streams, widen WINDOW or shard per service.
"""
import calendar
import heapq
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get("DEVLOGS_PORT", "8900"))
# macOS puts argocd in /opt/homebrew/bin, Linux installs usually in ~/.local/bin or /usr/local/bin
ARGOCD = (os.environ.get("ARGOCD_BIN") or shutil.which("argocd")
          or os.path.expanduser("~/.local/bin/argocd"))
DEFAULT_SERVER = os.environ.get("ARGOCD_SERVER", "argocd.example.com")
EXTRA_SERVERS = os.environ.get("ARGOCD_SERVERS", "")  # comma-separated, optional
WINDOW = 1.2  # seconds: reorder buffer; also the max added latency
IDLE_FLUSH = 0.4  # seconds of app silence that closes a multi-line event (adds to WINDOW)
MAX_EVENT_LINES = 200  # a runaway dump stays one event, but stops growing
HERE = os.path.dirname(os.path.abspath(__file__))

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
LEVEL_RE = re.compile(r"\b(ERROR|WARN|INFO|DEBUG|TRACE)\b")
# A continuation of the previous line: indented (stack frame, config dump), a stack-trace keyword,
# or an exception head at column 0 (`org.spring...HttpClientErrorException$Unauthorized: 401 ...`) —
# that one needs a package-qualified class name, so a Kong JSON line or `127.0.0.6 - -` is excluded.
CONT_RE = re.compile(r"^([ \t]|Caused by:|Suppressed:|(?:[a-z][\w]*\.)+[A-Z][\w$]*(?::|$))")

DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{2,252}(:\d{1,5})?$")
DOMAINS_FILE = os.path.join(HERE, ".devlogs-domains.json")

_tokens = {}  # domain -> token: memory only, one token per ArgoCD domain
_conf = {"domains": [], "active": ""}


def _load_domains():
    """Domain list from env + the saved file. The file holds hostnames only, never a token."""
    saved = []
    try:
        with open(DOMAINS_FILE) as f:
            saved = json.load(f) or []
    except (OSError, ValueError):
        pass
    out = []
    for d in [DEFAULT_SERVER, *EXTRA_SERVERS.split(","), *saved]:
        d = str(d).strip()
        if d and DOMAIN_RE.match(d) and d not in out:
            out.append(d)
    _conf["domains"] = out or ["argocd.example.com"]
    _conf["active"] = _conf["domains"][0]


def _save_domains():
    try:
        with open(DOMAINS_FILE, "w") as f:
            json.dump(_conf["domains"], f, indent=1)  # hostnames only — tokens stay in memory
    except OSError:
        pass


_load_domains()  # at import: the app list and every handler need an active domain


def active_token() -> str:
    return _tokens.get(_conf["active"], "")


def _env():
    e = dict(os.environ)
    e["ARGOCD_SERVER"] = _conf["active"]
    e["ARGOCD_OPTS"] = "--grpc-web"
    e["ARGOCD_AUTH_TOKEN"] = active_token()
    return e


def parse_ts(line: str) -> float:
    m = TS_RE.match(line)
    if not m:
        return time.time()
    try:
        t = time.strptime(m.group(1)[:19], "%Y-%m-%d %H:%M:%S")
        ms = int(m.group(1)[20:23])
        return calendar.timegm(t) + ms / 1000.0  # log ts is UTC
    except ValueError:
        return time.time()


def list_apps() -> list:
    """[{name, health, sync}] — one argocd call serves both the picker and the health dots."""
    out = subprocess.run([ARGOCD, "app", "list", "-o", "json"],
                         env=_env(), capture_output=True, text=True, timeout=30)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip() or "argocd app list failed")
    data = json.loads(out.stdout or "[]") or []
    if isinstance(data, dict):
        data = data.get("items") or []
    apps = []
    for a in data:
        st = a.get("status") or {}
        hist = st.get("history") or []
        apps.append({
            "name": (a.get("metadata") or {}).get("name", ""),
            "health": (st.get("health") or {}).get("status") or "Unknown",
            "sync": (st.get("sync") or {}).get("status") or "Unknown",
            # when the last sync actually landed; reconciledAt is the fallback for apps that
            # have never run a sync operation through this ArgoCD instance
            "syncedAt": ((st.get("operationState") or {}).get("finishedAt")
                         or (hist[-1].get("deployedAt") if hist else None)
                         or st.get("reconciledAt") or ""),
        })
    return sorted([a for a in apps if a["name"]], key=lambda a: a["name"])


APP_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{0,62}$")  # goes into a URL path — keep it a plain name
POD_TTL = 20  # seconds: the sidebar re-asks every 30s, so a short cache still spares ArgoCD a round
MAX_POD_APPS = 80
_pods = {}  # (domain, app) -> (fetched_at, {"ready": n, "total": n})


def _pod_count(app: str) -> dict:
    """ready/total pods from the app's resource tree. The CLI has no resource-tree command, so this
    is the one place that talks to the ArgoCD REST API directly."""
    domain = _conf["active"]
    hit = _pods.get((domain, app))
    if hit and time.time() - hit[0] < POD_TTL:
        return hit[1]
    req = urllib.request.Request(
        f"https://{domain}/api/v1/applications/{app}/resource-tree",
        headers={"Authorization": "Bearer " + active_token()})
    with urllib.request.urlopen(req, timeout=15) as r:
        tree = json.load(r)
    pods = [n for n in (tree.get("nodes") or []) if n.get("kind") == "Pod"]
    out = {"ready": sum(1 for n in pods if (n.get("health") or {}).get("status") == "Healthy"),
           "total": len(pods)}
    _pods[(domain, app)] = (time.time(), out)
    return out


def pod_counts(apps: list) -> dict:
    """Pod counts for the services the sidebar currently shows. Every requested app gets an answer,
    a count or an {"err"}: a badge that silently vanishes is indistinguishable from a bug."""
    apps = [a for a in dict.fromkeys(apps) if APP_NAME_RE.match(a)]
    over, apps = apps[MAX_POD_APPS:], apps[:MAX_POD_APPS]
    got = {}
    if apps:
        with ThreadPoolExecutor(max_workers=8) as ex:
            got = dict(ex.map(lambda a: (a, _safe_pod_count(a)), apps))
    got.update({a: {"err": f"over the {MAX_POD_APPS}-app batch limit"} for a in over})
    return got


def _safe_pod_count(app: str) -> dict:
    try:
        return _pod_count(app)
    except urllib.error.HTTPError as e:
        return {"err": f"HTTP {e.code} from resource-tree"}
    except (urllib.error.URLError, OSError, ValueError) as e:
        return {"err": type(e).__name__}


ACTIONS = {  # fixed argv per action: the app name is the only variable, and it is whitelisted below
    "refresh": lambda app: [ARGOCD, "app", "get", app, "--refresh", "-o", "name"],
    "sync": lambda app: [ARGOCD, "app", "sync", app, "--async"],
    "restart": lambda app: [ARGOCD, "app", "actions", "run", app, "restart",
                            "--kind", "Deployment", "--all"],
}
MAX_ACTION_APPS = 20  # a click should not be able to restart the whole cluster


def run_action(action: str, apps: list) -> list:
    """Run one whitelisted action per app. Apps are checked against the live app list so a
    crafted name cannot smuggle extra argocd flags into argv."""
    known = {a["name"] for a in list_apps()}
    results = []
    for app in apps:
        if app not in known:
            results.append({"app": app, "ok": False, "msg": "unknown app"})
            continue
        r = subprocess.run(ACTIONS[action](app), env=_env(),
                           capture_output=True, text=True, timeout=60)
        tail = (r.stdout + r.stderr).strip().splitlines()
        results.append({"app": app, "ok": r.returncode == 0,
                        "msg": tail[-1].strip() if tail else "done"})
    return results


class Stream:
    """Merge N `argocd app logs -f` subprocesses into one time-ordered SSE feed."""

    def __init__(self, apps):
        self.apps = apps
        self.heap = []
        self.lock = threading.Lock()
        self.alive = True
        self.procs = {}  # app -> its current subprocess (replaced, not stacked, on reconnect)
        self.open = {}  # app -> [ts, level, lines, last_seen]: the event still collecting lines
        self.recent = {}  # app -> deque of lines already shown, to spot a replayed tail
        self.resuming = set()  # apps whose stream just came back and may repeat itself

    def _emit(self, app, lvl, line):
        with self.lock:
            heapq.heappush(self.heap, (parse_ts(line), app, lvl, line))

    def _close(self, app):
        """Move the app's open event onto the heap. Caller holds the lock."""
        ev = self.open.pop(app, None)
        if ev:
            heapq.heappush(self.heap, (ev[0], app, ev[1], "\n".join(ev[2])))

    def _replayed(self, app, line):
        """--since-seconds still re-sends whatever straddles the boundary, so drop lines we
        already showed. Only right after a reconnect: a service can legitimately repeat a line."""
        with self.lock:
            recent = self.recent.setdefault(app, deque(maxlen=50))
            if app in self.resuming:
                if line in recent:
                    return True
                self.resuming.discard(app)
            recent.append(line)
            return False

    def _feed(self, app, line):
        """A line that looks like a continuation joins the previous one — a stack trace or a
        Kafka config dump is one event, not fifty INFO rows with invented timestamps.
        Looks-like matters: a Kong access log has no timestamp either, but starts at column 0,
        so it must stay a row of its own instead of gluing the whole stream together."""
        with self.lock:
            ev = self.open.get(app)
            if not CONT_RE.match(line) or ev is None:
                self._close(app)
                m = LEVEL_RE.search(line)
                self.open[app] = [parse_ts(line), m.group(1) if m else "INFO", [line], time.time()]
            else:
                ev[2].append(line)
                ev[3] = time.time()
                if len(ev[2]) >= MAX_EVENT_LINES:
                    self._close(app)

    def _tail(self, app):
        # the full app name goes on the wire; shortening names for display is the UI's job
        backoff, first, dropped_at, silent = 1, True, 0.0, True
        while self.alive:
            if first:
                window = ["--tail", "50"]
            else:
                # ask only for the gap the drop cost us — `--tail N` replays the last lines, which
                # on an idle service means re-printing the same old log after every timeout
                window = ["--since-seconds", str(max(1, min(60, int(time.time() - dropped_at) + 1)))]
            first = False
            cmd = [ARGOCD, "app", "logs", app, "--container", f"{app}-application", "-f", *window]
            p = subprocess.Popen(cmd, env=_env(), stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, text=True, bufsize=1)
            self.procs[app] = p
            for raw in p.stdout:
                if not self.alive:
                    return
                line = ANSI_RE.sub("", raw.rstrip("\n"))
                if not line:
                    continue
                # argocd CLI's own stream errors (Go JSON on stderr) — not an app log
                if line.startswith('{"level":') and ("rpc error" in line or "stream read failed" in line):
                    continue
                if self._replayed(app, line):
                    continue
                silent = False
                self._feed(app, line)
                backoff = 1
            p.wait()
            if not self.alive:
                return
            dropped_at = time.time()
            with self.lock:
                self.resuming.add(app)
            if not silent:  # an idle service reconnects forever; narrating every cycle is noise
                self._emit(app, "WARN", "— log stream dropped (idle/timeout), reconnecting —")
                silent = True
            time.sleep(backoff)
            backoff = min(backoff * 2, 15)

    def start(self):
        for a in self.apps:
            threading.Thread(target=self._tail, args=(a,), daemon=True).start()

    def drain(self):
        """Yield events whose ts is older than now-WINDOW, in timestamp order."""
        now = time.time()
        cutoff = now - WINDOW
        out = []
        with self.lock:
            for app in [a for a, ev in self.open.items() if now - ev[3] >= IDLE_FLUSH]:
                self._close(app)
            while self.heap and self.heap[0][0] <= cutoff:
                out.append(heapq.heappop(self.heap))
        for ts, app, lvl, line in out:
            yield {"app": app, "level": lvl, "ts": ts, "line": line}

    def stop(self):
        self.alive = False
        for p in list(self.procs.values()):
            try:
                p.terminate()
            except Exception:
                pass


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # silence access log (may contain app names)
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n) or b"{}")

    def _domains(self):
        return {"active": _conf["active"],
                "domains": [{"domain": d, "hasToken": bool(_tokens.get(d))} for d in _conf["domains"]]}

    def do_POST(self):
        if self.path == "/api/token":
            body = self._body()
            domain = (body.get("domain") or _conf["active"]).strip()
            if domain not in _conf["domains"]:
                self._send(400, {"ok": False, "error": "unknown domain"})
                return
            _tokens[domain] = (body.get("token") or "").strip()
            _conf["active"] = domain
            try:
                apps = list_apps()
                self._send(200, {"ok": True, "count": len(apps)})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"ok": False, "error": str(e)})
            return
        if self.path == "/api/domain":
            domain = (self._body().get("domain") or "").strip()
            if domain not in _conf["domains"]:
                self._send(400, {"error": "unknown domain"})
                return
            _conf["active"] = domain
            self._send(200, {"active": domain, "hasToken": bool(active_token())})
            return
        if self.path == "/api/domains":
            body = self._body()
            domain, action = (body.get("domain") or "").strip().lower(), body.get("action")
            if not DOMAIN_RE.match(domain):
                self._send(400, {"error": "not a hostname"})
                return
            if action == "add":
                if domain not in _conf["domains"]:
                    _conf["domains"].append(domain)
                    _save_domains()
            elif action == "remove":
                if len(_conf["domains"]) < 2:
                    self._send(400, {"error": "keep at least one domain"})
                    return
                if domain in _conf["domains"]:
                    _conf["domains"].remove(domain)
                    _tokens.pop(domain, None)  # drop the token with the domain
                    _save_domains()
                if _conf["active"] not in _conf["domains"]:
                    _conf["active"] = _conf["domains"][0]
            else:
                self._send(400, {"error": "unknown action"})
                return
            self._send(200, self._domains())
            return
        if self.path == "/api/action":
            if not active_token():
                self._send(401, {"error": "no token"})
                return
            body = self._body()
            action, apps = body.get("action"), body.get("apps") or []
            if action not in ACTIONS or not apps:
                self._send(400, {"error": "unknown action or no apps"})
                return
            if len(apps) > MAX_ACTION_APPS:
                self._send(400, {"error": f"{len(apps)} apps — max {MAX_ACTION_APPS} per action"})
                return
            try:
                self._send(200, {"results": run_action(action, apps)})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        self._send(404, {"error": "not found"})

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            with open(os.path.join(HERE, "index.html"), "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
            return
        if u.path == "/api/domains":
            self._send(200, self._domains())
            return
        if u.path == "/api/apps":
            if not active_token():
                self._send(401, {"error": "no token"})
                return
            try:
                self._send(200, {"apps": list_apps()})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if u.path == "/api/pods":
            if not active_token():
                self._send(401, {"error": "no token"})
                return
            apps = [a for a in parse_qs(u.query).get("apps", [""])[0].split(",") if a]
            self._send(200, {"pods": pod_counts(apps)})
            return
        if u.path == "/api/stream":
            self._stream(parse_qs(u.query).get("apps", [""])[0])
            return
        self._send(404, {"error": "not found"})

    def _stream(self, apps_csv):
        apps = [a for a in apps_csv.split(",") if a]
        if not apps or not active_token():
            self._send(400, {"error": "missing apps or token"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        st = Stream(apps)
        st.start()
        try:
            while True:
                sent = False
                for ev in st.drain():
                    self.wfile.write(f"data: {json.dumps(ev)}\n\n".encode())
                    sent = True
                if sent:
                    self.wfile.flush()
                else:
                    self.wfile.write(b": ping\n\n")  # keepalive
                    self.wfile.flush()
                time.sleep(0.3)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            st.stop()


def main():
    if os.environ.get("ARGOCD_AUTH_TOKEN"):
        _tokens[_conf["active"]] = os.environ["ARGOCD_AUTH_TOKEN"]
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"DevLogs UI: http://localhost:{PORT}  (domains: {', '.join(_conf['domains'])})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
