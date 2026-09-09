#!/usr/bin/env python3
"""Local log viewer backend: streams `argocd app logs` for many dev services over SSE.

No Loki, no docker. The only data tap is `argocd app logs` (ArgoCD read-only SSO token).
Token lives in memory only (set via POST /api/token), never written to disk, never logged.
A short buffer window re-orders lines by their own UTC timestamp so the merged stream is
roughly time-sorted across services.

ponytail: single-process, in-memory heap with a fixed reorder window. Good for a handful of
services; for many high-throughput streams, widen WINDOW or shard per service.
"""
import base64
import calendar
import heapq
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

PORT = int(os.environ.get("DEVLOGS_PORT", "8900"))
# macOS puts argocd in /opt/homebrew/bin, Linux installs usually in ~/.local/bin or /usr/local/bin
ARGOCD = (os.environ.get("ARGOCD_BIN") or shutil.which("argocd")
          or os.path.expanduser("~/.local/bin/argocd"))
GH = os.environ.get("GH_BIN") or shutil.which("gh") or os.path.expanduser("~/.local/bin/gh")
CLAUDE = os.environ.get("CLAUDE_BIN") or shutil.which("claude") or os.path.expanduser("~/.local/bin/claude")
# The bulk approve/merge scripts already parse PR references, classify mergeability, retry while
# GitHub says UNKNOWN and refuse to force-merge. Wrapping them beats a second implementation that
# would drift from the one used from the shell.


def _pr_tool(name: str, env: str) -> str:
    """Ship a copy in tools/ so a fresh clone works, but let a personal ~/bin version win: whoever
    already runs these from the shell keeps one copy to maintain."""
    if os.environ.get(env):
        return os.environ[env]
    home = os.path.expanduser(f"~/bin/{name}")
    if os.path.exists(home):
        return home
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools", name)


APPROVE_BIN = _pr_tool("approve-prs.py", "DEVLOGS_APPROVE_BIN")
MERGE_BIN = _pr_tool("merge-prs.py", "DEVLOGS_MERGE_BIN")
DEFAULT_SERVER = os.environ.get("ARGOCD_SERVER", "argocd.example.com")
EXTRA_SERVERS = os.environ.get("ARGOCD_SERVERS", "")  # comma-separated, optional
LIST_TIMEOUT = int(os.environ.get("DEVLOGS_LIST_TIMEOUT", "90"))  # 490 apps over grpc-web is slow
APPS_TTL = 10  # connect, the health poll and every action ask for the same list within a second
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
GH_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$")  # org and repo both land in argv
REPOS_FILE = os.environ.get("DEVLOGS_REPOS_FILE") or os.path.join(HERE, ".devlogs-repos.json")
RUN_FIELDS = ("databaseId,conclusion,status,headSha,headBranch,displayTitle,"
              "workflowName,createdAt,url")
FAILED = {"failure", "startup_failure", "timed_out"}  # cancelled is a human, not a breakage
RUNNER_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z ")  # every runner line
# a squash merge leaves the PR number in the commit subject. The LAST one is the merge into the
# default branch: "fix: … (#1245) (#1246)" is PR 1245 into a dev branch, merged to main by 1246.
PR_RE = re.compile(r"\(#(\d+)\)")
REPO_TTL = 600
RUN_TTL = 30

_tokens = {}  # domain -> token: memory only, one token per ArgoCD domain
_conf = {"domains": [], "active": ""}
_repos = {"org": os.environ.get("DEVLOGS_GH_ORG", ""), "map": {}}  # app -> repo name
_org_repos = [0, {}]  # fetched_at, {repo name: default branch}
_unlinked = set()  # apps unlinked in this process: a merge must not resurrect them from disk
_workload = [0, {}]  # fetched_at, {argocd app name: its directory in the workload repo}
_apps = [0, "", []]  # fetched_at, domain, apps
_runs = {}  # repo -> (fetched_at, runs)
_wl_open = [0, []]  # fetched_at, open PRs of the workload repo (shared by every service card)


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


def _load_repos():
    """App -> GitHub repo mapping. Names only: `gh` owns the GitHub credential, this file never
    sees one."""
    try:
        with open(REPOS_FILE) as f:
            saved = json.load(f) or {}
    except (OSError, ValueError):
        return
    if GH_NAME_RE.match(str(saved.get("org", ""))):
        _repos["org"] = saved["org"]
    if GH_NAME_RE.match(str(saved.get("workload", ""))):
        _repos["workload"] = saved["workload"]
    _repos["map"] = {a: r for a, r in (saved.get("map") or {}).items()
                     if APP_NAME_RE.match(a) and GH_NAME_RE.match(str(r))}


def _save_repos():
    """Merge with whatever is on disk before writing. Two instances of this server (a spare one on
    another port, a colleague's copy of the folder) each hold the whole map in memory, and a plain
    overwrite drops every link the other one made."""
    on_disk = {}
    try:
        with open(REPOS_FILE) as f:
            on_disk = ((json.load(f) or {}).get("map") or {})
    except (OSError, ValueError):
        pass
    merged = {**{a: r for a, r in on_disk.items()
                 if APP_NAME_RE.match(a) and GH_NAME_RE.match(str(r))}, **_repos["map"]}
    _repos["map"] = {a: r for a, r in merged.items() if a not in _unlinked}
    _unlinked.clear()
    try:
        with open(REPOS_FILE, "w") as f:
            json.dump({"org": _repos["org"], "workload": _repos.get("workload", ""),
                       "map": _repos["map"]}, f, indent=1, sort_keys=True)
    except OSError:
        pass


def _gh(args: list, timeout=45) -> str:
    r = subprocess.run([GH, *args], capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError((r.stderr or r.stdout).strip().splitlines()[-1:] or ["gh failed"])
    return r.stdout


def gh_org() -> str:
    """The configured org, or the first one `gh` can see — one less thing to set up by hand."""
    if not _repos["org"]:
        names = json.loads(_gh(["api", "user/orgs", "--jq", "[.[].login]"]) or "[]")
        if names and GH_NAME_RE.match(names[0]):
            _repos["org"] = names[0]
            _save_repos()
    return _repos["org"]


def org_repos() -> dict:
    """{repo: default branch}. The default branch comes free with the repo list, which spares a
    call per repo when the CI tab asks for "just main"."""
    if time.time() - _org_repos[0] < REPO_TTL:
        return _org_repos[1]
    org = gh_org()
    if not org:
        return {}
    out = _gh(["api", f"orgs/{org}/repos?per_page=100", "--paginate",
               "--jq", r'.[] | "\(.name) \(.default_branch)"'])
    repos = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2 and GH_NAME_RE.match(parts[0]) and GH_NAME_RE.match(parts[1]):
            repos[parts[0]] = parts[1]
    _org_repos[:] = [time.time(), repos]
    return _org_repos[1]


def suggest_repos(apps: list) -> dict:
    """Longest exact suffix wins: `vikki-mobile-dev-party-service` -> `party-service`. Only exact
    suffixes — a fuzzy guess would happily point a service at some other team's repo, and CI of the
    wrong repo is worse than no CI at all."""
    repos = list(org_repos())
    out = {}
    for a in apps:
        best = ""
        for r in repos:
            if a.endswith(r) and len(r) > len(best):
                best = r
        if best:
            out[a] = best
    return out


GQL_INBOX = """query($q:String!,$n:Int!){
  search(query:$q, type:ISSUE, first:$n){
    issueCount
    nodes{ ... on PullRequest {
      number title url isDraft reviewDecision updatedAt additions deletions changedFiles
      repository{ nameWithOwner } author{ login }
    } }
  }
}"""


def pr_inbox(query: str, limit=50) -> dict:
    """Every open PR the search matches, in one call. Review decision and diff size come with it,
    so a row can say "+3641/-461 and nobody has reviewed it" before anyone ticks approve."""
    raw = _gh(["api", "graphql", "-f", "query=" + GQL_INBOX, "-F", "q=" + query,
               "-F", f"n={min(limit, 100)}", "--jq", ".data.search"], 60)
    got = json.loads(raw or "{}")
    out = []
    for n in (got.get("nodes") or []):
        if not n:
            continue
        out.append({"repo": (n.get("repository") or {}).get("nameWithOwner", ""),
                    "number": n.get("number"), "title": n.get("title", ""),
                    "url": n.get("url", ""), "draft": bool(n.get("isDraft")),
                    "decision": n.get("reviewDecision") or "",
                    "author": (n.get("author") or {}).get("login", ""),
                    "at": n.get("updatedAt", ""), "adds": n.get("additions", 0),
                    "dels": n.get("deletions", 0), "files": n.get("changedFiles", 0)})
    return {"total": got.get("issueCount", 0), "prs": out}


def pr_refs(raw: list) -> list:
    """owner/repo#123 only. The references reach a subprocess, and one crafted entry would
    otherwise become an extra flag."""
    seen = list(dict.fromkeys(str(x) for x in raw))
    refs = [r for r in seen if PR_REF_RE.match(r)]
    if len(refs) != len(seen):
        raise ValueError("a PR reference must look like owner/repo#123")
    return refs[:MAX_PR_REFS]


def run_pr_tool(binary: str, refs: list, args: list, timeout=900) -> dict:
    if not os.path.exists(binary):
        raise RuntimeError(f"{binary} not found — set DEVLOGS_APPROVE_BIN / DEVLOGS_MERGE_BIN")
    r = subprocess.run([sys.executable, binary, *args, *refs],
                       capture_output=True, text=True, timeout=timeout)
    return {"ok": r.returncode == 0, "code": r.returncode,
            "out": (r.stdout or "").strip(), "err": (r.stderr or "").strip()}


HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


def diff_lines(diff: str) -> dict:
    """{path: {line numbers that GitHub will accept a comment on}}. The API rejects a comment on a
    line outside the diff, so a finding pointing at an untouched line has to be caught here rather
    than as a 422 halfway through posting a review."""
    out, path, line = {}, None, 0
    for row in diff.splitlines():
        if row.startswith("+++ b/"):
            path = row[6:].strip()
            out.setdefault(path, set())
        elif row.startswith("@@"):
            m = HUNK_RE.match(row)
            line = int(m.group(1)) if m else 0
        elif path and line:
            if row.startswith("+") or row.startswith(" "):
                out[path].add(line)
                line += 1
            elif not row.startswith("-") and not row.startswith("\\"):
                line += 1
    return out


def parse_findings(text: str, allowed: dict) -> list:
    """Claude is asked for JSON. Keep what parses, and mark a finding whose line is not in the diff
    as unpostable instead of dropping it — it may still be the most useful thing said."""
    block = text[text.find("["):text.rfind("]") + 1] if "[" in text and "]" in text else ""
    try:
        raw = json.loads(block)
    except ValueError:
        return []
    out = []
    for f in raw if isinstance(raw, list) else []:
        if not isinstance(f, dict) or not str(f.get("body", "")).strip():
            continue
        path = str(f.get("path", "")).strip().lstrip("/")
        try:
            line = int(f.get("line") or 0)
        except (TypeError, ValueError):
            line = 0
        ok = bool(path) and line in allowed.get(path, set())
        out.append({"path": path, "line": line, "severity": str(f.get("severity", ""))[:16],
                    "body": str(f["body"]).strip()[:MAX_REVIEW_BODY], "postable": ok,
                    "why": "" if ok else ("no such line in the diff" if path else "no file given")})
    return out[:MAX_FINDINGS]


def claude_review(repo: str, number: int) -> dict:
    """Hand the diff to the claude CLI already installed here: it reads the repo's own CLAUDE.md,
    which a bare API call would not, and the code stays on the path Claude Code already uses."""
    if not os.path.exists(CLAUDE):
        raise RuntimeError(f"{CLAUDE} not found — set CLAUDE_BIN")
    diff = _gh(["pr", "diff", str(number), "--repo", repo], 90)[:MAX_DIFF_BYTES]
    if not diff.strip():
        return {"review": "", "findings": [], "error": "empty diff"}
    r = subprocess.run([CLAUDE, "-p"], input=FINDINGS_PROMPT + "\n\n" + diff,
                       capture_output=True, text=True, timeout=900)
    if r.returncode != 0:
        tail = (r.stderr or r.stdout).strip().splitlines()
        raise RuntimeError(tail[-1] if tail else "claude failed")
    text = (r.stdout or "").strip()
    return {"review": text, "findings": parse_findings(text, diff_lines(diff)),
            "bytes": len(diff), "truncated": len(diff) >= MAX_DIFF_BYTES}


def post_review(repo: str, number: int, findings: list, body: str, event: str) -> dict:
    """One review with every comment attached, not a comment per call: a reviewer reading the PR
    gets a single notification and the comments stay grouped."""
    comments = []
    for f in findings[:MAX_FINDINGS]:
        path, text = str(f.get("path", "")), str(f.get("body", "")).strip()
        try:
            line = int(f.get("line") or 0)
        except (TypeError, ValueError):
            continue
        if path and line > 0 and text:
            comments.append({"path": path, "line": line, "side": "RIGHT",
                             "body": text[:MAX_REVIEW_BODY]})
    if not comments and not body.strip():
        raise RuntimeError("nothing to post")
    payload = {"event": event, "comments": comments}
    if body.strip():
        payload["body"] = body.strip()[:MAX_REVIEW_BODY]
    r = subprocess.run([GH, "api", f"repos/{repo}/pulls/{number}/reviews",
                        "--method", "POST", "--input", "-"],
                       input=json.dumps(payload), capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        tail = (r.stderr or r.stdout).strip().splitlines()
        raise RuntimeError(tail[-1] if tail else "gh api failed")
    got = json.loads(r.stdout or "{}")
    return {"ok": True, "url": got.get("html_url", ""), "posted": len(comments),
            "state": got.get("state", "")}


def workload_repo() -> str:
    """The GitOps repo that holds the ArgoCD Application manifests. Guessed once from the org's
    repo names, then remembered."""
    if not _repos.get("workload"):
        guess = next((r for r in sorted(org_repos()) if r.endswith("application-workload")), "")
        if guess:
            _repos["workload"] = guess
            _save_repos()
    return _repos.get("workload", "")


def workload_paths() -> dict:
    """{argocd app -> directory}, read off the manifest names. Every service directory carries a
    `.argocd-source-<app name>.yaml`, so this mapping is exact — no name guessing, and it covers
    the apps whose source repo lives outside this org."""
    if time.time() - _workload[0] < REPO_TTL:
        return _workload[1]
    repo = workload_repo()
    if not repo:
        return {}
    out = _gh(["api", f"repos/{gh_org()}/{repo}/git/trees/HEAD?recursive=1",
               "--jq", '.tree[] | select(.type=="blob") | .path'], 60)
    found = {}
    for path in out.splitlines():
        if path.startswith("_exclude/"):
            continue  # retired services, kept in the repo but not deployed
        head, _, name = path.rpartition("/")
        app = re.fullmatch(r"\.argocd-source-(.+)\.yaml", name)
        if app and head:
            found[app.group(1)] = head
    _workload[:] = [time.time(), found]
    return found


GQL_OPEN = """query($owner:String!,$repo:String!,$n:Int!){
  repository(owner:$owner,name:$repo){
    pullRequests(states:OPEN, first:$n, orderBy:{field:UPDATED_AT,direction:DESC}){
      nodes{ number title url updatedAt changedFiles files(first:100){ nodes{ path } } }
    }
  }
}"""
GQL_LANDED = """query($owner:String!,$repo:String!,$path:String!,$n:Int!){
  repository(owner:$owner,name:$repo){
    defaultBranchRef{ target{ ... on Commit {
      history(first:$n, path:$path){ nodes{
        oid messageHeadline committedDate url
        associatedPullRequests(first:1){ nodes{ number url state } }
      } }
    } } }
  }
}"""


def workload_open_prs() -> list:
    """Open PRs in the workload repo, with the files each one touches. One call serves every card:
    the same handful of open PRs is filtered per service, and a PR that has not merged yet is
    invisible to the commit history by definition — which is the whole reason to ask for it."""
    if time.time() - _wl_open[0] < RUN_TTL:
        return _wl_open[1]
    repo = workload_repo()
    if not repo:
        return []
    raw = _gh(["api", "graphql", "-f", f"query={GQL_OPEN}", "-F", f"owner={gh_org()}",
               "-F", f"repo={repo}", "-F", f"n={MAX_WL_OPEN_PRS}",
               "--jq", ".data.repository.pullRequests.nodes"], 60)
    out = []
    for pr in json.loads(raw or "[]"):
        files = [f["path"] for f in ((pr.get("files") or {}).get("nodes") or [])]
        out.append({"number": pr.get("number"), "title": pr.get("title", ""),
                    "url": pr.get("url", ""), "at": pr.get("updatedAt", ""),
                    "changedFiles": pr.get("changedFiles", 0), "paths": files})
    _wl_open[:] = [time.time(), out]
    return out


def workload_history(app: str, limit=5) -> dict:
    """What is changing for this service in the GitOps repo: the open PRs that touch its directory,
    then what already landed there with the PR each commit came from. The image-tag bot pushes
    straight to the branch, so plenty of landed commits have no PR — that is the truth, not a
    lookup that failed."""
    repo, path = workload_repo(), workload_paths().get(app, "")
    if not path:
        return {}
    raw = _gh(["api", "graphql", "-f", f"query={GQL_LANDED}", "-F", f"owner={gh_org()}",
               "-F", f"repo={repo}", "-F", f"path={path}", "-F", f"n={limit}",
               "--jq", ".data.repository.defaultBranchRef.target.history.nodes"], 60)
    landed = []
    for c in json.loads(raw or "[]"):
        pr = ((c.get("associatedPullRequests") or {}).get("nodes") or [{}])[0] or {}
        landed.append({"sha": c.get("oid", ""), "url": c.get("url", ""),
                       "date": c.get("committedDate", ""), "title": c.get("messageHeadline", ""),
                       "pr": str(pr.get("number") or ""), "prUrl": pr.get("url", "")})
    try:
        opens = [p for p in workload_open_prs()
                 if any(f.startswith(path + "/") for f in p["paths"])]
    except (RuntimeError, subprocess.SubprocessError, ValueError):
        opens = []
    return {"repo": repo, "path": path,
            "url": f"https://github.com/{gh_org()}/{repo}/tree/HEAD/{path}",
            "open": opens, "commits": landed}


def ci_runs(repo: str, limit=25) -> list:
    """Only the default branch. A red feature branch or a red release tag is not "main is broken",
    and mixing them in made every card noisy."""
    hit = _runs.get(repo)
    if hit and time.time() - hit[0] < RUN_TTL:
        return hit[1]
    branch = org_repos().get(repo) or "main"
    raw = _gh(["run", "list", "--repo", f"{gh_org()}/{repo}", "--branch", branch,
               "--limit", str(limit), "--json", RUN_FIELDS])
    runs = json.loads(raw or "[]")
    _runs[repo] = (time.time(), runs)
    return runs


def first_breakage(runs: list) -> dict:
    """`gh run list` is newest first. Walk back over the unbroken streak of failures at the head:
    the last one before a success is the run that broke it, and its commit is the answer to
    "which commit broke the build". Branch is not filtered — these workflows deploy per env and
    fire on tags, so a main-only rule would report "no breakage" while STG is red."""
    broke = None
    for r in runs:
        if r.get("conclusion") in FAILED:
            broke = r
        elif r.get("conclusion") == "success":
            break
    return broke


def failed_job_log(repo: str, run: str) -> str:
    """`gh run view --log-failed` returns nothing for these runs (it wants the whole-run zip, which
    404s), while the per-job log endpoint serves them fine — so go job by job."""
    full = f"{gh_org()}/{repo}"
    jobs = json.loads(_gh(["api", f"repos/{full}/actions/runs/{run}/jobs",
                           "--jq", "[.jobs[] | {id, name, conclusion}]"]) or "[]")
    bad = [j for j in jobs if j.get("conclusion") in FAILED]
    if not bad:
        return "no failed job in this run"
    out = []
    for j in bad[:2]:  # two failed jobs is already more than anyone reads in a pane
        try:
            text = _gh(["api", f"repos/{full}/actions/jobs/{j['id']}/logs"], 90)
        except (RuntimeError, subprocess.SubprocessError) as e:
            text = f"(log unavailable: {e})"
        lines = [RUNNER_TS_RE.sub("", l) for l in text.splitlines()[-MAX_CI_LOG_LINES:]]
        out.append(f"=== {j.get('name', '?')} ===\n" + "\n".join(lines))
    return "\n\n".join(out)


def with_pr(run: dict) -> dict:
    """The PR number parsed out of the commit subject, or "" when the commit did not come from a
    squash merge. Free — resolving it properly costs an API call per run."""
    if not run:
        return run
    found = PR_RE.findall(run.get("displayTitle") or "")
    return {**run, "pr": found[-1] if found else ""}


def ci_status(app: str) -> dict:
    try:
        workload = workload_history(app)
    except (RuntimeError, subprocess.SubprocessError, ValueError):
        workload = {}  # the workload repo is a bonus: never let it sink the CI card
    repo = _repos["map"].get(app)
    if not repo:
        return {"app": app, "repo": "", "workflows": [], "workload": workload}
    runs = ci_runs(repo)
    out = []
    for wf in dict.fromkeys(r.get("workflowName", "") for r in runs):
        mine = [r for r in runs if r.get("workflowName") == wf]
        out.append({"workflow": wf, "latest": with_pr(mine[0]), "broke": with_pr(first_breakage(mine))})
    return {"app": app, "repo": f"{gh_org()}/{repo}", "branch": org_repos().get(repo) or "main",
            "url": f"https://github.com/{gh_org()}/{repo}/actions", "workflows": out,
            "workload": workload}


def token_claims(domain: str) -> dict:
    """The token's own payload. Not verified — used only to show who is connected and for how long,
    both of which beat relaying a UUID or ArgoCD's "invalid session" text."""
    parts = _tokens.get(domain, "").split(".")
    if len(parts) != 3:
        return {}
    try:
        pad = parts[1] + "=" * (-len(parts[1]) % 4)
        return json.loads(base64.urlsafe_b64decode(pad)) or {}
    except (ValueError, TypeError, json.JSONDecodeError):
        return {}


def token_expiry(domain: str) -> int:
    """Unix `exp` out of the token's own payload, or 0 when there is none to read. Not verified —
    this only exists so the UI can say "expired 6 minutes ago" instead of relaying ArgoCD's
    "invalid session: failed to verify the token", which sends you hunting the wrong problem."""
    try:
        return int(token_claims(domain).get("exp") or 0)
    except (ValueError, TypeError):
        return 0


def active_token() -> str:
    return _tokens.get(_conf["active"], "")


ARGOCD_CLI_CONFIG = os.path.expanduser("~/.config/argocd/config")


def cli_session(domain: str) -> bool:
    """True when `argocd login <domain>` has already left a session on this machine: then the CLI
    authenticates itself and nobody has to paste a token every few hours."""
    # ponytail: substring match instead of a YAML parse — no dependency, and the file is ours
    try:
        with open(ARGOCD_CLI_CONFIG) as f:
            return f"server: {domain}" in f.read()
    except OSError:
        return False


def authed() -> bool:
    return bool(active_token()) or cli_session(_conf["active"])


def _env():
    e = dict(os.environ)
    e["ARGOCD_SERVER"] = _conf["active"]
    e["ARGOCD_OPTS"] = "--grpc-web"
    if active_token():
        e["ARGOCD_AUTH_TOKEN"] = active_token()
    else:
        e.pop("ARGOCD_AUTH_TOKEN", None)  # empty would override the CLI's own session
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


def whoami() -> dict:
    """Cheap auth check: `account get-user-info` answers in under a second, where `app list` has to
    marshal every application on the server. Returns {} when the session is not valid."""
    out = subprocess.run([ARGOCD, "account", "get-user-info", "-o", "json"],
                         env=_env(), capture_output=True, text=True, timeout=20)
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip().splitlines()[-1] if out.stderr.strip()
                           else "argocd account get-user-info failed")
    try:
        return json.loads(out.stdout or "{}") or {}
    except ValueError:
        return {}


def list_apps() -> list:
    """[{name, health, sync}] — one argocd call serves both the picker and the health dots."""
    if time.time() - _apps[0] < APPS_TTL and _apps[1] == _conf["active"]:
        return _apps[2]
    try:
        out = subprocess.run([ARGOCD, "app", "list", "-o", "json"], env=_env(),
                             capture_output=True, text=True, timeout=LIST_TIMEOUT)
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            f"argocd app list did not answer within {LIST_TIMEOUT}s — the ArgoCD server is slow "
            f"right now. Raise DEVLOGS_LIST_TIMEOUT if this is normal for your link.") from None
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
    _apps[:] = [time.time(), _conf["active"],
                sorted([a for a in apps if a["name"]], key=lambda a: a["name"])]
    return _apps[2]


APP_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{0,62}$")  # goes into a URL path — keep it a plain name
POD_TTL = 20  # seconds: the sidebar re-asks every 30s, so a short cache still spares ArgoCD a round
MAX_POD_APPS = 80
MAX_CI_APPS = 25  # one `gh run list` per repo: the CI tab shows the selected services, not all 500
MAX_CI_LOG_LINES = 400
PR_REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,99}/[A-Za-z0-9][A-Za-z0-9._-]{0,99}#[1-9]\d{0,9}$")
MAX_PR_REFS = 30  # a batch nobody can read the list of is a batch nobody should run
MERGE_METHODS = ("squash", "merge", "rebase")
REVIEW_EVENTS = ("COMMENT", "REQUEST_CHANGES", "APPROVE")
# Findings have to carry a file and a line, or they cannot become inline comments. The line is the
# new-file line number, which is what the review API wants with side=RIGHT.
FINDINGS_PROMPT = (
    'Review this pull request diff. Reply with JSON only: a list of '
    '{"path","line","severity","body"} objects, worst first, at most 8, no prose around it. '
    '"path" is the file as it appears in the diff, "line" is a line number in the NEW file that '
    'the diff touches, "severity" is one of high/medium/low, "body" is one or two sentences aimed '
    'at the author. Judge against the conventions in CLAUDE.md. Report only real defects; reply [] '
    'if the diff is clean.')
MAX_DIFF_BYTES = 400_000  # a 3600-line PR is normal here; past this a review is worthless anyway
MAX_REVIEW_BODY = 4000
MAX_FINDINGS = 12
MAX_WL_OPEN_PRS = 40  # the workload repo runs ~20 open PRs; 40 leaves headroom for one call
MAX_DIAG_ITEMS = 8  # a broken rollout can leave dozens of failed resources; eight tells the story
_pods = {}  # (domain, app) -> (fetched_at, {"ready": n, "total": n})


def _api(path: str, timeout=15):
    """The ArgoCD REST API. The CLI covers apps, logs and actions, but not the resource tree, the
    k8s events or a crashed container's log — those three only exist here."""
    req = urllib.request.Request(f"https://{_conf['active']}{path}",
                                 headers={"Authorization": "Bearer " + active_token()})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def resource_nodes(app: str) -> list:
    """The app's whole resource tree. Health messages and the info lines (ready containers, restart
    count) live here, and so does the reason an app is red without a single failing pod — a
    Degraded ExternalSecret or SparkApplication."""
    domain = _conf["active"]
    hit = _pods.get((domain, app))
    if hit and time.time() - hit[0] < POD_TTL:
        return hit[1]
    tree = json.loads(_api(f"/api/v1/applications/{app}/resource-tree") or "{}")
    nodes = tree.get("nodes") or []
    _pods[(domain, app)] = (time.time(), nodes)
    return nodes


def pod_nodes(app: str) -> list:
    return [n for n in resource_nodes(app) if n.get("kind") == "Pod"]


def _pod_count(app: str) -> dict:
    pods = pod_nodes(app)
    return {"ready": sum(1 for n in pods if (n.get("health") or {}).get("status") == "Healthy"),
            "total": len(pods)}


def pod_events(app: str, node: dict, limit=8) -> list:
    """Kubernetes events for one pod — the "Back-off restarting failed container" line that says
    what actually happened, which no health status ever tells you."""
    q = urllib.parse.urlencode({"resourceUID": node.get("uid", ""),
                                "resourceNamespace": node.get("namespace", ""),
                                "resourceName": node.get("name", "")})
    raw = json.loads(_api(f"/api/v1/applications/{app}/events?{q}") or "{}")
    out = []
    for e in (raw.get("items") or []):
        out.append({"type": e.get("type", ""), "reason": e.get("reason", ""),
                    "message": (e.get("message") or "").strip(),
                    "count": e.get("count", 1),
                    "at": e.get("lastTimestamp") or e.get("eventTime") or ""})
    out.sort(key=lambda e: e["at"] or "", reverse=True)
    out.sort(key=lambda e: e["type"] != "Warning")  # stable: keeps newest-first within each group
    return out[:limit]


# ArgoCD hands its own complaints back as log CONTENT, so a non-empty body is not proof of a log:
# "previous terminated container ... not found" when the pod never restarted, and
# "container <app>-application is not valid for pod ..." when the container is named differently.
# Every one of these arrived as a 100-190 byte "log" from a real broken pod: ArgoCD and kubelet
# both report failures through the log body, so an empty log has to be recognised, not printed.
LOG_ERR_RE = re.compile(r"previous terminated container.*not found"
                        r"|container .* is not valid for pod"
                        r"|a container name must be specified"
                        r"|container .* in pod .* is waiting to start", re.S)
CHOOSE_RE = re.compile(r"choose one of: \[([^\]]+)\]")
SIDECARS = ("istio-proxy", "istio-init", "vault-agent", "vault-agent-init", "linkerd-proxy")


def _usable_log(text: str) -> bool:
    return bool(text) and not LOG_ERR_RE.search(text)


def _container_rank(name: str, app: str) -> int:
    """The app's own container first, then anything else, then sidecars and init containers. On a
    pod that never started, the only logs on offer belong to istio-init or some init container —
    real, but not what you opened the pane to read."""
    if name.endswith("-application") or name in app or app.endswith(name):
        return 0
    if name in SIDECARS or name.startswith("istio") or name.endswith("-init"):
        return 2
    return 1


def pod_containers(app: str, node: dict) -> list:
    """Kubernetes answers a container-less log request with the list of containers to choose from,
    so the failed call doubles as the discovery call."""
    m = CHOOSE_RE.search(_pod_log_once(app, node, 1, False, "") or "")
    names = m.group(1).replace(",", " ").split() if m else []
    return sorted(names, key=lambda n: _container_rank(n, app))


def pod_log(app: str, node: dict, tail=120) -> dict:
    """The tail of a pod's log, preferring the container that already died — a crash loop wipes the
    useful part from the running one within seconds. The container name is a convention that does
    not always hold, so ask the pod when it does not. Returns which container answered: on an
    ImagePullBackOff pod the only container with a log is the sidecar, and saying so beats
    labelling istio-proxy's output as "the app that crashed"."""
    for previous in (True, False):
        text = _pod_log_once(app, node, tail, previous, f"{app}-application")
        if _usable_log(text):
            return {"text": text, "container": f"{app}-application", "previous": previous}
    for container in pod_containers(app, node):
        for previous in (True, False):
            text = _pod_log_once(app, node, tail, previous, container)
            if _usable_log(text):
                return {"text": text, "container": container, "previous": previous}
    return {"text": "", "container": "", "previous": False}


def _pod_log_once(app: str, node: dict, tail: int, previous: bool, container: str) -> str:
    pod, ns = node.get("name", ""), node.get("namespace", "")
    base = {"namespace": ns, "container": container, "tailLines": tail,
            "previous": "true" if previous else "false"}
    # two API shapes across ArgoCD versions: try both rather than betting on one
    paths = [f"/api/v1/applications/{app}/pods/{pod}/logs?"
             + urllib.parse.urlencode({k: v for k, v in base.items() if v != ""}),
             f"/api/v1/applications/{app}/logs?"
             + urllib.parse.urlencode({k: v for k, v in {**base, "podName": pod}.items() if v != ""})]
    raw = ""
    for path in paths:
        try:
            raw = _api(path, 30)
            break
        except (urllib.error.URLError, OSError):
            continue
    lines = []
    for line in raw.splitlines():
        try:
            lines.append(json.loads(line).get("result", {}).get("content", ""))
        except ValueError:
            lines.append(line)
    return "\n".join(l for l in lines if l).strip()


def diagnose(app: str) -> dict:
    """Why is this app red: the app's own conditions, then every unhealthy pod with its events and
    the tail of the container that died."""
    detail = json.loads(_api(f"/api/v1/applications/{app}") or "{}")
    st = detail.get("status") or {}
    conditions = [{"type": c.get("type", ""), "message": (c.get("message") or "").strip()}
                  for c in (st.get("conditions") or [])]
    op = (st.get("operationState") or {})
    if op.get("message") and op.get("phase") not in ("Succeeded", None):
        conditions.append({"type": f"Sync {op.get('phase')}", "message": op["message"].strip()})
    # any kind, not just Pods: plenty of apps are red purely because an ExternalSecret or a
    # SparkApplication is Degraded, and reporting "no unhealthy pod" there is useless
    bad = [n for n in resource_nodes(app)
           if (n.get("health") or {}).get("status") not in ("Healthy", "Progressing", None)]
    bad.sort(key=lambda n: n.get("kind") != "Pod")  # pods first: they are the ones with logs
    pods = []
    for n in bad[:MAX_DIAG_ITEMS]:
        health = n.get("health") or {}
        kind = n.get("kind", "")
        item = {"kind": kind, "name": n.get("name", ""), "namespace": n.get("namespace", ""),
                "status": health.get("status", ""), "message": (health.get("message") or "").strip(),
                "info": [f"{i.get('name')}: {i.get('value')}" for i in (n.get("info") or [])],
                "createdAt": n.get("createdAt", ""), "events": [], "log": ""}
        try:
            item["events"] = pod_events(app, n)
        except (urllib.error.URLError, OSError, ValueError) as e:
            item["events"] = [{"type": "Warning", "reason": "events unavailable",
                               "message": str(e), "count": 1, "at": ""}]
        if kind == "Pod":
            try:
                got = pod_log(app, n)
                item.update(log=got["text"], logContainer=got["container"],
                            logPrevious=got["previous"])
            except (urllib.error.URLError, OSError, ValueError):
                item["log"] = ""
        pods.append(item)
    return {"app": app, "health": (st.get("health") or {}).get("status", ""),
            "sync": (st.get("sync") or {}).get("status", ""),
            "conditions": conditions, "pods": pods, "podCount": len(bad)}


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


_load_repos()


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
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            # the tab reloaded, or a slow `gh`/pod batch finished after the UI gave up: the reply
            # has nowhere to go, and that is not a failure of this request
            pass

    def _body(self):
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n) or b"{}")

    def _domains(self):
        return {"active": _conf["active"], "now": int(time.time()),
                "domains": [{"domain": d, "hasToken": bool(_tokens.get(d)),
                             "cliSession": cli_session(d), "exp": token_expiry(d)}
                            for d in _conf["domains"]]}

    def do_POST(self):
        if self.path == "/api/token":
            body = self._body()
            domain = (body.get("domain") or _conf["active"]).strip()
            if domain not in _conf["domains"]:
                self._send(400, {"ok": False, "error": "unknown domain"})
                return
            was, prev = _conf["active"], _tokens.get(domain)
            _tokens[domain] = (body.get("token") or "").strip()
            _conf["active"] = domain
            try:
                who = whoami()
                # an unauthenticated get-user-info answers `{}` — that is the signal. Anything
                # else is accepted, so an unexpected payload shape cannot reject a good token.
                if not who or who.get("loggedIn") is False:
                    raise RuntimeError("ArgoCD did not accept this token (expired, or from "
                                       "another domain)")
                _apps[:] = [0, "", []]  # a new session must not serve the old session's list
                claims = token_claims(domain)
                self._send(200, {"ok": True, "user": claims.get("preferred_username")
                                 or claims.get("email") or who.get("username", "")})
            except Exception as e:  # noqa: BLE001
                _conf["active"] = was  # a rejected token left behind makes every 401 look like a 400
                if prev is None:
                    _tokens.pop(domain, None)
                else:
                    _tokens[domain] = prev
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
        if self.path == "/api/repos":
            # one app per call, merged server-side: two quick clicks in the picker used to race and
            # the second full-map write dropped the first link
            body = self._body()
            org, app, repo = (str(body.get(k, "")) for k in ("org", "app", "repo"))
            if org and not GH_NAME_RE.match(org):
                self._send(400, {"error": "bad org"})
                return
            if app and not APP_NAME_RE.match(app):
                self._send(400, {"error": "bad app"})
                return
            if repo and not GH_NAME_RE.match(repo):
                self._send(400, {"error": "bad repo"})
                return
            if org:
                _repos["org"] = org
                _org_repos[:] = [0, []]
            if app and not repo:
                _repos["map"].pop(app, None)
                _unlinked.add(app)
            elif app:
                _repos["map"][app] = repo
                _unlinked.discard(app)
            _save_repos()
            self._send(200, {"ok": True, "map": _repos["map"]})
            return
        if self.path in ("/api/pr-plan", "/api/pr-run"):
            # plan runs the tools with --dry-run so the confirmation dialog can list exactly what
            # would happen, including which PRs the merge tool refuses and why
            body = self._body()
            action, method = str(body.get("action", "")), str(body.get("method", "squash"))
            if action not in ("approve", "merge") or method not in MERGE_METHODS:
                self._send(400, {"error": "action must be approve or merge"})
                return
            try:
                refs = pr_refs(body.get("refs") or [])
            except ValueError as e:
                self._send(400, {"error": str(e)})
                return
            if not refs:
                self._send(400, {"error": "no PR selected"})
                return
            plan = self.path.endswith("plan")
            args = ["--dry-run"] if plan else ["--yes"]
            if action == "merge":
                args.append("--" + method)
            note = str(body.get("body", "")).strip()[:MAX_REVIEW_BODY]
            if action == "approve" and note and not plan:
                args += ["--body", note]
            try:
                got = run_pr_tool(APPROVE_BIN if action == "approve" else MERGE_BIN, refs, args)
                self._send(200, {**got, "refs": refs, "plan": plan})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if self.path == "/api/pr-comment":
            # the one place this tool writes to somebody else's pull request
            body = self._body()
            repo, number = str(body.get("repo", "")), str(body.get("number", ""))
            event = str(body.get("event", "COMMENT"))
            if not PR_REF_RE.match(f"{repo}#{number}"):
                self._send(400, {"error": "bad repo or number"})
                return
            if event not in REVIEW_EVENTS:
                self._send(400, {"error": "event must be one of " + ", ".join(REVIEW_EVENTS)})
                return
            findings = body.get("findings") or []
            if not isinstance(findings, list):
                self._send(400, {"error": "findings must be a list"})
                return
            try:
                self._send(200, post_review(repo, int(number), findings,
                                            str(body.get("body", "")), event))
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if self.path == "/api/pr-review":
            body = self._body()
            repo, number = str(body.get("repo", "")), str(body.get("number", ""))
            if not PR_REF_RE.match(f"{repo}#{number}"):
                self._send(400, {"error": "bad repo or number"})
                return
            try:
                self._send(200, claude_review(repo, int(number)))
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if self.path == "/api/ci-rerun":
            body = self._body()
            repo, run = str(body.get("repo", "")), str(body.get("run", ""))
            if not GH_NAME_RE.match(repo) or not run.isdigit():
                self._send(400, {"error": "bad repo or run id"})
                return
            args = ["run", "rerun", run, "--repo", f"{gh_org()}/{repo}"]
            if body.get("failedOnly"):
                args.append("--failed")
            try:
                _gh(args, 60)
                _runs.pop(repo, None)  # the next poll must not serve the pre-rerun status
                self._send(200, {"ok": True})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
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
            if not authed():
                self._send(401, {"error": "no token"})
                return
            try:
                self._send(200, {"apps": list_apps()})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if u.path == "/api/prs":
            q = parse_qs(u.query).get("q", [""])[0][:200]
            try:
                self._send(200, pr_inbox(q or f"org:{gh_org()} is:pr is:open sort:updated-desc"))
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if u.path == "/api/repos":
            try:
                apps = []
                if authed():
                    try:
                        apps = [a["name"] for a in list_apps()]
                    except Exception:  # noqa: BLE001
                        pass  # an expired ArgoCD session must not empty the repo picker
                self._send(200, {"org": gh_org(), "map": _repos["map"],
                                 "repos": sorted(org_repos()), "suggested": suggest_repos(apps)})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if u.path == "/api/ci":
            apps = [a for a in parse_qs(u.query).get("apps", [""])[0].split(",")
                    if APP_NAME_RE.match(a)][:MAX_CI_APPS]
            try:
                self._send(200, {"ci": [ci_status(a) for a in apps]})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if u.path == "/api/pr":
            q = parse_qs(u.query)
            repo, sha = q.get("repo", [""])[0], q.get("sha", [""])[0]
            if not GH_NAME_RE.match(repo) or not re.fullmatch(r"[0-9a-f]{7,40}", sha):
                self._send(400, {"error": "bad repo or sha"})
                return
            try:
                full = f"{gh_org()}/{repo}"
                got = json.loads(_gh(["api", f"repos/{full}/commits/{sha}/pulls",
                                      "--jq", "[.[] | .number]"]) or "[]")
                self._send(200, {"pr": str(got[0]) if got else ""})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if u.path == "/api/ci-log":
            q = parse_qs(u.query)
            repo, run = q.get("repo", [""])[0], q.get("run", [""])[0]
            if not GH_NAME_RE.match(repo) or not run.isdigit():
                self._send(400, {"error": "bad repo or run id"})
                return
            try:
                self._send(200, {"log": failed_job_log(repo, run)})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if u.path == "/api/diag":
            app = parse_qs(u.query).get("app", [""])[0]
            if not APP_NAME_RE.match(app):
                self._send(400, {"error": "bad app name"})
                return
            if not active_token():
                self._send(401, {"error": "paste a token: events and pod logs need the REST API"})
                return
            try:
                self._send(200, diagnose(app))
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": str(e)})
            return
        if u.path == "/api/pods":
            if not active_token():
                # the resource tree has no CLI command, so this one really does need a token
                self._send(401, {"error": "paste a token to see pod counts"})
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


class Server(ThreadingHTTPServer):
    daemon_threads = True  # a live SSE thread must not hold up Ctrl-C

    def handle_error(self, request, client_address):
        """A browser that navigated away mid-request is not worth twenty lines of traceback, and
        the flood buries the errors that do matter."""
        if isinstance(sys.exc_info()[1], (BrokenPipeError, ConnectionResetError)):
            return
        super().handle_error(request, client_address)


def main():
    if os.environ.get("ARGOCD_AUTH_TOKEN"):
        _tokens[_conf["active"]] = os.environ["ARGOCD_AUTH_TOKEN"]
    srv = Server(("127.0.0.1", PORT), Handler)
    print(f"DevLogs UI: http://localhost:{PORT}  (domains: {', '.join(_conf['domains'])})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
