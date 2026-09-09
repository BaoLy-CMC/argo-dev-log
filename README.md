# DevLogs

A local dev console for an ArgoCD estate: live logs from many services merged into one
time-ordered stream, why a service is red, its CI, and the open pull requests — in one page.

No Loki, no docker, no build step. One `index.html`, one `server.py`, and the `argocd` and `gh`
CLIs that are already on your machine.

[Hướng dẫn tiếng Việt](docs/HUONG-DAN.md)

![Light theme](docs/screenshot-light.png)

## Needs

`python3`, and the CLIs you already have: `argocd` for logs and health, `gh` for the ci and prs
tabs, `claude` only for the review button. Nothing to install, no lockfile.

## Run

```bash
./run.sh                       # or: python3 server.py
```

Then open <http://localhost:8900>.

```bash
export ARGOCD_SERVER=argocd.example.com          # first domain in the picker
export ARGOCD_SERVERS=argocd-a.example.com,...   # optional extras
export ARGOCD_BIN=/opt/homebrew/bin/argocd       # if it is not on PATH
```

Domains live in `.devlogs-domains.json` (hostnames only) and the `+` / `−` buttons maintain it.

## Token

DevTools → Application → Cookies → `argocd.token` → paste it in the header. The header then shows
how long the token has left and says so plainly once it is gone — an expired token makes ArgoCD
answer `invalid session: failed to verify the token`, which reads like a bug in this app.

The token lives in the server's memory only: never on disk, never in a log. F5 does not lose it.

If `argocd login <domain> --sso --grpc-web` works for you, the app uses that CLI session instead
and there is nothing to paste. It does not work against an SSO client with no
`http://localhost:8085/auth/callback` redirect registered, which is the common case.

## Tabs

### logs

Tick services on the left; their logs stream in merged and sorted by timestamp.

- One time column, then the service, the level, and the message. The `~140` characters of repeated
  prefix the clusters emit (level twice, empty request ids, thread, package path) are folded away —
  `raw` brings the untouched line back.
- Level buttons filter; the text box filters text or a traceId; click a traceId to isolate a flow.
- Stack traces and JSON stay in one event. Double-click a line to copy it.
- Each row shows its pod count (`ready/total`, red when short, nothing for a pod-less app).

### health

![Health](docs/screenshot-health.png)

Everything unhealthy, and for one service: its conditions, then each unhealthy **resource** with the
Kubernetes events (warnings first) and — for pods — the tail of the log from the container that
*died*. Not just pods: an app is often red only because an ExternalSecret cannot reach its provider.

### ci

GitHub Actions for the ticked services, default branch only. Per workflow: the latest run and, when
it is red, the commit that broke it plus that job's failing log. Below it, the service's deploy
history from the GitOps repo — the app → directory mapping is read from each
`.argocd-source-<app>.yaml`, so it is exact rather than guessed.

Link a service to its repo once; suggestions are offered only on an exact name match.

### prs

![Pull requests](docs/screenshot-prs.png)

Open pull requests, one GraphQL search, with the review decision and the diff size (a four-figure
diff is printed bold: not something to approve inside a batch).

- `claude` pipes the diff into the **local** `claude` CLI, so the review applies the conventions in
  your `CLAUDE.md` and no API key is involved. It comes back as a checklist of findings, each with a
  file and line; untick what you disagree with, pick `comment` or `request changes`, and they are
  posted as **one** inline review. A finding whose line is not in the diff is shown but cannot be
  posted inline — GitHub rejects those, so it is caught here instead of halfway through a review.
- `approve` / `merge` run `tools/approve-prs.py` and `tools/merge-prs.py`, which ship with the repo
  and also work from the shell. A personal `~/bin/approve-prs.py` wins over the shipped copy, and
  `DEVLOGS_APPROVE_BIN` / `DEVLOGS_MERGE_BIN` win over both.
- Both run `--dry-run` first and the confirmation dialog shows that output, so for a merge you see
  which PRs the tool refuses and why. The dialog also takes an optional review comment. Over three
  PRs you type the action. There is no approve-everything button.

This tab needs no ArgoCD token; it only talks to `gh`.

## Keys and layout

`/` focuses the service filter, `Escape` empties it. Drag the sidebar's right edge to resize it —
the width is remembered. `★` stars a service, `⊘` mutes it into a collapsed folder at the bottom.
Tick services and the sync/restart row appears.

## Notes

- **Colour rule:** chrome is indigo (`--accent`: buttons, focus, links, tabs, selection) and colour
  means status — `--ok` is green and means healthy / approved / success, nothing else. Every text
  colour is ≥ 4.5:1 on all three surfaces in both themes.
- Pod counts, events and previous-container logs come from the ArgoCD REST API because the CLI has
  no command for the resource tree. Everything else goes through `argocd` and `gh`.
- `restart`, `sync`, `rerun`, `approve` and `merge` all confirm first, and `restart` makes you type
  the word.
- `python3 test_server.py` runs the checks: no framework, no network.

![Dark theme](docs/screenshot-dark.png)
