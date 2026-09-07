#!/usr/bin/env bash
# DevLogs UI launcher — macOS and Linux, stdlib Python only.
set -euo pipefail
cd "$(dirname "$0")"

command -v python3 >/dev/null || {
  echo "python3 not found. macOS: xcode-select --install | Linux: install python3 from your package manager"
  exit 1
}

ARGOCD_BIN="${ARGOCD_BIN:-$(command -v argocd || true)}"
[ -x "${ARGOCD_BIN:-}" ] || ARGOCD_BIN="$HOME/.local/bin/argocd"
if [ ! -x "$ARGOCD_BIN" ]; then
  echo "argocd CLI not found."
  echo "  macOS: brew install argocd"
  echo "  Linux: curl -sSLo ~/.local/bin/argocd https://github.com/argoproj/argo-cd/releases/latest/download/argocd-linux-amd64 && chmod +x ~/.local/bin/argocd"
  echo "  or export ARGOCD_BIN=/path/to/argocd"
  exit 1
fi
export ARGOCD_BIN

# optional per-machine settings: ARGOCD_SERVER, ARGOCD_SERVERS, DEVLOGS_PORT, ARGOCD_AUTH_TOKEN
[ -f ~/.argocd-env ] && source ~/.argocd-env || true

PORT="${DEVLOGS_PORT:-8900}"
if [ "${DEVLOGS_OPEN:-1}" = "1" ]; then
  ( sleep 1
    if command -v open >/dev/null; then open "http://localhost:$PORT"
    elif command -v xdg-open >/dev/null; then xdg-open "http://localhost:$PORT"
    fi ) >/dev/null 2>&1 &
fi

exec python3 server.py
