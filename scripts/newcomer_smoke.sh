#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

if [[ ! -x .venv/bin/python ]]; then
  echo "Missing .venv/bin/python. Run \`make bootstrap\` first." >&2
  exit 1
fi

if ! command -v setsid >/dev/null 2>&1; then
  echo "Missing \`setsid\`, which is required to manage the reload server process." >&2
  exit 1
fi

TMP_DIR=$(mktemp -d)
SERVER_LOG="$TMP_DIR/newcomer-smoke-server.log"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill -- -"${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT

echo "[newcomer-smoke] bootstrapping services"
docker compose up -d postgres qdrant

echo "[newcomer-smoke] checking pre-seed runtime status"
.venv/bin/saw status --json-output >/dev/null

echo "[newcomer-smoke] seeding default local data"
.venv/bin/saw seed-dev-data

echo "[newcomer-smoke] validating post-seed runtime readiness"
POST_SEED_STATUS=$(.venv/bin/saw status --json-output)
printf '%s' "$POST_SEED_STATUS" | .venv/bin/python -c '
import json
import sys

status = json.load(sys.stdin)
assert status["overall_status"] == "ready", status
projection = next(service for service in status["services"] if service["name"] == "projection")
assert projection["ready"] is True, projection
'

echo "[newcomer-smoke] starting reload server"
setsid .venv/bin/saw serve --reload >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 60); do
  if .venv/bin/python -c '
import sys
from urllib.request import urlopen

try:
    with urlopen("http://127.0.0.1:8000/health", timeout=1.0) as response:
        sys.exit(0 if response.status == 200 else 1)
except Exception:
    sys.exit(1)
' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! .venv/bin/python -c '
import sys
from urllib.request import urlopen

with urlopen("http://127.0.0.1:8000/health", timeout=2.0) as response:
    sys.exit(0 if response.status == 200 else 1)
' >/dev/null 2>&1; then
  echo "[newcomer-smoke] server did not become ready" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

echo "[newcomer-smoke] validating HTTP surfaces"
.venv/bin/python - <<'PY'
import json
from urllib.request import urlopen


def fetch_text(url: str) -> str:
    with urlopen(url, timeout=5.0) as response:
        assert response.status == 200, (url, response.status)
        return response.read().decode("utf-8")


operator_html = fetch_text("http://127.0.0.1:8000/operator/")
assert "<!doctype html>" in operator_html.lower()

runtime = json.loads(fetch_text("http://127.0.0.1:8000/health/runtime"))
assert runtime["overall_status"] == "ready", runtime

workspace = json.loads(fetch_text("http://127.0.0.1:8000/v1/workspace/summary"))
assert workspace["project"]["project_id"] == "project-rouen-winter", workspace
assert workspace["next_actions"], workspace
PY

echo "[newcomer-smoke] success"
