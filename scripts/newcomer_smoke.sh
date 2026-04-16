#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

if [[ -x .venv/bin/python && -x .venv/bin/saw ]]; then
  PYTHON_BIN=.venv/bin/python
  SAW_BIN=.venv/bin/saw
elif command -v python >/dev/null 2>&1 && command -v saw >/dev/null 2>&1; then
  PYTHON_BIN=$(command -v python)
  SAW_BIN=$(command -v saw)
else
  echo "Missing a usable Python and saw CLI. Run \`make bootstrap\` or install the package first." >&2
  exit 1
fi

TMP_DIR=$(mktemp -d)
SERVER_LOG="$TMP_DIR/newcomer-smoke-server.log"
SERVER_PID=""
ORIGINAL_ENV="$TMP_DIR/original.env"
RESTORE_ENV=0
SMOKE_APP_PORT="${SOURCEBOUND_SMOKE_APP_PORT:-}"
APP_BASE_URL=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill "${SERVER_PID}" >/dev/null 2>&1 || true
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${RESTORE_ENV}" -eq 1 ]]; then
    mv "$ORIGINAL_ENV" .env
  else
    rm -f .env
  fi
  rm -rf "$TMP_DIR"
}

trap cleanup EXIT

pick_free_port() {
  "$PYTHON_BIN" - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

port_is_available() {
  local port="$1"
  "$PYTHON_BIN" - "$port" <<'PY'
import socket
import sys

port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError:
        sys.exit(1)
sys.exit(0)
PY
}

server_process_alive() {
  [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1
}

server_log_mentions_port() {
  [[ -f "$SERVER_LOG" ]] && grep -Eq "Uvicorn running on http://127\\.0\\.0\\.1:${SMOKE_APP_PORT}\\b" "$SERVER_LOG"
}

http_ready() {
  "$PYTHON_BIN" - "$APP_BASE_URL/health" <<'PY'
import sys
from urllib.request import urlopen

try:
    with urlopen(sys.argv[1], timeout=1.0) as response:
        sys.exit(0 if response.status == 200 else 1)
except Exception:
    sys.exit(1)
PY
}

echo "[newcomer-smoke] bootstrapping services"
if [[ -f .env ]]; then
  cp .env "$ORIGINAL_ENV"
  RESTORE_ENV=1
fi

cp .env.example .env
if [[ -z "$SMOKE_APP_PORT" ]]; then
  SMOKE_APP_PORT=$(pick_free_port)
elif ! port_is_available "$SMOKE_APP_PORT"; then
  echo "[newcomer-smoke] configured smoke app port ${SMOKE_APP_PORT} is already in use" >&2
  echo "[newcomer-smoke] set SOURCEBOUND_SMOKE_APP_PORT to a free port or unset it for auto-selection" >&2
  exit 1
fi
APP_BASE_URL="http://127.0.0.1:${SMOKE_APP_PORT}"
echo "[newcomer-smoke] using app port ${SMOKE_APP_PORT}"
RUN_SUFFIX=$("$PYTHON_BIN" - <<'PY'
from uuid import uuid4

print(uuid4().hex[:10])
PY
)
cat >> .env <<EOF
APP_HOST=127.0.0.1
APP_PORT=${SMOKE_APP_PORT}
APP_POSTGRES_SCHEMA=sourcebound_smoke_${RUN_SUFFIX}
QDRANT_COLLECTION=approved_claims_smoke_${RUN_SUFFIX}
RESEARCH_QDRANT_COLLECTION=research_findings_smoke_${RUN_SUFFIX}
EOF

docker compose up -d postgres qdrant

echo "[newcomer-smoke] waiting for default services to become reachable"
for _ in $(seq 1 60); do
  CURRENT_STATUS=$("$SAW_BIN" status --json-output || true)
  if printf '%s' "$CURRENT_STATUS" | "$PYTHON_BIN" -c '
import json
import sys

try:
    status = json.load(sys.stdin)
except Exception:
    sys.exit(1)

services = {service["name"]: service for service in status["services"]}
app_state = services.get("app_state", {})
truth_store = services.get("truth_store", {})
projection = services.get("projection", {})
postgres_ready = bool(app_state.get("ready")) and bool(truth_store.get("ready"))
projection_booted = projection.get("mode") in {"qdrant:uninitialized", "qdrant:ready"}
sys.exit(0 if postgres_ready and projection_booted else 1)
' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "[newcomer-smoke] checking pre-seed runtime status"
PRE_SEED_STATUS=$("$SAW_BIN" status --json-output)
printf '%s' "$PRE_SEED_STATUS" | "$PYTHON_BIN" -c '
import json
import sys

status = json.load(sys.stdin)
assert status["overall_status"] == "degraded", status
projection = next(service for service in status["services"] if service["name"] == "projection")
assert projection["mode"] == "qdrant:uninitialized", projection
assert projection["ready"] is False, projection
assert any("seed-dev-data" in step for step in status["next_steps"]), status["next_steps"]
'

echo "[newcomer-smoke] seeding default local data"
"$SAW_BIN" seed-dev-data

echo "[newcomer-smoke] validating post-seed runtime readiness"
POST_SEED_STATUS=$("$SAW_BIN" status --json-output)
printf '%s' "$POST_SEED_STATUS" | "$PYTHON_BIN" -c '
import json
import sys

status = json.load(sys.stdin)
assert status["overall_status"] == "ready", status
projection = next(service for service in status["services"] if service["name"] == "projection")
assert projection["mode"] == "qdrant:ready", projection
assert projection["ready"] is True, projection
research = next(
    service for service in status["services"] if service["name"] == "research_semantics"
)
assert research["mode"] == "disabled", research
assert research["ready"] is True, research
'

echo "[newcomer-smoke] starting smoke server"
"$PYTHON_BIN" -m source_aware_worldbuilding.cli serve >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 60); do
  if ! server_process_alive; then
    echo "[newcomer-smoke] launched server process exited before readiness on ${APP_BASE_URL}" >&2
    cat "$SERVER_LOG" >&2
    exit 1
  fi
  if http_ready && server_log_mentions_port; then
    break
  fi
  sleep 1
done

if ! server_process_alive; then
  echo "[newcomer-smoke] launched server process exited before validation on ${APP_BASE_URL}" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

if ! http_ready; then
  echo "[newcomer-smoke] server did not become ready on ${APP_BASE_URL}" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

if ! server_log_mentions_port; then
  echo "[newcomer-smoke] launched server on ${APP_BASE_URL} was not confirmed in the Uvicorn log" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

echo "[newcomer-smoke] validating HTTP surfaces"
"$PYTHON_BIN" - "$APP_BASE_URL" <<'PY'
import json
import sys
from urllib.request import Request, urlopen

base_url = sys.argv[1]


def fetch_text(url: str) -> str:
    with urlopen(url, timeout=5.0) as response:
        assert response.status == 200, (url, response.status)
        return response.read().decode("utf-8")


def post_json(url: str, payload: dict) -> dict:
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=5.0) as response:
        assert response.status == 200, (url, response.status)
        return json.loads(response.read().decode("utf-8"))


workspace_html = fetch_text(f"{base_url}/workspace/")
assert "<!doctype html>" in workspace_html.lower()

runtime = json.loads(fetch_text(f"{base_url}/health/runtime"))
assert runtime["overall_status"] == "ready", runtime
projection = next(service for service in runtime["services"] if service["name"] == "projection")
assert projection["mode"] == "qdrant:ready", projection

workspace = json.loads(fetch_text(f"{base_url}/v1/workspace/summary"))
assert workspace["project"]["project_id"] == "project-rouen-winter", workspace
assert workspace["next_actions"], workspace

query = post_json(
    f"{base_url}/v1/query",
    {"question": "Rouen bread prices", "mode": "strict_facts"},
)
assert query["metadata"]["retrieval_backend"] == "qdrant", query["metadata"]
assert query["metadata"]["retrieval_quality_tier"] == "projection", query["metadata"]
PY

echo "[newcomer-smoke] success"
