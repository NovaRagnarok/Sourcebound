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
ORIGINAL_ENV="$TMP_DIR/original.env"
RESTORE_ENV=0

cleanup() {
  if [[ -n "${SERVER_PID}" ]]; then
    kill -- -"${SERVER_PID}" >/dev/null 2>&1 || true
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

echo "[newcomer-smoke] bootstrapping services"
if [[ -f .env ]]; then
  cp .env "$ORIGINAL_ENV"
  RESTORE_ENV=1
fi

cp .env.example .env
RUN_SUFFIX=$(.venv/bin/python - <<'PY'
from uuid import uuid4

print(uuid4().hex[:10])
PY
)
cat >> .env <<EOF
APP_POSTGRES_SCHEMA=sourcebound_smoke_${RUN_SUFFIX}
QDRANT_COLLECTION=approved_claims_smoke_${RUN_SUFFIX}
RESEARCH_QDRANT_COLLECTION=research_findings_smoke_${RUN_SUFFIX}
EOF

docker compose up -d postgres qdrant

echo "[newcomer-smoke] waiting for default services to become reachable"
for _ in $(seq 1 60); do
  CURRENT_STATUS=$(.venv/bin/saw status --json-output || true)
  if printf '%s' "$CURRENT_STATUS" | .venv/bin/python -c '
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
PRE_SEED_STATUS=$(.venv/bin/saw status --json-output)
printf '%s' "$PRE_SEED_STATUS" | .venv/bin/python -c '
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
.venv/bin/saw seed-dev-data

echo "[newcomer-smoke] validating post-seed runtime readiness"
POST_SEED_STATUS=$(.venv/bin/saw status --json-output)
printf '%s' "$POST_SEED_STATUS" | .venv/bin/python -c '
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
from urllib.request import Request, urlopen


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


workspace_html = fetch_text("http://127.0.0.1:8000/workspace/")
assert "<!doctype html>" in workspace_html.lower()

runtime = json.loads(fetch_text("http://127.0.0.1:8000/health/runtime"))
assert runtime["overall_status"] == "ready", runtime
projection = next(service for service in runtime["services"] if service["name"] == "projection")
assert projection["mode"] == "qdrant:ready", projection

workspace = json.loads(fetch_text("http://127.0.0.1:8000/v1/workspace/summary"))
assert workspace["project"]["project_id"] == "project-rouen-winter", workspace
assert workspace["next_actions"], workspace

query = post_json(
    "http://127.0.0.1:8000/v1/query",
    {"question": "Rouen bread prices", "mode": "strict_facts"},
)
assert query["metadata"]["retrieval_backend"] == "qdrant", query["metadata"]
assert query["metadata"]["retrieval_quality_tier"] == "projection", query["metadata"]
PY

echo "[newcomer-smoke] success"
