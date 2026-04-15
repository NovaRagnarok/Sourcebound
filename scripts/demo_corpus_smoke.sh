#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

if [[ ! -x .venv/bin/python ]]; then
  echo "Missing .venv/bin/python. Run \`make bootstrap\` first." >&2
  exit 1
fi

OUT_DIR="runtime/demo/wheatley-london-bread"

echo "[demo-corpus-smoke] running checked-in demo corpus"
SUMMARY_JSON=$(.venv/bin/saw demo-corpus-run wheatley-london-bread --data-dir "$OUT_DIR" --json-output)

printf '%s' "$SUMMARY_JSON" | .venv/bin/python -c '
import json
import sys

summary = json.load(sys.stdin)
assert summary["corpus_id"] == "wheatley-london-bread", summary
assert summary["source_document_count"] == 2, summary
assert summary["text_unit_count"] == 2, summary
assert summary["candidate_count"] >= 4, summary
assert summary["approved_claim_count"] == 3, summary
assert summary["review_preview_span_start"] is not None, summary
assert summary["review_preview_span_end"] is not None, summary
assert summary["section_claim_ids"], summary
assert summary["section_generation_status"] in {"thin", "ready"}, summary
'

echo "[demo-corpus-smoke] wrote demo data to $OUT_DIR"
echo "[demo-corpus-smoke] success"
