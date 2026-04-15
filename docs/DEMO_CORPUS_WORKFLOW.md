# Demo Corpus Workflow

This repo now ships one narrow real-corpus demo that exercises the full Sourcebound trust path without requiring Zotero, Postgres, or hidden manual setup.

## Corpus

- Corpus id: `wheatley-london-bread`
- Source: Henry B. Wheatley, _The Story of London_ (1896)
- Public-domain source link: <https://www.gutenberg.org/ebooks/46618>
- Checked-in corpus path: `data/demo/corpora/wheatley-london-bread/`

The demo corpus is intentionally small:

- one source record
- one discoverable note document
- one discoverable attachment document
- one economics/material-culture section composed from explicitly approved canon

## Run It

Bootstrap once:

```bash
make bootstrap
```

Run the end-to-end demo into a clean local output directory:

```bash
.venv/bin/saw demo-corpus-run wheatley-london-bread \
  --data-dir runtime/demo/wheatley-london-bread \
  --json-output
```

Or run the smoke wrapper:

```bash
scripts/demo_corpus_smoke.sh
```

The run writes a repeatable file-backed dataset under `runtime/demo/wheatley-london-bread/`:

- `sources.json`
- `source_documents.json`
- `text_units.json`
- `candidates.json`
- `evidence.json`
- `review_events.json`
- `claims.json`
- `bible_sections.json`
- `demo_summary.json`
- `economics_and_material_culture.md`

## What The Demo Proves

The scripted path is:

```text
manifest-backed intake through IntakeService
  -> source document discovery
  -> normalization into text units
  -> heuristic candidate extraction with anchored evidence spans
  -> explicit review approvals
  -> approved canon in the truth store
  -> bible section composition from approved canon only
```

Important trust-boundary behavior:

- The corpus produces four candidate claims.
- The scripted review step explicitly approves three of them with reviewer notes and claim patches.
- One candidate remains pending.
- The generated Bible section references only approved claim ids, never pending candidate ids.

## Review Walkthrough

The review queue now has usable evidence anchors in the fallback heuristic path. A typical demo run produces a preview like:

- locator: `bread-market excerpt#s3`
- span: `[246, 337]`
- excerpt: `No maker of white bread was allowed to make tourte, nor a tourte baker to make white bread.`

That preview comes from the stored `text_unit_id` plus `span_start` / `span_end`, so the reviewer can see the exact grounded slice instead of a detached sentence blob.

## Bible Walkthrough

The demo composes `Economics And Material Culture` from the three approved claims. The generated section is intentionally marked thin, but it is grounded:

- approved claim: London bread regratresses received thirteen batches for every twelve purchased
- approved claim: London white bakers were barred from making tourte bread
- approved claim: House bread was prepared by the bakers of household bread

Rendered markdown lands at:

- `runtime/demo/wheatley-london-bread/economics_and_material_culture.md`

If you want to inspect the dataset through the UI, run the app against the demo directory:

```bash
APP_STATE_BACKEND=file \
APP_TRUTH_BACKEND=file \
APP_DATA_DIR=runtime/demo/wheatley-london-bread \
.venv/bin/saw serve --reload
```

Then open:

- `http://localhost:8000/operator/`
- `http://localhost:8000/workspace/`
- `http://localhost:8000/docs`

## Known Limitations

- Intake for this demo corpus is manifest-driven and local. It exercises the shipped intake service and discovery flow, but it does not exercise live Zotero write/pull behavior.
- The corpus is a very small public-domain excerpt, so the resulting section is intentionally thin and does not cover the full economics facet.
- Review in the demo is scripted but still explicit; this keeps the trust model intact while making the walkthrough reproducible.
- The heuristic extractor remains conservative and corpus-shaped. The span fix makes review evidence much better, but broader extraction quality still depends on future work or GraphRAG.

## Deferred UI Follow-Up

We are intentionally deferring screenshot capture and a polished UI walkthrough for this demo until the workspace and operator views are ready for stable presentation.

Follow-up TODO:

- capture screenshots for source review, evidence span preview, approval, and generated Bible output
- add a short operator/workspace walkthrough once the UI flow is considered presentation-ready
