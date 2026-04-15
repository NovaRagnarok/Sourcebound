# Extraction Evaluation

This benchmark answers a product question instead of an academic one:
are the extracted candidate claims and evidence spans good enough to trust
the review workflow?

## Corpus And Scope

- Dataset: `wheatley-london-bread`
- Real corpus: the checked-in Henry B. Wheatley bread-market excerpt from the
  demo corpus
- Gold set size: 6 atomic reviewable claims
- Compared paths:
  - `heuristic`
  - `graphrag_mapping_fixture`

`graphrag_mapping_fixture` is a reproducible fixture-backed GraphRAG mapping
check. It exercises the GraphRAG-to-Sourcebound mapping path on the same corpus
without requiring a live GraphRAG installation. It should not be read as a
live GraphRAG extraction benchmark.

## Run It

```bash
.venv/bin/saw evaluate-extraction --dataset wheatley-london-bread --json-output
```

Artifacts are written to `runtime/extraction_evals/wheatley-london-bread/`:

- `summary.json`
- `report.md`

## Current Snapshot

Measured on April 14, 2026 from the current checked-in code:

| Path | Kind | Claim Precision | Factual Support Precision | Important Fact Recall | Avg Anchor Focus | Avg Reviewer Actions | Stability |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `heuristic` | `extraction` | 0.0000 | 1.0000 | 0.6667 | 0.4285 | 3.2500 | 1.0000 |
| `graphrag_mapping_fixture` | `mapping_fixture` | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 0.0000 | 1.0000 |

What this means:

- The heuristic path is factually grounded on this corpus. Every candidate maps
  to a real gold fact.
- The heuristic path is still not review-ready as extracted. Claim precision is
  `0.0` because every matched candidate still needs edits or a split before it
  is acceptable as a review card.
- The heuristic path still misses 2 of 6 reviewer-ready facts because it keeps
  compound sentences bundled together.
- Evidence spans are present and stable, but they are broad sentence spans
  rather than focused review spans.
- Reviewer burden is the biggest practical issue. The current heuristic output
  needs an average of 3.25 edits per matched candidate on this corpus.

## Top Problems

1. Compound candidates hide atomic facts.
   The heuristic path misses the second clause in both the baker restriction
   sentence and the hosteller sentence. That is the biggest recall failure and
   the clearest reason reviewers would need to split claims manually.

2. Predicate extraction is too generic.
   All four heuristic candidates landed on `described_as`, which means review
   can trust the evidence but still has to rewrite the actual claim shape.

3. Subject extraction often falls back to weak phrasing.
   We see opening-phrase subjects like `These dealers` and title fallback like
   `The Story of London: bread-market excerpt` where the reviewable subject
   should be more specific.

## Strengths

- Factual support precision is `1.0` on the heuristic path for this dataset.
- Exact duplicate rate is `0.0` on both paths for this dataset.
- Stability across reruns is `1.0` on both paths.
- Every extracted heuristic candidate is grounded in span-backed evidence.

## Prioritized Improvements

1. Split coordinated sentence clauses into separate candidate claims before
   review. This is the most direct way to improve recall and lower manual split
   work.

2. Replace or post-process `described_as` with higher-signal predicates when
   the sentence encodes restrictions, preparation, standing places, or receipt
   rules.

3. Improve subject selection so the extractor prefers named entities and local
   noun phrases over source-title fallback or generic sentence-openers.

## Targets For Future Extraction Work

Use this dataset as the regression gate for product-relevant quality. A good
next target for the heuristic path is:

- important-fact recall `>= 0.83` on this dataset
- claim precision `>= 0.50` on this dataset
- average reviewer actions `<= 1.5`
- average anchor focus `>= 0.60`
- stability stays at `1.0`
