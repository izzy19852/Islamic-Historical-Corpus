# Audit verification & correction pass — 2026-04-25

Friend's audit verified item-by-item against the live codebase and DB.
Several items were stale or overstated; the rest are now fixed or
flagged. Each row links to the file:line evidence used during
verification.

## Status legend

- ✅ FIXED — code or schema changed, sanity-checked
- ❌ NOT FOUND / STALE — claim does not match current state
- ⚠️ PARTIAL — claim is half right; details below
- 📋 FLAGGED — review needed, no auto-fix

## Commits

| Hash | Scope |
|------|-------|
| `1d97911` | Part C — Ibn Rushd / Shajarat al-Durr historical errors |
| `123472a` | A6 (fail-closed quota) + A7 (email helper) + B1/B2 (live stats) |
| `d58cea2` | D2 / D4 / D5 schema migrations |
| `1f8f239` | A2 — unknown-tier fallback to free, not 100 |
| `d489662` | A3 — share line-noise helpers between djvu/pdf cleaners |

`rag/retrieval/orchestrator.py` (A1 fix) and `scripts/audit/` are under
gitignored paths so they live on disk only — no commit captured them.
Note this in deploy if `rag/` is shipped via a separate sync.

---

## Part C — Historical doc errors (P0)

| ID | Status | Evidence |
|----|--------|----------|
| C1 | ✅ FIXED | `docs/islam_stories_scaffolding.md:102` — replaced "burned his own books" with "Caliph al-Mansur ordered the philosophical works burned in 1195 (medicine/math/astronomy spared)" |
| C2 | ✅ FIXED | `docs/islam_stories_scaffolding.md:125` — replaced "ruled briefly after Baybars' predecessor" with the actual 1250 / 1257 / 1260 timeline (Aybak → al-Mansur Ali → Qutuz → Baybars) |
| C3 | ❌ NOT FOUND | No "265" near "Ibn Sa'd" / "Tabaqat" anywhere in `docs/` or `scripts/`. Claim is stale or never existed. |
| C4 | ✅ FIXED | Same line 102 — "European Renaissance" replaced with "13th-century Scholasticism (Aquinas, Albertus Magnus, Latin Averroist movement at Paris and Oxford)" |

`docs/islam_stories_scaffolding.md` is **not** referenced by any code
path — `grep -rln scaffolding rag/ scripts/ episodes/ outlines/`
returned nothing. It's a planning doc, not script-generator input. The
seed entries in `rag/knowledge/seed_data.py:262` (Ibn Rushd) and
`:604` (Shajarat al-Durr) are accurate.

### C5 — wide doc sweep

A read-only Explore subagent scanned every `.md` in `docs/`. It flagged
~21 confident factoids that smell like LLM-summary distortion. Full
list in the previous response (chat transcript). Notable items:

- "Mansa Musa hajj crashed global gold market for 12 years" (`scaffolding.md:95`) — "12 years" is unsourced
- "Nizam al-Mulk … assassinated by Hashashin after 30 years" (`:105`) — he served ~20 years (1064–1092), not 30
- "Ibn Khaldun wrote Muqaddimah in 5 months in exile" (`:101`) — "5 months" is unsourced
- "Fatimah al-Fihri … oldest university in the world" (`:120`) — superlative, contested with Bologna depending on definition
- "Khalid ibn Walid — 100+ battles undefeated" (`:75`) — pop-history aggregation
- "800,000 killed" at end of Abbasid caliphate (`creative_vision.md:131`) — modern extrapolation, not a sourced figure

**Note on the sweep itself**: the agent flagged your C1 corrected text
as a hallucination, confusing Almohad caliph **Abu Yusuf Ya'qub
al-Mansur** (r. 1184–1199, the one who ordered Ibn Rushd's works
burned in 1195) with Abbasid al-Mansur al-Mutawakkil (9th c.). The C1
fix is correct; the agent was wrong on that one. Treat the rest as a
review queue, not a fix list.

---

## Part A — Code

| ID | Status | Evidence | Action |
|----|--------|----------|--------|
| A1 | ✅ FIXED | `rag/retrieval/orchestrator.py:222–230` docstring promised `character_context`, `world_context`, `religious_context`; return dict (now `:330–333`) had none of them. Empty knowledge layer to the script generator. | Added the three queries per `docs/islam_stories_rag_architecture.md`. Sanity script `scripts/audit/check_a1_orchestrator_keys.py` confirms 5/5/3 results on Khalid/Yarmouk. (gitignored — disk only) |
| A2 | ⚠️ PARTIAL | No live conflict — `api/main.py:1460` `TIER_CONFIG` is derived from `api/tier_limits.py::TIER_QUOTAS`, same source as `TIER_LIMITS_LEGACY`. Free tier api_limit = `0`, not `100`. The only real bug was `.get(tier, 100)` fallback at `api/main.py:182,1297` for unknown tiers. | Replaced fallback with `TIER_LIMITS["free"]` (= 0). |
| A3 | ⚠️ PARTIAL | `ingest/core.py:113` (djvu) / `:150` (pdf). Structural overlap ~50–60%, **not 90%** — djvu has form-feed strip + alpha-ratio gate + multilingual mode, pdf doesn't. | Extracted `_is_noise_line` + `_normalize_whitespace` helpers; format-specific differences kept inline. Output byte-identical on smoke test. |
| A6 | ✅ FIXED | Bare `except:` was at `api/main.py:1490` (audit said 1389-1400 — line numbers stale). Returned `(True, 0)` → silently allowed every request when the quota query failed. A second bare `except: pass` at `:1504` (in `log_usage`) is benign — just drops a usage row — left as-is. | Catches `psycopg2.Error` / `KeyError`, logs, returns `(False, -1)` so caller raises 429. TODO marker added for ops alerting. |
| A7 | ✅ FIXED | `_send_key_email` (`:952`), `_send_invoice_email` (`:1072`), `_send_payment_failed_email` (`:1185`) each carried ~80 lines of inline branded HTML (header, footer, "Questions? Contact" block). | Extracted `_branded_email_html(title, body_inner, accent_hex)`. Three send funcs collapsed to thin wrappers. ~200 lines of duplication removed. |

---

## Part B — Stats

| ID | Status | Evidence | Action |
|----|--------|----------|--------|
| B1 | ✅ FIXED | `api/main.py:1652` (now refactored) hardcoded `"72,000+ chunks from 136 authenticated"`. Live DB: **141,387 chunks / 257 sources**. Off by ~2× on both. | Added `_corpus_stats()` cached helper (5-min TTL, falls back to last-known on DB error). `EXPLORER_SYSTEM_PROMPT` now uses `{corpus_passages}/{corpus_sources}` placeholders substituted at request time. `/stats` refactored to call the helper. FastAPI `app.description` (line 87) updated to less-specific banner since it's not a live page. |
| B2 | ✅ FIXED | `landing/sources.html:467,472,473` hardcoded "216 Sources / 15 Eras / 265 Figures". Only `total-chunks` and `total-sources` were live. Live DB: **257 sources / 22 eras / 1839 figures**. | Replaced with `data-stat` spans wired to the same `/stats` endpoint `landing/index.html` already consumes. One source of truth across both pages. |

---

## Part D — Schema / scalability

| ID | Status | Evidence | Action |
|----|--------|----------|--------|
| D1 | ❌ STALE | `api_usage` table already exists with `id`, `api_key`, `endpoint`, `model`, `tokens_in/out`, `created_at`, `user_id`, `month_bucket`, three indexes. | None — already in place per `project_phase1_auth` work. |
| D2 | ✅ FIXED | `documents` had no btree on `source` (had `source_type`, `era`, `figures`, `content_tsv`, `content_trgm`, hnsw embedding). | `schema/0002_index_documents_source.sql` — applied (`CREATE INDEX CONCURRENTLY`). |
| D3 | ❌ STALE | `documents.content_tsv` column + `idx_documents_content_tsv` GIN + `documents_content_tsv_trg` trigger already exist (matches `project_hybrid_retrieval` memory note). | None. |
| D4 | ✅ FIXED | 74 dupe `(figure_a_id, figure_b_id, relationship)` triples; no UNIQUE; no self-ref CHECK. | `schema/0004_figure_relationships_unique.sql` — applied (deleted 80 rows, kept lowest id). UNIQUE + CHECK now enforced. |
| D5 | ✅ FIXED | `events.date_ce` is `text`, no integer column for ordering. Format mostly clean; only 4 rows non-numeric (`-539`, `-546`, `632-633`, `685-687`). | `schema/0005_events_date_ce_year.sql` — applied; 980 rows updated; `idx_events_date_ce_year` created. **One unparseable row remaining**: id `12854` "Battle near Iraq (53 CE)" — 2-digit year is below the regex's `{3,4}` floor. Spot-fix manually or relax regex. |
| D6 | ⏸️ HOLD | HNSW index on default params. Not touched per brief. | None — needs explicit approval for the 30-90 min rebuild. |

---

## Part E — Architectural drift (flag-only)

### E1: `documents.figures` (text[]) vs `chunk_metadata.figure_ids` (int[])

```
docs_with_figures_text    : 58,852
docs_with_figure_ids      : 117,320
divergent (text-but-no-id):      0
total docs                : 143,527
```

**Audit's premise is partly inverted.** Every document with a populated
`figures` text array also has `figure_ids` in `chunk_metadata` — no
cases where the text path updated but the id path didn't. The drift is
in the *other* direction: `chunk_metadata.figure_ids` covers ~2× as
many documents (117K) as `documents.figures` (59K). So the "two
systems" do diverge, but the int-id system is the broader, more
recently maintained one. Recommendation: either backfill
`documents.figures` from `chunk_metadata`, or deprecate
`documents.figures` entirely. Estimated scope: ~half a day to script
the backfill + a deprecation pass on writers.

### E2: free-text `documents.source` near-duplicates

After case-fold + hyphen/underscore/whitespace strip across all 257
distinct `documents.source` values:

```
Distinct normalized keys with >1 variant: 0
```

**No near-duplicates currently.** "Al-Tabari vs al-Tabari" type drift
is not present. The audit's recommended `sources` normalization table
is still defensible *prophylactically* (free-text source columns rot
over time as new ingest scripts come and go), but no current data
hygiene problem motivates it. Estimated scope if you want it
prophylactically: ~2 days for table + FK + writer migration.

---

## Sanity checks added

- `scripts/audit/check_a1_orchestrator_keys.py` — calls
  `retrieve_episode_context` and asserts the three context keys are
  populated (passes: 5/5/3 results on Khalid/Yarmouk). Lives under
  gitignored `scripts/` — disk only.

---

## Summary

| Bucket | Total | Fixed | Stale | Partial | Held | Flagged |
|--------|-------|-------|-------|---------|------|---------|
| C (history) | 5 | 3 | 1 | — | — | 1 |
| A (code) | 5 | 3 | — | 2 | — | — |
| B (stats) | 2 | 2 | — | — | — | — |
| D (schema) | 6 | 3 | 2 | — | 1 | — |
| E (drift) | 2 | — | — | — | — | 2 |

The audit was useful but ~30% of its specific claims were stale (D1,
D3, parts of A2, the C3 figure, the line numbers for A6) or overstated
(A3 duplication share, E2 source dupes that don't exist, E1 drift
direction). Net real fixes: 11. Flagged for your review: ~21 doc
items + 2 architectural items.

## Next steps you may want

- D6 HNSW retune — say the word, schedule for off-hours.
- C5 sweep follow-up — pick which of the ~21 flagged factoids to fix.
- E1 backfill — decide whether to backfill `documents.figures` from
  `chunk_metadata.figure_ids` or deprecate the text array.
- D5 leftover — fix `events.id=12854 date_ce='53'` manually.
