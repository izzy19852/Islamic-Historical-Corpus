# Islam Stories — Updated Production Scaffolding
# Incorporates: Story Registry, Scene Composition, RAG Audit,
# Two-Model Pipeline, Format Tiers, Dramatic Spine Classification
# Date: April 2026

---

## Pipeline Overview

```
KNOWLEDGE LAYER (67K chunks, 735 figures, 24 tables)
        ↓
CHARACTER BIBLE (762 markdown + 737 JSON profiles)
        ↓
STORY EXTRACTION AGENT (3 passes — figure, event, theme)
        ↓
STORY REGISTRY (500-1000+ stories, classified + sequenced)
        ↓
PRODUCTION QUEUE (dependency-ordered, format-assigned)
        ↓
OUTLINE GENERATOR (spine-aware, RAG-grounded)
        ↓
RAG ACCURACY AUDIT (every claim verified against corpus)
        ↓
AUDITED OUTLINE + AUDIT REPORT
        ↓
HUMAN APPROVAL (informed by audit flags — ONLY human checkpoint)
        ↓
SCRIPT GENERATOR (scene block composition → JSON manifest)
        ↓
CHARACTER REFERENCE IMAGES (generate once, carry forward per arc)
        ↓
NARRATION (ElevenLabs — master timeline)
        ↓
ASSET GENERATION (Kling character/establishing + Seedance battle)
        ↓
FFmpeg ASSEMBLY + STYLIZATION (unified color grade)
        ↓
PUBLISH (YouTube + Shorts extraction)
```

---

## Phase 0 — INFRASTRUCTURE (COMPLETE)

### What's Built

| Component | Status | Detail |
|---|---|---|
| GCP VM | ✅ | Postgres + pgvector, us-east1-d |
| RAG Corpus | ✅ | 128,356 chunks, 216 sources, 22 eras |
| Knowledge Graph | ✅ | 27 tables (see DB inventory below) |
| Figures | ✅ | 735 in DB, all with sensitivity tiers |
| Character Bibles (MD) | ✅ | 762 markdown files across 4 era folders |
| Character Bibles (JSON) | ✅ | 737 structured profiles |
| Visual Columns | ✅ | 634 Tier B/C with full costume/weapon/armor data |
| Embeddings | ✅ | Voyage-2 1024-dim, query_rag_multi working |
| Pilot Outlines | ✅ | 5 outlines drafted |
| Pilot Scripts | ✅ | 5 draft scripts |
| Episode Queue | ✅ | 7 episodes seeded |

### Database — 27 Tables (Current State)

| Table | Rows | Status |
|---|---|---|
| figures | 735 | Fully populated |
| documents (chunks) | 128,356 | All ingested |
| chunk_metadata | 24,220 | 37% linked |
| sources | 216 | Complete |
| events | 36 | Seeded |
| figure_themes | 87 | Partial |
| figure_motivations | 23 | Partial |
| figure_relationships | 17 | Sparse |
| figure_lineage | 16 | Sparse |
| figure_deaths | 15 | Sparse |
| figure_quotes | 13 | Sparse |
| scholarly_debates | 8 | Seeded |
| episode_queue | 7 | S1 seeded |
| source_relationships | 6 | Seeded |
| Empty tables (10) | 0 | See full list in DB docs |

### Figures by Tier

| Tier | Count | Visual Rule |
|---|---|---|
| S (Prophets/Angels) | 25 | Never depicted |
| A (Caliphs/Wives) | 76 | Silhouette only |
| B (Major historical) | 565 | Full depiction |
| C (Minor/supporting) | 69 | Full depiction |

### Figures by Era

rashidun: 295 | umayyad: 256 | abbasid: 152 | south_asia: 41 |
ottoman: 37 | crusades: 19 | mongol: 11 | persia: 10 |
andalusia: 10 | resistance_colonial: 8 | mamluk: 7 | africa: 5

---

## Phase 1 — STORY EXTRACTION (NEW)

### Purpose

Systematically mine the knowledge layer to identify every
episode-worthy story across 1,400 years of Islamic history.
The scholars already decided what was important — they wrote
it down. This phase surfaces it.

### 1.1 Story-Worthy Criteria

A story qualifies if it contains **at least one** dramatic core:

**1. TRANSFORMATION** — A person becomes fundamentally different.
Test: Can you describe a clear BEFORE and AFTER?
Examples: Khalid's conversion, Malik bin Dinar's dream,
Umar's transformation from enemy to defender.

**2. IMPOSSIBLE CHOICE** — Two irreconcilable goods or two evils.
Test: Would the audience not know what they'd do?
Examples: Abu Ubayda staying in the plague, Khalid's dismissal,
Husayn at Karbala.

**3. CONTRADICTION** — The person's life contains a paradox.
Test: Can you state it in one sentence as a paradox?
Examples: Wahshi killed the best and worst of men,
Tariq the freed slave who conquered an empire.

**4. STAND** — Refusal to bend at enormous personal cost.
Test: Does the refusal cost them something enormous?
Examples: Ahmad ibn Hanbal under torture, Bilal under the rock,
Omar al-Mukhtar's execution.

**5. UNINTENDED CONSEQUENCE** — Action creates unforeseen result.
Test: Does knowing the consequence reframe the action?
Examples: Yamama → Quran compilation, Mansa Musa's hajj
crashes Egypt's economy.

**6. ENCOUNTER** — Two worlds meet, neither is the same after.
Test: Does the meeting produce something new?
Examples: Saladin and Richard, the Baghdad translation movement,
Nana Asma'u bridging Islamic scholarship and West African tradition.

**7. LAST STAND / FINAL ACT** — The ending defines the life.
Test: Do the last words reframe everything before?
Examples: Khalid dying in bed ("I die like a camel dies"),
Abdullah ibn Zubayr's final speech to his mother Asma.

**8. MYSTERY / SILENCE** — The sources don't tell us something
important, and the gap IS the story.
Test: Is the gap more dramatic than any answer could be?
Examples: Why did Ali wait? What did Khalid really think
about the dismissal? What happened to Alexandria's library?

### 1.2 Format Tiers

Every extracted story gets classified by format:

**FULL EPISODE (15 min)**
- Multiple dramatic cores (2+)
- Multiple documented scenes with source detail
- Dramatic question that takes time to unfold
- Enough source material for ~3,000 words of narration
- Budget: $35-50 per episode

**SHORT EPISODE (5-8 min)**
- At least one dramatic core
- One powerful documented scene
- Dramatic question that resolves in one sequence
- Enough source material for ~800-1,500 words
- Budget: $12-20 per episode

**SHORT (60-90 sec)**
- One unforgettable moment
- Documented dialogue or last words
- Can be understood with no prior context
- Budget: $2-5 per short

**SUPPORTING APPEARANCE**
- Figure appears in others' stories
- Not enough solo narrative for own episode
- Gets a character reference image
- Tagged with which episodes they appear in

### 1.3 Dramatic Spine Types

Every story is classified by dramatic pattern:

| Spine | Core Emotion | Battle Weight | Seedance Budget |
|---|---|---|---|
| TRIAL | Empathy, admiration | None usually | ZERO |
| CONQUEST | Awe, tension, release | Heavy | HIGH (3-4 clips) |
| TRAGEDY | Sorrow, dread | Sometimes | LOW (0-2 clips) |
| RISE | Inspiration, investment | Sometimes | MEDIUM (1-2 clips) |
| SUCCESSION | Weight of power | None | ZERO |
| DISCOVERY | Wonder, depth | None | ZERO |
| RESISTANCE | Pride, defiance | Medium | MEDIUM (1-3 clips) |

### 1.4 The Three Extraction Passes

**Pass 1 — Figure-Anchored (PRIMARY)**
For each of 762 character bibles + 735 DB figures:
- Query RAG for all documented moments
- Classify each moment by dramatic core
- Assign format tier
- Assign dramatic spine
- Flag source coverage (rich / adequate / thin)
Model: Haiku (classification) + RAG queries
Est. cost: ~$8-15

**Pass 2 — Event-Anchored**
Sweep each era/region for events NOT captured by figures:
- Civilizational moments (library foundings, trade routes)
- Natural disasters, plagues, famines
- Architectural/scientific achievements
- Events where the civilization itself is the protagonist
Model: Haiku + RAG queries
Est. cost: ~$5-10

**Pass 3 — Thematic Thread Extraction**
For each of 20 themes in themes table:
- Find every instance across all eras
- Sequence chronologically
- Identify cross-era episode groupings
- (e.g., "The Four Abdullahs" as a group portrait SHORT EPISODE)
Model: Haiku + RAG queries
Est. cost: ~$3-5

**Total extraction cost: ~$20-30**

### 1.5 Story Registry Schema

```sql
CREATE TABLE story_registry (
    story_id            SERIAL PRIMARY KEY,

    -- IDENTITY
    title               TEXT NOT NULL,
    subtitle            TEXT,
    one_line            TEXT NOT NULL,
    dramatic_question   TEXT,

    -- TEMPORAL
    date_start_ce       INTEGER,
    date_end_ce         INTEGER,
    date_ah             TEXT,
    era                 TEXT,

    -- SPATIAL
    region              TEXT,
    location            TEXT,

    -- FIGURES
    primary_figure_id   INTEGER REFERENCES figures(id),
    figure_ids          INTEGER[],
    sensitivity_max     CHAR(1),

    -- CLASSIFICATION
    dramatic_spine      TEXT NOT NULL,
    dramatic_cores      TEXT[],
    story_scale         TEXT NOT NULL,
    has_battle          BOOLEAN DEFAULT FALSE,
    has_political       BOOLEAN DEFAULT FALSE,
    has_spiritual       BOOLEAN DEFAULT FALSE,

    -- FORMAT
    format              TEXT NOT NULL,
    runtime_target      INTEGER,

    -- SERIES PLACEMENT
    series_id           TEXT,
    season              INTEGER,
    episode_number      INTEGER,
    is_crossover        BOOLEAN DEFAULT FALSE,
    crossover_with      TEXT[],
    thread              TEXT,

    -- SOURCE GROUNDING
    source_coverage     TEXT,
    primary_sources     TEXT[],
    chunk_ids           INTEGER[],
    known_conflicts     TEXT[],

    -- PRODUCTION
    estimated_budget    TEXT,
    seedance_clips_est  INTEGER DEFAULT 0,
    kling_clips_est     INTEGER DEFAULT 0,
    depends_on          INTEGER[],

    -- STATUS
    status              TEXT DEFAULT 'EXTRACTED',
    -- EXTRACTED → VALIDATED → SEQUENCED → OUTLINED
    -- → AUDITED → APPROVED → SCRIPTED → PRODUCED → PUBLISHED

    -- EXTRACTION METADATA
    extraction_pass     TEXT,
    extraction_source   TEXT,
    confidence          TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),
    notes               TEXT
);
```

### 1.6 Post-Extraction Processing

**Deduplication:**
Yarmouk from Khalid's perspective + Yarmouk from Abu Ubayda's
perspective = one story, marked as crossover.
Agent identifies overlapping figure_ids + date ranges and merges.

**Series Grouping:**
Stories about the same figure → character arc series.
Stories about the same era → season grouping.
Thematic stories → thread assignment.

**Dependency Mapping:**
"This episode reveals information needed before that episode."
Produces a DAG (directed acyclic graph) of production order.

**GATE:** story_registry has 300+ stories with format + spine assigned.

---

## Phase 2 — OUTLINE GENERATION + RAG AUDIT

### 2.1 Outline Generator

The outline generator pulls the next story from the production
queue and builds a structured outline using:

**Inputs:**
- Story registry entry (dramatic spine, format, figures, era)
- Character bible entries for all involved figures
- RAG context from retrieve_episode_context()
- Scene block library (see Phase 3)
- Composition rules for the assigned spine type

**Output:**
A structured outline with:
- Cold open description
- Act-by-act scene breakdown
- Each scene tagged with block type
- Source citations for every factual claim
- Conflict flags where sources disagree
- Sensitivity tier flags

### 2.2 RAG Accuracy Audit (NEW — CRITICAL)

**The outline generator uses the RAG to write.
The audit agent checks what it wrote against the same RAG.**

The human reviewer should never be the fact-checker.
The audit does that automatically.

File: `rag/generation/audit_outline.py`

**What the audit checks:**

**1. Source Verification**
Every claim attributed to a source → query RAG for that claim.
Three outcomes:
- CONFIRMED — chunk exists, says what outline claims
- PARTIAL — source covers event but specific detail not found
  (may be ingestion gap or outline embellishment)
- NOT FOUND — no chunk supports this claim from this source

**2. Chronological Consistency**
Cross-reference events table to verify sequence.
Flag any anachronisms (e.g., referencing an event before it happened).

**3. Figure Presence Verification**
If outline places a figure at an event, check chunk_metadata
for that figure_id linked to that event_id.
Flag if no documented link exists.

**4. Sensitivity Tier Compliance**
Scan outline for any scene depicting Tier S or Tier A figures.
Check against depiction rules (Tier S = never, Tier A = silhouette).
Automatic flag on any violation.

**5. Source Conflict Detection**
If outline presents one version of events, check whether
source_relationships table contains a CONTRADICTS entry.
Flag if the outline chose a side without surfacing the dispute.

**6. Invented Detail Detection**
Identify sensory/narrative details (weather, emotions, physical
settings) that don't trace to any chunk.
Flag as NARRATIVE INVENTION — not necessarily wrong, but
the human reviewer needs to know what's sourced vs. dramatized.

**Audit output format:**

```
OUTLINE: s1e1_khalid_ibn_walid_outline.md

AUDIT SUMMARY:
  Claims checked:        23
  CONFIRMED:             17
  PARTIAL:               4
  NOT FOUND:             1
  INVENTED DETAIL:       1

  Chronological errors:  0
  Figure presence flags: 0
  Sensitivity violations: 0
  Unresolved conflicts:  1

DETAILED FLAGS:

[PARTIAL] Scene 4 — "Khalid broke nine swords at Mu'tah"
  Source cited: Al-Waqidi
  RAG result: Chunk 2847 mentions Mu'tah but the nine swords
  detail traced to a different narration. VERIFY attribution.

[NOT FOUND] Scene 8 — "Vahan sent a delegation before battle"
  Source cited: Al-Tabari
  RAG result: No chunk supports pre-battle delegation from
  Vahan in Al-Tabari. May be from secondary source. CHECK.

[CONFLICT] Scene 6 — Khalid's dismissal reason
  Outline presents: hero-worship concern only
  Al-Tabari (chunk 3112): financial mismanagement
  Al-Baladhuri (chunk 4501): hero-worship concern
  BOTH perspectives should be surfaced.

[INVENTED] Scene 3 — "thick with dust and smell of horses"
  No source documents atmospheric conditions.
  ACCEPTABLE as dramatization but mark accordingly.
```

**Cost per audit:** ~$0.10-0.20 (Haiku parsing + RAG queries)

### 2.3 Human Approval

The human receives:
1. The outline document
2. The audit report with all flags

They approve, reject, or send back for revision with full
knowledge of what's solid and what's shaky.

**This is the ONLY human checkpoint in the entire pipeline.**
Everything before it is automated extraction and verification.
Everything after it is automated production.

---

## Phase 3 — SCRIPT GENERATION + SCENE COMPOSITION

### 3.1 Scene Block Library

The script generator composes episodes from a vocabulary
of scene blocks. It does NOT follow a fixed template.
The dramatic spine determines which blocks are available
and how they're weighted.

**HOOK blocks** (every episode gets exactly 1):
- `HOOK_DRAMA` — character in crisis
- `HOOK_MYSTERY` — question posed, no answer yet
- `HOOK_SCALE` — the sheer size of what's coming

**WORLD blocks** (every episode gets 2-4):
- `ESTABLISHING_WIDE` — landscape, geography, no figures
- `ESTABLISHING_LIFE` — civilization texture, daily life
- `ESTABLISHING_CLOSE` — detail shot, object, texture of era
- `CONTEXT_MAP` — political/geographic map

**CHARACTER blocks** (every episode gets 3-5):
- `CHARACTER_REVEAL` — first appearance of figure
- `CHARACTER_WORLD` — figure in their environment
- `CHARACTER_DEVOTION` — prayer, study, spiritual moment
- `CHARACTER_JOURNEY` — traveling, riding, arriving
- `CHARACTER_DECISION` — the internal turning point

**CONFRONTATION blocks** (0-5 per episode):
- `POLITICAL_DRAMA` — council, argument, power play
- `BATTLE_ESTABLISH` — two forces, wide scale
- `BATTLE_CHARGE` — the collision (Seedance hero clip)
- `BATTLE_CLOSE` — close combat (Seedance hero clip)
- `BATTLE_TURN` — decisive moment (Seedance hero clip)
- `TACTICAL_MAP` — parchment battle map
- `CONFRONTATION_VERBAL` — two people, tension, no swords
- `CROWD_MOMENT` — public speech, gathering, protest

**EMOTION blocks** (every episode gets 2-4):
- `AFTERMATH_EMOTION` — the weight of what happened
- `AFTERMATH_COST` — what was lost
- `GRIEF` — mourning, loss
- `TRIUMPH_QUIET` — victory without celebration
- `SOLITUDE` — a figure alone with their thoughts

**SOURCE blocks** (every episode gets 2-3):
- `SOURCE_MOMENT` — manuscript, primary source cited
- `SOURCE_CONFLICT` — two sources disagree
- `SOURCE_SILENCE` — what the sources DON'T say matters

**RESOLUTION blocks** (every episode gets exactly 1):
- `RESOLUTION_DRAMA` — the question answered
- `RESOLUTION_OPEN` — deliberately unanswered
- `CIVILIZATION_SCALE` — pull back to world scale
- `CLOSING_IMAGE` — final lingering frame

### 3.2 Composition Rules

```
UNIVERSAL RULES (all episodes):
1. Total duration: per format tier (900s / 360-480s / 60-90s)
2. REQUIRED: 1 HOOK + 2+ WORLD + 3+ CHARACTER + 2+ SOURCE + 1 RESOLUTION + 1 CLOSING_IMAGE
3. Structure: COLD OPEN → ACT 1 (world) → ACT 2 (person) → ACT 3 (question)

SPINE-SPECIFIC RULES:

CONQUEST → weighted toward BATTLE_* blocks (Seedance budget HIGH)
TRIAL → weighted toward CHARACTER_DECISION, CONFRONTATION_VERBAL, SOLITUDE (Seedance ZERO)
TRAGEDY → weighted toward TENSION_BUILD, AFTERMATH_*, GRIEF (Seedance LOW)
RISE → weighted toward CHARACTER_JOURNEY, CHARACTER_REVEAL, TRIUMPH_QUIET (Seedance MEDIUM)
SUCCESSION → weighted toward POLITICAL_DRAMA, CONFRONTATION_VERBAL, CROWD_MOMENT (Seedance ZERO)
DISCOVERY → weighted toward CHARACTER_DEVOTION, ESTABLISHING_LIFE, SOLITUDE (Seedance ZERO)
RESISTANCE → weighted toward CHARACTER_DECISION, BATTLE_* smaller scale, AFTERMATH_COST (Seedance MEDIUM)

BUDGET CEILINGS:
- Battle episode: max 4 Seedance clips + 7 Kling clips
- Non-battle: max 0 Seedance + 9-10 Kling clips
- World-building: max 0 Seedance + 5-6 Kling + extra images
- Short episode: max 2 Seedance + 4 Kling clips
- Short: max 1 Seedance + 1 Kling clip
```

### 3.3 Script Output: JSON Scene Manifest

The script generator outputs a JSON manifest that the
production pipeline reads mechanically:

```json
{
  "episode_id": "s1e1_sword_of_god",
  "story_id": 42,
  "format": "full_episode",
  "runtime_target": 900,
  "dramatic_spine": "conquest",
  "characters": [
    {"id": "khalid_ibn_walid", "tier": "B", "needs_ref_image": true},
    {"id": "abu_ubayda", "tier": "B", "needs_ref_image": true}
  ],
  "scenes": [
    {
      "scene_id": "scene_01",
      "block_type": "HOOK_DRAMA",
      "act": "cold_open",
      "asset_type": "video_i2v",
      "model": "kling",
      "duration_seconds": 10,
      "character_refs": ["khalid_ibn_walid"],
      "visual_brief": "Battle-worn commander receives sealed letter...",
      "narration": "Syria, 636 CE. A messenger rides through dust...",
      "source_citation": "Al-Tabari, Tarikh, Vol. XI",
      "sensitivity_tier": null
    }
  ]
}
```

---

## Phase 4 — VISUAL PRODUCTION PIPELINE

### 4.1 Two-Model Split

| Model | Used For | Endpoint (fal.ai) | Cost/sec |
|---|---|---|---|
| Kling 3.0 I2V | Character drama, establishing, atmosphere | fal-ai/kling-video/v3/standard/image-to-video | $0.084 (no audio) |
| Kling 3.0 T2V | Landscapes, wide shots (no character) | fal-ai/kling-video/v3/standard/text-to-video | $0.084 (no audio) |
| Seedance 2.0 I2V | Battle hero clips (charge, combat, turning point) | bytedance/seedance-2.0/fast/image-to-video | $0.242 |
| AI Image (Seedream v4) | Maps, manuscripts, stills, reference images | fal-ai/bytedance/seedream/v4/text-to-image | $0.03/image |

### 4.2 Scene Type → Model Routing

| Block Type | Asset Type | Model |
|---|---|---|
| HOOK_DRAMA | Video I2V | Kling |
| HOOK_MYSTERY | Video I2V | Kling |
| HOOK_SCALE | Video T2V or Image+FFmpeg | Kling |
| ESTABLISHING_WIDE | Video T2V | Kling |
| ESTABLISHING_LIFE | Image + FFmpeg parallax | AI Image |
| ESTABLISHING_CLOSE | Image + FFmpeg zoom | AI Image |
| CONTEXT_MAP | Image + FFmpeg animation | AI Image |
| CHARACTER_REVEAL | Video I2V | Kling |
| CHARACTER_WORLD | Video I2V | Kling |
| CHARACTER_DEVOTION | Video I2V | Kling |
| CHARACTER_JOURNEY | Video I2V | Kling |
| CHARACTER_DECISION | Video I2V | Kling |
| POLITICAL_DRAMA | Video I2V | Kling |
| CONFRONTATION_VERBAL | Video I2V | Kling |
| CROWD_MOMENT | Video I2V or Image+FFmpeg | Kling |
| BATTLE_ESTABLISH | Video T2V | Kling |
| BATTLE_CHARGE | Video I2V | **Seedance** |
| BATTLE_CLOSE | Video I2V | **Seedance** |
| BATTLE_TURN | Video I2V | **Seedance** |
| TACTICAL_MAP | Image + FFmpeg arrows | AI Image |
| AFTERMATH_EMOTION | Video I2V | Kling |
| AFTERMATH_COST | Image + FFmpeg slow push | AI Image |
| GRIEF | Video I2V or Image+FFmpeg | Kling |
| TRIUMPH_QUIET | Video I2V | Kling |
| SOLITUDE | Video I2V | Kling |
| SOURCE_MOMENT | Image + FFmpeg ink reveal | AI Image |
| SOURCE_CONFLICT | Image + FFmpeg split | AI Image |
| SOURCE_SILENCE | Image + FFmpeg | AI Image |
| RESOLUTION_DRAMA | Video I2V | Kling |
| RESOLUTION_OPEN | Video I2V or Image+FFmpeg | Kling |
| CIVILIZATION_SCALE | Video T2V or Image+FFmpeg | Kling |
| CLOSING_IMAGE | Image + FFmpeg | AI Image |
| TITLE_CARD | FFmpeg text overlay | FFmpeg |

### 4.3 Character Reference Image System

Before any video generation begins per episode:

1. Check `assets/{series}/characters/` for existing refs
2. For new characters: generate via Seedream v4 using
   kling_full_character column from figures table
3. Store reference images — carry forward across entire arc
4. ALL I2V calls for that character use the same reference

Khalid's reference image generated once → used in all 4 episodes.
Supporting characters introduced in Ep 1 → same refs in Ep 2+.

### 4.4 FFmpeg Stylization Layer

Applied to ALL assets uniformly before final assembly:

```bash
ffmpeg -i input.mp4 \
  -vf "curves=preset=cross_process, \
       eq=saturation=0.85:contrast=1.1:brightness=-0.02, \
       noise=alls=8:allf=t, \
       vignette=PI/4" \
  -c:a copy output_graded.mp4
```

This unifies Kling clips, Seedance clips, and AI images
into one cohesive visual identity. The exact filter chain
is tuned during pilot visual test and locked for all episodes.

### 4.5 Assembly Pipeline

```
1. Character reference images (generate or retrieve)
2. Narration audio (ElevenLabs) → master timeline
3. Whisper timestamps → per-scene timing locked
4. Asset generation (parallel, by model routing)
5. Quality gate (duration, face match, content filter)
6. FFmpeg color grade (uniform across all assets)
7. FFmpeg assembly (narration + assets + score + transitions)
8. Subtitle burn-in (from Whisper)
9. Shorts extraction (best 60-90s clips → 9:16 reformat)
```

---

## Phase 5 — COST MODEL

### Per-Episode Cost by Format

| Component | Full (15 min) | Short (5-8 min) | Short (60-90s) |
|---|---|---|---|
| RAG + script (Claude API) | ~$0.30 | ~$0.15 | ~$0.05 |
| Outline audit (Haiku) | ~$0.15 | ~$0.10 | N/A |
| Kling video clips | ~$5.50 | ~$2.50 | ~$0.80 |
| Seedance battle clips | ~$5.80 (if battle) | ~$2.40 (if battle) | ~$2.40 (if battle) |
| AI images | ~$0.30 | ~$0.15 | ~$0.03 |
| Narration (ElevenLabs) | ~$3-5 | ~$1.50 | ~$0.30 |
| Music (amortized) | ~$0 | ~$0 | ~$0 |
| Whisper subtitles | ~$0.12 | ~$0.06 | ~$0.02 |
| **Raw total** | **~$15-17** | **~$7-9** | **~$3-4** |
| **With 3x iteration** | **~$40-50** | **~$18-25** | **~$5-8** |

### Budget by Spine Type (Full Episode)

| Spine | Seedance Clips | Kling Clips | Images | Est. Total |
|---|---|---|---|---|
| CONQUEST | 3-4 | 7-8 | 8-10 | $40-50 |
| RESISTANCE | 1-3 | 7-8 | 8-10 | $30-40 |
| RISE | 1-2 | 7-8 | 8-10 | $28-38 |
| TRAGEDY | 0-2 | 7-8 | 8-10 | $25-35 |
| TRIAL | 0 | 8-10 | 8-10 | $22-30 |
| SUCCESSION | 0 | 8-10 | 8-10 | $22-30 |
| DISCOVERY | 0 | 5-7 | 10-12 | $18-25 |

### Runway Cost Forecast

| Content | Count | Avg. Cost | Total |
|---|---|---|---|
| Full episodes | 400 | $40 | $16,000 |
| Short episodes | 300 | $20 | $6,000 |
| Shorts | 500 | $5 | $2,500 |
| **Total content runway** | **1,200** | | **~$24,500** |

---

## Phase 6 — BUILD ORDER (Shorts-First Launch Strategy)

### Rationale

Instead of committing $40-50 to a full 15-minute pilot with no audience,
produce 20-40 shorts ($1.50-4.00 each) to test stories, eras, figures,
and hook styles against real viewers. Let the data tell you which full
episodes to produce first. Every short is a modular piece that stacks
into future full episodes — reference images carry forward, narration
scenes get reused, battle clips become cold opens.

Target: 2,000+ subscribers before first full episode ships.

### Step 1: Story Registry Table
Add story_registry to schema_graph.py.
Run migration on GCP VM.

### Step 2: Shorts-Priority Extraction (Pass 1A)
File: `rag/knowledge/extract_stories.py` (with --format-priority=short flag)
Process: Run Pass 1 (figure-anchored) across all 762 character bibles
but PRIORITIZE extraction of SHORT-format stories first:
- Documented last words (figure_deaths — expand from 15 rows)
- Documented quotes (figure_quotes — expand from 13 rows)
- Single transformation moments
- Famous stands and refusals
- Contradictions statable in one sentence
- "One unforgettable moment" candidates
Model: Haiku | Cost: ~$5-8
GATE: 50-100 short candidates extracted and classified

### Step 3: Spine Classification + Scene Blocks
File: `rag/generation/spine_classifier.py`
File: `rag/generation/scene_blocks.py`
Build the classification and composition systems.
Needed even for shorts — each short is 1-3 blocks composed
by the same rules as full episodes.

### Step 4: Shorts Production — Batch 1 (20-40 shorts)
For each short candidate:
  a. Generate character reference image (Seedream v4)
  b. Generate narration (ElevenLabs — 60-90 seconds)
  c. Generate 1 Kling video clip (I2V with character ref)
     OR 1 Seedance clip (if battle moment)
  d. Generate 1-2 AI images with FFmpeg motion
  e. FFmpeg assembly + color grade
  f. YouTube Shorts metadata (from marketing system)

Distribute across eras and figure types:
- 8-10 Rashidun shorts
- 5-6 Umayyad/Abbasid shorts
- 3-4 Andalusia/Ottoman shorts
- 3-4 Scholar/Mystic shorts
- 3-4 Women thread shorts
- 2-3 Resistance/Colonial shorts

Cost: ~$60-120 total (~$3 average per short)
Timeline: 2-3 weeks production, publish 1-2 per day

### Step 5: Measure + Analyze (Week 4-5)
Track per short:
- Views (first 48 hours, first 7 days)
- Subscriber conversion rate
- Comments (what are people asking for more of?)
- Completion rate (are people watching to the end?)
- Share rate

Classify results:
- TOP TIER: 50K+ views → immediate short episode candidate
- STRONG: 10-50K views → full episode candidate
- MODERATE: 1-10K views → supporting content
- WEAK: <1K views → investigate why (era? figure? hook?)

### Step 6: Full Story Extraction (Passes 1B, 2, 3 — parallel with Step 5)
Complete the remaining extraction passes:
- Pass 1B: remaining full episode + short episode stories
- Pass 2: event-anchored extraction
- Pass 3: thematic thread extraction
Deduplicate + sequence + dependency mapping.
Model: Haiku | Cost: ~$15-22
GATE: story_registry has 300+ unique stories across all format tiers

### Step 7: Short Episodes for Top Performers (Week 5-8)
Take the 5-10 best-performing shorts and expand to 5-8 min episodes.
The short's reference images, narration scene, and video clip
carry forward — you're adding context around a proven moment.

For each short episode:
  a. Pull from story_registry (already classified)
  b. Generate outline with spine classification
  c. Run RAG accuracy audit
  d. Human approval (outline + audit report)
  e. Script generation → JSON manifest
  f. Generate 3-4 Kling clips + 0-2 Seedance clips + 4-5 images
  g. Narration (800-1,500 words)
  h. FFmpeg assembly + color grade

Cost: ~$12-20 each | Produce 5-10 episodes
Timeline: 3-4 weeks

### Step 8: Outline Audit System
File: `rag/generation/audit_outline.py`
Build and test the RAG accuracy audit system.
Run against existing pilot outlines to validate.
Required before any full episode production.

### Step 9: Update Existing Pipeline Files
- `episode_outline.py` → spine-aware outline generation
- `script_prompt.py` → scene block composition + JSON manifest
- `visual_brief.py` → two-model routing (Kling + Seedance)
- `seed_queue.py` → pulls from story_registry

### Step 10: Pilot Visual Test (Seedance vs Kling battle quality)
Run head-to-head comparison:
- Same 3 battle prompts, both models
- Evaluate fight choreography, character consistency, dust physics
- Lock the two-model split based on results
- Tune FFmpeg stylization filter chain
GATE: visual style locked for production

### Step 11: First Full Episode (Week 8-12)
Produced for a PROVEN audience. The story is chosen based on
shorts performance data, not guesswork.

The cold open is likely an already-published short with known metrics.
The audience is already asking for it in the comments.

Full pipeline: registry → outline → RAG audit → approve →
script → manifest → character refs → narration → assets →
assembly → publish.

Cost: ~$40-50 | GATE: retention measured (target >50% avg view duration)

### Step 12: First Arc + Scale
If pilot retains:
- Produce remaining 3-4 episodes of the character arc
- Reference images carry forward from pilot
- Continue shorts production (2-3 per week)
- Begin parallel series based on performance data

Production rhythm:
- 1 full episode every 3-4 weeks
- 1 short episode every 1-2 weeks
- 2-3 shorts per week (ongoing)
- Registry provides the runway — never run out of stories

---

## File Inventory (New + Updated)

### New Files

```
rag/knowledge/
  extract_stories.py              # Pass 1: figure-anchored extraction
  extract_stories_events.py       # Pass 2: event-anchored extraction
  extract_stories_themes.py       # Pass 3: thematic thread extraction
  deduplicate_stories.py          # Collapse overlapping stories
  sequence_stories.py             # Dependency mapping + production queue

rag/generation/
  scene_blocks.py                 # Block library + composition rules
  spine_classifier.py             # Dramatic spine definitions + assignment
  audit_outline.py                # RAG accuracy audit

rag/pipeline/
  story_registry.py               # Registry queries + queue management
  generate_character_refs.py      # Reference image generation
  generate_assets.py              # Kling + Seedance routing
```

### Updated Files

```
rag/knowledge/
  schema_graph.py                 # ADD: story_registry table

rag/generation/
  episode_outline.py              # UPDATE: spine-aware, receives block library
  script_prompt.py                # UPDATE: scene block composition, JSON manifest output
  visual_brief.py                 # UPDATE: two-model routing

rag/pipeline/
  seed_queue.py                   # UPDATE: pulls from story_registry
  ffmpeg_assemble.py              # UPDATE: stylization layer added
```

### New Documentation

```
docs/
  islam_stories_scene_structure.md       # Scene block system
  islam_stories_story_criteria.md        # TV-worthy criteria + format tiers
  islam_stories_scaffolding_v2.md        # This document
```

---

## Decisions Locked in This Document

| Decision | Resolution |
|---|---|
| Narration vs dialogue | Narration only. No invented dialogue. |
| Video models | Kling 3.0 for character/establishing. Seedance 2.0 for battle hero clips. |
| API platform | fal.ai (familiar, single SDK, pay-per-use) |
| Audio on video clips | OFF. Narration + score added in post via FFmpeg. |
| Episode length | 15 min (full), 5-8 min (short), 60-90s (shorts) |
| Scene structure | Dynamic block composition, not fixed template |
| Story source | RAG extraction, not manual selection |
| Human checkpoint | Outline + audit report. One checkpoint only. |
| Fact checking | Automated RAG audit before human sees outline |
| Character consistency | Reference images generated once, carried forward per arc |
| Visual unification | FFmpeg color grade + grain applied to all assets uniformly |
| Format tiers | Full / Short / Short / Supporting — nothing excluded |
| Launch strategy | Shorts-first. 20-40 shorts before first full episode. Data-driven escalation. |

---

## What's NOT Decided Yet

| Decision | Options | Blocking |
|---|---|---|
| ElevenLabs voice | Audition required | Audio pipeline |
| Music approach | Epidemic Sound vs Artlist vs AI composition | Assembly |
| Exact FFmpeg filter chain | Tuned during pilot visual test | Post-production |
| Seedance vs Kling battle quality | Head-to-head test pending | Model routing confirmation |
| Higgsfield API vs fal.ai for Seedance | Access confirmed on both, pricing favors fal | Cost |
| Shorts publish cadence | 1/day vs 2/day vs batch drops | Weeks 1-4 |
| First full episode story | Determined by shorts performance data | Week 8-12 |
