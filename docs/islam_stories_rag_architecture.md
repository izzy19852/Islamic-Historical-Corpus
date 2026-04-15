# Islam Stories — RAG Knowledge Base Architecture
## Data Sources, Connection Logic, and Grounded Script Generation

---

## 1. The Core Problem: Dumb Retrieval vs. Grounded Generation

### What the RAG Does Right Now (Phase 1A)

When you query "Khalid ibn Walid Yarmouk," the system:
1. Embeds the query into a 1024-dim vector
2. Runs cosine similarity against all chunk vectors
3. Returns the top N most similar chunks by distance

That's it. No understanding of relationships. No conflict detection. No source weighting. No coherence checking.

### The Risk

The script generator receives 20 chunks that might include:
- Al-Tabari's account of Yarmouk (primary, reliable, eyewitness chain)
- A Quran verse about patience in battle (thematically similar, not about Yarmouk)
- An index page from Tabari Vol 11 (noise)
- A hadith about courage (tangentially related)
- Two chunks that directly contradict each other

Claude weaves them into a script without knowing which is authoritative. The result is plausible-sounding but potentially inaccurate, unsourced, or subtly wrong.

### The Solution: No Neural Network Needed

The question "is it a neural network?" has a simple answer: no. A neural network would try to learn relationships from data. Your corpus is too small and too specialized for a neural network to learn reliable Islamic historical relationships — it would hallucinate connections.

What you need instead is **explicit, human-verified relationship encoding** — a structured knowledge graph sitting on top of the vector store where every conflict flag is deliberately placed and every source reliability ranking is based on Islamic scholarly consensus (sahih/hasan/daif), not ML inference.

This is how professional historians work: not by pattern-matching, but by explicitly tracking which sources say what and where they disagree.

---

## 2. The Four-Layer Architecture

```
Layer 1: Vector Store (BUILT)
  Raw semantic similarity retrieval
  Fast, broad, catches relevant content
  3,745+ chunks across primary sources
        ↓
Layer 2: Knowledge Graph (TO BUILD — Phase 1A+)
  Relationships between figures, events, sources
  Source reliability rankings
  Conflict/contradiction flags
  Chronological ordering
        ↓
Layer 3: Retrieval Orchestrator (TO BUILD — Phase 1B)
  Smart multi-query planner
  Pulls from both vector store and knowledge graph
  Ranks and filters results
  Flags conflicts BEFORE passing to LLM
        ↓
Layer 4: Grounded Script Generator (TO BUILD — Phase 1C)
  Receives pre-structured, pre-validated context
  Must cite every claim to a source
  Must surface conflicts as narrative tension
  Cannot invent facts not in retrieved context
```

---

## 3. Layer 2 — Knowledge Graph Schema

### New Tables Required in PostgreSQL

**Table: `figures`**
Every historical figure with sensitivity tier, era, series placement.

```sql
CREATE TABLE figures (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,          -- canonical name
    name_variants   TEXT[],                 -- alt spellings/transliterations
    sensitivity_tier CHAR(1) NOT NULL,      -- S / A / B / C
    era             TEXT[],                 -- which eras they appear in
    series          TEXT[],                 -- which Islam Stories series
    birth_death     TEXT,                   -- approximate dates CE
    dramatic_question TEXT,                 -- the core human question
    primary_sources TEXT[],                 -- which sources cover them
    created_at      TIMESTAMP DEFAULT NOW()
);
```

**Table: `events`**
Every major battle/event with date, location, era, participants.

```sql
CREATE TABLE events (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,          -- canonical event name
    name_variants   TEXT[],
    date_ce         TEXT,                   -- approximate CE date
    date_ah         TEXT,                   -- AH date
    location        TEXT,
    era             TEXT,                   -- series era
    figure_ids      INT[],                  -- who was involved
    significance    TEXT,                   -- one sentence summary
    created_at      TIMESTAMP DEFAULT NOW()
);
```

**Table: `source_relationships`**
The critical table — which sources cover which events and how they relate.

```sql
CREATE TABLE source_relationships (
    id              SERIAL PRIMARY KEY,
    source_a        TEXT NOT NULL,          -- e.g. "al-tabari-v11"
    source_b        TEXT NOT NULL,          -- e.g. "al-baladhuri-v1"
    event_id        INT REFERENCES events(id),
    relationship    TEXT NOT NULL,
    -- CORROBORATES: both sources agree on the facts
    -- CONTRADICTS: sources give different accounts
    -- SUPPLEMENTS: source_b adds detail not in source_a
    -- CHALLENGES: source_b questions source_a's claim
    -- EARLIER_ACCOUNT: source_a is older, source_b is later
    -- LATER_COMPILATION: source_b draws from source_a
    conflict_note   TEXT,                   -- what specifically differs
    reliability_note TEXT,                  -- which is more reliable and why
    scholarly_consensus TEXT,               -- what modern historians say
    created_at      TIMESTAMP DEFAULT NOW()
);
```

**Table: `chunk_metadata`**
Links every chunk to the figure and event it describes.

```sql
CREATE TABLE chunk_metadata (
    id              SERIAL PRIMARY KEY,
    chunk_id        INT REFERENCES documents(id),
    figure_ids      INT[],                  -- figures mentioned
    event_id        INT REFERENCES events(id),
    account_type    TEXT,
    -- eyewitness: author was present
    -- transmitted: author records chain of narrators
    -- later_compilation: author compiled from earlier sources
    -- commentary: scholarly analysis
    chain_strength  TEXT,                   -- sahih/hasan/daif/unknown
    conflict_flag   BOOLEAN DEFAULT FALSE,  -- another source contradicts this
    conflict_note   TEXT,                   -- brief description of the conflict
    noise_flag      BOOLEAN DEFAULT FALSE,  -- index/bibliography page
    created_at      TIMESTAMP DEFAULT NOW()
);
```

### Why This Works

When the script generator asks about Khalid ibn Walid at Yarmouk, the orchestrator:
1. Retrieves relevant chunks (Layer 1)
2. Checks chunk_metadata to filter out noise and get reliability scores
3. Checks source_relationships to find known conflicts between sources
4. Assembles a structured context packet with conflicts pre-flagged
5. Passes structured packet to Claude — not raw chunks

Claude never sees random text. It sees attributed, ranked, conflict-annotated content.

---

## 4. Layer 3 — Retrieval Orchestrator

```python
def retrieve_episode_context(
    figure: str,
    event: str,
    era: str,
    series: str
) -> dict:
    """
    Multi-query retrieval with conflict detection.
    Returns structured context packet, not raw chunks.
    """

    # Query 1: Primary narrative accounts
    # Only primary_arabic sources, non-noise chunks
    primary = query_rag(
        f"{figure} {event}",
        source_type="primary_arabic",
        n_results=10,
        exclude_noise=True,
        account_types=["eyewitness", "transmitted"]
    )

    # Query 2: Character context
    # What sources say about this figure generally
    character = query_rag(
        f"{figure} biography character personality",
        figures=[figure],
        n_results=5,
        exclude_noise=True
    )

    # Query 3: World/geographic context
    # Era-specific setting details
    context = query_rag(
        f"{era} historical context geography society",
        era=era,
        n_results=5
    )

    # Query 4: Hadith/Quran support
    # Religious context that may be referenced
    religious = query_rag(
        f"{event} {figure} patience courage leadership",
        source_type="hadith",
        n_results=3
    )

    # Get pre-flagged conflicts from knowledge graph
    event_id = get_event_id(event)
    conflicts = get_conflicts_for_event(event_id)

    # Rank primary accounts by reliability
    ranked_primary = rank_by_reliability(primary)

    # Build source attribution map
    source_map = {
        chunk["source"]: {
            "reliability": chunk["chain_strength"],
            "account_type": chunk["account_type"],
            "conflict_flag": chunk["conflict_flag"]
        }
        for chunk in ranked_primary
    }

    return {
        "primary_accounts": ranked_primary,
        "character_context": character,
        "world_context": context,
        "religious_context": religious,
        "conflicts": conflicts,           # pre-flagged disagreements
        "source_map": source_map,         # attribution per source
        "coverage_score": len(ranked_primary),  # how much source material exists
        "conflict_count": len(conflicts)   # how contested this history is
    }
```

### Reliability Ranking Function

```python
def rank_by_reliability(chunks: list) -> list:
    """
    Rank chunks by source reliability for this topic.
    Islamic scholarly consensus on source reliability.
    """
    reliability_order = {
        "eyewitness": 1,        # author was present
        "transmitted": 2,       # direct chain to eyewitness
        "later_compilation": 3, # compiled from earlier sources
        "commentary": 4,        # scholarly analysis
    }

    chain_order = {
        "sahih": 1,   # sound chain of transmission
        "hasan": 2,   # good chain, minor weakness
        "daif": 3,    # weak chain, use with caution
        "scholarly": 2, # modern scholarly source
        "unknown": 4,
    }

    def score(chunk):
        account_score = reliability_order.get(
            chunk.get("account_type", "unknown"), 5
        )
        chain_score = chain_order.get(
            chunk.get("chain_strength", "unknown"), 5
        )
        # Penalize noise-flagged chunks
        noise_penalty = 10 if chunk.get("noise_flag") else 0
        return account_score + chain_score + noise_penalty

    return sorted(chunks, key=score)
```

---

## 5. Layer 4 — Grounded Script Generator Prompt

The script generation prompt changes fundamentally. Instead of "here are some chunks, write a script," it becomes a structured, citation-enforced generation:

```
You are writing for Islam Stories — a cinematic Islamic history
documentary series. Every claim you make must be traceable to a
specific source. History that cannot be sourced is not mentioned.

═══════════════════════════════════════════════
STRUCTURED CONTEXT PACKET
═══════════════════════════════════════════════

PRIMARY ACCOUNTS (ranked by reliability):

[SOURCE 1 — HIGHEST RELIABILITY]
Source: Al-Tabari, History of Prophets and Kings, Vol 11
Translator: Khalid Blankinship (SUNY Press)
Compiled: ~915 CE | Account type: Transmitted narration
Chain strength: Scholarly
Text: "[chunk content]"

[SOURCE 2]
Source: Al-Baladhuri, Futuh al-Buldan (Origins of Islamic State)
Translator: Philip Hitti (Columbia University)
Compiled: ~892 CE | Account type: Transmitted narration
Chain strength: Scholarly
Text: "[chunk content]"

CHARACTER CONTEXT:
Source: [source] | "[what sources say about figure's character]"

WORLD CONTEXT:
Source: [source] | "[era/geographic setting details]"

⚠️ CONFLICT FLAGS — YOU MUST SURFACE THESE IN THE SCRIPT:

CONFLICT 1:
  Al-Tabari records: "[version A of event]"
  Al-Baladhuri records: "[version B of event]"
  Scholarly note: "[what modern historians say about this discrepancy]"
  → Script instruction: Present both accounts. Attribute each. Do not resolve.

CONFLICT 2:
  Only ONE source covers this claim: [source name]
  → Script instruction: Flag as "in a narration reported only by [source]..."

═══════════════════════════════════════════════
GROUNDING RULES — NON-NEGOTIABLE
═══════════════════════════════════════════════

RULE 1 — CITE EVERYTHING
Every specific historical claim must be attributed inline:
  CORRECT: "Al-Tabari records that Khalid..."
  CORRECT: "According to Ibn Hisham..."
  WRONG:   "Khalid then rode to..." (no attribution)

RULE 2 — SURFACE CONFLICTS AS DRAMA
When sources disagree, this is your most powerful narrative tool.
  CORRECT: "Al-Tabari gives one account of this moment. Al-Baladhuri
            records something different. Both cannot be true. History
            has never resolved the contradiction."
  WRONG:   Smoothing over the conflict with a single version.

RULE 3 — ACKNOWLEDGE GAPS
When the historical record is silent, say so:
  CORRECT: "History is silent on what he said when he read the letter."
  WRONG:   Inventing dialogue or inner thoughts.

RULE 4 — FLAG WEAK SOURCES
When a claim rests on a single or weak source:
  CORRECT: "In a narration reported only by Al-Waqidi — a source
            some scholars consider unreliable — he is said to have..."
  WRONG:   Presenting a weak narration with same authority as sahih.

RULE 5 — NEVER INVENT
You may not add historical detail not present in the provided context.
If the context does not contain it, it does not appear in the script.

═══════════════════════════════════════════════
OUTPUT FORMAT
═══════════════════════════════════════════════

[NARRATION] — spoken text using Fall of Civilizations register
[SOURCE] — which source this claim comes from
[SCENE_TYPE] — kling_battle | kling_narrative | kling_establishing |
               ai_map | manuscript_quote | still_image
[VISUAL_PROMPT] — complete Kling-ready prompt
[DURATION_EST] — estimated seconds from narration length
[CONFLICT_FLAG] — "DISPUTED: [brief note]" or blank
[SENSITIVITY] — tier flag if relevant (Tier A/S figures referenced)
```

---

## 6. What's Missing vs. What's Built

### Current Status

| Layer | Status | Notes |
|---|---|---|
| Vector store (pgvector) | ✅ Built | 3,745+ chunks, scores 0.75-0.83 |
| Fawazahmed0 hadith API | ❌ Not built | No key needed, 9 collections |
| AlQuran.cloud API | ❌ Not built | Supplementary translations |
| Remaining Al-Tabari vols | ❌ Partial | 7 of 40 ingested |
| Ibn Battuta Travels | ❌ Not built | Africa + South Asia critical gap |
| Evliya Celebi | ❌ Not built | Ottoman primary source |
| Saladin (Baha ad-Din) | ❌ Not built | Crusades critical gap |
| OpenITI corpus (sampled) | ❌ Not built | Largest remaining source |
| Timbuktu manuscripts (LOC) | ❌ Not built | Africa series |
| Knowledge graph schema | ❌ Not built | Needed for grounding |
| Chunk metadata linking | ❌ Not built | Needed for grounding |
| Source relationships table | ❌ Not built | Needed for conflict detection |
| Retrieval orchestrator | ❌ Not built | Needed for grounding |
| Grounded script prompt | ❌ Not built | Phase 1C |

---

## 7. All Data Sources — Complete Inventory

### Tier A: Free APIs (No Key Required)

| Source | URL | Content | Status |
|---|---|---|---|
| Quran.com API | api.quran.com/api/v4/ | Full Quran, 90+ translations | ✅ Ingested |
| Fawazahmed0 Hadith | cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/ | 9 hadith collections, no rate limit | ❌ Not built |
| AlQuran.cloud | api.alquran.cloud/v1/ | Quran + Pickthall + Yusuf Ali translations | ❌ Not built |
| Sunnah.com API | api.sunnah.com/v1/ | All major hadith collections | ⏳ Key pending |

### Tier B: Internet Archive — Free PDFs

**Primary Islamic Sources (Arabic, English translation)**

| Source | Archive ID | Era Coverage | Status |
|---|---|---|---|
| Al-Tabari Vol 10 | history-of-al-tabarri | Rashidun — Ridda Wars 632-633 | ✅ Ingested |
| Al-Tabari Vol 11 | history-of-al-tabarri | Rashidun — Challenge to Empires 633-635 | ✅ Ingested |
| Al-Tabari Vol 12 | history-of-al-tabarri | Rashidun — Qadisiyyah, Syria 635-637 | ✅ Ingested |
| Al-Tabari Vol 13 | history-of-al-tabarri | Rashidun — Iraq, Egypt, Umar | ✅ Ingested |
| Al-Tabari Vol 15 | history-of-al-tabarri | Rashidun — Crisis, Uthman | ✅ Ingested |
| Al-Tabari Vol 17 | history-of-al-tabarri | Rashidun/Umayyad — Fitna, Ali | ✅ Ingested |
| Al-Tabari Vol 19 | history-of-al-tabarri | Umayyad — Yazid, Karbala 680 | ✅ Ingested |
| Al-Tabari Vol 14 | history-of-al-tabarri | Rashidun — Conquest of Iran | ❌ Not ingested |
| Al-Tabari Vol 16 | history-of-al-tabarri | Rashidun — Opposition to Uthman | ❌ Not ingested |
| Al-Tabari Vol 18 | history-of-al-tabarri | Umayyad — Caliphate of Muawiyah 661-680 | ❌ Not ingested |
| Al-Tabari Vol 20 | history-of-al-tabarri | Umayyad — Collapse of Sufyanids | ❌ Not ingested |
| Al-Tabari Vol 21 | history-of-al-tabarri | Umayyad — Victory of Marwanids | ❌ Not ingested |
| Al-Tabari Vol 22 | history-of-al-tabarri | Umayyad — Abd al-Malik 693-701 | ❌ Not ingested |
| Al-Tabari Vol 23 | history-of-al-tabarri | Umayyad — al-Walid 700-715 | ❌ Not ingested |
| Al-Tabari Vol 24 | history-of-al-tabarri | Umayyad — Sulayman, Umar II, Yazid II | ❌ Not ingested |
| Al-Tabari Vol 25 | history-of-al-tabarri | Umayyad — End of Expansion, Hisham | ❌ Not ingested |
| Al-Tabari Vol 26 | history-of-al-tabarri | Umawyad — Waning, Prelude to Revolution | ❌ Not ingested |
| Al-Tabari Vol 27 | history-of-al-tabarri | Abbasid Revolution 743-750 | ❌ Not ingested |
| Al-Tabari Vol 28 | history-of-al-tabarri | Abbasid — Early al-Mansur | ❌ Not ingested |
| Al-Tabari Vol 29 | history-of-al-tabarri | Abbasid — al-Mansur and al-Mahdi 763-786 | ❌ Not ingested |
| Al-Tabari Vol 30 | history-of-al-tabarri | Abbasid — Harun al-Rashid | ❌ Not ingested |
| Al-Tabari Vol 31 | history-of-al-tabarri | Abbasid — War of Brothers al-Amin/al-Mamun | ❌ Not ingested |
| Al-Tabari Vol 32 | history-of-al-tabarri | Abbasid — Reunification | ❌ Not ingested |
| Al-Tabari Vol 33 | history-of-al-tabarri | Abbasid — Northern Frontiers | ❌ Not ingested |
| Al-Tabari Vol 34 | history-of-al-tabarri | Abbasid — al-Mutawakkil, decline begins | ❌ Not ingested |
| Al-Tabari Vol 36 | history-of-al-tabarri | Abbasid — Incipient Decline | ❌ Not ingested |
| Al-Tabari Vol 38 | history-of-al-tabarri | Abbasid — Return to Baghdad | ❌ Not ingested |
| Al-Baladhuri Vol 1 | originsislamics00hittgoog | Conquests: Arabia, Syria, Egypt, Spain | ✅ Ingested |
| Al-Baladhuri Vol 2 | in.ernet.dli.2015.175259 | Conquests: Iraq, Persia, Sindh | ✅ Ingested |
| Ibn Hisham Sira | seerat-ibn-e-hisham-english-translation-2nd-edition | Prophet era, early companions | ❌ Not ingested |
| Guillaume (Ibn Ishaq) | lifeofmuhammadtr0000ibnh | Earliest biography, pre-dates Ibn Hisham | ❌ Not ingested |
| Ibn Khaldun Muqaddimah V1 | THEMUQADDIMAHOFIBNKHALDUNVOLUME1 | Civilization theory, asabiyyah | ❌ Not ingested |
| Ibn Khaldun Muqaddimah V2 | THEMUQADDIMAHOFIBNKHALDUNVOLUME2 | Dynasties, caliphate, governance | ✅ Ingested |
| Ibn Khaldun Muqaddimah V3 | THEMUQADDIMAHOFIBNKHALDUNVOLUME3 | Sciences, crafts, society | ❌ Not ingested |
| Ibn Battuta Travels V1 | the-travels-of-ibn-battuta-volume-1 | North Africa, Egypt, Syria, Arabia 1325 | ❌ Not ingested |
| Ibn Battuta Asia+Africa | in.ernet.dli.2015.62617 | Mali, Delhi, Ottoman, China 1325-1354 | ❌ Not ingested |
| Baburnama | gutenberg.org/ebooks/44608 | Mughal founding, Central Asia 1483-1530 | ✅ Ingested |
| Evliya Celebi Travels | Travel17thEvliyaChelebi | Ottoman Empire 17th century eyewitness | ❌ Not ingested |

**Crusades / Saladin Sources**

| Source | URL | Content | Status |
|---|---|---|---|
| Life of Saladin (Gibb) | cristoraul.org/.../LIFE_OF_SALADIN.pdf | Baha ad-Din + Imad ad-Din eyewitness | ❌ Not ingested |
| Arab Historians of the Crusades | archive.org | Francesco Gabrieli translation of Arabic sources | ❌ Not ingested |

**Ottoman Sources**

| Source | URL | Content | Status |
|---|---|---|---|
| Evliya Celebi (English 1834) | archive.org/details/Travel17thEvliyaChelebi | 17th century Ottoman eyewitness | ❌ Not ingested |

### Tier C: OAPEN — Free Academic Books

OAPEN (Open Access Publishing in European Networks) hosts hundreds of fully free peer-reviewed academic books. These are the scholarly secondary sources that provide context, conflict analysis, and modern historical interpretation.

**Confirmed Free on OAPEN (library.oapen.org):**

| Title | Authors | Relevance | URL |
|---|---|---|---|
| Conquered Populations in Early Islam | Elizabeth Urban | Rashidun, early Islamic society | library.oapen.org |
| Documents and the History of the Early Islamic World | Sijpesteijn & Schubert | Early Islamic documentary evidence | library.oapen.org/bitstream/id/abd61ff1... |
| The Making of Islamic Heritage | Various | Islamic civilization broadly | library.oapen.org |
| Medieval Damascus: Plurality and Diversity | Various | Crusades/Mamluk era Damascus | library.oapen.org |

**Search OAPEN for more:** https://library.oapen.org/discover?query=islamic+history

Filter by: Language = English, Open Access = Yes, Subject = Islamic history

### Tier D: University Open Access Repositories

These university repositories host thousands of free scholarly PDFs — theses, journal articles, and book chapters:

| Repository | URL | Best For |
|---|---|---|
| eScholarship (UC system) | escholarship.org | Islamic history, Middle East studies |
| DSpace at Harvard | dash.harvard.edu | Ottoman, Mughal, early Islamic |
| Princeton DataSpace | dataspace.princeton.edu | Near Eastern studies |
| Columbia Academic Commons | academiccommons.columbia.edu | Islamic history, Arabic sources |
| JSTOR Open Access | jstor.org/open | Journal articles, free |
| Academia.edu | academia.edu | Scholarly papers (requires free account) |
| ResearchGate | researchgate.net | Academic papers, many free |

**High-value search queries for these repositories:**
- "Battle of Yarmouk" primary sources
- "Khalid ibn Walid" historical analysis
- "Karbala 680" scholarly analysis
- "Abbasid golden age Baghdad" scholarship
- "Crusades Islamic sources" translated
- "Mamluk Sultanate" open access
- "Ottoman Empire 15th century" free PDF
- "Mali Empire Mansa Musa" primary sources
- "Omar al-Mukhtar Libya resistance" history
- "Tipu Sultan" academic history

### Tier E: Specific Institutional Digital Collections

**King Abdulaziz Foundation (Darah) — Riyadh, Saudi Arabia**
- Status: Physical institution, no public online database as of 2025
- Has: 500+ published books on Islamic/Arabian history, manuscripts, documents
- Access: Physical visit to Riyadh required, OR request digital copies by email
- Best use: After channel establishes credibility, request access as researchers
- Website: darah.org.sa
- Note: Their published books on Arabian Peninsula and Islamic world history are available as physical/digital purchases

**Cambridge University — Islamic Manuscripts**
- URL: cudl.lib.cam.ac.uk/collections/islamic
- Has: 5,000+ Islamic manuscripts, 10th-20th centuries
- Free: Yes, fully digitized
- Relevance: Primary source manuscripts, visual material for Mode 4 (manuscript quotes)

**Library of Congress — Mali/Timbuktu Manuscripts**
- URL: loc.gov/collections/mali-manuscripts/
- Has: 30+ Timbuktu manuscript PDFs
- Free: Yes
- Relevance: Africa series — Islamic scholarship in West Africa

**British Library — Islamic Manuscripts**
- URL: bl.uk/collection-guides/islamic-manuscripts
- Has: Large collection of Arabic, Persian, Ottoman manuscripts
- Free: Digitized portions via British Library Online Gallery

**Bibliothèque nationale de France (BnF)**
- URL: gallica.bnf.fr
- Has: Extensive Arabic manuscript collection, Andalusian sources
- Free: Yes, fully searchable
- Relevance: Andalusia series — Arabic manuscripts from Islamic Spain

**Al-Waraq Digital Library**
- URL: alwaraq.net
- Has: Thousands of classical Arabic texts in original Arabic
- Free: Yes
- Relevance: Arabic originals for scholarly verification (not for ingestion — Arabic only)

### Tier F: OpenITI Corpus (Strategic Sampling)

- GitHub: github.com/OpenITI/RELEASE
- Size: 10,202 texts, 2 billion words, pre-modern Arabic Islamic history
- Warning: Do NOT clone the full corpus — it would fill the VM disk
- Strategy: Download the metadata CSV, filter for most relevant English/translated texts, ingest top 50-100 by word count and topical relevance

**Filter criteria for OpenITI:**
- Language tag: English translations preferred (ara with English notes acceptable)
- Tags: history, sira, futuh, maghazi, tabaqat, tarikh, jihad, ansab, khilafa
- Date range: 0001-1400 AH (covers all 12 Islam Stories series)
- Limit: Top 50 by word count within filtered results

---

## 8. Coverage Status by Series

| Series | Era | Primary Sources Needed | Current Status |
|---|---|---|---|
| The Sword and the Succession | 632-661 | Al-Tabari V10-17, Ibn Hisham, Al-Baladhuri | ⚠️ Partial — missing Ibn Hisham |
| The Umayyad Paradox | 661-750 | Al-Tabari V18-27 | ⚠️ Partial — V18-26 missing |
| The Abbasid: Glory and Rot | 750-1258 | Al-Tabari V28-38, Ibn Khaldun V1+3 | ❌ Weak — V28-38 missing |
| The Andalusian Arc | 711-1492 | Al-Baladhuri V1, Al-Maqqari | ⚠️ Partial — Al-Maqqari missing |
| Crusades: Islamic Perspective | 1096-1291 | Baha ad-Din (Saladin), Ibn al-Athir | ❌ Missing |
| When Islam Stopped the Mongols | 1206-1300 | Ibn Battuta V1, specialist sources | ❌ Missing |
| Ottoman Empire | 1299-1566 | Evliya Celebi | ❌ Missing |
| South Asia | 711-1857 | Baburnama (done), Ibn Battuta India section | ⚠️ Partial |
| African Islam | 800-1900 | Ibn Battuta Mali, Timbuktu manuscripts | ❌ Missing |
| Scholars & Thinkers | 800-1400 | Ibn Khaldun V1+3, specialist sources | ⚠️ Partial |
| Women of Islam | 632-1900 | Scattered across above — no dedicated source | ⚠️ Indirect |
| Resistance & Colonialism | 1800-1931 | Colonial archives, no clean primary source | ❌ Needs different approach |

---

## 9. Build Order for Knowledge Graph

### Phase 1A+ — Knowledge Graph Foundation

**Step 1: Build schema**
File: `~/islam-stories/rag/knowledge/schema_graph.py`
- Create figures, events, source_relationships, chunk_metadata tables
- Run against islam_stories database

**Step 2: Seed figures and events**
File: `~/islam-stories/rag/knowledge/seed_data.py`
- Populate ~150 figures from Character Bible planning universe
- Populate ~500 key events (battles, political events, scholarly moments)
- This is the ONLY manual step — done once, human-verified

**Step 3: Link existing chunks to figures/events**
File: `~/islam-stories/rag/knowledge/link_chunks.py`
- Query Claude API to classify each chunk:
  - Which figure(s) does this describe?
  - Which event does this describe?
  - Is this eyewitness/transmitted/commentary/noise?
  - Does it contradict any other chunk?
- Batch process all 3,745+ chunks
- Cost: ~$2-5 in Claude API calls for full corpus

**Step 4: Build source relationships**
File: `~/islam-stories/rag/knowledge/build_relationships.py`
- For each event that has multiple sources:
  - Query Claude with both source versions
  - Classify relationship: CORROBORATES / CONTRADICTS / SUPPLEMENTS
  - Write conflict_note when CONTRADICTS
- This creates the conflict detection layer

**Step 5: Build retrieval orchestrator**
File: `~/islam-stories/rag/retrieval/orchestrator.py`
- retrieve_episode_context() function
- Multi-query retrieval
- Conflict pre-flagging
- Structured context packet output

### Phase 1B — Character Bible (Post-Graph)

With the knowledge graph in place, the Character Bible generation changes from "query the vector store" to "query the vector store AND the knowledge graph" to get:
- Authenticated source citations (from chunk_metadata)
- Pre-flagged conflicts (from source_relationships)
- Reliability ratings (from chain_strength)
- Cross-references (from figure_ids in events table)

### Phase 1C — Grounded Script Generation

Scripts generated using the grounded prompt (Section 5 above).
Every claim attributed. Every conflict surfaced. Every gap acknowledged.

---

## 10. The Accuracy Guarantee

The system cannot guarantee 100% historical accuracy — no system can. What it guarantees:

1. **Every claim is attributed** — if Al-Tabari says it, the script says "Al-Tabari says it"
2. **Conflicts are surfaced** — if two sources disagree, the viewer is told
3. **Gaps are acknowledged** — if history is silent, the narrator says so
4. **Weak sources are flagged** — single-source or daif narrations are marked
5. **No invention** — Claude cannot add detail not present in the retrieved context

This is the same standard academic historians use. It is higher than any competing Islamic history YouTube channel currently applies.

The channel's credibility comes not from claiming infallibility but from being transparent about the sources, their limitations, and their disagreements. This builds the kind of trust that OnePath Network and Kings and Generals do not have — because neither shows their work.
