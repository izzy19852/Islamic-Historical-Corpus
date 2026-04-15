# Islam Stories — Project Context

## The Vision

A cinematic Islamic civilizational history channel covering 1,400 years of history across the Middle East, South Asia, Africa, Europe, and Eastern Europe — told from **primary Islamic sources**, not Western colonial narratives. Think Fall of Civilizations depth + Kings and Generals battle structure + Kling 3.0 cinematic visuals + moral complexity that no existing channel provides.

**Core thesis:** The same battles and people told from Al-Tabari, Al-Waqidi, Ibn Hisham, and Ibn Khaldun instead of Wikipedia. Hypocrites and believers alike rendered as full humans. History that makes Muslims proud and non-Muslims riveted.

**Target:** Post-Prophet (PBUH) — starting from the Rashidun era through Ottoman, Mughal, African Islamic kingdoms, and Andalusia.

---

## Content Strategy

### Structure
**Era-based series** with people as the dramatic engine. Not standalone biographies, not pure battle videos — characters threaded across multi-episode arcs within named series.

```
SERIES (searchable container)
  e.g., "The Rashidun Conquests" | "When Islam Stopped the Mongols"
    
  EPISODE (20-35 min)
  One battle or turning point as centerpiece
  One primary figure as protagonist
  Told through primary Islamic sources
  Conflicts between accounts surfaced as drama
```

### Planned Series Arc
- **Series 1:** The Sword and the Succession (632–661) — Ridda Wars, Yarmouk, Khalid's dismissal, the Fitna
- **Series 2:** The Umayyad Paradox (661–750) — Karbala, Tariq ibn Ziyad, Al-Hajjaj, fall of the dynasty
- **Series 3:** When Islam Stopped the Mongols — Baghdad 1258, Baybars, Ain Jalut
- **Series 4:** The Andalusian Arc — Tariq, the golden age, the Reconquista from the Muslim side
- **Series 5:** The Abbasid Golden Age and Rot
- **Series 6:** Crusades: Islamic Perspective — Saladin's full story from Islamic eyewitness accounts
- **Series 7:** South Asia — Delhi Sultanate, Mughals, Muhammad ibn Qasim
- **Series 8:** African Islam — Mali, Songhai, Mansa Musa, scholars of Timbuktu
- **Series 9:** Ottoman Rise — Constantinople, Suleiman, decline

### Key Characters (50-70 total across all series)
High-priority figures:
- Khalid ibn Walid — undefeated general, the dismissal story
- Abu Ubayda ibn al-Jarrah — replaced Khalid, contrasting character
- Al-Hajjaj ibn Yusuf — brutal Umayyad governor, theologically complex
- Tariq ibn Ziyad — 7,000 men, burned the boats (did he?)
- Baybars — slave to sultan, stopped the Mongols
- Muhammad ibn Qasim — conquered Sindh at 17, executed at 20
- Saladin's inner circle (not Saladin himself — covered everywhere)
- Mansa Musa — hajj that crashed Egypt's gold market
- Ibn Battuta — traveled more than Marco Polo

---

## Competitive Landscape

### Direct Islamic Competitors
- **OnePath Network** (2M subs, Sydney) — already running "AI Visualised" series (Umar ibn Khattab, Battle of Badr, March 2026). Surface level, devotional/hagiographic tone, no primary sources cited, generic AI visuals.

### Non-Islamic History Channels
- **Kings and Generals** (4.1M subs, 31 min avg) — military history, animated maps, multi-episode arcs. Islamic content is their weakest. No primary Arabic sources.
- **Fall of Civilizations** (100M+ downloads) — literary immersive narration, zero Islamic episodes. The gold standard for format and voice.

### Gap Islam Stories Fills
Nobody combines: primary Islamic sources + cinematic AI battle visuals + moral complexity + dual audience (Muslim core + non-Muslim history enthusiasts) + 1,400 year scope.

---

## Technical Stack

### Video Generation
**Model:** Kling 3.0 (klingai.com) — validated via test clips
- Test results: multi-combatant melee PASSED, army scale shots PASSED, content filters not triggered
- Three visual styles validated: Realistic cinematic, Zack Snyder/300 style, Anime/cel-shaded
- Physics limitation: melee contact dynamics weaker than atmosphere/scale — edit rhythm compensates

**API access:** WaveSpeed or PiAPI (~$0.084–0.126/sec)
**COGS per episode:** ~$5–15 in video generation

**Previous model (Veo3):** Abandoned — battle content poor, melee impossible, $5/video

### Video Pipeline (to rebuild)
Previous stack used n8n (unreliable) + FFmpeg subtitles (buggy) + Veo3 (poor melee). Rebuild in Claude Code on GCP infrastructure.

**New pipeline:**
```
Script (Claude API + RAG)
  → Narration (ElevenLabs)
  → Scene breakdown timed to audio
  → Kling 3.0 generations per scene (violence-safe prompts)
  → FFmpeg stitch + Whisper subtitles
  → Final MP4
```

### RAG Knowledge System (Most Critical Component)

**Purpose:** AI script writer queries authenticated Islamic primary sources to generate depth no other channel has. ChatGPT training data = Wikipedia surface level. RAG from primary sources = Al-Tabari quoting eyewitnesses.

**Architecture:** Simple and cheap
```
PDFs/texts downloaded locally
  → Chunked + embedded → pgvector on existing GCP VM
  → Episode query → retrieve relevant chunks
  → Chunks + cinematic template → Claude API → script
  → Pay only Claude API tokens (~$0.06/episode)
```

**One-time embedding cost:** ~$20–50 for full corpus
**Per-episode generation cost:** ~$0.06

### Free Source APIs (query live, no ingestion needed)
| Source | Content | Access |
|--------|---------|--------|
| Sunnah.com API | All major hadith collections | Free API key via GitHub |
| Quran.com API | Full Quran, 90+ translations, tafsir | Free, no key |
| Fawazahmed0 Hadith API | 9 hadith collections, no rate limit | Free, open source |
| AlQuran.cloud API | Quran verses, audio | Free, no rate limit |

### Free Text Corpora (ingest once)
| Source | Content | Format |
|--------|---------|--------|
| **OpenITI Corpus** | 10,202 text files, 2B words, pre-modern Arabic Islamic texts | Plain text on GitHub |
| Internet Archive | Al-Tabari SUNY translation PDFs, Ibn Hisham Sira, Al-Baladhuri | PDF (free) |
| Project Gutenberg | Ibn Khaldun Muqaddimah (Rosenthal translation) | Text |
| Library of Congress | 30+ digitized Timbuktu manuscripts | PDF |
| JSTOR Open Access | 320+ Timbuktu manuscripts via Aluka | PDF |

### Priority Paid Sources (~$200 total)
- Al-Tabari SUNY translations — individual volumes ~$35-50 (PDFs exist online)
- Carole Hillenbrand — *The Crusades: Islamic Perspectives* (~$40)
- Hugh Kennedy — *The Great Arab Conquests* (~$20)
- Fred Donner — *The Early Islamic Conquests* (~$30)
- Osprey Islamic Military History series — David Nicolle (~$8-15/book)

### Infrastructure
- **GCP VM** — existing (strikerix project), Postgres + pgvector already running
- **Embedding model:** Voyage AI (better multilingual Arabic+English, ~$0.10/million tokens)
- **Script generation:** Claude API (claude-sonnet-4-6)
- **Video generation:** Kling 3.0 API via WaveSpeed/PiAPI

---

## Script Generation System Prompt (Draft)

```
You are the writer for Islam Stories, a cinematic historical 
documentary series covering Islamic civilization from 632 CE to 1900 CE.

VOICE: Fall of Civilizations literary register — immersive, 
present-tense narration, sensory detail, human scale. 
NOT a lecture. NOT a Wikipedia summary.

STRUCTURE per episode:
- Opening: sensory scene-setting (where are we, what does it 
  look/smell/sound like)
- Context: the world this person was born into
- The human: specific details from primary sources revealing character
- The conflict: tactical + political + personal
- The battle/event: cinematic sequence, sourced to specific narrations
- The aftermath: immediate consequence + long arc
- The mystery: what we don't know, what scholars dispute — stated honestly

CITATION RULES:
- Every specific claim references its source: "Al-Tabari records..." 
  "Ibn Hisham notes..."
- When sources conflict: present both and name them
- Never smooth over disputes — they are dramatic gold
- Flag weak narrations: "A disputed account suggests..."
- Flag gaps: "History is silent on why..."

FORBIDDEN:
- Hagiography — no one is perfectly righteous
- Demonization — no one is purely evil  
- Western colonial framing
- Unattributed claims
- Wikipedia-level surface summary

OUTPUT FORMAT:
[NARRATION] — spoken text
[VISUAL CUE] — what Kling should generate
[SOURCE NOTE] — citation for fact-checking
[CONFLICT FLAG] — where accounts diverge
```

---

## Kling 3.0 Prompt Principles (Validated)

**Content filter safe language:**
- ✅ "warriors clash in brutal melee combat" — passes
- ✅ "shields colliding, swords raised" — passes
- ✅ "no gore, no blood" — always append
- ❌ Direct violence/death descriptions — filtered

**Visual style tags that work:**
- Realistic cinematic: "golden hour lighting, dust particles, low angle, God-rays"
- Snyder style: "extreme slow motion, speed ramp, deep amber/black grade, God-rays, IMAX feel"
- Anime: "cel-shaded, Studio Ghibli meets live action, lightning energy effects, cherry blossoms"
- Witcher dark fantasy: "desaturated cold blue, magical energy on blade, foggy battlefield, rack focus"

**Scale shots (strongest Kling capability):**
- Army formations in thousands, eagle standards, dust clouds
- Establishing shots before cutting to melee
- These are the opening 10 seconds of every episode

---

## Build Order

**Phase 1 — Knowledge Foundation (before any code)**
1. Download free sources: OpenITI (GitHub clone), Internet Archive PDFs
2. Request Sunnah.com API key
3. Chunk + embed corpus into pgvector
4. Write episode generation prompt
5. Test on 3 pilot scripts — judge quality

**Phase 2 — Pilot Episode**
Pick: **Khalid ibn Walid — The Sword of God (Part 1)**
- Al-Waqidi on Mu'tah
- Al-Tabari on Yarmouk
- The dismissal by Umar — competing accounts
- Cliffhanger ending

**Phase 3 — Video Pipeline**
Build in Claude Code: narration → scene timing → Kling generation → FFmpeg stitch → subtitles

**Phase 4 — Channel Launch**
Post pilot. Evaluate retention. Iterate.

---

## What Validated So Far

- ✅ Kling 3.0 produces movie-quality battle scenes Veo3 could not
- ✅ Multi-combatant melee passes content filters with correct prompting
- ✅ Three distinct visual styles work (realistic, Snyder, anime)
- ✅ Army scale shots are exceptional — no other history channel has this
- ✅ The content gap in Islamic history YouTube is real and large
- ✅ OnePath Network is the closest competitor but lacks depth and visual quality
- ✅ RAG from primary sources solves the "surface level boring" problem from v1
- ✅ Simple architecture: download → embed → query → Claude generates script

## What's Not Built Yet

- ❌ RAG database (source ingestion + embedding)
- ❌ Script generation pipeline
- ❌ Video stitching pipeline (FFmpeg + audio sync)
- ❌ Subtitle system (Whisper-based)
- ❌ Channel identity/branding
- ❌ Pilot episode

---

## Notes on Previous Islam Stories v1 Failures
1. **Expensive** → Solved: Kling ~$5–15/episode vs Veo3
2. **n8n workflow unreliable** → Solved: rebuild in Claude Code as FastAPI pipeline
3. **FFmpeg subtitle bugs** → Solve: generate narration first, Whisper timestamps, validated SRT
4. **Content violations (violence)** → Solved: cinematic safe prompting validated
5. **Audio sync issues** → Solve: audio-first pipeline (generate narration → time scenes to audio → generate video)
6. **Surface-level boring scripts** → Solve: RAG from primary Islamic sources
