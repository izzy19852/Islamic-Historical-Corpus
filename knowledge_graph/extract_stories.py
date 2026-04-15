"""
Islam Stories — Story Extraction Pass 1: Figure-Anchored
For each character bible / DB figure, query RAG for all documented
moments, classify by dramatic core, format tier, and dramatic spine.

Run:  python3 -m rag.knowledge.extract_stories
      python3 -m rag.knowledge.extract_stories --limit 10  # test run
"""

import os
import sys
import json
import time
import psycopg2
import psycopg2.extras
import anthropic
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from rag.embeddings.query import query_rag, query_rag_multi

client = anthropic.Anthropic()

# ═══════════════════════════════════════════════════════════════════════
# DRAMATIC CORE DEFINITIONS (for the classifier prompt)
# ═══════════════════════════════════════════════════════════════════════

DRAMATIC_CORES = """
1. TRANSFORMATION — A person becomes fundamentally different. Test: Clear BEFORE and AFTER.
2. IMPOSSIBLE_CHOICE — Two irreconcilable goods or two evils. Test: Audience wouldn't know what they'd do.
3. CONTRADICTION — The person's life contains a paradox. Test: One-sentence paradox.
4. STAND — Refusal to bend at enormous personal cost. Test: The refusal costs them enormously.
5. UNINTENDED_CONSEQUENCE — Action creates unforeseen result. Test: Knowing consequence reframes the action.
6. ENCOUNTER — Two worlds meet, neither the same after. Test: Meeting produces something new.
7. LAST_STAND — The ending defines the life. Test: Last words reframe everything before.
8. MYSTERY — The sources don't tell us something important, and the gap IS the story.
"""

FORMAT_TIERS = """
FULL_EPISODE (15 min): Multiple dramatic cores (2+), multiple documented scenes, dramatic question that takes time to unfold, ~3000 words of narration material.
SHORT_EPISODE (5-8 min): At least one dramatic core, one powerful documented scene, dramatic question resolving in one sequence, ~800-1500 words.
SHORT (60-90 sec): One unforgettable moment, documented dialogue or last words, understood with no prior context.
SUPPORTING: Figure appears in others' stories, not enough solo narrative for own episode.
"""

SPINE_TYPES = """
TRIAL — Empathy, admiration. No battle usually.
CONQUEST — Awe, tension, release. Heavy battle.
TRAGEDY — Sorrow, dread. Sometimes battle.
RISE — Inspiration, investment. Sometimes battle.
SUCCESSION — Weight of power. No battle.
DISCOVERY — Wonder, depth. No battle.
RESISTANCE — Pride, defiance. Medium battle.
"""

CLASSIFICATION_PROMPT = """You are a story analyst for a cinematic Islamic history series.
Given a historical figure and their documented moments from primary sources, identify
every episode-worthy story and classify each one.

For each story you identify, output a JSON object with these fields:
- title: compelling episode title
- subtitle: optional subtitle
- one_line: one-sentence pitch
- dramatic_question: the question the episode poses
- dramatic_cores: list of applicable cores from: TRANSFORMATION, IMPOSSIBLE_CHOICE, CONTRADICTION, STAND, UNINTENDED_CONSEQUENCE, ENCOUNTER, LAST_STAND, MYSTERY
- dramatic_spine: one of: TRIAL, CONQUEST, TRAGEDY, RISE, SUCCESSION, DISCOVERY, RESISTANCE
- format: one of: FULL_EPISODE, SHORT_EPISODE, SHORT, SUPPORTING
- has_battle: boolean
- has_political: boolean
- has_spiritual: boolean
- story_scale: "personal", "political", "civilizational"
- source_coverage: "rich", "adequate", "thin"
- date_start_ce: integer or null
- date_end_ce: integer or null
- date_ah: string or null
- location: string or null
- primary_sources: list of source names mentioned
- confidence: "high", "medium", "low"
- notes: any relevant notes

DRAMATIC CORES:
{cores}

FORMAT TIERS:
{formats}

SPINE TYPES:
{spines}

RULES:
- Only identify stories with genuine dramatic weight — not every event is a story.
- A figure may yield 0 stories (SUPPORTING only) or 5+ stories.
- Be conservative with FULL_EPISODE — it needs rich source material.
- If the source material is thin, mark source_coverage as "thin" and format as SHORT or SUPPORTING.
- Output ONLY a JSON array of story objects. No other text.
"""


def _get_all_figures(limit: int = None) -> list[dict]:
    """Fetch all figures from the DB."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    sql = """
        SELECT id, name, name_variants, sensitivity_tier, era, series,
               birth_death, dramatic_question, primary_sources, generation,
               known_for, death_circumstance
        FROM figures
        ORDER BY
            CASE sensitivity_tier WHEN 'S' THEN 1 WHEN 'A' THEN 2
                                  WHEN 'B' THEN 3 WHEN 'C' THEN 4 END,
            name
    """
    if limit:
        sql += f" LIMIT {limit}"
    cur.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _get_character_bible(figure_name: str) -> str | None:
    """Try to load the character bible markdown for a figure."""
    import re
    project_root = os.path.join(os.path.dirname(__file__), '..', '..')
    bible_dir = os.path.join(project_root, 'character_bible')
    if not os.path.isdir(bible_dir):
        return None

    slug = figure_name.lower().replace(" ", "_")
    slug = re.sub(r"[''()]", "", slug)

    for era_dir in os.listdir(bible_dir):
        era_path = os.path.join(bible_dir, era_dir)
        if not os.path.isdir(era_path):
            continue
        for fname in os.listdir(era_path):
            if fname.endswith('.md') and (slug in fname or fname.replace('.md', '') in slug):
                with open(os.path.join(era_path, fname)) as f:
                    return f.read()[:4000]
    return None


def _query_figure_moments(figure: dict) -> str:
    """Query RAG for all documented moments of a figure."""
    name = figure['name']
    variants = figure.get('name_variants') or []
    era = figure['era'][0] if figure.get('era') else None

    topics = [
        f"{name} key events moments",
        f"{name} biography life",
        f"{name} battles conflicts",
        f"{name} death last words legacy",
    ]

    kwargs = {}
    if era:
        kwargs["era"] = era
    kwargs["figures"] = [name] + list(variants)

    accounts = query_rag_multi(topics, n_results=15, **kwargs)

    if len(accounts) < 3:
        accounts = query_rag_multi(topics, n_results=15)

    chunks = []
    for i, acc in enumerate(accounts[:20], 1):
        chunks.append(
            f"[ACCOUNT {i}] Source: {acc['source']} | "
            f"Score: {acc['similarity_score']:.3f}\n"
            f"{acc['content'][:600]}"
        )
    return "\n\n".join(chunks)


def _repair_truncated_json(text: str) -> list[dict]:
    """Attempt to repair truncated JSON arrays by closing open structures."""
    import re
    # Find the last complete object in the array
    # Strategy: find all complete {...} objects, wrap in array
    objects = []
    depth = 0
    start = None
    for i, ch in enumerate(text):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    obj = json.loads(text[start:i + 1])
                    objects.append(obj)
                except json.JSONDecodeError:
                    pass
                start = None
    return objects


def _classify_figure_stories(figure: dict, rag_context: str, bible: str = None) -> list[dict]:
    """Use Haiku to classify stories for a figure.

    For major figures (Tier S/A, or known high-source figures), uses higher
    max_tokens and truncates context to fit. Retries with reduced context
    on JSON parse failure.
    """
    import re as _re

    figure_summary = (
        f"Name: {figure['name']}\n"
        f"Tier: {figure['sensitivity_tier']}\n"
        f"Era: {', '.join(figure.get('era') or ['unknown'])}\n"
        f"Known for: {figure.get('known_for', 'N/A')}\n"
        f"Birth-Death: {figure.get('birth_death', 'N/A')}\n"
        f"Generation: {figure.get('generation', 'N/A')}\n"
        f"Death: {figure.get('death_circumstance', 'N/A')}\n"
        f"Dramatic question: {figure.get('dramatic_question', 'N/A')}\n"
    )

    system = CLASSIFICATION_PROMPT.format(
        cores=DRAMATIC_CORES,
        formats=FORMAT_TIERS,
        spines=SPINE_TYPES,
    )

    # Major figures get more output tokens.
    # Tier S/A always major. For Tier B/C, check if they have heavy source material
    # (the RAG context length is a proxy — longer context = more stories = more output).
    rag_len = len(rag_context)
    is_major = (
        figure['sensitivity_tier'] in ('S', 'A')
        or rag_len > 6000  # heavy RAG context → likely many stories
        or figure['name'] in (
            'Khalid ibn Walid', 'Saladin', 'Amr ibn al-As',
            'Muawiyah ibn Abi Sufyan', 'Malik ibn Anas', 'Abu Dawud',
            'Harun al-Rashid', 'Tariq ibn Ziyad', 'Baybars',
            'Ibn Khaldun', 'Mehmed II', 'Suleiman the Magnificent',
            'Yazid ibn Muawiyah', 'Akbar', 'Babur',
        )
    )
    max_tokens = 8192 if is_major else 4096

    # Retry loop: try full context, then reduced on failure
    for attempt in range(3):
        # Reduce context on retries
        bible_limit = 3000 - (attempt * 1000)
        rag_limit = len(rag_context) - (attempt * 2000)
        rag_limit = max(rag_limit, 2000)

        bible_block = ""
        if bible:
            bible_block = f"\n\nCHARACTER BIBLE EXCERPT:\n{bible[:bible_limit]}\n"

        rag_trimmed = rag_context[:rag_limit]

        user = (
            f"Analyze this figure and identify all episode-worthy stories:\n\n"
            f"FIGURE PROFILE:\n{figure_summary}\n"
            f"{bible_block}\n"
            f"PRIMARY SOURCE ACCOUNTS:\n{rag_trimmed}\n\n"
            f"Return a JSON array of story objects. If this figure has no "
            f"episode-worthy stories (only supporting appearances), return "
            f"a single object with format='SUPPORTING'."
        )

        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=max_tokens,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
        except (anthropic._exceptions.OverloadedError,
                anthropic._exceptions.RateLimitError,
                anthropic._exceptions.APIConnectionError) as e:
            if attempt < 2:
                wait = (2 ** attempt) + 1
                print(f"    API error ({e.__class__.__name__}), retry {attempt + 1} in {wait}s...")
                time.sleep(wait)
                continue
            raise

        text = resp.content[0].text.strip()

        # Check if response was truncated (stop_reason != 'end_turn')
        was_truncated = resp.stop_reason != 'end_turn'

        # Try clean parse first
        try:
            if text.startswith('['):
                return json.loads(text)
            match = _re.search(r'\[.*\]', text, _re.DOTALL)
            if match:
                return json.loads(match.group())
        except json.JSONDecodeError:
            pass

        # JSON broken — try repair
        if was_truncated or True:  # always try repair on parse failure
            repaired = _repair_truncated_json(text)
            if repaired:
                if attempt < 2:
                    print(f"    JSON truncated, repaired {len(repaired)} stories (attempt {attempt + 1}, retrying with less context)...")
                    time.sleep(1)
                    continue  # retry with less context for potentially more stories
                else:
                    print(f"    JSON truncated, recovered {len(repaired)} stories after {attempt + 1} attempts")
                    return repaired

        if attempt < 2:
            print(f"    JSON parse failed (attempt {attempt + 1}), retrying with reduced context...")
            time.sleep(1)
        else:
            print(f"    JSON parse failed after 3 attempts, returning empty")

    return []


def _insert_story(cur, story: dict, figure: dict) -> int | None:
    """Insert a story into story_registry. Returns story_id or None."""
    try:
        cur.execute("""
            INSERT INTO story_registry (
                title, subtitle, one_line, dramatic_question,
                date_start_ce, date_end_ce, date_ah,
                era, region, location,
                primary_figure_id, figure_ids, sensitivity_max,
                dramatic_spine, dramatic_cores, story_scale,
                has_battle, has_political, has_spiritual,
                format, source_coverage, primary_sources,
                confidence, extraction_pass, extraction_source,
                notes
            ) VALUES (
                %(title)s, %(subtitle)s, %(one_line)s, %(dramatic_question)s,
                %(date_start_ce)s, %(date_end_ce)s, %(date_ah)s,
                %(era)s, %(region)s, %(location)s,
                %(primary_figure_id)s, %(figure_ids)s, %(sensitivity_max)s,
                %(dramatic_spine)s, %(dramatic_cores)s, %(story_scale)s,
                %(has_battle)s, %(has_political)s, %(has_spiritual)s,
                %(format)s, %(source_coverage)s, %(primary_sources)s,
                %(confidence)s, %(extraction_pass)s, %(extraction_source)s,
                %(notes)s
            )
            RETURNING story_id
        """, {
            'title': story.get('title', 'Untitled'),
            'subtitle': story.get('subtitle'),
            'one_line': story.get('one_line', ''),
            'dramatic_question': story.get('dramatic_question'),
            'date_start_ce': story.get('date_start_ce'),
            'date_end_ce': story.get('date_end_ce'),
            'date_ah': story.get('date_ah'),
            'era': figure['era'][0] if figure.get('era') else None,
            'region': story.get('region'),
            'location': story.get('location'),
            'primary_figure_id': figure['id'],
            'figure_ids': [figure['id']],
            'sensitivity_max': figure['sensitivity_tier'],
            'dramatic_spine': story.get('dramatic_spine', 'TRIAL'),
            'dramatic_cores': story.get('dramatic_cores', []),
            'story_scale': story.get('story_scale', 'personal'),
            'has_battle': story.get('has_battle', False),
            'has_political': story.get('has_political', False),
            'has_spiritual': story.get('has_spiritual', False),
            'format': story.get('format', 'SUPPORTING'),
            'source_coverage': story.get('source_coverage', 'thin'),
            'primary_sources': story.get('primary_sources', []),
            'confidence': story.get('confidence', 'medium'),
            'extraction_pass': 'figure_anchored',
            'extraction_source': f'figure:{figure["name"]}',
            'notes': story.get('notes'),
        })
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"    ERROR inserting story: {e}")
        return None


def run_extraction(limit: int = None, dry_run: bool = False):
    """Run Pass 1: Figure-Anchored Story Extraction."""
    figures = _get_all_figures(limit)
    print(f"Pass 1 — Figure-Anchored Extraction")
    print(f"  Figures to process: {len(figures)}")
    print(f"  Dry run: {dry_run}")
    print("=" * 60)

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    total_stories = 0
    total_full = 0
    total_short_ep = 0
    total_short = 0
    total_supporting = 0
    errors = 0

    for i, fig in enumerate(figures, 1):
        name = fig['name']
        tier = fig['sensitivity_tier']
        print(f"\n[{i}/{len(figures)}] {name} (Tier {tier})")

        try:
            # 1. Query RAG for documented moments
            rag_context = _query_figure_moments(fig)
            if not rag_context:
                print(f"  No RAG results — skipping")
                continue

            # 2. Load character bible
            bible = _get_character_bible(name)

            # 3. Classify stories via Haiku
            stories = _classify_figure_stories(fig, rag_context, bible)
            print(f"  Stories identified: {len(stories)}")

            # 4. Insert into story_registry
            for s in stories:
                fmt = s.get('format', 'SUPPORTING')
                title = s.get('title', 'Untitled')

                if dry_run:
                    print(f"    [DRY] {fmt}: {title}")
                else:
                    story_id = _insert_story(cur, s, fig)
                    if story_id:
                        print(f"    ✓ {fmt}: {title} (id={story_id})")
                    else:
                        errors += 1
                        continue

                total_stories += 1
                if fmt == 'FULL_EPISODE':
                    total_full += 1
                elif fmt == 'SHORT_EPISODE':
                    total_short_ep += 1
                elif fmt == 'SHORT':
                    total_short += 1
                else:
                    total_supporting += 1

            # Rate limiting for API
            time.sleep(0.5)

        except Exception as e:
            print(f"  ERROR: {e}")
            errors += 1
            continue

    conn.close()

    print("\n" + "=" * 60)
    print("PASS 1 COMPLETE")
    print(f"  Total stories: {total_stories}")
    print(f"    FULL_EPISODE:  {total_full}")
    print(f"    SHORT_EPISODE: {total_short_ep}")
    print(f"    SHORT:         {total_short}")
    print(f"    SUPPORTING:    {total_supporting}")
    print(f"  Errors: {errors}")

    if total_stories >= 500:
        print("\n  GATE PASSED: 500+ stories extracted")
    else:
        print(f"\n  GATE NOT YET MET: {total_stories}/500 stories")

    return total_stories


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pass 1: Figure-Anchored Story Extraction")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of figures to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't insert into DB, just classify")
    args = parser.parse_args()

    run_extraction(limit=args.limit, dry_run=args.dry_run)
