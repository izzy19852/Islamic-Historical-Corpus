"""
Islam Stories — Story Extraction Pass 2: Event-Anchored
Sweep each era/region for events NOT captured by figure extraction:
civilizational moments, natural disasters, architectural/scientific
achievements, events where the civilization itself is the protagonist.

Run:  python3 -m rag.knowledge.extract_stories_events
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
# ERA SWEEP TOPICS — events that may not be figure-anchored
# ═══════════════════════════════════════════════════════════════════════

ERA_TOPICS = {
    "rashidun": [
        "compilation of the Quran after Yamama",
        "founding of Kufa Basra new cities",
        "plague of Amwas impact on leadership",
        "Ridda Wars aftermath and consolidation",
        "conquest of Jerusalem peaceful surrender",
    ],
    "umayyad": [
        "Dome of the Rock construction significance",
        "arabization of administration under Abd al-Malik",
        "founding of Kairouan Fez North African cities",
        "Umayyad postal system roads infrastructure",
        "Berber revolts North Africa 740s",
    ],
    "abbasid": [
        "founding of Baghdad Round City design",
        "Bayt al-Hikma translation movement",
        "paper making Samarkand spread",
        "Zanj revolt Basra",
        "Abbasid scientific golden age astronomy mathematics",
        "fall of Baghdad Mongol invasion 1258",
    ],
    "andalusia": [
        "Cordoba library scholarship golden age",
        "convivencia interfaith coexistence",
        "fall of Granada 1492",
        "translation movement Toledo",
        "Alhambra construction art architecture",
    ],
    "crusades": [
        "fall of Jerusalem First Crusade 1099",
        "reconquest of Jerusalem Saladin 1187",
        "siege of Acre Third Crusade",
        "Children's Crusade tragedy",
        "Mamluk defeat of Mongols Ain Jalut",
    ],
    "ottoman": [
        "conquest of Constantinople 1453",
        "Suleiman the Magnificent architecture law",
        "siege of Vienna 1529 1683",
        "Ottoman navy Mediterranean dominance",
        "millet system religious governance",
    ],
    "south_asia": [
        "Delhi Sultanate founding",
        "Mughal architecture Taj Mahal",
        "Akbar religious dialogue Din-i-Ilahi",
        "Tipu Sultan resistance British",
        "Partition 1947 Muslim experience",
    ],
    "africa": [
        "Mansa Musa hajj pilgrimage economy",
        "Timbuktu university scholarship Sankore",
        "Swahili coast trade civilization",
        "Sokoto Caliphate Uthman dan Fodio",
        "Great Zimbabwe Indian Ocean trade",
    ],
    "mongol": [
        "destruction of Baghdad libraries 1258",
        "Mongol conversion to Islam",
        "Timur conquests Central Asia",
        "Ilkhanate cultural synthesis",
    ],
    "resistance_colonial": [
        "Omar al-Mukhtar resistance Libya",
        "Algerian war of independence",
        "Abdel Kader resistance Algeria",
        "Indian Mutiny 1857 Muslim dimension",
    ],
}

CLASSIFICATION_PROMPT = """You are a story analyst for a cinematic Islamic history series.
Given RAG results about an era/event topic, identify episode-worthy stories where the
CIVILIZATION or EVENT is the protagonist (not a single figure).

For each story, output a JSON object with:
- title: compelling episode title
- subtitle: optional
- one_line: one-sentence pitch
- dramatic_question: the question the episode poses
- dramatic_cores: list from: TRANSFORMATION, IMPOSSIBLE_CHOICE, CONTRADICTION, STAND, UNINTENDED_CONSEQUENCE, ENCOUNTER, LAST_STAND, MYSTERY
- dramatic_spine: one of: TRIAL, CONQUEST, TRAGEDY, RISE, SUCCESSION, DISCOVERY, RESISTANCE
- format: one of: FULL_EPISODE, SHORT_EPISODE, SHORT
- has_battle: boolean
- has_political: boolean
- has_spiritual: boolean
- story_scale: "personal", "political", "civilizational"
- source_coverage: "rich", "adequate", "thin"
- date_start_ce: integer or null
- date_end_ce: integer or null
- location: string or null
- primary_sources: list of source names
- figure_names: list of any figures involved (may be empty for civilizational stories)
- confidence: "high", "medium", "low"
- notes: any relevant notes

RULES:
- Focus on events where no single figure is the protagonist.
- Library foundings, trade routes, natural disasters, plagues, architectural achievements.
- Be conservative — only flag genuinely dramatic events.
- Output ONLY a JSON array. No other text.
"""


def _get_existing_stories_for_era(era: str) -> set[str]:
    """Get titles of stories already extracted for this era."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute(
        "SELECT LOWER(title) FROM story_registry WHERE era = %s",
        (era,)
    )
    titles = {r['lower'] for r in cur.fetchall()}
    conn.close()
    return titles


def _resolve_figure_id(cur, name: str) -> int | None:
    """Try to resolve a figure name to an ID."""
    cur.execute("""
        SELECT id FROM figures
        WHERE name ILIKE %s OR %s = ANY(name_variants)
        LIMIT 1
    """, (f"%{name}%", name))
    row = cur.fetchone()
    return row[0] if row else None


def _insert_event_story(cur, story: dict, era: str) -> int | None:
    """Insert an event-anchored story into story_registry."""
    # Try to resolve figure IDs
    figure_ids = []
    primary_figure_id = None
    for name in story.get('figure_names', []):
        fid = _resolve_figure_id(cur, name)
        if fid:
            figure_ids.append(fid)
            if not primary_figure_id:
                primary_figure_id = fid

    # Determine max sensitivity
    sensitivity_max = 'B'
    if figure_ids:
        cur.execute(
            "SELECT MIN(sensitivity_tier) FROM figures WHERE id = ANY(%s)",
            (figure_ids,)
        )
        row = cur.fetchone()
        if row and row[0]:
            sensitivity_max = row[0]

    try:
        cur.execute("""
            INSERT INTO story_registry (
                title, subtitle, one_line, dramatic_question,
                date_start_ce, date_end_ce, era, location,
                primary_figure_id, figure_ids, sensitivity_max,
                dramatic_spine, dramatic_cores, story_scale,
                has_battle, has_political, has_spiritual,
                format, source_coverage, primary_sources,
                confidence, extraction_pass, extraction_source, notes
            ) VALUES (
                %(title)s, %(subtitle)s, %(one_line)s, %(dramatic_question)s,
                %(date_start_ce)s, %(date_end_ce)s, %(era)s, %(location)s,
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
            'era': era,
            'location': story.get('location'),
            'primary_figure_id': primary_figure_id,
            'figure_ids': figure_ids or None,
            'sensitivity_max': sensitivity_max,
            'dramatic_spine': story.get('dramatic_spine', 'DISCOVERY'),
            'dramatic_cores': story.get('dramatic_cores', []),
            'story_scale': story.get('story_scale', 'civilizational'),
            'has_battle': story.get('has_battle', False),
            'has_political': story.get('has_political', False),
            'has_spiritual': story.get('has_spiritual', False),
            'format': story.get('format', 'SHORT_EPISODE'),
            'source_coverage': story.get('source_coverage', 'adequate'),
            'primary_sources': story.get('primary_sources', []),
            'confidence': story.get('confidence', 'medium'),
            'extraction_pass': 'event_anchored',
            'extraction_source': f'era:{era}',
            'notes': story.get('notes'),
        })
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"    ERROR inserting: {e}")
        return None


def run_extraction(eras: list[str] = None, dry_run: bool = False):
    """Run Pass 2: Event-Anchored Story Extraction."""
    target_eras = eras or list(ERA_TOPICS.keys())

    print("Pass 2 — Event-Anchored Extraction")
    print(f"  Eras to sweep: {target_eras}")
    print(f"  Dry run: {dry_run}")
    print("=" * 60)

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    total_stories = 0
    errors = 0

    for era in target_eras:
        topics = ERA_TOPICS.get(era, [])
        if not topics:
            continue

        existing = _get_existing_stories_for_era(era)
        print(f"\n{'='*40}")
        print(f"ERA: {era} ({len(topics)} topics, {len(existing)} existing stories)")

        for topic in topics:
            print(f"\n  Topic: {topic}")

            # Query RAG for this topic
            accounts = query_rag_multi(
                [topic, f"{topic} significance impact"],
                n_results=10,
                era=era,
            )

            if len(accounts) < 2:
                accounts = query_rag_multi(
                    [topic, f"{topic} significance impact"],
                    n_results=10,
                )

            if not accounts:
                print(f"    No RAG results — skipping")
                continue

            # Build context
            chunks = []
            for i, acc in enumerate(accounts[:12], 1):
                chunks.append(
                    f"[ACCOUNT {i}] Source: {acc['source']} | "
                    f"Score: {acc['similarity_score']:.3f}\n"
                    f"{acc['content'][:500]}"
                )
            rag_context = "\n\n".join(chunks)

            # Classify via Haiku
            try:
                resp = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=3000,
                    system=CLASSIFICATION_PROMPT,
                    messages=[{"role": "user", "content": (
                        f"ERA: {era}\nTOPIC: {topic}\n\n"
                        f"SOURCE ACCOUNTS:\n{rag_context}\n\n"
                        f"Return a JSON array of episode-worthy stories. "
                        f"If nothing qualifies, return an empty array []."
                    )}],
                )

                text = resp.content[0].text.strip()
                import re
                match = re.search(r'\[.*\]', text, re.DOTALL)
                stories = json.loads(match.group()) if match else []

            except Exception as e:
                print(f"    Classification error: {e}")
                errors += 1
                continue

            # Filter duplicates and insert
            for s in stories:
                title = s.get('title', '')
                if title.lower() in existing:
                    print(f"    [SKIP] Already exists: {title}")
                    continue

                if dry_run:
                    print(f"    [DRY] {s.get('format','?')}: {title}")
                else:
                    story_id = _insert_event_story(cur, s, era)
                    if story_id:
                        print(f"    ✓ {s.get('format','?')}: {title} (id={story_id})")
                    else:
                        errors += 1
                        continue

                existing.add(title.lower())
                total_stories += 1

            time.sleep(0.5)

    conn.close()

    print("\n" + "=" * 60)
    print("PASS 2 COMPLETE")
    print(f"  New event-anchored stories: {total_stories}")
    print(f"  Errors: {errors}")

    return total_stories


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pass 2: Event-Anchored Story Extraction")
    parser.add_argument("--era", type=str, nargs="+", default=None, help="Specific eras to sweep")
    parser.add_argument("--dry-run", action="store_true", help="Don't insert, just classify")
    args = parser.parse_args()

    run_extraction(eras=args.era, dry_run=args.dry_run)
