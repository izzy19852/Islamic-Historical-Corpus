"""
Islam Stories — Story Extraction Pass 3: Thematic Thread Extraction
For each of the themes in the themes table, find every instance
across all eras, sequence chronologically, and identify cross-era
episode groupings.

Run:  python3 -m rag.knowledge.extract_stories_themes
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
# FALLBACK THEMES — used if themes table is empty
# ═══════════════════════════════════════════════════════════════════════

FALLBACK_THEMES = [
    {"slug": "loyalty_and_betrayal", "name": "Loyalty and Betrayal"},
    {"slug": "justice_vs_power", "name": "Justice vs Power"},
    {"slug": "succession_crisis", "name": "Succession and Legitimacy"},
    {"slug": "conversion_transformation", "name": "Conversion and Transformation"},
    {"slug": "martyrdom", "name": "Martyrdom and Sacrifice"},
    {"slug": "exile_displacement", "name": "Exile and Displacement"},
    {"slug": "scholarship_knowledge", "name": "Scholarship and Knowledge"},
    {"slug": "mercy_forgiveness", "name": "Mercy and Forgiveness"},
    {"slug": "women_in_history", "name": "Women in Islamic History"},
    {"slug": "interfaith_encounter", "name": "Interfaith Encounter"},
    {"slug": "military_genius", "name": "Military Genius and Strategy"},
    {"slug": "plague_disaster", "name": "Plague, Famine, and Natural Disaster"},
    {"slug": "trade_economy", "name": "Trade and Economic Power"},
    {"slug": "architecture_art", "name": "Architecture and Artistic Achievement"},
    {"slug": "resistance_colonialism", "name": "Resistance to Colonialism"},
    {"slug": "unity_fragmentation", "name": "Unity and Fragmentation"},
    {"slug": "spiritual_journey", "name": "Spiritual Journey and Sufism"},
    {"slug": "father_son", "name": "Father and Son / Mentor and Student"},
    {"slug": "last_words", "name": "Last Words and Final Acts"},
    {"slug": "unintended_consequences", "name": "Unintended Consequences"},
]

THEMATIC_PROMPT = """You are a story analyst for a cinematic Islamic history series.
Given a THEME and RAG results showing instances of that theme across Islamic history,
identify cross-era episode groupings — stories that connect multiple instances of the
same theme across centuries.

For each thematic grouping, output a JSON object with:
- title: compelling group episode title (e.g., "The Four Abdullahs", "When Libraries Burned")
- subtitle: optional
- one_line: one-sentence pitch connecting the theme across eras
- dramatic_question: the thematic question posed
- dramatic_cores: list from: TRANSFORMATION, IMPOSSIBLE_CHOICE, CONTRADICTION, STAND, UNINTENDED_CONSEQUENCE, ENCOUNTER, LAST_STAND, MYSTERY
- dramatic_spine: one of: TRIAL, CONQUEST, TRAGEDY, RISE, SUCCESSION, DISCOVERY, RESISTANCE
- format: one of: FULL_EPISODE, SHORT_EPISODE, SHORT
- story_scale: "personal", "political", "civilizational"
- source_coverage: "rich", "adequate", "thin"
- instances: list of {era, date_ce, figure_or_event, brief_description} for each instance
- thread: the thematic thread name (slug)
- confidence: "high", "medium", "low"
- notes: any relevant notes

RULES:
- A thematic grouping needs at least 2 instances across different eras to qualify.
- The story must have a CONNECTING ARGUMENT — not just "these things happened."
- The theme must be dramatizable, not just academic.
- These are often SHORT_EPISODE or SHORT format — group portraits, not deep dives.
- Output ONLY a JSON array. No other text.
"""


def _get_themes() -> list[dict]:
    """Get all themes from DB or use fallback."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("SELECT slug, name, description FROM themes ORDER BY slug")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    if rows:
        return rows
    return FALLBACK_THEMES


def _get_figures_for_theme(theme_slug: str) -> list[dict]:
    """Get figures tagged with this theme."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT f.id, f.name, f.era, f.sensitivity_tier, ft.relevance
        FROM figure_themes ft
        JOIN themes t ON ft.theme_id = t.id
        JOIN figures f ON ft.figure_id = f.id
        WHERE t.slug = %s
        ORDER BY f.name
    """, (theme_slug,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def _get_existing_thread_stories(thread: str) -> set[str]:
    """Get existing stories for this thread."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute(
        "SELECT LOWER(title) FROM story_registry WHERE thread = %s",
        (thread,)
    )
    titles = {r['lower'] for r in cur.fetchall()}
    conn.close()
    return titles


def _resolve_figure_id(cur, name: str) -> int | None:
    cur.execute("""
        SELECT id FROM figures
        WHERE name ILIKE %s OR %s = ANY(name_variants)
        LIMIT 1
    """, (f"%{name}%", name))
    row = cur.fetchone()
    return row[0] if row else None


def _insert_thematic_story(cur, story: dict) -> int | None:
    """Insert a thematic thread story."""
    # Resolve figure IDs from instances
    figure_ids = []
    for inst in story.get('instances', []):
        fig_name = inst.get('figure_or_event', '')
        fid = _resolve_figure_id(cur, fig_name)
        if fid:
            figure_ids.append(fid)

    primary_figure_id = figure_ids[0] if figure_ids else None

    # Date range from instances
    dates = [inst.get('date_ce') for inst in story.get('instances', []) if inst.get('date_ce')]
    date_start = min(dates) if dates else None
    date_end = max(dates) if dates else None

    try:
        cur.execute("""
            INSERT INTO story_registry (
                title, subtitle, one_line, dramatic_question,
                date_start_ce, date_end_ce,
                primary_figure_id, figure_ids, sensitivity_max,
                dramatic_spine, dramatic_cores, story_scale,
                has_battle, has_political, has_spiritual,
                format, source_coverage,
                confidence, extraction_pass, extraction_source,
                thread, is_crossover, notes
            ) VALUES (
                %(title)s, %(subtitle)s, %(one_line)s, %(dramatic_question)s,
                %(date_start_ce)s, %(date_end_ce)s,
                %(primary_figure_id)s, %(figure_ids)s, %(sensitivity_max)s,
                %(dramatic_spine)s, %(dramatic_cores)s, %(story_scale)s,
                %(has_battle)s, %(has_political)s, %(has_spiritual)s,
                %(format)s, %(source_coverage)s,
                %(confidence)s, %(extraction_pass)s, %(extraction_source)s,
                %(thread)s, %(is_crossover)s, %(notes)s
            )
            RETURNING story_id
        """, {
            'title': story.get('title', 'Untitled'),
            'subtitle': story.get('subtitle'),
            'one_line': story.get('one_line', ''),
            'dramatic_question': story.get('dramatic_question'),
            'date_start_ce': date_start,
            'date_end_ce': date_end,
            'primary_figure_id': primary_figure_id,
            'figure_ids': figure_ids or None,
            'sensitivity_max': 'B',
            'dramatic_spine': story.get('dramatic_spine', 'DISCOVERY'),
            'dramatic_cores': story.get('dramatic_cores', []),
            'story_scale': story.get('story_scale', 'civilizational'),
            'has_battle': story.get('has_battle', False),
            'has_political': story.get('has_political', False),
            'has_spiritual': story.get('has_spiritual', False),
            'format': story.get('format', 'SHORT_EPISODE'),
            'source_coverage': story.get('source_coverage', 'adequate'),
            'confidence': story.get('confidence', 'medium'),
            'extraction_pass': 'thematic_thread',
            'extraction_source': f'theme:{story.get("thread", "unknown")}',
            'thread': story.get('thread'),
            'is_crossover': True,
            'notes': story.get('notes'),
        })
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"    ERROR inserting: {e}")
        return None


def run_extraction(themes: list[str] = None, dry_run: bool = False):
    """Run Pass 3: Thematic Thread Extraction."""
    all_themes = _get_themes()
    if themes:
        all_themes = [t for t in all_themes if t['slug'] in themes]

    print("Pass 3 — Thematic Thread Extraction")
    print(f"  Themes to process: {len(all_themes)}")
    print(f"  Dry run: {dry_run}")
    print("=" * 60)

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    total_stories = 0
    errors = 0

    for theme in all_themes:
        slug = theme['slug']
        name = theme['name']
        print(f"\n{'='*40}")
        print(f"THEME: {name} ({slug})")

        # Get figures tagged with this theme
        tagged_figures = _get_figures_for_theme(slug)
        figure_names = [f['name'] for f in tagged_figures]

        # Query RAG broadly for this theme
        rag_topics = [
            f"{name} Islamic history examples",
            f"{name} across eras centuries",
        ]
        if figure_names:
            rag_topics.append(f"{' '.join(figure_names[:5])} {name}")

        accounts = query_rag_multi(rag_topics, n_results=15)

        if not accounts:
            print(f"  No RAG results — skipping")
            continue

        # Build context
        chunks = []
        for i, acc in enumerate(accounts[:15], 1):
            chunks.append(
                f"[ACCOUNT {i}] Source: {acc['source']} | Era: {acc.get('era','?')} | "
                f"Score: {acc['similarity_score']:.3f}\n"
                f"{acc['content'][:500]}"
            )
        rag_context = "\n\n".join(chunks)

        # Add tagged figures context
        fig_context = ""
        if tagged_figures:
            fig_context = "\n\nFIGURES TAGGED WITH THIS THEME:\n"
            for f in tagged_figures[:20]:
                eras = ', '.join(f.get('era') or ['?'])
                fig_context += f"  - {f['name']} ({eras}): {f.get('relevance', '')}\n"

        # Classify via Haiku
        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4000,
                system=THEMATIC_PROMPT,
                messages=[{"role": "user", "content": (
                    f"THEME: {name} (slug: {slug})\n"
                    f"DESCRIPTION: {theme.get('description', '')}\n"
                    f"{fig_context}\n\n"
                    f"SOURCE ACCOUNTS:\n{rag_context}\n\n"
                    f"Return a JSON array of cross-era thematic groupings. "
                    f"Set the 'thread' field to '{slug}' for each."
                )}],
            )

            text = resp.content[0].text.strip()
            import re
            match = re.search(r'\[.*\]', text, re.DOTALL)
            stories = json.loads(match.group()) if match else []

        except Exception as e:
            print(f"  Classification error: {e}")
            errors += 1
            continue

        # Check for duplicates and insert
        existing = _get_existing_thread_stories(slug)

        for s in stories:
            s['thread'] = slug
            title = s.get('title', '')

            if title.lower() in existing:
                print(f"  [SKIP] Already exists: {title}")
                continue

            instances = s.get('instances', [])
            eras_covered = set(inst.get('era', '?') for inst in instances)

            if dry_run:
                print(f"  [DRY] {s.get('format','?')}: {title} (eras: {eras_covered})")
            else:
                story_id = _insert_thematic_story(cur, s)
                if story_id:
                    print(f"  ✓ {s.get('format','?')}: {title} (id={story_id}, eras: {eras_covered})")
                else:
                    errors += 1
                    continue

            existing.add(title.lower())
            total_stories += 1

        time.sleep(0.5)

    conn.close()

    print("\n" + "=" * 60)
    print("PASS 3 COMPLETE")
    print(f"  New thematic thread stories: {total_stories}")
    print(f"  Errors: {errors}")

    return total_stories


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pass 3: Thematic Thread Extraction")
    parser.add_argument("--theme", type=str, nargs="+", default=None,
                        help="Specific theme slugs to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't insert, just classify")
    args = parser.parse_args()

    run_extraction(themes=args.theme, dry_run=args.dry_run)
