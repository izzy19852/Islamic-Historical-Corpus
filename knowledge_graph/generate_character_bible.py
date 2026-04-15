"""
Islam Stories — Character Bible Pass 1 Generator
Generates comprehensive markdown Character Bible entries from knowledge graph.
Does NOT require chunk_metadata — pulls from seeded tables only.

Output: ~/islam-stories/character_bible/{era}/{figure_name}.md

Run:  python -m rag.knowledge.generate_character_bible [--figure "Name"] [--all] [--concurrency 3]
"""

import os
import sys
import json
import time
import argparse
import psycopg2
import psycopg2.extras
import anthropic
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
MODEL = "claude-sonnet-4-20250514"
OUTPUT_BASE = Path(os.path.dirname(__file__)).parent.parent / "character_bible"

ERA_FOLDER_MAP = {
    "rashidun": "rashidun",
    "umayyad": "umayyad",
    "abbasid": "abbasid",
    "mongol": "later",
    "mamluk": "later",
    "crusades": "later",
    "andalusia": "later",
    "ottoman": "later",
    "south_asia": "later",
    "africa": "later",
    "resistance_colonial": "later",
    "persia": "later",
}

# ═══════════════════════════════════════════════════════════════════════
# SERIES RULES — embedded in every prompt
# ═══════════════════════════════════════════════════════════════════════

SERIES_RULES = """SERIES RULES:
- Tier S figures (Prophet PBUH, Four Caliphs, Prophet's wives) are NEVER depicted.
  Referenced only through others' eyes. No invented dialogue ever.
- Tier A figures require scholarly care — no invented dialogue, cite all claims,
  Karbala always presents Sunni AND Shia accounts without resolving.
- Tier B figures can be fully depicted with dramatized dialogue clearly framed.
- Tier C figures: document actions only, acknowledge dispute, give their reasoning, never simple villain.
- Every claim must trace to a primary source.
- Conflicts between sources are surfaced, never smoothed over.
- The Four Caliphs were NOT antagonists to each other — handle succession with nuance.
- Gaps in the record are acknowledged explicitly ("History is silent on...").
- This is a character-driven cinematic universe, not a documentary."""


def _get_conn():
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def get_figure_full_profile(figure_id: int) -> dict:
    """Pull complete knowledge graph profile for a figure."""
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM figures WHERE id = %s", (figure_id,))
    fig = cur.fetchone()
    if not fig:
        conn.close()
        return None

    cur.execute("""
        SELECT motivation, is_primary, conflicts_with, evidence
        FROM figure_motivations WHERE figure_id = %s ORDER BY is_primary DESC
    """, (figure_id,))
    motivations = cur.fetchall()

    cur.execute("SELECT * FROM figure_deaths WHERE figure_id = %s", (figure_id,))
    death = cur.fetchone()

    cur.execute("""
        SELECT quote, context, chain_strength, source, use_in_script
        FROM figure_quotes WHERE figure_id = %s
    """, (figure_id,))
    quotes = cur.fetchall()

    cur.execute("""
        SELECT fl.lineage_type, fl.direction, fl.divergence, fl.notes,
               COALESCE(f2.name, fl.related_name) AS related_name
        FROM figure_lineage fl
        LEFT JOIN figures f2 ON fl.related_id = f2.id
        WHERE fl.figure_id = %s
    """, (figure_id,))
    lineage = cur.fetchall()

    cur.execute("""
        SELECT fr.relationship, fr.description, fr.resolution,
               fa.name AS figure_a_name, fb.name AS figure_b_name
        FROM figure_relationships fr
        JOIN figures fa ON fr.figure_a_id = fa.id
        JOIN figures fb ON fr.figure_b_id = fb.id
        WHERE fr.figure_a_id = %s OR fr.figure_b_id = %s
    """, (figure_id, figure_id))
    relationships = cur.fetchall()

    cur.execute("""
        SELECT t.slug, t.name
        FROM figure_themes ft JOIN themes t ON ft.theme_id = t.id
        WHERE ft.figure_id = %s
    """, (figure_id,))
    themes = cur.fetchall()

    cur.execute("""
        SELECT name, date_ce, significance
        FROM events WHERE %s = ANY(figure_ids) ORDER BY date_ce
    """, (figure_id,))
    events = cur.fetchall()

    cur.execute("""
        SELECT topic, position_a, position_b, key_scholars, script_instruction
        FROM scholarly_debates WHERE figure_id = %s
    """, (figure_id,))
    debates = cur.fetchall()

    cur.execute("SELECT COUNT(*) as cnt FROM chunk_metadata WHERE %s = ANY(figure_ids)", (figure_id,))
    chunk_count = cur.fetchone()["cnt"]

    conn.close()

    return {
        **dict(fig),
        "motivations": [dict(m) for m in motivations],
        "death": dict(death) if death else None,
        "quotes": [dict(q) for q in quotes],
        "lineage": [dict(l) for l in lineage],
        "relationships": [dict(r) for r in relationships],
        "themes": [dict(t) for t in themes],
        "events": [dict(e) for e in events],
        "debates": [dict(d) for d in debates],
        "chunk_coverage": chunk_count,
    }


def generate_bible_entry(client, profile: dict) -> str:
    """Generate a full Character Bible markdown entry via Claude."""
    prompt = f"""{SERIES_RULES}

FIGURE PROFILE FROM KNOWLEDGE GRAPH:
{json.dumps(profile, indent=2, default=str)}

Write a comprehensive Character Bible entry with these sections:

## {profile['name']} — {profile.get('known_for') or 'Figure in Islamic History'}

### Who They Are
2-3 paragraphs. Who this person is, what era they lived in,
why they matter to the Islam Stories universe.
Every factual claim attributed to its source.

### Sensitivity Tier: {profile.get('sensitivity_tier', 'B')}
Clear statement of what can and cannot be depicted.
Specific rules for this figure based on their tier.

### The Dramatic Question
The single question their life poses that the audience
will sit with across their arc.

### Documented Motivations
What actually drove them — from the knowledge graph.
Where motivations conflict with each other, name that tension.

### Key Relationships
Who they connect to in the universe.
Nature of each relationship. Dramatic potential.

### Their Arc in the Series
Which season(s) and/or series they appear in.
The trajectory of their story.

### Depiction Rules
Specific guidance for scriptwriters:
- What scenes can show
- What must be handled with care
- Which scholarly debates must be acknowledged
- Specific lines/moments from primary sources that anchor their story

### Primary Sources
Which sources contain authenticated material about them.
What those sources say — briefly.

### Last Words / Death
If documented — cite source and chain strength.
Dramatic significance for their final episode.

Keep the tone: authoritative, specific, useful to a writer.
Not an encyclopedia entry — a writer's tool."""

    response = client.messages.create(
        model=MODEL,
        max_tokens=3000,
        system=[{
            "type": "text",
            "text": (
                "You are the lead researcher for Islam Stories, a character-driven cinematic "
                "Islamic civilizational universe. You write Character Bible entries that scriptwriters "
                "will use as their primary reference. Be specific, cite sources, surface conflicts, "
                "and never smooth over scholarly disagreements. Every entry must be useful to someone "
                "writing a scene — not just informative, but dramatically actionable."
            ),
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text


def generate_stub(profile: dict) -> str:
    """Write a minimal stub for figures with thin data."""
    name = profile.get("name", "Unknown")
    tier = profile.get("sensitivity_tier", "B")
    gen = profile.get("generation", "unknown")
    era = profile.get("era", [])
    series = profile.get("series", [])
    birth = profile.get("birth_death", "unknown")

    return f"""# {name}

**Tier:** {tier}
**Generation:** {gen}
**Era:** {', '.join(era) if era else 'unknown'}
**Series:** {', '.join(series) if series else 'unassigned'}
**Dates:** {birth}

_Stub entry — awaiting chunk classification and enrichment for full profile._

_Run Pass 2 after chunk_metadata is populated._
"""


def run_generator(figure_name=None, all_figures=False, concurrency=3):
    conn = _get_conn()
    cur = conn.cursor()

    if figure_name:
        cur.execute("""
            SELECT id, name, era, sensitivity_tier
            FROM figures
            WHERE name = %s OR %s = ANY(name_variants)
        """, (figure_name, figure_name))
        figures = cur.fetchall()
    elif all_figures:
        cur.execute("""
            SELECT id, name, era, sensitivity_tier
            FROM figures
            ORDER BY
                CASE sensitivity_tier
                    WHEN 'B' THEN 1
                    WHEN 'A' THEN 2
                    WHEN 'C' THEN 3
                    WHEN 'S' THEN 4
                END,
                name
        """)
        figures = cur.fetchall()
    else:
        print("Specify --figure 'Name' or --all")
        conn.close()
        return

    conn.close()

    print("=" * 60)
    print("CHARACTER BIBLE — PASS 1 GENERATOR")
    print(f"Figures: {len(figures)} | Model: {MODEL} | Concurrency: {concurrency}")
    print(f"Output: {OUTPUT_BASE}")
    print("=" * 60)

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    generated = 0
    skipped = 0
    stubs = 0
    errors = 0
    start_time = time.time()

    def process_figure(fig):
        fig_id = fig["id"]
        fig_name = fig["name"]
        fig_era = fig["era"] or ["unknown"]
        primary_era = fig_era[0] if fig_era else "unknown"
        folder = ERA_FOLDER_MAP.get(primary_era, "later")

        output_dir = OUTPUT_BASE / folder
        output_dir.mkdir(parents=True, exist_ok=True)

        safe_name = fig_name.lower().replace(" ", "_").replace("'", "").replace("/", "_").replace("(", "").replace(")", "")
        output_file = output_dir / f"{safe_name}.md"

        # Skip if already generated
        if output_file.exists() and output_file.stat().st_size > 500:
            return fig_name, "skip", output_file

        profile = get_figure_full_profile(fig_id)
        if not profile:
            return fig_name, "error", None

        has_data = (
            profile.get("dramatic_question") or
            profile.get("known_for") or
            profile.get("motivations") or
            profile.get("death")
        )

        if not has_data:
            output_file.write_text(generate_stub(profile))
            return fig_name, "stub", output_file

        try:
            entry = generate_bible_entry(client, profile)
            output_file.write_text(entry)
            return fig_name, "ok", output_file
        except Exception as e:
            if "rate_limit" in str(e):
                time.sleep(15)
                try:
                    entry = generate_bible_entry(client, profile)
                    output_file.write_text(entry)
                    return fig_name, "ok", output_file
                except Exception as e2:
                    return fig_name, "error", str(e2)
            return fig_name, "error", str(e)

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(process_figure, fig): fig for fig in figures}

        for i, future in enumerate(as_completed(futures), 1):
            fig_name, status, detail = future.result()

            if status == "ok":
                generated += 1
            elif status == "skip":
                skipped += 1
            elif status == "stub":
                stubs += 1
            else:
                errors += 1

            if i % 10 == 0 or i == len(figures):
                elapsed = time.time() - start_time
                print(f"  [{i}/{len(figures)}] gen={generated} skip={skipped} stub={stubs} err={errors} | {elapsed:.0f}s")

    elapsed = time.time() - start_time

    print(f"\n{'=' * 60}")
    print("PASS 1 COMPLETE")
    print(f"  Generated (full): {generated}")
    print(f"  Stubs:            {stubs}")
    print(f"  Skipped:          {skipped}")
    print(f"  Errors:           {errors}")
    print(f"  Time:             {elapsed:.0f}s")
    print(f"  Output:           {OUTPUT_BASE}")
    print("=" * 60)

    # Validation
    for era_dir in ["rashidun", "umayyad", "abbasid", "later"]:
        p = OUTPUT_BASE / era_dir
        if p.exists():
            count = len(list(p.glob("*.md")))
            print(f"  {era_dir}/: {count} entries")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Character Bible Pass 1 Generator")
    parser.add_argument("--figure", type=str, help="Generate for a single figure")
    parser.add_argument("--all", action="store_true", help="Generate for all figures")
    parser.add_argument("--concurrency", type=int, default=3, help="Parallel API calls")
    args = parser.parse_args()

    run_generator(
        figure_name=args.figure,
        all_figures=args.all,
        concurrency=args.concurrency,
    )
