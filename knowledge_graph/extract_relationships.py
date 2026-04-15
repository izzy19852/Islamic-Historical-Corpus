"""
Islam Stories — Relationship & Lineage Extractor
Extracts figure_relationships and figure_lineage from character bibles
and known_for data using Claude Haiku for structured extraction.

Processes figures by era group to provide cross-figure context.
Only inserts relationships where both figures exist in DB.

Run:  python -m rag.knowledge.extract_relationships [--dry-run] [--era rashidun]
"""

import argparse
import json
import os
import sys
import time
import random

import anthropic
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from pathlib import Path

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..', '..')
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
MODEL = "claude-haiku-4-5-20251001"
BIBLE_DIR = Path(PROJECT_ROOT) / "character_bible"

# Valid enum values from schema
VALID_RELATIONSHIPS = {
    'ALLY', 'ANTAGONIST', 'RIVAL', 'MUTUAL_RESPECT',
    'IDEOLOGICAL_OPPONENT', 'PARALLEL', 'POLITICAL_OPPONENT',
}

VALID_LINEAGE_TYPES = {
    'BIOLOGICAL', 'POLITICAL_HEIR', 'MILITARY_PATRON',
    'INTELLECTUAL', 'SUFI_SILSILA',
}

VALID_DIRECTIONS = {'ancestor', 'descendant'}

VALID_DIVERGENCES = {
    'SURPASSED', 'BETRAYED', 'COMPLETED', 'CORRUPTED', 'ABANDONED', 'MARTYRED',
    None,
}

VALID_RESOLUTIONS = {
    'RECONCILED', 'UNRESOLVED', 'VICTORY_A', 'VICTORY_B',
    'MUTUAL_DESTRUCTION', 'TRANSCENDED', 'DEATH_ENDED_IT',
    None,
}

EXTRACTION_PROMPT = """You are extracting relationships and lineage connections from
Islamic history figure profiles. Return ONLY valid JSON.

Here are the figures in this era group. For each, I provide their name, known_for,
and character bible excerpt (if available).

{figure_block}

Extract ALL relationships and lineage connections between these figures.

Return JSON with two arrays:

{{
  "relationships": [
    {{
      "figure_a": "exact name from list",
      "figure_b": "exact name from list",
      "relationship": "ALLY|ANTAGONIST|RIVAL|MUTUAL_RESPECT|IDEOLOGICAL_OPPONENT|PARALLEL|POLITICAL_OPPONENT",
      "description": "one sentence describing the relationship",
      "resolution": "RECONCILED|UNRESOLVED|VICTORY_A|VICTORY_B|MUTUAL_DESTRUCTION|TRANSCENDED|DEATH_ENDED_IT|null"
    }}
  ],
  "lineage": [
    {{
      "figure": "exact name from list",
      "related": "exact name from list",
      "type": "BIOLOGICAL|POLITICAL_HEIR|MILITARY_PATRON|INTELLECTUAL|SUFI_SILSILA",
      "direction": "ancestor|descendant",
      "divergence": "SURPASSED|BETRAYED|COMPLETED|CORRUPTED|ABANDONED|MARTYRED|null",
      "notes": "brief note"
    }}
  ]
}}

RULES:
- Only use names EXACTLY as they appear in the figure list
- Both figures in a relationship must be from this list
- BIOLOGICAL lineage: parent/child, sibling, spouse
- POLITICAL_HEIR: succeeded in political office
- MILITARY_PATRON: commander/subordinate military relationship
- INTELLECTUAL: teacher/student, scholarly influence
- For direction: "ancestor" means figure is the parent/teacher/predecessor, "descendant" means figure is the child/student/successor
- Include ALL connections you can identify — companions who fought together, rivals, etc.
- Do NOT invent relationships not supported by the data
"""


def _call_haiku(client: anthropic.Anthropic, prompt: str, max_retries: int = 4) -> str:
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=8192,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except (anthropic._exceptions.OverloadedError,
                anthropic._exceptions.RateLimitError,
                anthropic._exceptions.APIConnectionError) as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            if attempt < max_retries - 1:
                print(f"    API error ({e.__class__.__name__}), retrying in {wait:.0f}s...")
                time.sleep(wait)
            else:
                raise


def _parse_json(raw: str) -> dict:
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)


def get_figures_by_era() -> dict:
    """Get all figures grouped by primary era."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, era, known_for, sensitivity_tier, ethnicity,
               death_circumstance, generation
        FROM figures
        ORDER BY id
    """)
    rows = cur.fetchall()
    conn.close()

    groups = {}
    for fig in rows:
        era = (fig["era"] or ["unknown"])[0]
        groups.setdefault(era, []).append(dict(fig))
    return groups


def get_figure_id_map() -> dict:
    """Get name -> id mapping for all figures."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("SELECT id, name, name_variants FROM figures")
    rows = cur.fetchall()
    conn.close()

    name_map = {}
    for r in rows:
        name_map[r["name"].lower()] = r["id"]
        for v in (r["name_variants"] or []):
            name_map[v.lower()] = r["id"]
    return name_map


def load_bible_excerpt(name: str) -> str:
    """Load first 500 chars of a character bible if it exists."""
    safe = name.lower().replace(" ", "_").replace("'", "").replace("/", "_").replace("(", "").replace(")", "")
    for era_dir in ["rashidun", "umayyad", "abbasid", "later"]:
        path = BIBLE_DIR / era_dir / f"{safe}.md"
        if path.exists():
            text = path.read_text()
            return text[:500]
    return ""


def build_figure_block(figures: list[dict]) -> str:
    """Build the prompt block for a group of figures."""
    parts = []
    for fig in figures:
        bible = load_bible_excerpt(fig["name"])
        block = f"""---
Name: {fig['name']}
Known for: {fig.get('known_for') or 'unknown'}
Tier: {fig['sensitivity_tier']}
Death: {fig.get('death_circumstance') or 'unknown'}
"""
        if bible:
            block += f"Bible excerpt: {bible}\n"
        parts.append(block)
    return "\n".join(parts)


def resolve_figure_id(name: str, name_map: dict) -> int | None:
    """Resolve a figure name to its DB id."""
    return name_map.get(name.lower())


def get_existing_pairs() -> set:
    """Get already-inserted relationship pairs to avoid duplicates."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT figure_a_id, figure_b_id FROM figure_relationships")
    rel_pairs = {(r[0], r[1]) for r in cur.fetchall()}
    cur.execute("SELECT figure_id, related_id FROM figure_lineage")
    lin_pairs = {(r[0], r[1]) for r in cur.fetchall()}
    conn.close()
    return rel_pairs, lin_pairs


def insert_relationships(relationships: list[dict], name_map: dict,
                         existing_pairs: set, dry_run: bool) -> int:
    """Insert validated relationships into DB."""
    if dry_run or not relationships:
        return 0

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    inserted = 0

    for rel in relationships:
        a_id = resolve_figure_id(rel["figure_a"], name_map)
        b_id = resolve_figure_id(rel["figure_b"], name_map)
        rtype = (rel.get("relationship") or "").upper()
        resolution = rel.get("resolution")
        if resolution and isinstance(resolution, str):
            resolution = resolution.upper() if resolution.lower() != "null" else None
        else:
            resolution = None

        if not a_id or not b_id or a_id == b_id:
            continue
        if rtype not in VALID_RELATIONSHIPS:
            continue
        if resolution and resolution not in VALID_RESOLUTIONS:
            resolution = None

        pair = (min(a_id, b_id), max(a_id, b_id))
        if pair in existing_pairs:
            continue

        cur.execute("""
            INSERT INTO figure_relationships (figure_a_id, figure_b_id, relationship, description, resolution)
            VALUES (%s, %s, %s, %s, %s)
        """, (a_id, b_id, rtype, rel.get("description", ""), resolution))
        existing_pairs.add(pair)
        inserted += 1

    conn.commit()
    conn.close()
    return inserted


def insert_lineage(lineage: list[dict], name_map: dict,
                   existing_pairs: set, dry_run: bool) -> int:
    """Insert validated lineage links into DB."""
    if dry_run or not lineage:
        return 0

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    inserted = 0

    for lin in lineage:
        fig_id = resolve_figure_id(lin["figure"], name_map)
        rel_id = resolve_figure_id(lin["related"], name_map)
        ltype = (lin.get("type") or "").upper()
        direction = (lin.get("direction") or "").lower()
        divergence = lin.get("divergence")
        if divergence and isinstance(divergence, str):
            divergence = divergence.upper() if divergence.lower() != "null" else None
        else:
            divergence = None

        if not fig_id or not rel_id or fig_id == rel_id:
            continue
        if ltype not in VALID_LINEAGE_TYPES:
            continue
        if direction not in VALID_DIRECTIONS:
            continue
        if divergence and divergence not in VALID_DIVERGENCES:
            divergence = None

        pair = (fig_id, rel_id)
        if pair in existing_pairs:
            continue

        cur.execute("""
            INSERT INTO figure_lineage (figure_id, related_id, related_name, lineage_type,
                                        direction, divergence, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (fig_id, rel_id, lin["related"], ltype, direction, divergence,
              lin.get("notes", "")))
        existing_pairs.add(pair)
        inserted += 1

    conn.commit()
    conn.close()
    return inserted


def run_extraction(era_filter: str = None, dry_run: bool = False, batch_size: int = 30):
    """Extract relationships for all figures, grouped by era."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    era_groups = get_figures_by_era()
    name_map = get_figure_id_map()
    rel_pairs, lin_pairs = get_existing_pairs()

    if era_filter:
        era_groups = {k: v for k, v in era_groups.items() if k == era_filter}

    print("=" * 60)
    print("RELATIONSHIP & LINEAGE EXTRACTOR")
    print(f"Model: {MODEL}")
    print(f"Dry run: {dry_run}")
    print(f"Batch size: {batch_size}")
    print(f"Eras: {list(era_groups.keys())}")
    print(f"Existing relationships: {len(rel_pairs)}")
    print(f"Existing lineage: {len(lin_pairs)}")
    print("=" * 60)

    total_rels = 0
    total_lins = 0
    total_calls = 0

    for era, figures in sorted(era_groups.items(), key=lambda x: -len(x[1])):
        print(f"\n── {era} ({len(figures)} figures) ──")

        # Process in batches to fit context window
        for i in range(0, len(figures), batch_size):
            batch = figures[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(figures) + batch_size - 1) // batch_size

            print(f"  Batch {batch_num}/{total_batches} ({len(batch)} figures)...",
                  end=" ", flush=True)

            figure_block = build_figure_block(batch)
            prompt = EXTRACTION_PROMPT.format(figure_block=figure_block)

            try:
                raw = _call_haiku(client, prompt)
                data = _parse_json(raw)
                total_calls += 1
            except (json.JSONDecodeError, Exception) as e:
                print(f"FAILED: {e}")
                continue

            rels = data.get("relationships", [])
            lins = data.get("lineage", [])

            r_inserted = insert_relationships(rels, name_map, rel_pairs, dry_run)
            l_inserted = insert_lineage(lins, name_map, lin_pairs, dry_run)

            total_rels += r_inserted
            total_lins += l_inserted

            print(f"OK — {len(rels)} rels ({r_inserted} new), {len(lins)} lin ({l_inserted} new)")
            time.sleep(0.3)

    print(f"\n{'=' * 60}")
    print("EXTRACTION REPORT")
    print(f"  API calls: {total_calls}")
    print(f"  New relationships: {total_rels}")
    print(f"  New lineage links: {total_lins}")

    # Final counts
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM figure_relationships")
    print(f"  Total relationships in DB: {cur.fetchone()[0]}")
    cur.execute("SELECT count(*) FROM figure_lineage")
    print(f"  Total lineage in DB: {cur.fetchone()[0]}")
    conn.close()
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract relationships from character bibles")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--era", type=str, default=None, help="Process only this era")
    parser.add_argument("--batch-size", type=int, default=30,
                        help="Figures per API call (default: 30)")
    args = parser.parse_args()
    run_extraction(era_filter=args.era, dry_run=args.dry_run, batch_size=args.batch_size)
