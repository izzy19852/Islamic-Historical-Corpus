"""
Islam Stories — Generate Rashidun Character Guide
Pulls all Rashidun-era figures from the knowledge graph and generates
a comprehensive character depiction guide for script writers.

Output: docs/islam_stories_rashidun_character_guide.md

Run:  python -m rag.knowledge.generate_rashidun_guide
"""

import os
import sys
import json
import psycopg2
import psycopg2.extras
import anthropic
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def load_rashidun_figures():
    """Pull all Rashidun-era figures with full knowledge graph data."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    # Core figure data — only figures who appear in Rashidun seasons
    cur.execute("""
        SELECT f.id, f.name, f.sensitivity_tier, f.dramatic_question,
               f.known_for, f.death_circumstance, f.birth_death,
               f.generation, f.era, f.series
        FROM figures f
        WHERE 'rashidun' = ANY(f.era)
        ORDER BY
            CASE f.sensitivity_tier
                WHEN 'S' THEN 0 WHEN 'A' THEN 1
                WHEN 'B' THEN 2 WHEN 'C' THEN 3
            END,
            f.name
    """)
    figures = [dict(r) for r in cur.fetchall()]

    for fig in figures:
        fid = fig["id"]

        # Motivations
        cur.execute("""
            SELECT motivation, is_primary, conflicts_with, evidence
            FROM figure_motivations WHERE figure_id = %s
        """, (fid,))
        fig["motivations"] = [dict(r) for r in cur.fetchall()]

        # Relationships
        cur.execute("""
            SELECT f2.name AS other_figure, fr.relationship, fr.description, fr.resolution
            FROM figure_relationships fr
            JOIN figures f2 ON f2.id = CASE
                WHEN fr.figure_a_id = %s THEN fr.figure_b_id
                ELSE fr.figure_a_id END
            WHERE fr.figure_a_id = %s OR fr.figure_b_id = %s
        """, (fid, fid, fid))
        fig["relationships"] = [dict(r) for r in cur.fetchall()]

        # Scholarly debates
        cur.execute("""
            SELECT topic, position_a, position_b, key_scholars, script_instruction
            FROM scholarly_debates WHERE figure_id = %s
        """, (fid,))
        fig["debates"] = [dict(r) for r in cur.fetchall()]

        # Death record
        cur.execute("""
            SELECT circumstance, last_words, last_words_source,
                   witnesses, location, date_ce, source
            FROM figure_deaths WHERE figure_id = %s
        """, (fid,))
        fig["death"] = dict(cur.fetchone()) if cur.rowcount else None

        # Quotes
        cur.execute("""
            SELECT quote, context, chain_strength, source, use_in_script
            FROM figure_quotes WHERE figure_id = %s
        """, (fid,))
        fig["quotes"] = [dict(r) for r in cur.fetchall()]

    conn.close()
    return figures


def generate_guide(figures):
    """Generate the character guide via Claude API."""
    client = anthropic.Anthropic()

    # Focus on the key ensemble: Tier S (caliphs), Tier A, and key Tier B
    # Full data dump would blow token limits, so serialize key figures
    key_figures = [f for f in figures if (
        f["sensitivity_tier"] in ("S", "A") or
        f["motivations"] or f["relationships"] or f["debates"] or
        f["death"] or f["quotes"] or
        f["name"] in {
            "Khalid ibn Walid", "Abu Ubayda ibn al-Jarrah", "Amr ibn al-As",
            "Muawiyah ibn Abi Sufyan", "Marwan ibn al-Hakam", "Wahshi ibn Harb",
            "Nusayba bint Ka'ab", "Abu Sufyan ibn Harb", "Al-Ashtar al-Nakha'i",
            "Hind bint Utba", "Ubaydallah ibn Umar",
        }
    )]

    # Serialize compactly
    fig_data = json.dumps(key_figures, indent=1, ensure_ascii=False, default=str)

    prompt = f"""You are building the definitive production reference document for Islam Stories,
a cinematic Islamic history YouTube series. This is the RASHIDUN CHARACTER GUIDE —
the document every writer reads before touching a Rashidun episode (632-661 CE).

You have knowledge graph data for {len(key_figures)} key figures (of {len(figures)} total Rashidun-era).

Generate the complete guide with this structure:

## Introduction — The Rashidun Ensemble Concept
- The Four Caliphs are Tier S: NEVER depicted. Their presence drives the drama through
  effects on Tier B characters, documented decisions, letters, and others' reactions.
- The drama is NOT between the caliphs. It is driven by third parties, structural forces,
  and the impossible speed of empire-building.
- Every episode centers a Tier B figure as protagonist. The caliphs are the gravity.

## Tier S Figures — Handling Rules
For Abu Bakr, Umar, Uthman, Ali: per-figure rules for how they appear WITHOUT being shown.
Key documented moments to reference (never depict). Visual rules.

## Tier A Figures — Special Handling
For each Tier A figure in the data: what requires scholarly care, what is documented
and usable, where the sensitivity line is.

## Tier B Figures — Full Depiction Rules
For each key Tier B figure:
1. DEPICTION: Full — Tier B
2. ERA PRESENCE: Which seasons
3. WHAT CAN BE SHOWN: documented actions, words, personality
4. WHAT CANNOT BE INVENTED: specific forbidden inventions
5. SCHOLARLY DEBATE TO SURFACE: the unresolved question for the audience
6. SIGNATURE DETAILS: specific sourced details that make them human
7. RELATIONSHIP DYNAMICS: how they interact with ensemble

## Tier C Figures — Contested
For each Tier C figure: depiction rules, what makes them complex, both sides.

## What Every Rashidun Script Must Do
Closing checklist for writers.

CRITICAL RULES:
- Source every claim. "Al-Tabari records..." not "sources say..."
- The Fitna is a TRAGEDY, not a villain story. All sides had principled positions.
- Marwan is the dramatic engine of Uthman's fall (scholarly consensus on the letter).
- Ali PROTECTED Uthman. His son Hasan was wounded guarding Uthman's house.
- Never resolve scholarly debates. Surface them. Let the audience sit with ambiguity.
- End episodes on the question, not the answer.

FIGURE DATA:
{fig_data}

Write the complete document. Production-ready markdown.
This is a working reference, not an academic paper — tell writers exactly
what to do and what not to do for each figure."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=16000,
        messages=[{"role": "user", "content": prompt}]
    )

    return response.content[0].text


def main():
    print("=" * 60)
    print("GENERATE RASHIDUN CHARACTER GUIDE")
    print("=" * 60)

    print("\nLoading Rashidun figures from knowledge graph...")
    figures = load_rashidun_figures()
    print(f"  {len(figures)} total Rashidun-era figures")

    tier_counts = {}
    for f in figures:
        t = f["sensitivity_tier"]
        tier_counts[t] = tier_counts.get(t, 0) + 1
    for t in ("S", "A", "B", "C"):
        print(f"  Tier {t}: {tier_counts.get(t, 0)}")

    enriched = sum(1 for f in figures if f["motivations"] or f["relationships"]
                   or f["debates"] or f["death"] or f["quotes"])
    print(f"  With KG enrichment: {enriched}")

    print("\nGenerating guide via Claude API...")
    guide_text = generate_guide(figures)

    output_path = PROJECT_ROOT / "docs" / "islam_stories_rashidun_character_guide.md"
    output_path.write_text(guide_text, encoding="utf-8")

    print(f"\n  Written: {output_path}")
    print(f"  Length:  {len(guide_text):,} chars, {guide_text.count(chr(10))} lines")
    print("=" * 60)


if __name__ == "__main__":
    main()
