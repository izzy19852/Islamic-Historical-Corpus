"""
Islam Stories — Generate Sensitivity Guide
Generates the expanded sensitivity tier document with per-figure
depiction rules, the Karbala Protocol, and the Never/Always list.

Output: docs/islam_stories_sensitivity_guide.md

Run:  python -m rag.knowledge.generate_sensitivity_guide
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


def load_figures_by_tier():
    """Pull figures grouped by sensitivity tier with depiction-relevant data."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    cur.execute("""
        SELECT f.id, f.name, f.sensitivity_tier, f.known_for,
               f.generation, f.era, f.series, f.dramatic_question
        FROM figures f
        ORDER BY
            CASE f.sensitivity_tier
                WHEN 'S' THEN 0 WHEN 'A' THEN 1
                WHEN 'B' THEN 2 WHEN 'C' THEN 3
            END,
            f.name
    """)
    figures = [dict(r) for r in cur.fetchall()]
    conn.close()

    by_tier = {"S": [], "A": [], "B": [], "C": []}
    for f in figures:
        t = f["sensitivity_tier"].strip()
        if t in by_tier:
            by_tier[t].append(f)
    return by_tier


def generate_guide(by_tier):
    """Generate the sensitivity guide via Claude API."""
    client = anthropic.Anthropic()

    # Serialize tier data compactly
    tier_data = {}
    for tier, figs in by_tier.items():
        tier_data[tier] = [
            {"name": f["name"], "known_for": f.get("known_for", ""),
             "generation": f.get("generation"), "era": f.get("era", [])}
            for f in figs
        ]

    data_json = json.dumps(tier_data, indent=1, ensure_ascii=False, default=str)

    prompt = f"""Generate islam_stories_sensitivity_guide.md — a production reference document
for Islam Stories, a cinematic Islamic history YouTube series.

This document governs how EVERY figure is depicted. Writers consult it mid-production.

CURRENT FIGURE COUNTS:
- Tier S: {len(by_tier['S'])} figures (Prophet, wives, caliphs, Quranic prophets)
- Tier A: {len(by_tier['A'])} figures (senior companions, Ahl al-Bayt)
- Tier B: {len(by_tier['B'])} figures (tabi'un, later figures, full depiction)
- Tier C: {len(by_tier['C'])} figures (contested/controversial figures)

STRUCTURE:

## The Four Tiers — Expanded

### TIER S: NEVER DEPICTED
The Prophet Muhammad (PBUH), his wives (Mothers of the Believers),
the Four Rightly Guided Caliphs (Abu Bakr, Umar, Uthman, Ali).

WHAT THIS MEANS IN PRACTICE:
- No visual depiction of any kind
- No invented dialogue under any circumstances
- No description of physical appearance
- Referenced only through others' reactions and documented effects
- Their presence is felt through: empty seats, turned backs,
  reactions of Tier B characters, letters/manuscripts shown,
  consequences of their documented decisions

THE ONE EXCEPTION:
Authenticated hadith quotes may be used in NARRATION (not dialogue) with
explicit citation: "Al-Tabari records that Abu Bakr said..."

### TIER A: SCHOLARLY CARE REQUIRED
Senior companions, members of Ahl al-Bayt. List ALL {len(by_tier['A'])} Tier A figures.

For the key ones (Husayn, Hassan, Zaynab, Aisha, Bilal, Hamza, Fatimah,
Salman al-Farisi, Abu Dharr, Ammar ibn Yasir), provide INDIVIDUAL rules:
- What can be depicted
- What requires care
- Specific documented moments usable in scripts

### TIER B: FULL DEPICTION PERMITTED
Full cinematic depiction. Dramatized dialogue clearly framed.
Still requires source attribution for specific historical claims.

### TIER C: CONTESTED FIGURES
Yazid, Al-Hajjaj, Aurangzeb, etc. Document actions without moral judgment.
Acknowledge scholarly dispute. Give their stated reasoning. Never use as simple villains.

## THE KARBALA PROTOCOL
Karbala (680 CE) is the most sensitive event in Islamic history.
MANDATORY RULES:
- Never take a position on the legitimacy question
- Always present both Sunni AND Shia accounts where they differ
- Husayn is never depicted directly — Zaynab is the witness and voice
- The 72: documented names in sources, not anonymous masses
- The water cut off: documented, must appear
- Secondary betrayal (Kufa's abandonment) as important as primary

## THE FOUR-SOURCE CHECK
Before any claim appears in a script:
1. Which primary source documents this?
2. What is the chain strength?
3. Do other sources agree or disagree?
4. If disagree — is the disagreement surfaced?

## THE NEVER/ALWAYS LIST

NEVER:
- Show the face of any Tier S figure
- Write invented dialogue for Tier S/A figures
- Have a character explain their own symbolic meaning
- Resolve scholarly debates — surface them and leave them open
- Depict the Prophet (PBUH) in any way
- Portray the Fitna as a simple good vs evil story
- Use anachronistic framing or modern moral vocabulary

ALWAYS:
- Attribute claims to specific sources
- Flag scholarly disputes
- Show Tier S figures' impact through Tier B characters' reactions
- Give every major figure a question the episode leaves unanswered
- End on ambiguity, not resolution

FIGURE DATA FOR REFERENCE:
{data_json}

Write the complete document. Production-ready markdown.
Precise and actionable — writers consult this mid-shoot."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=12000,
        messages=[{"role": "user", "content": prompt}]
    )

    return response.content[0].text


def main():
    print("=" * 60)
    print("GENERATE SENSITIVITY GUIDE")
    print("=" * 60)

    print("\nLoading figures by tier...")
    by_tier = load_figures_by_tier()
    for t in ("S", "A", "B", "C"):
        print(f"  Tier {t}: {len(by_tier[t])} figures")

    print("\nGenerating guide via Claude API...")
    guide_text = generate_guide(by_tier)

    output_path = PROJECT_ROOT / "docs" / "islam_stories_sensitivity_guide.md"
    output_path.write_text(guide_text, encoding="utf-8")

    print(f"\n  Written: {output_path}")
    print(f"  Length:  {len(guide_text):,} chars, {guide_text.count(chr(10))} lines")
    print("=" * 60)


if __name__ == "__main__":
    main()
