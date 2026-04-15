"""
Islam Stories — Drama Table Extractor
Extracts motivations, deaths, quotes, legacies, transformations, and journeys
from character bibles using Claude Haiku.

Processes figures in batches by era.

Run:  python -m rag.knowledge.extract_drama_tables [--dry-run] [--batch-size 10]
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


EXTRACTION_PROMPT = """You are extracting structured dramatic data from Islamic history figure profiles.
Return ONLY valid JSON — no markdown, no commentary.

For each figure below, extract what the data supports. If no data exists for a field, omit it.

{figure_block}

Return JSON:
{{
  "figures": [
    {{
      "name": "exact name",
      "motivations": [
        {{
          "motivation": "FAITH|POWER|LOYALTY|SURVIVAL|PRAGMATISM|JUSTICE|REVENGE|LEGACY|IDEOLOGY|KNOWLEDGE",
          "is_primary": true/false,
          "conflicts_with": "what this motivation conflicts with, or null",
          "evidence": "brief citation from the profile"
        }}
      ],
      "death": {{
        "circumstance": "how they died — battle, illness, assassination, natural, martyrdom, execution, unknown",
        "last_words": "documented last words or null",
        "last_words_source": "source attribution or null",
        "location": "where they died or null",
        "date_ce": "approximate date CE or null"
      }},
      "quotes": [
        {{
          "quote": "exact or near-exact quote attributed to this figure",
          "context": "when/why they said it",
          "chain_strength": "sahih|hasan|daif|unknown|scholarly",
          "source": "source attribution",
          "use_in_script": "how this could be used dramatically — voiceover, dialogue, etc."
        }}
      ],
      "legacy": [
        {{
          "legacy": "what lasting impact this figure had",
          "time_horizon": "IMMEDIATE|GENERATIONAL|CIVILIZATIONAL",
          "still_active": true/false
        }}
      ],
      "transformations": [
        {{
          "transformation": "CONVERSION|WITHDRAWAL|REDEMPTION|HARDENING|RADICALIZATION|DISILLUSIONMENT|ENLIGHTENMENT",
          "trigger_event": "what caused the change",
          "before_state": "who they were before",
          "after_state": "who they became after"
        }}
      ]
    }}
  ]
}}

RULES:
- Use names EXACTLY as provided
- Only extract what the profile data supports — do NOT invent
- Quotes must be attributed, not fabricated
- For chain_strength: use "scholarly" for quotes from historical accounts, "unknown" if uncertain
- Deaths: use "unknown" if death circumstances not described
- Each figure should have at least 1 motivation if any data exists
- Transformations are major character arc changes, not minor events
- motivation MUST be one of: FAITH, POWER, LOYALTY, SURVIVAL, PRAGMATISM, JUSTICE, REVENGE, LEGACY, IDEOLOGY, KNOWLEDGE
- transformation MUST be one of: CONVERSION, WITHDRAWAL, REDEMPTION, HARDENING, RADICALIZATION, DISILLUSIONMENT, ENLIGHTENMENT
"""


def _call_haiku(client, prompt, max_retries=4):
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


def _parse_json(raw):
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)


def load_bible(name):
    safe = name.lower().replace(" ", "_").replace("'", "").replace("/", "_").replace("(", "").replace(")", "")
    for era_dir in ["rashidun", "umayyad", "abbasid", "later"]:
        path = BIBLE_DIR / era_dir / f"{safe}.md"
        if path.exists():
            return path.read_text()[:1500]
    return ""


def get_figures():
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, era, known_for, death_circumstance, sensitivity_tier
        FROM figures
        ORDER BY
            CASE sensitivity_tier WHEN 'B' THEN 1 WHEN 'C' THEN 2 WHEN 'A' THEN 3 WHEN 'S' THEN 4 END,
            id
    """)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_existing_ids(table, col="figure_id"):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute(f"SELECT DISTINCT {col} FROM {table}")
    ids = {r[0] for r in cur.fetchall()}
    conn.close()
    return ids


def build_figure_block(figures):
    parts = []
    for fig in figures:
        bible = load_bible(fig["name"])
        block = f"""---
Name: {fig['name']}
Known for: {fig.get('known_for') or 'unknown'}
Tier: {fig['sensitivity_tier']}
Death circumstance: {fig.get('death_circumstance') or 'unknown'}
"""
        if bible:
            block += f"Character bible:\n{bible}\n"
        parts.append(block)
    return "\n".join(parts)


def insert_data(fig_name, fig_data, name_to_id, dry_run, existing_death_ids=set()):
    fig_id = name_to_id.get(fig_name.lower())
    if not fig_id:
        return 0, 0, 0, 0, 0

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    m_count = d_count = q_count = l_count = t_count = 0

    VALID_MOTIVATIONS = {'FAITH','POWER','LOYALTY','SURVIVAL','PRAGMATISM','JUSTICE','REVENGE','LEGACY','IDEOLOGY','KNOWLEDGE'}
    VALID_TRANSFORMATIONS = {'CONVERSION','WITHDRAWAL','REDEMPTION','HARDENING','RADICALIZATION','DISILLUSIONMENT','ENLIGHTENMENT'}

    # Motivations
    for mot in fig_data.get("motivations", []):
        motivation = (mot.get("motivation") or "").upper()
        if motivation not in VALID_MOTIVATIONS:
            continue
        cur.execute("""
            INSERT INTO figure_motivations (figure_id, motivation, is_primary, conflicts_with, evidence)
            VALUES (%s, %s, %s, %s, %s)
        """, (fig_id, motivation, mot.get("is_primary", False),
              mot.get("conflicts_with"), mot.get("evidence")))
        m_count += 1

    # Death (unique constraint — one per figure)
    death = fig_data.get("death")
    if death and death.get("circumstance") and death["circumstance"].lower() != "unknown":
        if fig_id not in existing_death_ids:
            try:
                cur.execute("""
                    INSERT INTO figure_deaths (figure_id, circumstance, last_words, last_words_source,
                                               location, date_ce)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (fig_id, death["circumstance"], death.get("last_words"),
                      death.get("last_words_source"), death.get("location"), death.get("date_ce")))
                existing_death_ids.add(fig_id)
                d_count += 1
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                existing_death_ids.add(fig_id)

    # Quotes
    for qt in fig_data.get("quotes", []):
        quote = qt.get("quote")
        if not quote:
            continue
        chain = (qt.get("chain_strength") or "unknown").lower()
        if chain not in ("sahih", "hasan", "daif", "unknown", "scholarly"):
            chain = "unknown"
        cur.execute("""
            INSERT INTO figure_quotes (figure_id, quote, context, chain_strength, source, use_in_script)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (fig_id, quote, qt.get("context"), chain,
              qt.get("source"), qt.get("use_in_script")))
        q_count += 1

    # Legacies
    for leg in fig_data.get("legacy", []):
        legacy_text = leg.get("legacy")
        if not legacy_text:
            continue
        time_h = (leg.get("time_horizon") or "GENERATIONAL").upper()
        if time_h not in ("IMMEDIATE", "GENERATIONAL", "CIVILIZATIONAL"):
            time_h = "GENERATIONAL"
        cur.execute("""
            INSERT INTO figure_legacies (figure_id, legacy, time_horizon, still_active)
            VALUES (%s, %s, %s, %s)
        """, (fig_id, legacy_text, time_h, leg.get("still_active", False)))
        l_count += 1

    # Transformations
    for tr in fig_data.get("transformations", []):
        transformation = (tr.get("transformation") or "").upper()
        if transformation not in VALID_TRANSFORMATIONS:
            continue
        cur.execute("""
            INSERT INTO figure_transformations (figure_id, transformation, trigger_event,
                                                before_state, after_state)
            VALUES (%s, %s, %s, %s, %s)
        """, (fig_id, transformation, tr.get("trigger_event"),
              tr.get("before_state"), tr.get("after_state")))
        t_count += 1

    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"    DB commit error for {fig_name}: {e}")
    conn.close()
    return m_count, d_count, q_count, l_count, t_count


def run_extraction(dry_run=False, batch_size=10):
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    figures = get_figures()

    # Build name->id map
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("SELECT id, name, name_variants FROM figures")
    name_to_id = {}
    for r in cur.fetchall():
        name_to_id[r["name"].lower()] = r["id"]
        for v in (r["name_variants"] or []):
            name_to_id[v.lower()] = r["id"]
    conn.close()

    # Skip figures that already have motivations
    existing_mot = get_existing_ids("figure_motivations")
    figures = [f for f in figures if f["id"] not in existing_mot]

    print("=" * 60)
    print("DRAMA TABLE EXTRACTOR")
    print(f"Model: {MODEL}")
    print(f"Figures to process: {len(figures)}")
    print(f"Batch size: {batch_size}")
    print(f"Dry run: {dry_run}")
    print("=" * 60)

    total_m = total_d = total_q = total_l = total_t = 0
    total_calls = 0
    failed = 0
    existing_death_ids = get_existing_ids("figure_deaths")

    for i in range(0, len(figures), batch_size):
        batch = figures[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(figures) + batch_size - 1) // batch_size

        print(f"  [{batch_num}/{total_batches}] {len(batch)} figures...", end=" ", flush=True)

        figure_block = build_figure_block(batch)
        prompt = EXTRACTION_PROMPT.format(figure_block=figure_block)

        try:
            raw = _call_haiku(client, prompt)
            data = _parse_json(raw)
            total_calls += 1
        except Exception as e:
            print(f"FAILED: {e}")
            failed += 1
            continue

        for fig_data in data.get("figures", []):
            name = fig_data.get("name", "")
            if not dry_run:
                m, d, q, l, t = insert_data(name, fig_data, name_to_id, dry_run, existing_death_ids)
                total_m += m
                total_d += d
                total_q += q
                total_l += l
                total_t += t

        batch_names = [f["name"][:20] for f in batch[:3]]
        print(f"OK — {', '.join(batch_names)}...")
        time.sleep(0.3)

    print(f"\n{'=' * 60}")
    print("EXTRACTION REPORT")
    print(f"  API calls: {total_calls}")
    print(f"  Failed: {failed}")
    print(f"  New motivations:      {total_m}")
    print(f"  New deaths:           {total_d}")
    print(f"  New quotes:           {total_q}")
    print(f"  New legacies:         {total_l}")
    print(f"  New transformations:  {total_t}")

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    for table in ["figure_motivations", "figure_deaths", "figure_quotes",
                  "figure_legacies", "figure_transformations"]:
        cur.execute(f"SELECT count(*) FROM {table}")
        print(f"  Total in {table}: {cur.fetchone()[0]}")
    conn.close()
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract drama tables from character bibles")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=10)
    args = parser.parse_args()
    run_extraction(dry_run=args.dry_run, batch_size=args.batch_size)
