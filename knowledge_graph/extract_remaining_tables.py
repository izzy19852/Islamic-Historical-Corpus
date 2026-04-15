"""
Islam Stories — Remaining Tables Extractor
Populates: political_factions, figure_factions, figure_journeys,
           political_betrayals, alliance_reversals, cultural_encounters,
           event_causes, event_economics, event_geography, event_themes.

Uses character bibles + events + figure data via Haiku.
Processes in phases: factions first (others depend on them).

Run:  python -m rag.knowledge.extract_remaining_tables [--dry-run]
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
            return path.read_text()[:800]
    return ""


def get_conn():
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def get_name_to_id():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name, name_variants FROM figures")
    m = {}
    for r in cur.fetchall():
        m[r["name"].lower()] = r["id"]
        for v in (r["name_variants"] or []):
            m[v.lower()] = r["id"]
    conn.close()
    return m


def resolve_id(name, name_map):
    return name_map.get(name.lower())


# ═══════════════════════════════════════════════════════════════════
# PHASE 1: Political Factions
# ═══════════════════════════════════════════════════════════════════

FACTIONS_PROMPT = """You are extracting political factions from Islamic history.
For the {era} era, list all major political/military/ideological factions.

Return JSON only:
{{
  "factions": [
    {{
      "name": "faction name",
      "era": "{era}",
      "ideology": "brief ideology/goal",
      "description": "1-2 sentence description",
      "key_figures": ["figure name 1", "figure name 2"]
    }}
  ]
}}

Known figures in this era: {figure_names}

Include factions like: ruling dynasties, rebel movements, religious schools in conflict,
military groups, tribal coalitions, rival claimants. 5-15 factions per era.
"""


def extract_factions(client, dry_run):
    print("\n── PHASE 1: Political Factions ──")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT era_val, array_agg(name ORDER BY id) as names
        FROM figures, unnest(era) AS era_val
        GROUP BY era_val ORDER BY count(*) DESC
    """)
    era_groups = cur.fetchall()
    conn.close()

    all_factions = []
    for eg in era_groups:
        era = eg["era_val"]
        names = eg["names"][:30]
        name_str = ", ".join(names)
        print(f"  {era} ({len(names)} figures)...", end=" ", flush=True)

        prompt = FACTIONS_PROMPT.format(era=era, figure_names=name_str)
        try:
            raw = _call_haiku(client, prompt)
            data = _parse_json(raw)
            factions = data.get("factions", [])
            print(f"OK — {len(factions)} factions")
            for f in factions:
                f["era"] = era
            all_factions.extend(factions)
        except Exception as e:
            print(f"FAILED: {e}")
        time.sleep(0.3)

    # Insert factions
    if not dry_run:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        for f in all_factions:
            cur.execute("""
                INSERT INTO political_factions (name, era, ideology, description)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE SET era = EXCLUDED.era
                RETURNING id
            """, (f["name"], f["era"], f.get("ideology"), f.get("description")))
            f["db_id"] = cur.fetchone()[0]
        conn.commit()
        conn.close()

    print(f"  Total factions: {len(all_factions)}")
    return all_factions


# ═══════════════════════════════════════════════════════════════════
# PHASE 2: Figure-Factions, Journeys, Betrayals, Alliance Reversals
# ═══════════════════════════════════════════════════════════════════

FIGURE_DETAILS_PROMPT = """Extract structured data for these Islamic history figures.
Return ONLY valid JSON.

Available factions (use exact names): {faction_names}

Figures:
{figure_block}

Return JSON:
{{
  "figure_factions": [
    {{
      "figure": "exact name",
      "faction": "exact faction name from list",
      "joined_date": "approximate date CE or null",
      "exit_date": "date CE or null",
      "exit_reason": "why they left or null"
    }}
  ],
  "journeys": [
    {{
      "figure": "exact name",
      "sequence": 1,
      "location": "place name",
      "date_ce": "approximate date CE",
      "significance": "why this location matters in their story"
    }}
  ],
  "betrayals": [
    {{
      "betrayer": "exact name from list",
      "betrayed": "exact name from list",
      "betrayal_type": "ASSASSINATION|DEFECTION|BROKEN_OATH|ABANDONMENT",
      "context": "what happened",
      "consequence": "what resulted",
      "was_it_justified": "brief — competing scholarly views if any"
    }}
  ],
  "alliance_reversals": [
    {{
      "figure": "exact name",
      "from_faction": "faction name they left",
      "to_faction": "faction name they joined",
      "reversal_type": "OPPORTUNISTIC|PRINCIPLED|COERCED|DISILLUSIONED|STRATEGIC",
      "description": "what happened"
    }}
  ]
}}

RULES:
- Only use figure names EXACTLY as provided
- Only use faction names from the provided list
- Journeys: major life locations (birth, migration, exile, death place) — 2-5 per figure
- Betrayals: only documented historical betrayals, not speculation
- Alliance reversals: figures who switched sides
"""


def extract_figure_details(client, factions, name_map, dry_run):
    print("\n── PHASE 2: Figure Details ──")
    faction_names = [f["name"] for f in factions]
    faction_name_to_id = {f["name"].lower(): f.get("db_id") for f in factions}

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, era, known_for, death_circumstance
        FROM figures
        WHERE sensitivity_tier IN ('B', 'C', 'A')
        ORDER BY
            CASE sensitivity_tier WHEN 'B' THEN 1 WHEN 'A' THEN 2 WHEN 'C' THEN 3 END, id
    """)
    figures = cur.fetchall()
    conn.close()

    batch_size = 10
    total_ff = total_j = total_b = total_ar = 0

    for i in range(0, len(figures), batch_size):
        batch = figures[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(figures) + batch_size - 1) // batch_size

        print(f"  [{batch_num}/{total_batches}] {len(batch)} figures...", end=" ", flush=True)

        figure_block = ""
        for fig in batch:
            bible = load_bible(fig["name"])
            figure_block += f"---\nName: {fig['name']}\nKnown for: {fig.get('known_for') or 'unknown'}\n"
            if bible:
                figure_block += f"Bible: {bible}\n"

        prompt = FIGURE_DETAILS_PROMPT.format(
            faction_names=", ".join(faction_names),
            figure_block=figure_block,
        )

        try:
            raw = _call_haiku(client, prompt)
            data = _parse_json(raw)
        except Exception as e:
            print(f"FAILED: {e}")
            continue

        if not dry_run:
            conn2 = psycopg2.connect(DB_URL)
            cur2 = conn2.cursor()

            # Figure factions
            for ff in data.get("figure_factions", []):
                fig_id = resolve_id(ff.get("figure", ""), name_map)
                fac_id = faction_name_to_id.get((ff.get("faction") or "").lower())
                if fig_id and fac_id:
                    try:
                        cur2.execute("""
                            INSERT INTO figure_factions (figure_id, faction_id, joined_date, exit_date, exit_reason)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (fig_id, fac_id, ff.get("joined_date"), ff.get("exit_date"), ff.get("exit_reason")))
                        total_ff += 1
                    except Exception:
                        conn2.rollback()

            # Journeys
            for j in data.get("journeys", []):
                fig_id = resolve_id(j.get("figure", ""), name_map)
                if fig_id:
                    try:
                        cur2.execute("""
                            INSERT INTO figure_journeys (figure_id, sequence, location, date_ce, significance)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (fig_id, j.get("sequence", 1), j.get("location"),
                              j.get("date_ce"), j.get("significance")))
                        total_j += 1
                    except Exception:
                        conn2.rollback()

            # Betrayals
            VALID_BETRAYAL = {'ASSASSINATION', 'DEFECTION', 'BROKEN_OATH', 'ABANDONMENT'}
            for b in data.get("betrayals", []):
                betrayer_id = resolve_id(b.get("betrayer", ""), name_map)
                betrayed_id = resolve_id(b.get("betrayed", ""), name_map)
                btype = (b.get("betrayal_type") or "").upper()
                if betrayer_id and betrayed_id and btype in VALID_BETRAYAL:
                    try:
                        cur2.execute("""
                            INSERT INTO political_betrayals (betrayer_id, betrayed_id, betrayal_type,
                                                             context, consequence, was_it_justified)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (betrayer_id, betrayed_id, btype,
                              b.get("context"), b.get("consequence"), b.get("was_it_justified")))
                        total_b += 1
                    except Exception:
                        conn2.rollback()

            # Alliance reversals
            VALID_REVERSAL = {'OPPORTUNISTIC', 'PRINCIPLED', 'COERCED', 'DISILLUSIONED', 'STRATEGIC'}
            for ar in data.get("alliance_reversals", []):
                fig_id = resolve_id(ar.get("figure", ""), name_map)
                from_fac = faction_name_to_id.get((ar.get("from_faction") or "").lower())
                to_fac = faction_name_to_id.get((ar.get("to_faction") or "").lower())
                rtype = (ar.get("reversal_type") or "").upper()
                if fig_id and rtype in VALID_REVERSAL:
                    try:
                        cur2.execute("""
                            INSERT INTO alliance_reversals (figure_id, from_faction_id, to_faction_id,
                                                            reversal_type, description)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (fig_id, from_fac, to_fac, rtype, ar.get("description")))
                        total_ar += 1
                    except Exception:
                        conn2.rollback()

            conn2.commit()
            conn2.close()

        ff_n = len(data.get("figure_factions", []))
        j_n = len(data.get("journeys", []))
        b_n = len(data.get("betrayals", []))
        ar_n = len(data.get("alliance_reversals", []))
        print(f"OK — ff={ff_n} j={j_n} b={b_n} ar={ar_n}")
        time.sleep(0.3)

    print(f"\n  Figure-factions: {total_ff}")
    print(f"  Journeys: {total_j}")
    print(f"  Betrayals: {total_b}")
    print(f"  Alliance reversals: {total_ar}")
    return total_ff, total_j, total_b, total_ar


# ═══════════════════════════════════════════════════════════════════
# PHASE 3: Event tables (causes, economics, geography, themes)
# ═══════════════════════════════════════════════════════════════════

EVENT_PROMPT = """Extract structured event data from these Islamic history events.
Return ONLY valid JSON.

Events:
{event_block}

Available themes (use IDs): {theme_list}

Return JSON:
{{
  "event_causes": [
    {{
      "cause_event": "exact event name that CAUSED something",
      "effect_event": "exact event name that was THE EFFECT",
      "time_gap_years": 0,
      "relationship": "DIRECT_CAUSE|CONTRIBUTING_FACTOR|PRECONDITION|CONSEQUENCE|REACTION|PARALLEL_DEVELOPMENT",
      "description": "how they're connected"
    }}
  ],
  "event_economics": [
    {{
      "event": "exact event name",
      "factor": "economic factor — trade routes, taxation, plunder, treasury, famine",
      "impact": "what economic impact this had",
      "source": "source attribution or null"
    }}
  ],
  "event_geography": [
    {{
      "event": "exact event name",
      "terrain": "desert, mountains, river valley, coastal, urban, steppe",
      "climate": "arid, temperate, tropical, etc.",
      "strategic_significance": "why geography mattered",
      "visual_description": "what this landscape looks like for Kling video — 1 sentence"
    }}
  ],
  "event_themes": [
    {{
      "event": "exact event name",
      "theme_id": 1,
      "relevance": "how this theme applies to this event"
    }}
  ],
  "cultural_encounters": [
    {{
      "event": "exact event name",
      "culture_a": "first culture",
      "culture_b": "second culture",
      "encounter_type": "conquest, trade, diplomacy, intellectual exchange, conflict",
      "outcome": "what resulted from the encounter",
      "figure_names": ["figures involved"]
    }}
  ]
}}
"""


def extract_event_details(client, name_map, dry_run):
    print("\n── PHASE 3: Event Details ──")
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, name, era, date_ce, significance FROM events ORDER BY id")
    events = [dict(r) for r in cur.fetchall()]

    cur.execute("SELECT id, name FROM themes ORDER BY id")
    themes = cur.fetchall()
    theme_list = ", ".join(f"{t['id']}={t['name']}" for t in themes)

    # Event name -> id
    event_map = {e["name"].lower(): e["id"] for e in events}
    conn.close()

    if not events:
        print("  No events to process")
        return

    # Process all events in one call (only 36)
    event_block = ""
    for e in events:
        event_block += f"---\nName: {e['name']}\nEra: {e.get('era') or 'unknown'}\nDate: {e.get('date_ce') or 'unknown'}\nSignificance: {e.get('significance') or 'unknown'}\n"

    prompt = EVENT_PROMPT.format(event_block=event_block, theme_list=theme_list)

    print(f"  Processing {len(events)} events...", end=" ", flush=True)
    try:
        raw = _call_haiku(client, prompt)
        data = _parse_json(raw)
    except Exception as e:
        print(f"FAILED: {e}")
        return

    if dry_run:
        print(f"OK (dry run)")
        return

    conn2 = psycopg2.connect(DB_URL)
    cur2 = conn2.cursor()
    ec_count = ee_count = eg_count = et_count = ce_count = 0

    # Event causes
    for ec in data.get("event_causes", []):
        cause_id = event_map.get((ec.get("cause_event") or "").lower())
        effect_id = event_map.get((ec.get("effect_event") or "").lower())
        rel = (ec.get("relationship") or "").upper()
        valid_rels = {'DIRECT_CAUSE','CONTRIBUTING_FACTOR','PRECONDITION','CONSEQUENCE','REACTION','PARALLEL_DEVELOPMENT'}
        if cause_id and effect_id and rel in valid_rels:
            try:
                cur2.execute("""
                    INSERT INTO event_causes (cause_event_id, effect_event_id, time_gap_years, relationship, description)
                    VALUES (%s, %s, %s, %s, %s)
                """, (cause_id, effect_id, ec.get("time_gap_years"), rel, ec.get("description")))
                ec_count += 1
            except Exception:
                conn2.rollback()

    # Event economics
    for ee in data.get("event_economics", []):
        eid = event_map.get((ee.get("event") or "").lower())
        if eid:
            try:
                cur2.execute("""
                    INSERT INTO event_economics (event_id, factor, impact, source)
                    VALUES (%s, %s, %s, %s)
                """, (eid, ee.get("factor"), ee.get("impact"), ee.get("source")))
                ee_count += 1
            except Exception:
                conn2.rollback()

    # Event geography
    for eg in data.get("event_geography", []):
        eid = event_map.get((eg.get("event") or "").lower())
        if eid:
            try:
                cur2.execute("""
                    INSERT INTO event_geography (event_id, terrain, climate, strategic_significance, visual_description)
                    VALUES (%s, %s, %s, %s, %s)
                """, (eid, eg.get("terrain"), eg.get("climate"),
                      eg.get("strategic_significance"), eg.get("visual_description")))
                eg_count += 1
            except Exception:
                conn2.rollback()

    # Event themes
    for et in data.get("event_themes", []):
        eid = event_map.get((et.get("event") or "").lower())
        tid = et.get("theme_id")
        if eid and tid:
            try:
                cur2.execute("""
                    INSERT INTO event_themes (event_id, theme_id, relevance)
                    VALUES (%s, %s, %s)
                """, (eid, tid, et.get("relevance")))
                et_count += 1
            except Exception:
                conn2.rollback()

    # Cultural encounters
    for ce in data.get("cultural_encounters", []):
        eid = event_map.get((ce.get("event") or "").lower())
        fig_names = ce.get("figure_names", [])
        fig_ids = [resolve_id(n, name_map) for n in fig_names if resolve_id(n, name_map)]
        try:
            cur2.execute("""
                INSERT INTO cultural_encounters (event_id, culture_a, culture_b, encounter_type, outcome, figure_ids)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (eid, ce.get("culture_a"), ce.get("culture_b"),
                  ce.get("encounter_type"), ce.get("outcome"), fig_ids or None))
            ce_count += 1
        except Exception:
            conn2.rollback()

    conn2.commit()
    conn2.close()

    print(f"OK")
    print(f"  Event causes: {ec_count}")
    print(f"  Event economics: {ee_count}")
    print(f"  Event geography: {eg_count}")
    print(f"  Event themes: {et_count}")
    print(f"  Cultural encounters: {ce_count}")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def run(dry_run=False):
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    name_map = get_name_to_id()

    print("=" * 60)
    print("REMAINING TABLES EXTRACTOR")
    print(f"Model: {MODEL}")
    print(f"Dry run: {dry_run}")
    print("=" * 60)

    # Phase 1: Factions
    factions = extract_factions(client, dry_run)

    # Phase 2: Figure details
    extract_figure_details(client, factions, name_map, dry_run)

    # Phase 3: Event details
    extract_event_details(client, name_map, dry_run)

    # Final report
    print(f"\n{'=' * 60}")
    print("FINAL TABLE COUNTS")
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    for table in ["political_factions", "figure_factions", "figure_journeys",
                  "political_betrayals", "alliance_reversals", "cultural_encounters",
                  "event_causes", "event_economics", "event_geography", "event_themes"]:
        cur.execute(f"SELECT count(*) FROM {table}")
        print(f"  {table}: {cur.fetchone()[0]}")
    conn.close()
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
