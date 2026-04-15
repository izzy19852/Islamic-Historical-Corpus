"""
Islam Stories — Event Tables Extractor
Populates: event_causes, event_economics, event_geography, event_themes, cultural_encounters
Processes events in small batches to avoid JSON truncation.

Run:  python -m rag.knowledge.extract_event_tables [--dry-run]
"""

import argparse, json, os, time, random
import anthropic, psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
MODEL = "claude-haiku-4-5-20251001"

EVENT_PROMPT = """Extract structured data for these Islamic history events. Return ONLY valid JSON.

Events:
{event_block}

Available themes (use IDs): {theme_list}

All event names for cross-references: {all_event_names}

Return JSON:
{{
  "event_causes": [
    {{
      "cause_event": "exact event name",
      "effect_event": "exact event name",
      "time_gap_years": 0,
      "relationship": "DIRECT_CAUSE|CONTRIBUTING_FACTOR|PRECONDITION|CONSEQUENCE|REACTION|PARALLEL_DEVELOPMENT",
      "description": "brief connection"
    }}
  ],
  "event_economics": [
    {{
      "event": "exact event name",
      "factor": "economic factor",
      "impact": "economic impact",
      "source": "source or null"
    }}
  ],
  "event_geography": [
    {{
      "event": "exact event name",
      "terrain": "desert/mountains/river valley/coastal/urban/steppe",
      "climate": "arid/temperate/tropical",
      "strategic_significance": "why geography mattered",
      "visual_description": "what this looks like for video — 1 sentence"
    }}
  ],
  "event_themes": [
    {{
      "event": "exact event name",
      "theme_id": 1,
      "relevance": "how this theme applies"
    }}
  ],
  "cultural_encounters": [
    {{
      "event": "exact event name",
      "culture_a": "first culture",
      "culture_b": "second culture",
      "encounter_type": "conquest/trade/diplomacy/intellectual exchange/conflict",
      "outcome": "what resulted"
    }}
  ]
}}

RULES:
- Use event names EXACTLY as provided
- For event_causes, both events must be from the full event list
- Keep descriptions concise
"""


def _call_haiku(client, prompt, max_retries=4):
    for attempt in range(max_retries):
        try:
            return client.messages.create(
                model=MODEL, max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            ).content[0].text.strip()
        except (anthropic._exceptions.OverloadedError,
                anthropic._exceptions.RateLimitError,
                anthropic._exceptions.APIConnectionError) as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            if attempt < max_retries - 1:
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


def run(dry_run=False):
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    cur.execute("SELECT id, name, era, date_ce, significance FROM events ORDER BY id")
    events = [dict(r) for r in cur.fetchall()]
    event_map = {e["name"].lower(): e["id"] for e in events}
    all_event_names = ", ".join(e["name"] for e in events)

    cur.execute("SELECT id, name FROM themes ORDER BY id")
    themes = cur.fetchall()
    theme_list = ", ".join(f"{t['id']}={t['name']}" for t in themes)
    conn.close()

    print(f"Events: {len(events)} | Themes: {len(themes)}")

    # Name map for cultural encounters
    conn2 = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur2 = conn2.cursor()
    cur2.execute("SELECT id, name FROM figures")
    name_map = {r["name"].lower(): r["id"] for r in cur2.fetchall()}
    conn2.close()

    ec_total = ee_total = eg_total = et_total = ce_total = 0
    batch_size = 8

    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(events) + batch_size - 1) // batch_size

        event_block = ""
        for e in batch:
            event_block += f"---\nName: {e['name']}\nEra: {e.get('era') or 'unknown'}\nDate: {e.get('date_ce') or 'unknown'}\nSignificance: {e.get('significance') or 'unknown'}\n"

        prompt = EVENT_PROMPT.format(
            event_block=event_block,
            theme_list=theme_list,
            all_event_names=all_event_names,
        )

        print(f"  [{batch_num}/{total_batches}] {len(batch)} events...", end=" ", flush=True)

        try:
            raw = _call_haiku(client, prompt)
            data = _parse_json(raw)
        except Exception as e:
            print(f"FAILED: {e}")
            continue

        if dry_run:
            print("OK (dry run)")
            continue

        conn3 = psycopg2.connect(DB_URL)
        cur3 = conn3.cursor()

        VALID_CAUSE_RELS = {'DIRECT_CAUSE','CONTRIBUTING_FACTOR','PRECONDITION','CONSEQUENCE','REACTION','PARALLEL_DEVELOPMENT'}

        for ec in data.get("event_causes", []):
            cid = event_map.get((ec.get("cause_event") or "").lower())
            eid = event_map.get((ec.get("effect_event") or "").lower())
            rel = (ec.get("relationship") or "").upper()
            if cid and eid and rel in VALID_CAUSE_RELS:
                try:
                    cur3.execute("INSERT INTO event_causes (cause_event_id, effect_event_id, time_gap_years, relationship, description) VALUES (%s,%s,%s,%s,%s)",
                        (cid, eid, ec.get("time_gap_years"), rel, ec.get("description")))
                    ec_total += 1
                except Exception:
                    conn3.rollback()

        for ee in data.get("event_economics", []):
            eid = event_map.get((ee.get("event") or "").lower())
            if eid:
                try:
                    cur3.execute("INSERT INTO event_economics (event_id, factor, impact, source) VALUES (%s,%s,%s,%s)",
                        (eid, ee.get("factor"), ee.get("impact"), ee.get("source")))
                    ee_total += 1
                except Exception:
                    conn3.rollback()

        for eg in data.get("event_geography", []):
            eid = event_map.get((eg.get("event") or "").lower())
            if eid:
                try:
                    cur3.execute("INSERT INTO event_geography (event_id, terrain, climate, strategic_significance, visual_description) VALUES (%s,%s,%s,%s,%s)",
                        (eid, eg.get("terrain"), eg.get("climate"), eg.get("strategic_significance"), eg.get("visual_description")))
                    eg_total += 1
                except Exception:
                    conn3.rollback()

        for et in data.get("event_themes", []):
            eid = event_map.get((et.get("event") or "").lower())
            tid = et.get("theme_id")
            if eid and tid:
                try:
                    cur3.execute("INSERT INTO event_themes (event_id, theme_id, relevance) VALUES (%s,%s,%s)",
                        (eid, tid, et.get("relevance")))
                    et_total += 1
                except Exception:
                    conn3.rollback()

        for ce in data.get("cultural_encounters", []):
            eid = event_map.get((ce.get("event") or "").lower())
            try:
                cur3.execute("INSERT INTO cultural_encounters (event_id, culture_a, culture_b, encounter_type, outcome) VALUES (%s,%s,%s,%s,%s)",
                    (eid, ce.get("culture_a"), ce.get("culture_b"), ce.get("encounter_type"), ce.get("outcome")))
                ce_total += 1
            except Exception:
                conn3.rollback()

        conn3.commit()
        conn3.close()

        print(f"OK — ec={len(data.get('event_causes',[]))} ee={len(data.get('event_economics',[]))} eg={len(data.get('event_geography',[]))} et={len(data.get('event_themes',[]))} ce={len(data.get('cultural_encounters',[]))}")
        time.sleep(0.3)

    print(f"\nEvent causes: {ec_total}")
    print(f"Event economics: {ee_total}")
    print(f"Event geography: {eg_total}")
    print(f"Event themes: {et_total}")
    print(f"Cultural encounters: {ce_total}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
