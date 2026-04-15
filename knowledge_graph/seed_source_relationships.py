"""
Islam Stories — Seed Source Relationships (5 Pilot Events)
Seeds the source_relationships table with scholarly source conflicts
for the 5 pilot events. These are the disagreements between primary
sources that scripts must surface as narrative tension.

Prerequisite: schema_graph.py already ran (table exists).
This script ALTERs the table to add columns the seed data needs.

Run:  python -m rag.knowledge.seed_source_relationships
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


# ═══════════════════════════════════════════════════════════════════════
# STEP 1: Schema migration — add columns the seed data needs
# ═══════════════════════════════════════════════════════════════════════

MIGRATION_SQL = """
-- Add columns for richer source relationship data
ALTER TABLE source_relationships ADD COLUMN IF NOT EXISTS topic TEXT;
ALTER TABLE source_relationships ADD COLUMN IF NOT EXISTS position_a TEXT;
ALTER TABLE source_relationships ADD COLUMN IF NOT EXISTS position_b TEXT;
ALTER TABLE source_relationships ADD COLUMN IF NOT EXISTS reconciliation TEXT;
ALTER TABLE source_relationships ADD COLUMN IF NOT EXISTS script_instruction TEXT;
ALTER TABLE source_relationships ADD COLUMN IF NOT EXISTS dramatic_weight TEXT
    CHECK (dramatic_weight IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'));
"""


# ═══════════════════════════════════════════════════════════════════════
# STEP 2: Ensure pilot events exist
# ═══════════════════════════════════════════════════════════════════════

MISSING_EVENTS = [
    {
        "name": "Siege of Uthman",
        "name_variants": ["Murder of Uthman", "Assassination of Uthman"],
        "date_ce": "656",
        "date_ah": "35",
        "location": "Medina",
        "era": "rashidun",
        "significance": (
            "Rebels besieged Caliph Uthman in his house for 40+ days. "
            "His murder fractured the ummah permanently. Marwan's forged letter "
            "is the proximate cause per scholarly consensus."
        ),
    },
]

# Map prompt event names → actual DB event names
EVENT_NAME_MAP = {
    "Battle of Yarmouk": "Battle of Yarmouk",
    "Ridda Wars": "Ridda Wars",
    "Khalid's Dismissal": "Dismissal of Khalid",
    "Siege of Uthman": "Siege of Uthman",
    "Battle of Siffin": "Battle of Siffin",
}


# ═══════════════════════════════════════════════════════════════════════
# STEP 3: Source relationship seed data
# Each row maps a specific disagreement between two primary sources
# on a specific aspect of a pilot event. The orchestrator retrieves
# these as "conflicts" and the script prompt surfaces them as drama.
# ═══════════════════════════════════════════════════════════════════════

ROWS = [
    # ── Battle of Yarmouk ──────────────────────────────────────────
    {
        "event": "Battle of Yarmouk",
        "source_a": "Al-Baladhuri (Futuh al-Buldan)",
        "source_b": "Al-Tabari (Annals Vol 11)",
        "relationship": "CONTRADICTS",
        "topic": "Byzantine troop count",
        "position_a": "Al-Baladhuri reports ~200,000 Byzantine troops",
        "position_b": "Al-Tabari gives lower figures, ~100,000",
        "reconciliation": (
            "Scholars note both may be rhetorical exaggeration; "
            "accept decisive Muslim victory as undisputed"
        ),
        "script_instruction": (
            "Present the scale as vast without citing a number; "
            'use "the horizon filled with banners"'
        ),
        "dramatic_weight": "LOW",
    },
    {
        "event": "Battle of Yarmouk",
        "source_a": "Al-Waqidi (Futuh al-Sham)",
        "source_b": "Al-Tabari (Annals Vol 11)",
        "relationship": "CHALLENGES",
        "topic": "Khalid's command authority at Yarmouk",
        "position_a": "Al-Waqidi: Khalid held effective command throughout the battle",
        "position_b": (
            "Al-Tabari: Abu Ubayda was overall commander; "
            "Khalid led the mobile guard"
        ),
        "reconciliation": (
            "Both likely true — Abu Ubayda held formal authority, "
            "Khalid directed battlefield tactics"
        ),
        "script_instruction": (
            "Show Khalid as the tactical mind; acknowledge Abu Ubayda "
            "as the commander he reports to"
        ),
        "dramatic_weight": "HIGH",
    },

    # ── Ridda Wars ─────────────────────────────────────────────────
    {
        "event": "Ridda Wars",
        "source_a": "Al-Tabari (Annals Vol 10)",
        "source_b": "Ibn Hisham / Guillaume (Life of Muhammad)",
        "relationship": "CONTRADICTS",
        "topic": "Nature of the ridda — apostasy or tax refusal",
        "position_a": (
            "Al-Tabari frames many tribes as apostates "
            "who rejected Islam itself"
        ),
        "position_b": (
            "Ibn Hisham and later scholars argue some tribes "
            "refused zakat only, not the faith"
        ),
        "reconciliation": (
            "Both accounts reflect different tribal situations — "
            "not monolithic"
        ),
        "script_instruction": (
            "Surface this as the unresolved question: "
            "'Were they abandoning God or refusing Abu Bakr?' "
            "Do not resolve it."
        ),
        "dramatic_weight": "HIGH",
    },

    # ── Khalid's Dismissal ─────────────────────────────────────────
    {
        "event": "Khalid's Dismissal",
        "source_a": "Al-Tabari (Annals)",
        "source_b": "Ibn Sa'd (Tabaqat Vol III)",
        "relationship": "CHALLENGES",
        "topic": "Umar's reason for dismissing Khalid",
        "position_a": (
            "Al-Tabari: Umar dismissed Khalid because people were "
            "over-glorifying him above the faith"
        ),
        "position_b": (
            "Ibn Sa'd: The blood money incident (killing of Malik "
            "ibn Nuwayra) was the primary cause"
        ),
        "reconciliation": (
            "Both reasons cited in sources; Umar likely acted "
            "on both simultaneously"
        ),
        "script_instruction": (
            "Present both reasons explicitly. Let the audience sit "
            "with the ambiguity. Neither Umar nor Khalid is wrong."
        ),
        "dramatic_weight": "CRITICAL",
    },

    # ── Siege of Uthman ────────────────────────────────────────────
    {
        "event": "Siege of Uthman",
        "source_a": "Al-Tabari (Annals Vol 15)",
        "source_b": "Ibn Sa'd (Tabaqat)",
        "relationship": "CONTRADICTS",
        "topic": "Marwan ibn al-Hakam's role in Uthman's death",
        "position_a": (
            "Al-Tabari: Marwan sent a letter in Uthman's name "
            "that escalated the rebellion"
        ),
        "position_b": (
            "Some accounts absolve Marwan; the letter's "
            "existence is disputed"
        ),
        "reconciliation": (
            "The letter appears in multiple chains; its sending "
            "without Uthman's knowledge is widely accepted"
        ),
        "script_instruction": (
            "Marwan is the dramatic engine of Uthman's fall. "
            "Show the letter. Attribute it clearly to Marwan. "
            "Do not implicate Uthman in his own death."
        ),
        "dramatic_weight": "CRITICAL",
    },

    # ── Battle of Siffin ──────────────────────────────────────────
    {
        "event": "Battle of Siffin",
        "source_a": "Al-Tabari (Annals)",
        "source_b": "Nasr ibn Muzahim (Waq'at Siffin — earliest dedicated source)",
        "relationship": "SUPPLEMENTS",
        "topic": "Who initiated the arbitration proposal",
        "position_a": (
            "Al-Tabari: Muawiyah's forces raised Qurans on spears; "
            "Amr ibn al-As engineered it"
        ),
        "position_b": (
            "Nasr ibn Muzahim provides more detail; attributes "
            "initiative clearly to Amr ibn al-As"
        ),
        "reconciliation": (
            "Sources agree on the tactic; Amr's role as architect "
            "is consistent across accounts"
        ),
        "script_instruction": (
            "Amr ibn al-As raising Qurans is the scene. Show Ali's "
            "dilemma — he knew it was a trick but could not order "
            "his men to fight against the Quran."
        ),
        "dramatic_weight": "HIGH",
    },
]


def main():
    print("=" * 60)
    print("SEED SOURCE RELATIONSHIPS — 5 Pilot Events")
    print("=" * 60)

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # ── Step 1: Migrate schema ─────────────────────────────────────
    print("\n── Schema migration ──")
    cur.execute(MIGRATION_SQL)
    print("  Added columns: topic, position_a, position_b, reconciliation,")
    print("                 script_instruction, dramatic_weight")

    # ── Step 2: Ensure pilot events exist ──────────────────────────
    print("\n── Ensuring pilot events exist ──")
    for evt in MISSING_EVENTS:
        cur.execute("SELECT id FROM events WHERE name = %s", (evt["name"],))
        if cur.fetchone():
            print(f"  {evt['name']} — already exists")
            continue

        cur.execute("""
            INSERT INTO events (name, name_variants, date_ce, date_ah,
                                location, era, significance)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            evt["name"], evt.get("name_variants", []),
            evt.get("date_ce"), evt.get("date_ah"),
            evt.get("location"), evt.get("era"),
            evt.get("significance"),
        ))
        new_id = cur.fetchone()[0]
        print(f"  {evt['name']} — INSERTED (id {new_id})")

    # ── Step 3: Load event map ─────────────────────────────────────
    cur.execute("SELECT id, name FROM events")
    all_events = {name: eid for eid, name in cur.fetchall()}

    # ── Step 4: Insert source relationships ────────────────────────
    print("\n── Inserting source relationships ──")
    conn.autocommit = False
    inserted = 0
    skipped = 0

    for r in ROWS:
        # Resolve event name through the map
        db_event_name = EVENT_NAME_MAP.get(r["event"], r["event"])
        eid = all_events.get(db_event_name)
        if not eid:
            print(f"  WARNING: event not found — '{r['event']}' "
                  f"(mapped to '{db_event_name}')")
            continue

        # Check for existing duplicate (same event + same topic)
        cur.execute("""
            SELECT id FROM source_relationships
            WHERE event_id = %s AND topic = %s
        """, (eid, r["topic"]))
        if cur.fetchone():
            print(f"  SKIP (exists): {r['topic'][:60]}...")
            skipped += 1
            continue

        cur.execute("""
            INSERT INTO source_relationships
                (event_id, source_a, source_b, relationship,
                 topic, position_a, position_b,
                 reconciliation, script_instruction, dramatic_weight,
                 conflict_note, scholarly_consensus)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            eid,
            r["source_a"],
            r["source_b"],
            r["relationship"],
            r["topic"],
            r["position_a"],
            r["position_b"],
            r["reconciliation"],
            r["script_instruction"],
            r["dramatic_weight"],
            # Also populate the original columns for backward compat
            f"{r['position_a']} vs {r['position_b']}",  # conflict_note
            r["reconciliation"],  # scholarly_consensus
        ))
        inserted += 1
        print(f"  ✓ {db_event_name}: {r['topic'][:60]}")

    conn.commit()

    # ── Verify ─────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM source_relationships")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT e.name, sr.topic, sr.relationship, sr.dramatic_weight
        FROM source_relationships sr
        JOIN events e ON sr.event_id = e.id
        ORDER BY e.name, sr.topic
    """)
    rows = cur.fetchall()

    print(f"\n{'=' * 60}")
    print(f"SEED COMPLETE")
    print(f"  Inserted: {inserted}")
    print(f"  Skipped:  {skipped}")
    print(f"  Total in table: {total}")
    print(f"\nSource relationships by event:")
    for event_name, topic, rel, weight in rows:
        print(f"  [{weight:8s}] {event_name}: {topic} ({rel})")
    print(f"{'=' * 60}")

    conn.close()


if __name__ == "__main__":
    main()
