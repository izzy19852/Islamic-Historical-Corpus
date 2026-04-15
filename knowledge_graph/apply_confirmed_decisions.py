"""
Islam Stories — Apply Confirmed Character Decisions
Applies tier corrections, depiction notes, and series assignments
from the reviewed character evaluation report.

Run AFTER apply_character_fixes --apply --mechanical-only

Usage:
  python -m rag.knowledge.apply_confirmed_decisions             # dry run
  python -m rag.knowledge.apply_confirmed_decisions --apply      # commit
"""

import os
import sys
import argparse
from dotenv import load_dotenv
import psycopg2

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


def run(dry_run=True):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    prefix = "[DRY RUN] " if dry_run else ""
    applied = 0
    warnings = []

    def update_figure(cur, name, updates, where_id=None):
        """Update a figure by exact name (or id). Returns (id, name) or None."""
        nonlocal applied
        set_clauses = []
        params = []
        for col, val in updates.items():
            set_clauses.append(f"{col} = %s")
            params.append(val)

        if where_id:
            params.append(where_id)
            where = "id = %s"
        else:
            params.append(name)
            where = "name = %s"

        sql = f"UPDATE figures SET {', '.join(set_clauses)} WHERE {where} RETURNING id, name, sensitivity_tier"
        if not dry_run:
            cur.execute(sql, params)
            result = cur.fetchone()
        else:
            # Check it exists
            if where_id:
                cur.execute("SELECT id, name, sensitivity_tier FROM figures WHERE id = %s", (where_id,))
            else:
                cur.execute("SELECT id, name, sensitivity_tier FROM figures WHERE name = %s", (name,))
            result = cur.fetchone()

        if result:
            print(f"  {prefix}✓ {result[1]} (id {result[0]}) → tier {updates.get('sensitivity_tier', result[2])}")
            applied += 1
        else:
            warnings.append(f"NOT FOUND: {name}")
            print(f"  {prefix}⚠ Not found: {name}")
        return result

    # ═══════════════════════════════════════════════════════════════════
    # TIER CORRECTIONS — from sensitivity guide
    # ═══════════════════════════════════════════════════════════════════
    print("\n── Tier Corrections: Prophet's Wives → S ──")
    for name in ["Maymuna bint al-Harith", "Hafsa bint Umar", "Zaynab bint Jahsh", "Juwayria"]:
        update_figure(cur, name, {"sensitivity_tier": "S"})

    print("\n── Tier Corrections: Senior Companions → A ──")
    for name in ["Bilal ibn Rabah", "Abu Dharr al-Ghifari", "Salman al-Farisi", "Ammar ibn Yasir"]:
        update_figure(cur, name, {"sensitivity_tier": "A"})

    # ═══════════════════════════════════════════════════════════════════
    # CONFIRMED DECISIONS — 10 flagged figures
    # ═══════════════════════════════════════════════════════════════════

    print("\n── Confirmed: Hind bint Utba → Tier B, ANTAGONIST→ENSEMBLE ──")
    update_figure(cur, "Hind bint Utba", {
        "sensitivity_tier": "B",
        "known_for": (
            "Meccan noblewoman who initially opposed Islam and ordered the mutilation of Hamza's "
            "body at Uhud, but later converted. | ANTAGONIST→ENSEMBLE arc across Season 1. "
            "Pre-Islam actions (including mutilation of Hamza) are documented and showable — "
            "but depict through companions' grief, not the act directly. "
            "Her conversion is documented and must be shown with full weight. "
            "She is proof the series does not flatten enemies into villains."
        ),
    })

    print("\n── Confirmed: Abu Talib → Tier A, CORPUS_ONLY ──")
    update_figure(cur, "Abu Talib ibn Abd al-Muttalib", {
        "sensitivity_tier": "A",
        "known_for": (
            "Uncle and protector of Prophet Muhammad who shielded him during the early years "
            "of his mission. | CORPUS_ONLY. Narration reference only. Never depicted on screen. "
            "Do not resolve his post-death status — Sunni/Shia positions differ. "
            "His protection of early Muslims IS documentable and usable in narration."
        ),
    })

    print("\n── Confirmed: Ubaydallah ibn Umar → Tier B, SUPPORTING S3 ──")
    update_figure(cur, "Ubaydallah ibn Umar", {
        "sensitivity_tier": "B",
        "known_for": (
            "Son of Umar ibn al-Khattab who killed Al-Hurmuzan in revenge for his father's "
            "assassination. | SUPPORTING — Spine Season 3 (Uthman era). "
            "Killed al-Hurmuzan without trial after Umar's assassination. "
            "Uthman's decision to spare him feeds the 'weakness' narrative. "
            "Use this as evidence of how Uthman's clemency was weaponized against him."
        ),
    })

    print("\n── Confirmed: Sayf ibn Umar → Tier C, keep + source warning ──")
    update_figure(cur, "Sayf ibn Umar", {
        "sensitivity_tier": "C",
        "known_for": (
            "Controversial early Islamic historian whose accounts are considered unreliable "
            "by modern scholars. | SOURCE WARNING: Sayf's narrations are widely rejected by "
            "hadith scholars (Ibn Hibban, al-Dhahabi). When his accounts appear in script "
            "generation, grounding_rules.py must flag them as disputed. Never use Sayf as "
            "sole source for any claim. Always cross-reference with Al-Tabari or Ibn Sa'd."
        ),
    })

    print("\n── Confirmed: Ahmad ibn Abi Du'ad → Tier C, ANTAGONIST Abbasid ──")
    update_figure(cur, "Ahmad ibn Abi Du'ad", {
        "sensitivity_tier": "C",
        "known_for": (
            "Abbasid chief judge who promoted the Mu'tazila doctrine and persecuted scholars "
            "during the Mihna. | ANTAGONIST — Abbasid Golden Age series. "
            "The Mihna's enforcer. Show his intellectual conviction alongside the human cost. "
            "His paralysis and fall from power after al-Mutawakkil is documented — "
            "present it as consequence, not divine punishment."
        ),
        "series": ["The Abbasid Golden Age"],
    })

    print("\n── Confirmed: Ubaidullah (336) → Tier C, ANTAGONIST Karbala ──")
    update_figure(cur, "Ubaidullah", updates={
        "sensitivity_tier": "C",
        "known_for": (
            "Umayyad governor who ordered the killing of Husayn ibn Ali at the Battle of "
            "Karbala. | ANTAGONIST — Karbala Arc. His orders led to the massacre. "
            "Show his political calculation, not cartoon villainy. "
            "His own death at the Battle of Khazir (686) at Mukhtar's hands is documented."
        ),
        "series": ["The Karbala Arc", "The Umayyad Paradox"],
    }, where_id=336)

    print("\n── Confirmed: Shabath ibn Rib'i → Tier C, ANTAGONIST Karbala ──")
    update_figure(cur, "Shabath ibn Rib'i", {
        "sensitivity_tier": "C",
        "known_for": (
            "Tribal leader who initially supported Husayn ibn Ali but later betrayed him and "
            "fought against him at Karbala. | ANTAGONIST — Karbala Arc. "
            "Invited Husayn to Kufa then fought against him. Show the full betrayal arc. "
            "He represents the Kufan abandonment — the secondary betrayal that is as "
            "important as Yazid's army per the Karbala Protocol."
        ),
        "series": ["The Karbala Arc"],
    })

    print("\n── Confirmed: Timur → Tier C, CORPUS_ONLY ──")
    update_figure(cur, "Timur", {
        "sensitivity_tier": "C",
        "known_for": (
            "Turco-Mongol conqueror who claimed Islamic legitimacy while devastating much of "
            "the Muslim world through brutal campaigns. | CORPUS_ONLY — future Central Asia "
            "series. No planned screen time yet."
        ),
    })

    print("\n── Confirmed: Urwa ibn al-Zubayr → Tier B, transmission note ──")
    # Use the kept id after dedup — original id 84 was the one named "Urwa ibn al-Zubayr"
    # but after dedup, the kept entry is "Urwa ibn al-Zubayr" (id 84)
    # Wait — the dedup kept "Urwa ibn al-Zubayr" (id 84) but the DB originally had
    # "Urwa ibn al-Zubayr" at id 84. Let me use the exact name.
    cur.execute("SELECT id, name FROM figures WHERE name LIKE 'Urw%%Zubayr%'")
    urwa = cur.fetchone()
    if urwa:
        update_figure(cur, urwa[1], {
            "sensitivity_tier": "B",
            "known_for": (
                "Early Islamic historian and hadith narrator, son of al-Zubayr ibn al-Awwam "
                "and nephew of Aisha. | TRANSMISSION NOTE: Primary transmitter of Aisha's "
                "accounts (Tier S). When his narrations appear in scripts, citation must show "
                "the chain: Urwa ← Aisha. The information is usable. The Tier S proximity "
                "requires explicit attribution. Never paraphrase as if the account is Urwa's "
                "own observation."
            ),
        })
    else:
        print(f"  {prefix}⚠ Urwa ibn al-Zubayr not found after dedup")

    print("\n── Confirmed: Baha ad-Din ibn Shaddad → Crusades era ──")
    update_figure(cur, "Baha ad-Din ibn Shaddad", {
        "series": ["Crusades: Islamic Perspective"],
    })

    # ═══════════════════════════════════════════════════════════════════
    # OUTSIDE SCOPE — set to CORPUS_ONLY
    # ═══════════════════════════════════════════════════════════════════
    print("\n── Outside Scope → CORPUS_ONLY ──")
    outside_scope = [
        (149, "Husayn Mirza"),
        (278, "Ahmad Mirza Safavi"),
        (279, "Ali Mirza Safavi"),
        (280, "Qasim Beg Haydar"),
        (355, "Shaibaq Khan"),
    ]
    for fid, name in outside_scope:
        cur.execute("SELECT known_for FROM figures WHERE id = %s", (fid,))
        row = cur.fetchone()
        if row:
            current = row[0] or ""
            if "CORPUS_ONLY" not in current:
                new_known = current + " | CORPUS_ONLY — outside current planned series scope."
                if not dry_run:
                    cur.execute("UPDATE figures SET known_for = %s WHERE id = %s", (new_known, fid))
                print(f"  {prefix}✓ {name} (id {fid}) → CORPUS_ONLY note")
                applied += 1
            else:
                print(f"  {prefix}  {name} already marked CORPUS_ONLY")

    # ═══════════════════════════════════════════════════════════════════
    # COMMIT
    # ═══════════════════════════════════════════════════════════════════
    if not dry_run:
        conn.commit()
    else:
        conn.rollback()

    conn.close()

    print(f"\n{'=' * 60}")
    print(f"{'DRY RUN' if dry_run else 'APPLIED'}: {applied} changes")
    if warnings:
        print(f"Warnings: {len(warnings)}")
        for w in warnings:
            print(f"  ⚠ {w}")
    if dry_run:
        print(f"\nTo apply: python -m rag.knowledge.apply_confirmed_decisions --apply")
    else:
        print(f"\nAll decisions committed.")
        print(f"Next: python -m rag.knowledge.apply_character_fixes --apply --skip-dedup")
        print(f"  (applies series enrichments from Claude data)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    run(dry_run=not args.apply)
