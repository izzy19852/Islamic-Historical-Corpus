"""
Islam Stories — Apply Character Fixes After Audit Review
Reads the evaluation outputs and applies:
  1. Duplicate removal (merge refs, delete dups)
  2. Tier corrections
  3. Series/season enrichment from Claude data
  4. Era fixes for Quranic prophets

Run AFTER reading docs/character_evaluation_report.md and confirming.

Usage:
  python -m rag.knowledge.apply_character_fixes             # dry run
  python -m rag.knowledge.apply_character_fixes --apply      # apply changes
  python -m rag.knowledge.apply_character_fixes --apply --skip-dedup  # skip dedup, do tier+series only
"""

import os
import sys
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Tables that reference figures(id) via foreign key
FK_TABLES = [
    ("figure_lineage", "figure_id"),
    ("figure_lineage", "related_id"),
    ("figure_relationships", "figure_a_id"),
    ("figure_relationships", "figure_b_id"),
    ("figure_themes", "figure_id"),
    ("figure_motivations", "figure_id"),
    ("figure_deaths", "figure_id"),
    ("figure_quotes", "figure_id"),
    ("figure_factions", "figure_id"),
    ("figure_journeys", "figure_id"),
    ("figure_legacies", "figure_id"),
    ("figure_transformations", "figure_id"),
    ("alliance_reversals", "figure_id"),
    ("political_betrayals", "betrayer_id"),
    ("political_betrayals", "betrayed_id"),
    ("scholarly_debates", "figure_id"),
]

# Tables that reference figures by id in an INT[] column
ARRAY_FK_TABLES = [
    ("events", "figure_ids"),
    ("chunk_metadata", "figure_ids"),
]

# ═══════════════════════════════════════════════════════════════════════
# TIER CORRECTIONS — from sensitivity guide
# ═══════════════════════════════════════════════════════════════════════

TIER_FIXES = {
    # Wives of the Prophet → S
    "Maymuna bint al-Harith": "S",
    "Hafsa bint Umar": "S",
    "Zaynab bint Jahsh": "S",
    "Juwayria": "S",
    # Senior companions → A
    "Bilal ibn Rabah": "A",
    "Abu Dharr al-Ghifari": "A",
    "Salman al-Farisi": "A",
    "Ammar ibn Yasir": "A",
}

# Quranic prophets — set era to empty (they're pre-Islamic)
# Also fix miscategorized eras and missing eras
QURANIC_ERA_FIXES = {
    "Lot": [],
    "Ishmael": [],
    "Shuayb": [],
    "Imran": [],
}

OTHER_ERA_FIXES = {
    "Ibn Hajar al-Asqalani": ["mamluk"],
}


def load_review_data():
    """Load the needs_review JSON."""
    path = PROJECT_ROOT / "data" / "characters_needs_review.json"
    if not path.exists():
        print("ERROR: characters_needs_review.json not found. Run evaluate_characters.py first.")
        sys.exit(1)
    return json.loads(path.read_text())


def load_enrichments():
    """Load Claude enrichment data if available."""
    path = PROJECT_ROOT / "data" / "claude_enrichments.json"
    if path.exists():
        return json.loads(path.read_text())
    return {}


def apply_dedup(cur, dry_run=True):
    """Remove duplicate figures, repointing all FK references to the kept entry."""
    # Load the evaluation report to get dup groups
    # Re-run dedup logic from evaluate_characters
    from rag.knowledge.evaluate_characters import load_figures, gate1_deduplication

    figures = load_figures()
    dup_groups = gate1_deduplication(figures)

    if not dup_groups:
        print("  No duplicates to fix")
        return 0

    total_removed = 0
    for group in dup_groups:
        keep_id = group["keep"]["id"]
        keep_name = group["keep"]["name"]

        for remove_entry in group["remove"]:
            remove_id = remove_entry["id"]
            remove_name = remove_entry["name"]

            print(f"  {'[DRY RUN] ' if dry_run else ''}Merge {remove_name} (id {remove_id}) → {keep_name} (id {keep_id})")

            if not dry_run:
                # Repoint FK references
                for table, col in FK_TABLES:
                    cur.execute(f"""
                        UPDATE {table} SET {col} = %s
                        WHERE {col} = %s
                        AND NOT EXISTS (
                            SELECT 1 FROM {table} t2
                            WHERE t2.{col} = %s
                            AND t2.id != {table}.id
                        )
                    """, (keep_id, remove_id, keep_id))
                    # Delete any that would create duplicates
                    cur.execute(f"DELETE FROM {table} WHERE {col} = %s", (remove_id,))

                # Repoint INT[] references
                for table, col in ARRAY_FK_TABLES:
                    cur.execute(f"""
                        UPDATE {table}
                        SET {col} = array_replace({col}, %s, %s)
                        WHERE %s = ANY({col})
                    """, (remove_id, keep_id, remove_id))

                # Merge name_variants from removed into kept
                cur.execute(
                    "SELECT name_variants FROM figures WHERE id = %s",
                    (remove_id,)
                )
                remove_variants = cur.fetchone()
                if remove_variants and remove_variants[0]:
                    cur.execute("""
                        UPDATE figures
                        SET name_variants = (
                            SELECT array_agg(DISTINCT v)
                            FROM unnest(name_variants || %s) AS v
                        )
                        WHERE id = %s
                    """, (remove_variants[0] + [remove_name], keep_id))
                else:
                    cur.execute("""
                        UPDATE figures
                        SET name_variants = (
                            SELECT array_agg(DISTINCT v)
                            FROM unnest(name_variants || %s) AS v
                        )
                        WHERE id = %s
                    """, ([remove_name], keep_id))

                # Delete the duplicate
                cur.execute("DELETE FROM figures WHERE id = %s", (remove_id,))

            total_removed += 1

    return total_removed


def apply_tier_fixes(cur, dry_run=True):
    """Apply canonical tier corrections."""
    fixed = 0
    for name, correct_tier in TIER_FIXES.items():
        cur.execute("SELECT id, sensitivity_tier FROM figures WHERE name = %s", (name,))
        row = cur.fetchone()
        if not row:
            print(f"  WARNING: {name} not found in figures table")
            continue

        fid, current_tier = row
        if current_tier == correct_tier:
            continue

        print(f"  {'[DRY RUN] ' if dry_run else ''}Tier fix: {name} (id {fid}): {current_tier} → {correct_tier}")
        if not dry_run:
            cur.execute("UPDATE figures SET sensitivity_tier = %s WHERE id = %s", (correct_tier, fid))
        fixed += 1

    return fixed


def apply_era_fixes(cur, dry_run=True):
    """Fix Quranic prophet era assignments and other era issues."""
    fixed = 0

    all_era_fixes = {**QURANIC_ERA_FIXES, **OTHER_ERA_FIXES}
    for name, correct_era in all_era_fixes.items():
        cur.execute("SELECT id, era FROM figures WHERE name = %s", (name,))
        row = cur.fetchone()
        if not row:
            continue

        fid, current_era = row
        if current_era == correct_era:
            continue

        print(f"  {'[DRY RUN] ' if dry_run else ''}Era fix: {name} (id {fid}): {current_era} → {correct_era}")
        if not dry_run:
            cur.execute("UPDATE figures SET era = %s WHERE id = %s", (correct_era, fid))
        fixed += 1

    return fixed


def apply_enrichments(cur, dry_run=True):
    """Apply Claude enrichment data — series assignments."""
    enrichments = load_enrichments()
    if not enrichments:
        print("  No Claude enrichments found (run with --enrich first)")
        return 0

    updated = 0
    for fid_str, data in enrichments.items():
        fid = int(fid_str)
        series = data.get("series", [])
        if not series or data.get("flag"):
            continue

        # Normalize series to list of strings
        if isinstance(series, str):
            series = [series]

        cur.execute("SELECT series FROM figures WHERE id = %s", (fid,))
        row = cur.fetchone()
        if not row:
            continue

        current_series = row[0] or []
        if current_series:
            continue  # Don't overwrite existing series

        if not dry_run:
            cur.execute("UPDATE figures SET series = %s WHERE id = %s", (series, fid))
        updated += 1

    return updated


def main():
    parser = argparse.ArgumentParser(description="Apply character fixes after audit review")
    parser.add_argument("--apply", action="store_true", help="Actually apply changes (default: dry run)")
    parser.add_argument("--skip-dedup", action="store_true", help="Skip dedup, apply tier+series only")
    parser.add_argument("--mechanical-only", action="store_true",
                        help="Only dedup + era fixes (no tier corrections or enrichments)")
    args = parser.parse_args()

    dry_run = not args.apply

    print("=" * 60)
    print("APPLY CHARACTER FIXES")
    if dry_run:
        print("  DRY RUN — no changes will be made")
        print("  Use --apply to make changes")
    else:
        print("  LIVE MODE — changes will be committed")
    print("=" * 60)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # 1. Dedup
    if not args.skip_dedup:
        print("\n── Step 1: Remove Duplicates ──")
        removed = apply_dedup(cur, dry_run=dry_run)
        print(f"  {'Would remove' if dry_run else 'Removed'}: {removed} duplicate figures")
    else:
        print("\n── Step 1: Dedup SKIPPED ──")

    # 2. Tier fixes (skip if mechanical-only)
    if not args.mechanical_only:
        print("\n── Step 2: Tier Corrections ──")
        tier_fixed = apply_tier_fixes(cur, dry_run=dry_run)
        print(f"  {'Would fix' if dry_run else 'Fixed'}: {tier_fixed} tier assignments")
    else:
        print("\n── Step 2: Tier Corrections SKIPPED (mechanical-only) ──")

    # 3. Era fixes (always runs)
    print("\n── Step 3: Era Corrections ──")
    era_fixed = apply_era_fixes(cur, dry_run=dry_run)
    print(f"  {'Would fix' if dry_run else 'Fixed'}: {era_fixed} era assignments")

    # 4. Claude enrichments (skip if mechanical-only)
    if not args.mechanical_only:
        print("\n── Step 4: Apply Series Enrichments ──")
        enriched = apply_enrichments(cur, dry_run=dry_run)
        print(f"  {'Would update' if dry_run else 'Updated'}: {enriched} series assignments")
    else:
        print("\n── Step 4: Series Enrichments SKIPPED (mechanical-only) ──")

    if not dry_run:
        conn.commit()
        print("\n  All changes committed.")

        # Verify
        cur.execute("SELECT COUNT(*) FROM figures")
        total = cur.fetchone()[0]
        print(f"  Figures remaining: {total}")
    else:
        conn.rollback()
        print("\n  Dry run complete. Use --apply to execute.")

    conn.close()

    print(f"\n{'=' * 60}")
    if not dry_run:
        print("FIXES APPLIED")
        print("  Re-run evaluate_characters.py to verify:")
        print("  python -m rag.knowledge.evaluate_characters")
    else:
        print("DRY RUN COMPLETE — review output above")
        print("  To apply: python -m rag.knowledge.apply_character_fixes --apply")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
