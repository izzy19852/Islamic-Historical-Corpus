"""
Islam Stories — Figure Backfill Pass
Scans chunk_metadata entries with empty figure_ids and checks
the chunk text for known figure names/variants using string matching.

This catches cases where Haiku missed a figure reference.

Run:  python -m rag.knowledge.backfill_figures [--dry-run] [--limit 1000]
"""

import os
import re
import sys
import argparse
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


def load_figure_patterns():
    """Build regex patterns for each figure from name + variants."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("SELECT id, name, name_variants FROM figures ORDER BY id")
    figures = cur.fetchall()
    conn.close()

    patterns = []
    for fig in figures:
        names = [fig["name"]]
        if fig["name_variants"]:
            names.extend(fig["name_variants"])

        # Build word-boundary patterns, sorted longest first to avoid partial matches
        names_sorted = sorted(names, key=len, reverse=True)
        # Escape regex special chars, require word boundaries
        regexes = [re.compile(r'\b' + re.escape(n) + r'\b', re.IGNORECASE) for n in names_sorted]

        patterns.append({
            "id": fig["id"],
            "name": fig["name"],
            "regexes": regexes,
            "search_names": names_sorted,
        })

    return patterns


def run_backfill(dry_run=False, limit=None):
    print("=" * 60)
    print("FIGURE BACKFILL PASS")
    if dry_run:
        print("DRY RUN — no DB writes")
    print("=" * 60)

    # Load figure patterns
    print("\nLoading figure patterns...")
    patterns = load_figure_patterns()
    print(f"  {len(patterns)} figures with {sum(len(p['search_names']) for p in patterns)} total name variants")

    # Get chunks with empty figure_ids
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    cur.execute("""
        SELECT cm.id AS cm_id, cm.chunk_id, cm.figure_ids, d.content
        FROM chunk_metadata cm
        JOIN documents d ON d.id = cm.chunk_id
        WHERE cm.figure_ids = '{}'
        ORDER BY cm.chunk_id
    """)
    empty_chunks = cur.fetchall()
    total_empty = len(empty_chunks)
    print(f"  Chunks with empty figure_ids: {total_empty}")

    if limit:
        empty_chunks = empty_chunks[:limit]
        print(f"  Processing: {len(empty_chunks)}")

    # Also scan chunks that DO have figure_ids — they might be missing some
    cur.execute("""
        SELECT cm.id AS cm_id, cm.chunk_id, cm.figure_ids, d.content
        FROM chunk_metadata cm
        JOIN documents d ON d.id = cm.chunk_id
        WHERE cm.figure_ids != '{}'
        ORDER BY cm.chunk_id
    """)
    populated_chunks = cur.fetchall()
    print(f"  Chunks with existing figure_ids: {len(populated_chunks)} (will check for missing refs)")

    updated_empty = 0
    updated_partial = 0
    total_new_links = 0

    # Process empty chunks
    print("\nScanning empty chunks...")
    for chunk in empty_chunks:
        content = chunk["content"].lower() if chunk["content"] else ""
        found_ids = []

        for pat in patterns:
            for regex in pat["regexes"]:
                if regex.search(chunk["content"] or ""):
                    found_ids.append(pat["id"])
                    break  # one match per figure is enough

        if found_ids:
            if not dry_run:
                cur.execute(
                    "UPDATE chunk_metadata SET figure_ids = %s WHERE id = %s",
                    (found_ids, chunk["cm_id"])
                )
            updated_empty += 1
            total_new_links += len(found_ids)

    print(f"  Empty chunks updated: {updated_empty} ({total_new_links} new figure links)")

    # Process populated chunks — check for missing figures
    print("\nScanning populated chunks for missing refs...")
    for chunk in populated_chunks:
        existing_ids = set(chunk["figure_ids"])
        found_ids = set()

        for pat in patterns:
            if pat["id"] in existing_ids:
                continue  # already linked
            for regex in pat["regexes"]:
                if regex.search(chunk["content"] or ""):
                    found_ids.add(pat["id"])
                    break

        if found_ids:
            merged = list(existing_ids | found_ids)
            if not dry_run:
                cur.execute(
                    "UPDATE chunk_metadata SET figure_ids = %s WHERE id = %s",
                    (merged, chunk["cm_id"])
                )
            updated_partial += 1
            total_new_links += len(found_ids)

    print(f"  Populated chunks augmented: {updated_partial}")

    if not dry_run:
        conn.commit()

    conn.close()

    print(f"\n{'=' * 60}")
    print("BACKFILL COMPLETE")
    print(f"  Empty chunks filled:    {updated_empty}")
    print(f"  Partial chunks augmented: {updated_partial}")
    print(f"  Total new figure links: {total_new_links}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill missing figure links via string matching")
    parser.add_argument("--dry-run", action="store_true", help="Report without writing")
    parser.add_argument("--limit", type=int, default=None, help="Max empty chunks to process")
    args = parser.parse_args()

    run_backfill(dry_run=args.dry_run, limit=args.limit)
