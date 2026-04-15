"""
Islam Stories — Chunk Metadata Creator
Creates chunk_metadata rows for documents that don't have one,
populating figure_ids via string matching against figure names/variants.

Run:  python -m rag.knowledge.backfill_chunk_metadata [--dry-run] [--batch-size 5000]
"""

import os
import re
import argparse
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


def load_figure_patterns():
    """Build compiled regex patterns for each figure."""
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

        # Sorted longest first, compiled with word boundaries
        names_sorted = sorted(names, key=len, reverse=True)
        regexes = [re.compile(r'\b' + re.escape(n) + r'\b', re.IGNORECASE)
                   for n in names_sorted if len(n) > 2]

        if regexes:
            patterns.append({
                "id": fig["id"],
                "name": fig["name"],
                "regexes": regexes,
            })

    return patterns


def run_backfill(dry_run=False, batch_size=5000):
    print("=" * 60)
    print("CHUNK METADATA CREATOR")
    if dry_run:
        print("DRY RUN — no DB writes")
    print("=" * 60)

    # Load figure patterns
    print("\nLoading figure patterns...")
    patterns = load_figure_patterns()
    print(f"  {len(patterns)} figures loaded")

    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    # Get documents without chunk_metadata
    cur.execute("""
        SELECT d.id, d.content
        FROM documents d
        WHERE NOT EXISTS (
            SELECT 1 FROM chunk_metadata cm WHERE cm.chunk_id = d.id
        )
        ORDER BY d.id
    """)
    docs = cur.fetchall()
    total = len(docs)
    print(f"  Documents without chunk_metadata: {total}")

    created = 0
    with_figures = 0
    total_links = 0

    for i, doc in enumerate(docs, 1):
        content = doc["content"] or ""
        found_ids = []

        for pat in patterns:
            for regex in pat["regexes"]:
                if regex.search(content):
                    found_ids.append(pat["id"])
                    break

        if not dry_run:
            cur.execute("""
                INSERT INTO chunk_metadata (chunk_id, figure_ids)
                VALUES (%s, %s)
            """, (doc["id"], found_ids))

        created += 1
        if found_ids:
            with_figures += 1
            total_links += len(found_ids)

        if i % batch_size == 0:
            if not dry_run:
                conn.commit()
            pct = 100 * i / total
            print(f"  [{i}/{total}] ({pct:.0f}%) created={created} with_figures={with_figures} links={total_links}")

    if not dry_run:
        conn.commit()

    conn.close()

    print(f"\n{'=' * 60}")
    print("BACKFILL COMPLETE")
    print(f"  Rows created: {created}")
    print(f"  With figure links: {with_figures}")
    print(f"  Empty (no matches): {created - with_figures}")
    print(f"  Total figure links: {total_links}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create chunk_metadata for unlinked documents")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=5000)
    args = parser.parse_args()
    run_backfill(dry_run=args.dry_run, batch_size=args.batch_size)
