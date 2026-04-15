"""
Islam Stories — Story Deduplication
Collapse overlapping stories in the story_registry:
- Same figure + overlapping date range → merge or mark crossover
- Same event from different figure perspectives → single story, crossover flag

Run:  python3 -m rag.knowledge.deduplicate_stories
      python3 -m rag.knowledge.deduplicate_stories --dry-run
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


def _find_duplicates(cur) -> list[dict]:
    """
    Find potential duplicate stories based on:
    1. Overlapping figure_ids + date ranges
    2. Very similar titles (Levenshtein-like)
    3. Same primary_figure + same era + similar dramatic_spine
    """
    duplicates = []

    # Strategy 1: Same primary figure, same era, overlapping dates
    cur.execute("""
        SELECT a.story_id AS id_a, b.story_id AS id_b,
               a.title AS title_a, b.title AS title_b,
               a.primary_figure_id, a.era,
               a.date_start_ce AS start_a, a.date_end_ce AS end_a,
               b.date_start_ce AS start_b, b.date_end_ce AS end_b,
               a.format AS format_a, b.format AS format_b
        FROM story_registry a
        JOIN story_registry b ON a.story_id < b.story_id
        WHERE a.primary_figure_id = b.primary_figure_id
          AND a.primary_figure_id IS NOT NULL
          AND a.era = b.era
          AND a.status = 'EXTRACTED'
          AND b.status = 'EXTRACTED'
          AND (
            -- Date overlap check
            (a.date_start_ce IS NOT NULL AND b.date_start_ce IS NOT NULL
             AND a.date_start_ce <= COALESCE(b.date_end_ce, b.date_start_ce)
             AND b.date_start_ce <= COALESCE(a.date_end_ce, a.date_start_ce))
            -- Or both have same start date
            OR (a.date_start_ce = b.date_start_ce)
          )
        ORDER BY a.primary_figure_id, a.date_start_ce
    """)
    for row in cur.fetchall():
        duplicates.append({
            'type': 'same_figure_overlap',
            'id_a': row['id_a'], 'id_b': row['id_b'],
            'title_a': row['title_a'], 'title_b': row['title_b'],
            'figure_id': row['primary_figure_id'],
            'era': row['era'],
            'format_a': row['format_a'], 'format_b': row['format_b'],
        })

    # Strategy 2: Different figures but overlapping figure_ids arrays
    # (e.g., Yarmouk from Khalid's POV and Yarmouk from Abu Ubayda's POV)
    cur.execute("""
        SELECT a.story_id AS id_a, b.story_id AS id_b,
               a.title AS title_a, b.title AS title_b,
               a.figure_ids AS figs_a, b.figure_ids AS figs_b,
               a.format AS format_a, b.format AS format_b
        FROM story_registry a
        JOIN story_registry b ON a.story_id < b.story_id
        WHERE a.primary_figure_id != b.primary_figure_id
          AND a.figure_ids IS NOT NULL AND b.figure_ids IS NOT NULL
          AND a.figure_ids && b.figure_ids  -- array overlap
          AND a.date_start_ce = b.date_start_ce
          AND a.status = 'EXTRACTED'
          AND b.status = 'EXTRACTED'
        ORDER BY a.date_start_ce
    """)
    for row in cur.fetchall():
        duplicates.append({
            'type': 'multi_figure_same_event',
            'id_a': row['id_a'], 'id_b': row['id_b'],
            'title_a': row['title_a'], 'title_b': row['title_b'],
            'format_a': row['format_a'], 'format_b': row['format_b'],
        })

    return duplicates


def _merge_stories(cur, keep_id: int, remove_id: int, keep_title: str, remove_title: str):
    """
    Merge two stories: keep the richer one, mark the other as crossover reference.
    The kept story absorbs figure_ids from the removed one.
    """
    # Get figure_ids from both
    cur.execute("SELECT figure_ids FROM story_registry WHERE story_id = %s", (remove_id,))
    removed = cur.fetchone()
    removed_figs = removed['figure_ids'] if removed and removed['figure_ids'] else []

    # Add removed figures to kept story
    if removed_figs:
        cur.execute("""
            UPDATE story_registry
            SET figure_ids = (
                SELECT ARRAY(SELECT DISTINCT unnest(COALESCE(figure_ids, '{}'::int[]) || %s::int[]))
            ),
            is_crossover = TRUE,
            crossover_with = ARRAY_APPEND(COALESCE(crossover_with, '{}'::text[]), %s),
            notes = COALESCE(notes, '') || E'\nMerged with: ' || %s
            WHERE story_id = %s
        """, (removed_figs, remove_title, remove_title, keep_id))

    # Mark removed story
    cur.execute("""
        UPDATE story_registry
        SET status = 'MERGED',
            notes = COALESCE(notes, '') || E'\nMerged into story_id=' || %s::text || ': ' || %s
        WHERE story_id = %s
    """, (keep_id, keep_title, remove_id))


def _pick_keeper(dup: dict) -> tuple[int, int]:
    """Pick which story to keep based on format richness."""
    format_rank = {'FULL_EPISODE': 4, 'SHORT_EPISODE': 3, 'SHORT': 2, 'SUPPORTING': 1}
    rank_a = format_rank.get(dup['format_a'], 0)
    rank_b = format_rank.get(dup['format_b'], 0)

    if rank_a >= rank_b:
        return dup['id_a'], dup['id_b']
    else:
        return dup['id_b'], dup['id_a']


def run_deduplication(dry_run: bool = False):
    """Find and merge duplicate stories."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = True
    cur = conn.cursor()

    print("Story Deduplication")
    print("=" * 60)

    duplicates = _find_duplicates(cur)
    print(f"  Potential duplicates found: {len(duplicates)}")

    if not duplicates:
        print("  No duplicates to merge.")
        conn.close()
        return

    merged = 0
    crossovers = 0

    for dup in duplicates:
        dtype = dup['type']
        print(f"\n  [{dtype}]")
        print(f"    A: [{dup['format_a']}] {dup['title_a']} (id={dup['id_a']})")
        print(f"    B: [{dup['format_b']}] {dup['title_b']} (id={dup['id_b']})")

        if dtype == 'same_figure_overlap':
            # True duplicate — merge
            keep_id, remove_id = _pick_keeper(dup)
            keep_title = dup['title_a'] if keep_id == dup['id_a'] else dup['title_b']
            remove_title = dup['title_b'] if keep_id == dup['id_a'] else dup['title_a']

            if dry_run:
                print(f"    [DRY] Would merge: keep id={keep_id}, remove id={remove_id}")
            else:
                _merge_stories(cur, keep_id, remove_id, keep_title, remove_title)
                print(f"    ✓ Merged: kept id={keep_id}, removed id={remove_id}")
            merged += 1

        elif dtype == 'multi_figure_same_event':
            # Different perspectives — mark as crossover, don't merge
            if dry_run:
                print(f"    [DRY] Would mark as crossover pair")
            else:
                cur.execute("""
                    UPDATE story_registry
                    SET is_crossover = TRUE,
                        crossover_with = ARRAY_APPEND(
                            COALESCE(crossover_with, '{}'::text[]), %s)
                    WHERE story_id = %s
                """, (dup['title_b'], dup['id_a']))
                cur.execute("""
                    UPDATE story_registry
                    SET is_crossover = TRUE,
                        crossover_with = ARRAY_APPEND(
                            COALESCE(crossover_with, '{}'::text[]), %s)
                    WHERE story_id = %s
                """, (dup['title_a'], dup['id_b']))
                print(f"    ✓ Marked as crossover pair")
            crossovers += 1

    conn.close()

    print("\n" + "=" * 60)
    print("DEDUPLICATION COMPLETE")
    print(f"  Stories merged: {merged}")
    print(f"  Crossover pairs linked: {crossovers}")

    # Print final counts
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT status, COUNT(*) FROM story_registry
        GROUP BY status ORDER BY status
    """)
    print("\n  Registry status:")
    for row in cur.fetchall():
        print(f"    {row['status']}: {row['count']}")
    conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Deduplicate stories in registry")
    parser.add_argument("--dry-run", action="store_true", help="Report but don't merge")
    args = parser.parse_args()

    run_deduplication(dry_run=args.dry_run)
