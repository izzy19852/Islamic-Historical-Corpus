"""
Islam Stories — Story Sequencing & Dependency Mapping
After deduplication, this script:
1. Groups stories into series (character arcs, era seasons, thematic threads)
2. Maps dependencies ("this episode reveals info needed before that episode")
3. Produces a production-order DAG
4. Assigns season/episode numbers

Run:  python3 -m rag.knowledge.sequence_stories
"""

import os
import psycopg2
import psycopg2.extras
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


def _get_active_stories(cur) -> list[dict]:
    """Get all non-merged stories."""
    cur.execute("""
        SELECT sr.*, f.name AS figure_name, f.era AS figure_eras
        FROM story_registry sr
        LEFT JOIN figures f ON sr.primary_figure_id = f.id
        WHERE sr.status NOT IN ('MERGED')
        ORDER BY sr.date_start_ce NULLS LAST, sr.era, sr.title
    """)
    return [dict(r) for r in cur.fetchall()]


def _assign_series(stories: list[dict]) -> dict[str, list[dict]]:
    """
    Group stories into series:
    - Character arc series: 2+ stories about the same figure
    - Era series: stories in the same era without a dominant figure
    - Thematic threads: stories with the same thread field
    """
    # Group by primary figure
    by_figure = defaultdict(list)
    # Group by thread
    by_thread = defaultdict(list)
    # Group by era (for unassigned)
    by_era = defaultdict(list)

    for s in stories:
        if s.get('thread'):
            by_thread[s['thread']].append(s)
        elif s.get('primary_figure_id'):
            by_figure[s['primary_figure_id']].append(s)
        else:
            era = s.get('era') or 'unknown'
            by_era[era].append(s)

    series = {}

    # Character arc series (2+ stories about same figure)
    for fig_id, fig_stories in by_figure.items():
        if len(fig_stories) >= 2:
            fig_name = fig_stories[0].get('figure_name', f'figure_{fig_id}')
            series_id = f"arc_{fig_name.lower().replace(' ', '_')}"
            series[series_id] = sorted(fig_stories, key=lambda s: s.get('date_start_ce') or 9999)
        else:
            # Single stories go to era grouping
            s = fig_stories[0]
            era = s.get('era') or 'unknown'
            by_era[era].append(s)

    # Thematic thread series
    for thread, thread_stories in by_thread.items():
        series_id = f"thread_{thread}"
        series[series_id] = sorted(thread_stories, key=lambda s: s.get('date_start_ce') or 9999)

    # Era series (remaining stories)
    for era, era_stories in by_era.items():
        if era_stories:
            series_id = f"era_{era}"
            series[series_id] = sorted(era_stories, key=lambda s: s.get('date_start_ce') or 9999)

    return series


def _map_dependencies(stories: list[dict]) -> list[tuple[int, int]]:
    """
    Map story dependencies: story A must come before story B if:
    - Same figure, A is chronologically earlier
    - A introduces a figure who appears in B
    - A covers a cause event and B covers the effect
    """
    deps = []
    stories_by_id = {s['story_id']: s for s in stories}

    # Chronological dependencies within same primary figure
    by_figure = defaultdict(list)
    for s in stories:
        if s.get('primary_figure_id'):
            by_figure[s['primary_figure_id']].append(s)

    for fig_id, fig_stories in by_figure.items():
        sorted_stories = sorted(fig_stories, key=lambda s: s.get('date_start_ce') or 9999)
        for i in range(len(sorted_stories) - 1):
            a = sorted_stories[i]
            b = sorted_stories[i + 1]
            if a.get('date_start_ce') and b.get('date_start_ce'):
                deps.append((a['story_id'], b['story_id']))

    # Cross-figure dependencies: if story A's figure_ids overlap with
    # story B's primary_figure_id and A is chronologically first
    for a in stories:
        for b in stories:
            if a['story_id'] >= b['story_id']:
                continue
            a_figs = set(a.get('figure_ids') or [])
            b_primary = b.get('primary_figure_id')
            if b_primary and b_primary in a_figs:
                a_date = a.get('date_start_ce') or 9999
                b_date = b.get('date_start_ce') or 9999
                if a_date < b_date:
                    deps.append((a['story_id'], b['story_id']))

    # Deduplicate
    return list(set(deps))


def _assign_episode_numbers(series: dict[str, list[dict]]) -> list[dict]:
    """Assign season and episode numbers within each series."""
    updates = []
    season_counter = defaultdict(int)

    for series_id, stories in sorted(series.items()):
        # Determine season based on era or series type
        if series_id.startswith('arc_'):
            era = stories[0].get('era') or 'unknown'
        elif series_id.startswith('era_'):
            era = series_id.replace('era_', '')
        else:
            era = 'thematic'

        season_counter[era] += 1
        season = season_counter[era]

        for ep_num, story in enumerate(stories, 1):
            updates.append({
                'story_id': story['story_id'],
                'series_id': series_id,
                'season': season,
                'episode_number': ep_num,
            })

    return updates


def _topological_sort(stories: list[dict], deps: list[tuple[int, int]]) -> list[int]:
    """
    Topological sort of stories based on dependencies.
    Returns production order (list of story_ids).
    """
    from collections import deque

    graph = defaultdict(list)
    in_degree = defaultdict(int)
    all_ids = {s['story_id'] for s in stories}

    for a, b in deps:
        if a in all_ids and b in all_ids:
            graph[a].append(b)
            in_degree[b] += 1

    # Initialize with stories that have no dependencies
    queue = deque()
    for sid in all_ids:
        if in_degree[sid] == 0:
            queue.append(sid)

    order = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # Add any remaining (cycle detection — shouldn't happen)
    remaining = all_ids - set(order)
    order.extend(sorted(remaining))

    return order


def run_sequencing(dry_run: bool = False):
    """Assign series, map dependencies, sequence for production."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = True
    cur = conn.cursor()

    print("Story Sequencing & Dependency Mapping")
    print("=" * 60)

    stories = _get_active_stories(cur)
    print(f"  Active stories: {len(stories)}")

    if not stories:
        print("  No stories to sequence.")
        conn.close()
        return

    # 1. Assign series groupings
    series = _assign_series(stories)
    print(f"\n  Series identified: {len(series)}")
    for sid, s_stories in sorted(series.items()):
        print(f"    {sid}: {len(s_stories)} stories")

    # 2. Map dependencies
    deps = _map_dependencies(stories)
    print(f"\n  Dependencies mapped: {len(deps)}")

    # 3. Assign episode numbers
    updates = _assign_episode_numbers(series)

    # 4. Topological sort for production order
    production_order = _topological_sort(stories, deps)
    print(f"  Production order: {len(production_order)} stories sequenced")

    # 5. Apply updates
    if dry_run:
        print("\n  [DRY RUN] Would update:")
        for u in updates[:20]:
            print(f"    story_id={u['story_id']}: series={u['series_id']}, "
                  f"S{u['season']}E{u['episode_number']}")
        if len(updates) > 20:
            print(f"    ... and {len(updates) - 20} more")
    else:
        for u in updates:
            cur.execute("""
                UPDATE story_registry
                SET series_id = %s,
                    season = %s,
                    episode_number = %s,
                    status = CASE WHEN status = 'EXTRACTED' THEN 'SEQUENCED' ELSE status END
                WHERE story_id = %s
            """, (u['series_id'], u['season'], u['episode_number'], u['story_id']))

        # Write dependencies
        for before_id, after_id in deps:
            cur.execute("""
                UPDATE story_registry
                SET depends_on = ARRAY_APPEND(COALESCE(depends_on, '{}'::int[]), %s)
                WHERE story_id = %s
                  AND NOT (%s = ANY(COALESCE(depends_on, '{}'::int[])))
            """, (before_id, after_id, before_id))

        print(f"\n  ✓ Updated {len(updates)} stories with series/episode assignments")
        print(f"  ✓ Written {len(deps)} dependency edges")

    conn.close()

    # Print final summary
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT format, COUNT(*) FROM story_registry
        WHERE status NOT IN ('MERGED')
        GROUP BY format ORDER BY format
    """)
    print("\n  Stories by format:")
    for row in cur.fetchall():
        print(f"    {row['format']}: {row['count']}")

    cur.execute("""
        SELECT status, COUNT(*) FROM story_registry
        GROUP BY status ORDER BY status
    """)
    print("\n  Stories by status:")
    for row in cur.fetchall():
        print(f"    {row['status']}: {row['count']}")

    cur.execute("SELECT COUNT(*) FROM story_registry WHERE status NOT IN ('MERGED')")
    total = cur.fetchone()['count']
    conn.close()

    if total >= 300:
        print(f"\n  GATE PASSED: {total} unique stories sequenced (target: 300+)")
    else:
        print(f"\n  GATE NOT YET MET: {total}/300 unique stories")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sequence stories and map dependencies")
    parser.add_argument("--dry-run", action="store_true", help="Report but don't update DB")
    args = parser.parse_args()

    run_sequencing(dry_run=args.dry_run)
