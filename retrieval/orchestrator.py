"""
Islam Stories — Retrieval Orchestrator
Assembles grounded context packets for script generation by querying
the knowledge graph + vector store.

Primary function: retrieve_episode_context(figure, event, era, series)
Returns a dict with 15+ keys for the script prompt.
"""

import os
import sys
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")

# Import the vector query function
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from rag.embeddings.query import query_rag, query_rag_multi


def _get_conn():
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def _resolve_figure(cur, figure_name: str) -> dict | None:
    """Resolve a figure by name or variant."""
    cur.execute("""
        SELECT * FROM figures
        WHERE name = %s OR %s = ANY(name_variants)
        LIMIT 1
    """, (figure_name, figure_name))
    return cur.fetchone()


def _resolve_event(cur, event_name: str) -> dict | None:
    """Resolve an event by name or variant."""
    if not event_name:
        return None
    cur.execute("""
        SELECT * FROM events
        WHERE name = %s OR %s = ANY(name_variants)
        LIMIT 1
    """, (event_name, event_name))
    return cur.fetchone()


def _get_figure_lineage(cur, figure_id: int) -> list[dict]:
    cur.execute("""
        SELECT fl.*, f2.name AS related_figure_name
        FROM figure_lineage fl
        LEFT JOIN figures f2 ON fl.related_id = f2.id
        WHERE fl.figure_id = %s
        ORDER BY fl.lineage_type, fl.direction
    """, (figure_id,))
    return cur.fetchall()


def _get_figure_relationships(cur, figure_id: int) -> list[dict]:
    cur.execute("""
        SELECT fr.*,
               fa.name AS figure_a_name,
               fb.name AS figure_b_name
        FROM figure_relationships fr
        JOIN figures fa ON fr.figure_a_id = fa.id
        JOIN figures fb ON fr.figure_b_id = fb.id
        WHERE fr.figure_a_id = %s OR fr.figure_b_id = %s
        ORDER BY fr.relationship
    """, (figure_id, figure_id))
    return cur.fetchall()


def _get_figure_themes(cur, figure_id: int) -> list[dict]:
    cur.execute("""
        SELECT t.slug, t.name, t.description, ft.relevance
        FROM figure_themes ft
        JOIN themes t ON ft.theme_id = t.id
        WHERE ft.figure_id = %s
    """, (figure_id,))
    return cur.fetchall()


def _get_figure_motivations(cur, figure_id: int) -> list[dict]:
    cur.execute("""
        SELECT * FROM figure_motivations
        WHERE figure_id = %s
        ORDER BY is_primary DESC
    """, (figure_id,))
    return cur.fetchall()


def _get_figure_death(cur, figure_id: int) -> dict | None:
    cur.execute("SELECT * FROM figure_deaths WHERE figure_id = %s", (figure_id,))
    return cur.fetchone()


def _get_figure_quotes(cur, figure_id: int) -> list[dict]:
    cur.execute("""
        SELECT * FROM figure_quotes
        WHERE figure_id = %s
        ORDER BY chain_strength
    """, (figure_id,))
    return cur.fetchall()


def _get_scholarly_debates(cur, figure_id: int = None, event_id: int = None) -> list[dict]:
    conditions = []
    params = []
    if figure_id:
        conditions.append("figure_id = %s")
        params.append(figure_id)
    if event_id:
        conditions.append("event_id = %s")
        params.append(event_id)
    if not conditions:
        return []
    where = " OR ".join(conditions)
    cur.execute(f"SELECT * FROM scholarly_debates WHERE {where}", params)
    return cur.fetchall()


def _get_source_conflicts(cur, event_id: int) -> list[dict]:
    cur.execute("""
        SELECT * FROM source_relationships
        WHERE event_id = %s AND relationship IN ('CONTRADICTS', 'CHALLENGES')
    """, (event_id,))
    return cur.fetchall()


def _get_event_geography(cur, event_id: int) -> dict | None:
    cur.execute("SELECT * FROM event_geography WHERE event_id = %s", (event_id,))
    return cur.fetchone()


def _get_causal_context(cur, event_id: int) -> list[dict]:
    cur.execute("""
        SELECT ec.*,
               cause.name AS cause_name,
               effect.name AS effect_name
        FROM event_causes ec
        JOIN events cause ON ec.cause_event_id = cause.id
        JOIN events effect ON ec.effect_event_id = effect.id
        WHERE ec.cause_event_id = %s OR ec.effect_event_id = %s
        ORDER BY ec.time_gap_years
    """, (event_id, event_id))
    return cur.fetchall()


# ── Phase B lookups (return empty if tables are unpopulated) ────────

def _get_political_factions(cur, figure_id: int) -> list[dict]:
    cur.execute("""
        SELECT ff.*, pf.name AS faction_name, pf.ideology
        FROM figure_factions ff
        JOIN political_factions pf ON ff.faction_id = pf.id
        WHERE ff.figure_id = %s
        ORDER BY ff.joined_date
    """, (figure_id,))
    return cur.fetchall()


def _get_betrayals_committed(cur, figure_id: int) -> list[dict]:
    cur.execute("""
        SELECT pb.*, f.name AS betrayed_name
        FROM political_betrayals pb
        JOIN figures f ON pb.betrayed_id = f.id
        WHERE pb.betrayer_id = %s
    """, (figure_id,))
    return cur.fetchall()


def _get_betrayals_suffered(cur, figure_id: int) -> list[dict]:
    cur.execute("""
        SELECT pb.*, f.name AS betrayer_name
        FROM political_betrayals pb
        JOIN figures f ON pb.betrayer_id = f.id
        WHERE pb.betrayed_id = %s
    """, (figure_id,))
    return cur.fetchall()


def _get_alliance_reversals(cur, figure_id: int) -> list[dict]:
    cur.execute("""
        SELECT ar.*,
               pf1.name AS from_faction_name,
               pf2.name AS to_faction_name
        FROM alliance_reversals ar
        LEFT JOIN political_factions pf1 ON ar.from_faction_id = pf1.id
        LEFT JOIN political_factions pf2 ON ar.to_faction_id = pf2.id
        WHERE ar.figure_id = %s
    """, (figure_id,))
    return cur.fetchall()


def _build_source_map(accounts: list[dict]) -> dict:
    """Build a source frequency/score map from RAG results."""
    source_map = {}
    for acc in accounts:
        src = acc.get("source", "unknown")
        if src not in source_map:
            source_map[src] = {"count": 0, "max_score": 0.0}
        source_map[src]["count"] += 1
        score = acc.get("similarity_score", 0)
        if score > source_map[src]["max_score"]:
            source_map[src]["max_score"] = score
    return source_map


def retrieve_episode_context(
    figure: str,
    event: str,
    era: str = None,
    series: str = None,
    n_accounts: int = 20,
) -> dict:
    """
    Assemble a complete grounded context packet for script generation.

    Returns dict with keys:
        figure, event, primary_accounts, character_context,
        world_context, religious_context, source_map, coverage_score,
        conflicts, conflict_count, lineage, relationships, themes,
        causal_context, sensitivity_tier, series, era,
        motivations, figure_death, figure_quotes, scholarly_debates,
        event_geography,
        political_factions*, betrayals_committed*, betrayals_suffered*,
        alliance_reversals*
    """
    conn = _get_conn()
    cur = conn.cursor()

    # ── Resolve figure and event ────────────────────────────────────
    fig = _resolve_figure(cur, figure)
    evt = _resolve_event(cur, event)

    if not fig:
        conn.close()
        raise ValueError(f"Figure not found: {figure}")

    fig_id = fig["id"]
    evt_id = evt["id"] if evt else None
    resolved_era = era or (fig["era"][0] if fig["era"] else None)

    # ── RAG queries — multi-topic for comprehensive coverage ────────
    topics = [
        f"{figure} {event}",
        f"{figure} biography early life",
        f"{event} battle details accounts",
    ]
    rag_kwargs = {}
    if resolved_era:
        rag_kwargs["era"] = resolved_era
    if fig.get("name_variants"):
        rag_kwargs["figures"] = [figure] + list(fig["name_variants"])
    else:
        rag_kwargs["figures"] = [figure]

    primary_accounts = query_rag_multi(topics, n_results=n_accounts, **rag_kwargs)

    # Fallback: broader query without era filter if too few results
    if len(primary_accounts) < 5:
        broader = query_rag_multi(topics, n_results=n_accounts)
        seen = {a["content"][:200] for a in primary_accounts}
        for acc in broader:
            if acc["content"][:200] not in seen:
                primary_accounts.append(acc)
                seen.add(acc["content"][:200])

    # ── Build source map ────────────────────────────────────────────
    source_map = _build_source_map(primary_accounts)
    coverage_score = len(primary_accounts)

    # ── Source reliability checks ──────────────────────────────────
    from rag.generation.grounding_rules import check_source_reliability
    for account in primary_accounts:
        warning = check_source_reliability(account.get("source", ""))
        if warning:
            account["reliability_warning"] = warning

    # ── Knowledge graph lookups ─────────────────────────────────────
    lineage = _get_figure_lineage(cur, fig_id)
    relationships = _get_figure_relationships(cur, fig_id)
    themes = _get_figure_themes(cur, fig_id)
    motivations = _get_figure_motivations(cur, fig_id)
    figure_death = _get_figure_death(cur, fig_id)
    figure_quotes = _get_figure_quotes(cur, fig_id)

    scholarly_debates = _get_scholarly_debates(cur, figure_id=fig_id, event_id=evt_id)

    # Event-specific lookups
    conflicts = []
    causal_context = []
    event_geography = None
    if evt_id:
        conflicts = _get_source_conflicts(cur, evt_id)
        causal_context = _get_causal_context(cur, evt_id)
        event_geography = _get_event_geography(cur, evt_id)

    # Phase B lookups (may be empty)
    political_factions = _get_political_factions(cur, fig_id)
    betrayals_committed = _get_betrayals_committed(cur, fig_id)
    betrayals_suffered = _get_betrayals_suffered(cur, fig_id)
    alliance_reversals = _get_alliance_reversals(cur, fig_id)

    conn.close()

    # ── Assemble context packet ─────────────────────────────────────
    return {
        # Core identifiers
        "figure": dict(fig),
        "event": dict(evt) if evt else None,
        "sensitivity_tier": fig["sensitivity_tier"],
        "era": resolved_era,
        "series": series or (fig["series"][0] if fig["series"] else None),

        # RAG results
        "primary_accounts": [dict(a) for a in primary_accounts],
        "source_map": source_map,
        "coverage_score": coverage_score,

        # Knowledge graph — character
        "lineage": [dict(l) for l in lineage],
        "relationships": [dict(r) for r in relationships],
        "themes": [dict(t) for t in themes],
        "motivations": [dict(m) for m in motivations],
        "figure_death": dict(figure_death) if figure_death else None,
        "figure_quotes": [dict(q) for q in figure_quotes],

        # Knowledge graph — event/world
        "conflicts": [dict(c) for c in conflicts],
        "conflict_count": len(conflicts),
        "causal_context": [dict(c) for c in causal_context],
        "event_geography": dict(event_geography) if event_geography else None,
        "scholarly_debates": [dict(d) for d in scholarly_debates],

        # Phase B (may be empty)
        "political_factions": [dict(f) for f in political_factions],
        "betrayals_committed": [dict(b) for b in betrayals_committed],
        "betrayals_suffered": [dict(b) for b in betrayals_suffered],
        "alliance_reversals": [dict(a) for a in alliance_reversals],
    }


# ═══════════════════════════════════════════════════════════════════════
# GATE TEST
# ═══════════════════════════════════════════════════════════════════════

def run_gate_test():
    """
    Gate test: Khalid ibn Walid + Battle of Yarmouk.
    Must return ALL required fields populated.
    """
    print("=" * 60)
    print("GATE TEST: Khalid ibn Walid / Battle of Yarmouk")
    print("=" * 60)

    ctx = retrieve_episode_context(
        figure="Khalid ibn Walid",
        event="Battle of Yarmouk",
        era="rashidun",
        series="The Sword and the Succession",
    )

    checks = []

    # 1. 5+ primary accounts
    n_accounts = len(ctx["primary_accounts"])
    passed = n_accounts >= 5
    checks.append(("5+ primary_accounts", passed, f"{n_accounts} accounts"))
    if ctx["primary_accounts"]:
        top = ctx["primary_accounts"][0]
        top_source = top["source"]
        top_score = top["similarity_score"]
        checks.append(("top source relevant", top_score > 0.70,
                        f"{top_source} score={top_score:.4f}"))

    # 2. Figure resolved with tier B
    fig_tier = ctx["sensitivity_tier"]
    checks.append(("sensitivity_tier = B", fig_tier == "B", f"tier={fig_tier}"))

    # 3. Event resolved with date_ce = 636
    evt = ctx["event"]
    evt_date = evt["date_ce"] if evt else None
    checks.append(("event date_ce = 636", evt_date == "636", f"date={evt_date}"))

    # 4. Source conflicts populated (from source_relationships table)
    n_conflicts = len(ctx["conflicts"])
    checks.append(("source_conflicts >= 1", n_conflicts >= 1,
                    f"{n_conflicts} conflicts"))

    # 5. Lineage populated
    has_lineage = len(ctx["lineage"]) > 0
    checks.append(("lineage populated", has_lineage, f"{len(ctx['lineage'])} entries"))

    # 6. Themes include required
    theme_slugs = [t["slug"] for t in ctx["themes"]]
    has_loyalty = "loyalty_and_betrayal" in theme_slugs
    has_justice = "justice_vs_power" in theme_slugs
    checks.append(("themes: loyalty_and_betrayal", has_loyalty, str(theme_slugs)))
    checks.append(("themes: justice_vs_power", has_justice, str(theme_slugs)))

    # 7. Motivations present
    has_motivations = len(ctx["motivations"]) > 0
    mot_types = [m["motivation"] for m in ctx["motivations"]]
    checks.append(("motivations (LOYALTY, JUSTICE)", has_motivations, str(mot_types)))

    # 8. Death entry
    has_death = ctx["figure_death"] is not None
    last_words = ctx["figure_death"]["last_words"][:50] if has_death else None
    checks.append(("figure_death with last_words", has_death and bool(last_words),
                    f"last_words={last_words}..."))

    # 9. Quotes present
    has_quotes = len(ctx["figure_quotes"]) > 0
    checks.append(("figure_quotes present", has_quotes,
                    f"{len(ctx['figure_quotes'])} quotes"))

    # Print results
    all_pass = True
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        print(f"  [{status}] {name}: {detail}")

    print()
    if all_pass:
        print("GATE TEST PASSED — Phase 1B unlocked.")
    else:
        print("GATE TEST FAILED — fix issues above.")

    return all_pass


if __name__ == "__main__":
    run_gate_test()
