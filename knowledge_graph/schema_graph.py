"""
Islam Stories — Knowledge Graph Schema
Creates all 22 knowledge-graph tables on top of the existing
documents + sources vector store.

Run:  python -m rag.knowledge.schema_graph
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

# ── Tier 1: figures (new table — replaces pipeline-only list) ────────────

FIGURES_SQL = """
CREATE TABLE IF NOT EXISTS figures (
    id                  SERIAL PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    name_variants       TEXT[] DEFAULT '{}',
    sensitivity_tier    CHAR(1) NOT NULL CHECK (sensitivity_tier IN ('S','A','B','C')),
    era                 TEXT[] DEFAULT '{}',
    series              TEXT[] DEFAULT '{}',
    birth_death         TEXT,
    dramatic_question   TEXT,
    primary_sources     TEXT[] DEFAULT '{}',
    -- Session 7 additions
    generation          TEXT CHECK (generation IN (
        'sahabi','tabi_i','tabi_al_tabi_in','later'
    )),
    tabaqat_volume      INT,
    sahabi_categories   TEXT[] DEFAULT '{}',
    bayah_pledges       TEXT[] DEFAULT '{}',
    known_for           TEXT,
    primary_hadith_count INT DEFAULT 0,
    death_circumstance  TEXT CHECK (death_circumstance IN (
        'battle','plague','martyrdom','natural','executed','assassinated','unknown'
    )),
    created_at          TIMESTAMP DEFAULT NOW()
);
"""

# ── Tier 2: Core Knowledge Graph ────────────────────────────────────────

EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS events (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    name_variants   TEXT[] DEFAULT '{}',
    date_ce         TEXT,
    date_ah         TEXT,
    location        TEXT,
    era             TEXT,
    figure_ids      INT[],
    significance    TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

SOURCE_RELATIONSHIPS_SQL = """
CREATE TABLE IF NOT EXISTS source_relationships (
    id                  SERIAL PRIMARY KEY,
    source_a            TEXT NOT NULL,
    source_b            TEXT NOT NULL,
    event_id            INT REFERENCES events(id),
    relationship        TEXT NOT NULL CHECK (relationship IN (
        'CORROBORATES','CONTRADICTS','SUPPLEMENTS',
        'CHALLENGES','EARLIER_ACCOUNT','LATER_COMPILATION'
    )),
    conflict_note       TEXT,
    reliability_note    TEXT,
    scholarly_consensus TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);
"""

CHUNK_METADATA_SQL = """
CREATE TABLE IF NOT EXISTS chunk_metadata (
    id              SERIAL PRIMARY KEY,
    chunk_id        INT REFERENCES documents(id),
    figure_ids      INT[],
    event_id        INT REFERENCES events(id),
    account_type    TEXT CHECK (account_type IN (
        'eyewitness','transmitted','later_compilation','commentary'
    )),
    chain_strength  TEXT CHECK (chain_strength IN (
        'sahih','hasan','daif','unknown','scholarly'
    )),
    conflict_flag   BOOLEAN DEFAULT FALSE,
    conflict_note   TEXT,
    noise_flag      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

FIGURE_LINEAGE_SQL = """
CREATE TABLE IF NOT EXISTS figure_lineage (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id),
    related_id      INT REFERENCES figures(id),
    related_name    TEXT,
    lineage_type    TEXT NOT NULL CHECK (lineage_type IN (
        'BIOLOGICAL','POLITICAL_HEIR','MILITARY_PATRON',
        'INTELLECTUAL','SUFI_SILSILA'
    )),
    direction       TEXT NOT NULL CHECK (direction IN ('ancestor','descendant')),
    divergence      TEXT CHECK (divergence IN (
        'SURPASSED','BETRAYED','COMPLETED','CORRUPTED','ABANDONED','MARTYRED'
    )),
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

FIGURE_RELATIONSHIPS_SQL = """
CREATE TABLE IF NOT EXISTS figure_relationships (
    id              SERIAL PRIMARY KEY,
    figure_a_id     INT NOT NULL REFERENCES figures(id),
    figure_b_id     INT NOT NULL REFERENCES figures(id),
    relationship    TEXT NOT NULL CHECK (relationship IN (
        'ALLY','ANTAGONIST','RIVAL','MUTUAL_RESPECT',
        'IDEOLOGICAL_OPPONENT','PARALLEL','POLITICAL_OPPONENT'
    )),
    description     TEXT,
    resolution      TEXT CHECK (resolution IN (
        'RECONCILED','UNRESOLVED','VICTORY_A','VICTORY_B',
        'MUTUAL_DESTRUCTION','TRANSCENDED','DEATH_ENDED_IT'
    )),
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

EVENT_CAUSES_SQL = """
CREATE TABLE IF NOT EXISTS event_causes (
    id              SERIAL PRIMARY KEY,
    cause_event_id  INT NOT NULL REFERENCES events(id),
    effect_event_id INT NOT NULL REFERENCES events(id),
    time_gap_years  INT,
    relationship    TEXT NOT NULL CHECK (relationship IN (
        'DIRECT_CAUSE','CONTRIBUTING_FACTOR','PRECONDITION',
        'CONSEQUENCE','REACTION','PARALLEL_DEVELOPMENT'
    )),
    description     TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

THEMES_SQL = """
CREATE TABLE IF NOT EXISTS themes (
    id              SERIAL PRIMARY KEY,
    slug            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    description     TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

FIGURE_THEMES_SQL = """
CREATE TABLE IF NOT EXISTS figure_themes (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id),
    theme_id        INT NOT NULL REFERENCES themes(id),
    relevance       TEXT,
    UNIQUE(figure_id, theme_id)
);
"""

EVENT_THEMES_SQL = """
CREATE TABLE IF NOT EXISTS event_themes (
    id              SERIAL PRIMARY KEY,
    event_id        INT NOT NULL REFERENCES events(id),
    theme_id        INT NOT NULL REFERENCES themes(id),
    relevance       TEXT,
    UNIQUE(event_id, theme_id)
);
"""

FIGURE_JOURNEYS_SQL = """
CREATE TABLE IF NOT EXISTS figure_journeys (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id),
    sequence        INT NOT NULL,
    location        TEXT NOT NULL,
    lat             FLOAT,
    lon             FLOAT,
    date_ce         TEXT,
    significance    TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

FIGURE_LEGACIES_SQL = """
CREATE TABLE IF NOT EXISTS figure_legacies (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id),
    legacy          TEXT NOT NULL,
    time_horizon    TEXT,
    still_active    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

# ── Tier 3: Political Layer ─────────────────────────────────────────────

POLITICAL_FACTIONS_SQL = """
CREATE TABLE IF NOT EXISTS political_factions (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    era             TEXT,
    ideology        TEXT,
    description     TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

FIGURE_FACTIONS_SQL = """
CREATE TABLE IF NOT EXISTS figure_factions (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id),
    faction_id      INT NOT NULL REFERENCES political_factions(id),
    joined_date     TEXT,
    exit_date       TEXT,
    exit_reason     TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

FIGURE_MOTIVATIONS_SQL = """
CREATE TABLE IF NOT EXISTS figure_motivations (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id),
    motivation      TEXT NOT NULL CHECK (motivation IN (
        'FAITH','POWER','LOYALTY','SURVIVAL','PRAGMATISM',
        'JUSTICE','REVENGE','LEGACY','IDEOLOGY','KNOWLEDGE'
    )),
    is_primary      BOOLEAN DEFAULT FALSE,
    conflicts_with  TEXT,
    evidence        TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

POLITICAL_BETRAYALS_SQL = """
CREATE TABLE IF NOT EXISTS political_betrayals (
    id              SERIAL PRIMARY KEY,
    betrayer_id     INT NOT NULL REFERENCES figures(id),
    betrayed_id     INT NOT NULL REFERENCES figures(id),
    event_id        INT REFERENCES events(id),
    betrayal_type   TEXT NOT NULL CHECK (betrayal_type IN (
        'ASSASSINATION','DEFECTION','BROKEN_OATH','ABANDONMENT'
    )),
    context         TEXT,
    consequence     TEXT,
    was_it_justified TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

ALLIANCE_REVERSALS_SQL = """
CREATE TABLE IF NOT EXISTS alliance_reversals (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id),
    from_faction_id INT REFERENCES political_factions(id),
    to_faction_id   INT REFERENCES political_factions(id),
    reversal_type   TEXT NOT NULL CHECK (reversal_type IN (
        'OPPORTUNISTIC','PRINCIPLED','COERCED','DISILLUSIONED','STRATEGIC'
    )),
    event_id        INT REFERENCES events(id),
    description     TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

# ── Tier 4: Narrative Layer ─────────────────────────────────────────────

EVENT_GEOGRAPHY_SQL = """
CREATE TABLE IF NOT EXISTS event_geography (
    id                  SERIAL PRIMARY KEY,
    event_id            INT NOT NULL REFERENCES events(id) UNIQUE,
    terrain             TEXT,
    climate             TEXT,
    strategic_significance TEXT,
    visual_description  TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);
"""

SCHOLARLY_DEBATES_SQL = """
CREATE TABLE IF NOT EXISTS scholarly_debates (
    id                  SERIAL PRIMARY KEY,
    topic               TEXT NOT NULL,
    event_id            INT REFERENCES events(id),
    figure_id           INT REFERENCES figures(id),
    position_a          TEXT,
    position_b          TEXT,
    key_scholars        TEXT[],
    script_instruction  TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);
"""

FIGURE_TRANSFORMATIONS_SQL = """
CREATE TABLE IF NOT EXISTS figure_transformations (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id),
    transformation  TEXT NOT NULL CHECK (transformation IN (
        'CONVERSION','WITHDRAWAL','REDEMPTION','HARDENING',
        'RADICALIZATION','DISILLUSIONMENT','ENLIGHTENMENT'
    )),
    trigger_event   TEXT,
    before_state    TEXT,
    after_state     TEXT,
    source          TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

FIGURE_DEATHS_SQL = """
CREATE TABLE IF NOT EXISTS figure_deaths (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id) UNIQUE,
    circumstance    TEXT,
    last_words      TEXT,
    last_words_source TEXT,
    witnesses       TEXT[],
    location        TEXT,
    date_ce         TEXT,
    source          TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

FIGURE_QUOTES_SQL = """
CREATE TABLE IF NOT EXISTS figure_quotes (
    id              SERIAL PRIMARY KEY,
    figure_id       INT NOT NULL REFERENCES figures(id),
    quote           TEXT NOT NULL,
    context         TEXT,
    chain_strength  TEXT CHECK (chain_strength IN (
        'sahih','hasan','daif','unknown','scholarly'
    )),
    source          TEXT,
    use_in_script   TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

EVENT_ECONOMICS_SQL = """
CREATE TABLE IF NOT EXISTS event_economics (
    id              SERIAL PRIMARY KEY,
    event_id        INT NOT NULL REFERENCES events(id),
    factor          TEXT NOT NULL,
    impact          TEXT,
    source          TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

CULTURAL_ENCOUNTERS_SQL = """
CREATE TABLE IF NOT EXISTS cultural_encounters (
    id              SERIAL PRIMARY KEY,
    event_id        INT REFERENCES events(id),
    culture_a       TEXT NOT NULL,
    culture_b       TEXT NOT NULL,
    encounter_type  TEXT,
    outcome         TEXT,
    figure_ids      INT[],
    created_at      TIMESTAMP DEFAULT NOW()
);
"""

# ── Tier 5: Story Registry (v2 scaffolding) ───────────────────────────

STORY_REGISTRY_SQL = """
CREATE TABLE IF NOT EXISTS story_registry (
    story_id            SERIAL PRIMARY KEY,

    -- IDENTITY
    title               TEXT NOT NULL,
    subtitle            TEXT,
    one_line            TEXT NOT NULL,
    dramatic_question   TEXT,

    -- TEMPORAL
    date_start_ce       INTEGER,
    date_end_ce         INTEGER,
    date_ah             TEXT,
    era                 TEXT,

    -- SPATIAL
    region              TEXT,
    location            TEXT,

    -- FIGURES
    primary_figure_id   INTEGER REFERENCES figures(id),
    figure_ids          INTEGER[],
    sensitivity_max     CHAR(1),

    -- CLASSIFICATION
    dramatic_spine      TEXT NOT NULL,
    dramatic_cores      TEXT[],
    story_scale         TEXT NOT NULL,
    has_battle          BOOLEAN DEFAULT FALSE,
    has_political       BOOLEAN DEFAULT FALSE,
    has_spiritual       BOOLEAN DEFAULT FALSE,

    -- FORMAT
    format              TEXT NOT NULL,
    runtime_target      INTEGER,

    -- SERIES PLACEMENT
    series_id           TEXT,
    season              INTEGER,
    episode_number      INTEGER,
    is_crossover        BOOLEAN DEFAULT FALSE,
    crossover_with      TEXT[],
    thread              TEXT,

    -- SOURCE GROUNDING
    source_coverage     TEXT,
    primary_sources     TEXT[],
    chunk_ids           INTEGER[],
    known_conflicts     TEXT[],

    -- PRODUCTION
    estimated_budget    TEXT,
    seedance_clips_est  INTEGER DEFAULT 0,
    kling_clips_est     INTEGER DEFAULT 0,
    depends_on          INTEGER[],

    -- STATUS
    status              TEXT DEFAULT 'EXTRACTED',

    -- EXTRACTION METADATA
    extraction_pass     TEXT,
    extraction_source   TEXT,
    confidence          TEXT,
    created_at          TIMESTAMP DEFAULT NOW(),
    notes               TEXT
);
"""

# ── Indices ─────────────────────────────────────────────────────────────

INDICES_SQL = """
CREATE INDEX IF NOT EXISTS idx_figures_tier ON figures(sensitivity_tier);
CREATE INDEX IF NOT EXISTS idx_figures_era ON figures USING GIN(era);
CREATE INDEX IF NOT EXISTS idx_figures_generation ON figures(generation);
CREATE INDEX IF NOT EXISTS idx_events_era ON events(era);
CREATE INDEX IF NOT EXISTS idx_events_figure_ids ON events USING GIN(figure_ids);
CREATE INDEX IF NOT EXISTS idx_chunk_metadata_chunk_id ON chunk_metadata(chunk_id);
CREATE INDEX IF NOT EXISTS idx_chunk_metadata_figure_ids ON chunk_metadata USING GIN(figure_ids);
CREATE INDEX IF NOT EXISTS idx_chunk_metadata_event_id ON chunk_metadata(event_id);
CREATE INDEX IF NOT EXISTS idx_figure_lineage_figure ON figure_lineage(figure_id);
CREATE INDEX IF NOT EXISTS idx_figure_relationships_a ON figure_relationships(figure_a_id);
CREATE INDEX IF NOT EXISTS idx_figure_relationships_b ON figure_relationships(figure_b_id);
CREATE INDEX IF NOT EXISTS idx_figure_themes_figure ON figure_themes(figure_id);
CREATE INDEX IF NOT EXISTS idx_figure_themes_theme ON figure_themes(theme_id);
CREATE INDEX IF NOT EXISTS idx_figure_motivations_figure ON figure_motivations(figure_id);
CREATE INDEX IF NOT EXISTS idx_figure_deaths_figure ON figure_deaths(figure_id);
CREATE INDEX IF NOT EXISTS idx_figure_quotes_figure ON figure_quotes(figure_id);
CREATE INDEX IF NOT EXISTS idx_scholarly_debates_event ON scholarly_debates(event_id);
CREATE INDEX IF NOT EXISTS idx_scholarly_debates_figure ON scholarly_debates(figure_id);
CREATE INDEX IF NOT EXISTS idx_story_registry_era ON story_registry(era);
CREATE INDEX IF NOT EXISTS idx_story_registry_spine ON story_registry(dramatic_spine);
CREATE INDEX IF NOT EXISTS idx_story_registry_format ON story_registry(format);
CREATE INDEX IF NOT EXISTS idx_story_registry_status ON story_registry(status);
CREATE INDEX IF NOT EXISTS idx_story_registry_primary_figure ON story_registry(primary_figure_id);
CREATE INDEX IF NOT EXISTS idx_story_registry_figure_ids ON story_registry USING GIN(figure_ids);
CREATE INDEX IF NOT EXISTS idx_story_registry_depends ON story_registry USING GIN(depends_on);
"""

# ── Ordered list for creation ───────────────────────────────────────────

ALL_TABLES = [
    ("figures",               FIGURES_SQL),
    ("events",                EVENTS_SQL),
    ("source_relationships",  SOURCE_RELATIONSHIPS_SQL),
    ("chunk_metadata",        CHUNK_METADATA_SQL),
    ("figure_lineage",        FIGURE_LINEAGE_SQL),
    ("figure_relationships",  FIGURE_RELATIONSHIPS_SQL),
    ("event_causes",          EVENT_CAUSES_SQL),
    ("themes",                THEMES_SQL),
    ("figure_themes",         FIGURE_THEMES_SQL),
    ("event_themes",          EVENT_THEMES_SQL),
    ("figure_journeys",       FIGURE_JOURNEYS_SQL),
    ("figure_legacies",       FIGURE_LEGACIES_SQL),
    ("political_factions",    POLITICAL_FACTIONS_SQL),
    ("figure_factions",       FIGURE_FACTIONS_SQL),
    ("figure_motivations",    FIGURE_MOTIVATIONS_SQL),
    ("political_betrayals",   POLITICAL_BETRAYALS_SQL),
    ("alliance_reversals",    ALLIANCE_REVERSALS_SQL),
    ("event_geography",       EVENT_GEOGRAPHY_SQL),
    ("scholarly_debates",     SCHOLARLY_DEBATES_SQL),
    ("figure_transformations", FIGURE_TRANSFORMATIONS_SQL),
    ("figure_deaths",         FIGURE_DEATHS_SQL),
    ("figure_quotes",         FIGURE_QUOTES_SQL),
    ("event_economics",       EVENT_ECONOMICS_SQL),
    ("cultural_encounters",   CULTURAL_ENCOUNTERS_SQL),
    ("story_registry",        STORY_REGISTRY_SQL),
]


def create_knowledge_graph(db_url: str = None):
    url = db_url or os.getenv("ISLAM_STORIES_DB_URL")
    if not url:
        print("ERROR: ISLAM_STORIES_DB_URL not set")
        sys.exit(1)

    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur = conn.cursor()

    print("Creating knowledge graph tables...")
    for name, sql in ALL_TABLES:
        cur.execute(sql)
        print(f"  ✓ {name}")

    print("\nCreating indices...")
    cur.execute(INDICES_SQL)
    print("  ✓ all indices created")

    # Verify
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = [r[0] for r in cur.fetchall()]
    print(f"\nAll tables ({len(tables)}): {tables}")

    cur.close()
    conn.close()
    print("\nKnowledge graph schema complete.")


if __name__ == "__main__":
    create_knowledge_graph()
