"""
Islamic Historical Corpus API
Authenticated RAG access to 64K+ chunks of classical Islamic sources.

Tiers:
  free:        100 queries/month
  developer:   5,000 queries/month  ($15/mo)
  institutional: unlimited          ($75/mo)
"""

import os
import hashlib
import secrets
import re
import psycopg2
import voyageai
from datetime import date
from typing import Optional
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Header, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request

import sys
sys.path.insert(0, os.path.expanduser('~/islam-stories'))

load_dotenv(os.path.expanduser('~/islam-stories/.env'))

# ── Setup ─────────────────────────────────────────────────────────

app = FastAPI(
    title="Islamic Historical Corpus API",
    description="""
Authenticated access to the world's first structured,
classified Islamic historical corpus.

64,000+ chunks across 96+ sources spanning 1,400 years —
Al-Tabari, Ibn Kathir, Ibn Sa'd, all major hadith collections,
and the full classical Islamic canon. Every chunk is era-tagged,
source-attributed, and chain-strength classified.

**Authentication:** Pass your API key as `X-API-Key` header.
**Get a key:** https://islamiccorpus.com

**Ethical note:** We charge for infrastructure access only.
The classical scholarship belongs to the scholars.
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

vo = voyageai.Client(api_key=os.getenv('VOYAGE_AI_API_KEY'))

TIER_LIMITS = {
    "free":          100,
    "developer":     5000,
    "institutional": 999999,
}

# ── Auth ──────────────────────────────────────────────────────────

def get_db():
    return psycopg2.connect(os.getenv('ISLAM_STORIES_DB_URL'))

def verify_api_key(x_api_key: str = Header(..., alias="X-API-Key")):
    """Verify key, check quota, increment counter."""
    if not x_api_key:
        raise HTTPException(401, "X-API-Key header required")

    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, tier, month_count, month_reset, active
        FROM api_keys WHERE key_hash = %s
    """, (key_hash,))
    row = cur.fetchone()

    if not row:
        conn.close()
        raise HTTPException(401, "Invalid API key")

    key_id, tier, month_count, month_reset, active = row

    if not active:
        conn.close()
        raise HTTPException(403, "API key disabled")

    # Reset monthly counter if new month
    today = date.today()
    if month_reset < today.replace(day=1):
        cur.execute("""
            UPDATE api_keys SET month_count=0, month_reset=%s WHERE id=%s
        """, (today, key_id))
        month_count = 0

    # Check quota
    limit = TIER_LIMITS.get(tier, 100)
    if month_count >= limit:
        conn.close()
        raise HTTPException(429, f"Monthly quota exceeded ({limit} queries). Upgrade at islamiccorpus.com")

    # Increment
    cur.execute("""
        UPDATE api_keys SET query_count=query_count+1, month_count=month_count+1
        WHERE id=%s
    """, (key_id,))
    conn.commit()
    conn.close()

    return {"tier": tier, "queries_used": month_count + 1, "limit": limit}

# ── Request / Response models ─────────────────────────────────────

class QueryRequest(BaseModel):
    q: str
    era: Optional[str] = None
    source_type: Optional[str] = None
    n: Optional[int] = 5
    authenticated_only: Optional[bool] = False

class ResearchRequest(BaseModel):
    figure: str
    event: Optional[str] = None
    era: Optional[str] = "rashidun"
    series: Optional[str] = None

class KeyRequest(BaseModel):
    email: str

# ── Helpers ───────────────────────────────────────────────────────

def clean_chunk(row: dict) -> dict:
    """Strip embedding vector, clean for API response."""
    return {
        "content":      row.get("content", ""),
        "source":       row.get("source", ""),
        "source_type":  row.get("source_type", ""),
        "era":          row.get("era", ""),
        "score":        round(float(row.get("similarity_score", 0)), 4),
        "chain_strength": row.get("chain_strength", "unknown"),
        "account_type": row.get("account_type", "unknown"),
        "conflict_flag": row.get("conflict_flag", False),
        "authentication": row.get("metadata", {}).get("authentication", ""),
    }

def run_vector_search(query: str, era=None, source_type=None,
                      n=5, authenticated_only=False):
    """Embed query and search DB with optional filters."""
    emb = vo.embed([query], model="voyage-2", input_type="query").embeddings[0]

    conn = get_db()
    cur = conn.cursor()

    filters = []
    params = [emb]

    if era:
        filters.append("d.era = %s")
        params.append(era)

    if source_type:
        filters.append("d.source_type = %s")
        params.append(source_type)

    if authenticated_only:
        filters.append("d.metadata->>'authentication' = 'classical_islamic'")

    # Exclude noise chunks if chunk_metadata exists
    filters.append("""
        NOT EXISTS (
            SELECT 1 FROM chunk_metadata cm
            WHERE cm.chunk_id = d.id AND cm.noise_flag = TRUE
        )
    """)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    params.extend([emb, n])

    cur.execute(f"""
        SELECT
            d.id,
            d.content,
            d.source,
            d.source_type,
            d.era,
            d.metadata,
            1 - (d.embedding <=> %s::vector) AS similarity_score,
            COALESCE(cm.account_type, 'unknown')   AS account_type,
            COALESCE(cm.chain_strength, 'unknown') AS chain_strength,
            COALESCE(cm.conflict_flag, FALSE)       AS conflict_flag
        FROM documents d
        LEFT JOIN chunk_metadata cm ON cm.chunk_id = d.id
        {where}
        ORDER BY d.embedding <=> %s::vector
        LIMIT %s
    """, params)

    cols = ['id','content','source','source_type','era','metadata',
            'similarity_score','account_type','chain_strength','conflict_flag']
    results = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()
    return results

# ── Endpoints ─────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def root():
    return {
        "name": "Islamic Historical Corpus API",
        "version": "1.0.0",
        "docs": "/docs",
        "corpus": "64,000+ chunks | 96+ sources | 1,400 years | 15 eras",
        "get_key": "https://islamiccorpus.com"
    }

@app.get("/health")
def health():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM documents")
        count = cur.fetchone()[0]
        conn.close()
        return {"status": "ok", "chunks": count}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.post("/query")
@limiter.limit("30/minute")
def query_corpus(
    request: Request,
    body: QueryRequest,
    auth = Depends(verify_api_key)
):
    """
    Semantic search across the full corpus.

    Returns ranked chunks with source attribution,
    chain strength, and conflict flags.

    **Parameters:**
    - `q`: Your search query (natural language)
    - `era`: Filter by era (rashidun, umayyad, abbasid, crusades, mongol, ottoman, south_asia, africa, andalusia)
    - `source_type`: Filter by type (primary_arabic, hadith, quran, scholarly_western)
    - `n`: Number of results (1-20, default 5)
    - `authenticated_only`: If true, return only classical_islamic authenticated sources
    """
    if not body.q or len(body.q.strip()) < 3:
        raise HTTPException(400, "Query must be at least 3 characters")

    n = min(max(body.n or 5, 1), 20)

    try:
        results = run_vector_search(
            query=body.q,
            era=body.era,
            source_type=body.source_type,
            n=n,
            authenticated_only=body.authenticated_only or False,
        )
    except Exception as e:
        raise HTTPException(500, f"Retrieval error: {str(e)}")

    return {
        "query": body.q,
        "filters": {
            "era": body.era,
            "source_type": body.source_type,
            "authenticated_only": body.authenticated_only,
        },
        "count": len(results),
        "results": [clean_chunk(r) for r in results],
        "quota": auth,
    }


@app.post("/research")
@limiter.limit("10/minute")
def research_brief(
    request: Request,
    body: ResearchRequest,
    auth = Depends(verify_api_key)
):
    """
    Structured research brief for a figure and/or event.

    Returns a multi-query context packet used by Islam Stories
    script generation — primary accounts, character context,
    religious context, and pre-flagged source conflicts.

    This is the highest-value endpoint: it runs 4 parallel
    queries and assembles a structured packet, not raw chunks.
    Costs 4 queries against your monthly quota.
    """
    try:
        from rag.retrieval.orchestrator import retrieve_episode_context
        context = retrieve_episode_context(
            figure=body.figure,
            event=body.event or "",
            era=body.era or "rashidun",
            series=body.series or "",
        )
    except Exception as e:
        raise HTTPException(500, f"Orchestrator error: {str(e)}")

    # Clean for API response — remove raw embeddings
    def clean_list(chunks):
        return [clean_chunk(c) for c in (chunks or [])]

    return {
        "figure":           body.figure,
        "event":            body.event,
        "era":              body.era,
        "primary_accounts": clean_list(context.get("primary_accounts", [])),
        "character_context": clean_list(context.get("character_context", [])),
        "religious_context": clean_list(context.get("religious_context", [])),
        "world_context":    clean_list(context.get("world_context", [])),
        "conflicts":        context.get("conflicts", []),
        "coverage_score":   context.get("coverage_score", 0),
        "conflict_count":   context.get("conflict_count", 0),
        "source_map":       context.get("source_map", {}),
        "quota":            auth,
    }


@app.get("/figure/{name}")
@limiter.limit("30/minute")
def get_figure(
    request: Request,
    name: str,
    auth = Depends(verify_api_key)
):
    """
    Retrieve knowledge graph entry for a historical figure.

    Returns sensitivity tier, era, dramatic function,
    known relationships, lineage, and scholarly debates.
    """
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, era, sensitivity_tier, known_for,
               birth_death, death_circumstance, dramatic_question, generation
        FROM figures WHERE LOWER(name) ILIKE %s
        LIMIT 1
    """, (f"%{name.lower()}%",))
    fig = cur.fetchone()

    if not fig:
        conn.close()
        raise HTTPException(404, f"Figure '{name}' not found in knowledge graph")

    fig_id = fig[0]
    cols = ['id','name','era','sensitivity_tier','known_for',
            'birth_death','death_circumstance','dramatic_question','generation']
    figure = dict(zip(cols, fig))

    # Get relationships
    cur.execute("""
        SELECT
            CASE WHEN fr.figure_a_id = %s THEN fb.name ELSE fa.name END AS other,
            fr.relationship, fr.description, fr.resolution
        FROM figure_relationships fr
        JOIN figures fa ON fa.id = fr.figure_a_id
        JOIN figures fb ON fb.id = fr.figure_b_id
        WHERE fr.figure_a_id = %s OR fr.figure_b_id = %s
        LIMIT 10
    """, (fig_id, fig_id, fig_id))
    figure['relationships'] = [
        dict(zip(['other','relationship','description','resolution'], r))
        for r in cur.fetchall()
    ]

    # Get scholarly debates
    cur.execute("""
        SELECT topic, position_a, position_b, script_instruction
        FROM scholarly_debates WHERE figure_id = %s
    """, (fig_id,))
    figure['scholarly_debates'] = [
        dict(zip(['topic','position_a','position_b','instruction'], r))
        for r in cur.fetchall()
    ]

    # Get source coverage
    cur.execute("""
        SELECT COUNT(DISTINCT d.source), COUNT(d.id)
        FROM documents d
        JOIN chunk_metadata cm ON cm.chunk_id = d.id
        WHERE %s = ANY(cm.figure_ids)
    """, (fig_id,))
    coverage = cur.fetchone()
    figure['source_coverage'] = {
        "sources": coverage[0] if coverage else 0,
        "chunks":  coverage[1] if coverage else 0,
    }

    conn.close()
    return figure


@app.get("/sources")
@limiter.limit("20/minute")
def list_sources(
    request: Request,
    era: Optional[str] = Query(None),
    source_type: Optional[str] = Query(None),
    authenticated_only: bool = Query(False),
    auth = Depends(verify_api_key)
):
    """
    List all sources in the corpus with chunk counts.

    Filter by era, source_type, or authenticated_only
    to see only classical Islamic sources.
    """
    conn = get_db()
    cur = conn.cursor()

    filters = []
    params = []

    if era:
        filters.append("era = %s")
        params.append(era)
    if source_type:
        filters.append("source_type = %s")
        params.append(source_type)
    if authenticated_only:
        filters.append("metadata->>'authentication' = 'classical_islamic'")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    cur.execute(f"""
        SELECT source, source_type, era,
               COUNT(*) as chunks,
               metadata->>'authentication' as authentication,
               metadata->>'scholar_tier' as scholar_tier
        FROM documents
        {where}
        GROUP BY source, source_type, era,
                 metadata->>'authentication',
                 metadata->>'scholar_tier'
        ORDER BY COUNT(*) DESC
    """, params)

    cols = ['source','source_type','era','chunks','authentication','scholar_tier']
    sources = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    return {
        "total_sources": len(sources),
        "filters": {"era": era, "source_type": source_type,
                    "authenticated_only": authenticated_only},
        "sources": sources,
    }


@app.post("/api/request-key")
@limiter.limit("5/hour")
def request_free_key(request: Request, body: KeyRequest):
    """Generate a free tier API key from the landing page."""
    email = body.email.strip().lower()

    # Basic email validation
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        raise HTTPException(400, "Invalid email address")

    conn = get_db()
    cur = conn.cursor()

    # Check if email already has a key
    cur.execute("""
        SELECT key_hash FROM api_keys
        WHERE name = %s AND active = TRUE
    """, (f"free:{email}",))

    if cur.fetchone():
        conn.close()
        raise HTTPException(409,
            "An API key already exists for this email. "
            "Contact hello@islamiccorpus.com to retrieve it.")

    # Generate key
    raw    = "isk_" + secrets.token_urlsafe(24)
    hashed = hashlib.sha256(raw.encode()).hexdigest()

    cur.execute("""
        INSERT INTO api_keys (key_hash, name, tier)
        VALUES (%s, %s, 'free')
    """, (hashed, f"free:{email}"))
    conn.commit()
    conn.close()

    return {"key": raw, "tier": "free", "limit": 100}
