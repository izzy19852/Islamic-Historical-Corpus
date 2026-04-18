"""
Islamic Historical Corpus API
Authenticated RAG access to 128K+ chunks of classical Islamic sources.

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
import stripe
import resend
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

stripe.api_key      = os.getenv('STRIPE_SECRET_KEY')
resend.api_key      = os.getenv('RESEND_API_KEY')
FROM_EMAIL           = os.getenv('FROM_EMAIL', 'salam@islamiccorpus.com')
DEVELOPER_PRICE      = os.getenv('DEVELOPER_PRICE_ID', '')
INSTITUTIONAL_PRICE  = os.getenv('INSTITUTIONAL_PRICE_ID', '')
WEBHOOK_SECRET       = os.getenv('STRIPE_WEBHOOK_SECRET', '')

# ── Setup ─────────────────────────────────────────────────────────

app = FastAPI(
    title="Islamic Historical Corpus API",
    description="""
Authenticated access to the world's first structured,
classified Islamic historical corpus.

128,000+ chunks across 216 sources spanning 1,400 years —
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
        "corpus": "128,356 chunks | 216 sources | 1,400 years | 22 eras",
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
@app.get("/api/sources")
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
    """Generate a free tier API key. One key per email — requesting again replaces the old one."""
    email = body.email.strip().lower()

    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        raise HTTPException(400, "Invalid email address")

    conn = get_db()
    cur = conn.cursor()

    # Delete any existing key for this email
    cur.execute("DELETE FROM api_keys WHERE name = %s", (f"free:{email}",))

    # Generate new key
    raw    = "isk_" + secrets.token_urlsafe(24)
    hashed = hashlib.sha256(raw.encode()).hexdigest()

    cur.execute("""
        INSERT INTO api_keys (key_hash, name, tier)
        VALUES (%s, %s, 'free')
    """, (hashed, f"free:{email}"))
    conn.commit()
    conn.close()

    return {"key": raw, "tier": "free", "limit": 100}


# ── Stripe: create checkout session ───────────────────────────

class CheckoutRequest(BaseModel):
    email: str
    tier: str  # "developer" or "institutional"

@app.post("/api/checkout")
@limiter.limit("10/hour")
def create_checkout(request: Request, body: CheckoutRequest):
    """
    Create a Stripe checkout session for paid tiers.
    Returns a checkout URL to redirect the user to.
    """
    email = body.email.strip().lower()
    tier  = body.tier.strip().lower()

    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        raise HTTPException(400, "Invalid email address")

    if tier not in ('developer', 'institutional'):
        raise HTTPException(400, "tier must be developer or institutional")

    price_id = (
        DEVELOPER_PRICE if tier == 'developer'
        else INSTITUTIONAL_PRICE
    )

    if not price_id:
        raise HTTPException(500, "Stripe price not configured")

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            mode='subscription',
            customer_email=email,
            line_items=[{
                'price':    price_id,
                'quantity': 1,
            }],
            subscription_data={
                'metadata': {'email': email, 'tier': tier},
            },
            success_url=(
                'https://islamiccorpus.com'
                '/?checkout=success&tier=' + tier
            ),
            cancel_url='https://islamiccorpus.com/?checkout=cancelled',
            metadata={
                'email': email,
                'tier':  tier,
            }
        )
        return {'checkout_url': session.url}

    except stripe.error.StripeError as e:
        raise HTTPException(500, f"Stripe error: {str(e)}")


# ── Stripe: webhook ───────────────────────────────────────────

@app.post("/api/webhook")
async def stripe_webhook(request: Request):
    """
    Stripe sends payment events here.
    On successful payment: generate API key + email it.
    """
    payload    = await request.body()
    sig_header = request.headers.get('stripe-signature', '')

    # Verify webhook signature
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, WEBHOOK_SECRET
        )
    except stripe.error.SignatureVerificationError:
        raise HTTPException(400, "Invalid webhook signature")
    except Exception as e:
        raise HTTPException(400, f"Webhook error: {str(e)}")

    # Handle successful subscription
    if event['type'] == 'checkout.session.completed':
        session  = event['data']['object']
        email    = session.get('customer_email') or \
                   session.get('metadata', {}).get('email', '')
        tier     = session.get('metadata', {}).get('tier', 'developer')

        # Detect tier from price ID as fallback
        try:
            line_items = stripe.checkout.Session.list_line_items(session['id'])
            price_id = line_items.data[0].price.id if line_items.data else None
            if price_id:
                if price_id == os.getenv('STRIPE_RESEARCHER_PRICE_ID'):
                    tier = 'researcher'
                elif price_id == os.getenv('STRIPE_DEVELOPER_PRICE_ID'):
                    tier = 'developer'
                elif price_id == os.getenv('STRIPE_INSTITUTIONAL_PRICE_ID'):
                    tier = 'institutional'
        except Exception as e:
            print(f"Price ID lookup error: {e}")

        tier_map = {
            'researcher':    'researcher',
            'developer':     'developer',
            'institutional': 'institutional',
        }
        db_tier = tier_map.get(tier, 'developer')

        if email:
            api_key = _generate_and_store_key(email, db_tier)
            if api_key:
                _send_key_email(email, api_key, db_tier)

    # Handle invoice paid — send receipt email
    elif event['type'] == 'invoice.paid':
        invoice = event['data']['object']
        email   = invoice.get('customer_email', '')
        if email:
            _send_invoice_email(email, invoice)

    # Handle invoice payment failed — notify customer
    elif event['type'] == 'invoice.payment_failed':
        invoice = event['data']['object']
        email   = invoice.get('customer_email', '')
        if email:
            _send_payment_failed_email(email, invoice)

    # Handle cancelled subscription
    elif event['type'] == 'customer.subscription.deleted':
        sub      = event['data']['object']
        customer = stripe.Customer.retrieve(sub['customer'])
        email    = customer.get('email', '')
        if email:
            _deactivate_key_for_email(email)

    return {"status": "ok"}


# ── Invoices ─────────────────────────────────────────────────

@app.get("/api/invoices")
@limiter.limit("10/minute")
def list_invoices(
    request: Request,
    email: str = Query(..., description="Customer email address"),
):
    """
    Retrieve invoice history for a customer.

    Returns all Stripe invoices (paid, open, uncollectible)
    with PDF download links and amounts.
    """
    email = email.strip().lower()
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        raise HTTPException(400, "Invalid email address")

    try:
        # Find customer by email
        customers = stripe.Customer.list(email=email, limit=1)
        if not customers.data:
            return {"email": email, "invoices": [], "count": 0}

        customer_id = customers.data[0].id

        # Fetch invoices
        invoices = stripe.Invoice.list(
            customer=customer_id,
            limit=24,
        )

        results = []
        for inv in invoices.data:
            results.append({
                "id":           inv.id,
                "number":       inv.number,
                "status":       inv.status,
                "amount_due":   inv.amount_due,
                "amount_paid":  inv.amount_paid,
                "currency":     inv.currency,
                "created":      inv.created,
                "period_start": inv.period_start,
                "period_end":   inv.period_end,
                "invoice_pdf":  inv.invoice_pdf,
                "hosted_url":   inv.hosted_invoice_url,
                "tier":         inv.metadata.get("tier", ""),
            })

        return {
            "email":    email,
            "count":    len(results),
            "invoices": results,
        }

    except stripe.error.StripeError as e:
        raise HTTPException(500, f"Stripe error: {str(e)}")


# ── Helper functions ──────────────────────────────────────────

def _generate_and_store_key(email: str, tier: str) -> str | None:
    """Generate API key and store in DB. Returns raw key."""
    try:
        raw    = "isk_" + secrets.token_urlsafe(24)
        hashed = hashlib.sha256(raw.encode()).hexdigest()
        conn   = get_db()
        cur    = conn.cursor()

        # Deactivate any existing key for this email
        cur.execute("""
            UPDATE api_keys SET active = FALSE
            WHERE name ILIKE %s
        """, (f"%{email}%",))

        # Create new key
        cur.execute("""
            INSERT INTO api_keys (key_hash, name, tier)
            VALUES (%s, %s, %s)
        """, (hashed, f"{tier}:{email}", tier))

        conn.commit()
        conn.close()
        return raw

    except Exception as e:
        print(f"Key generation error: {e}")
        return None


def _send_key_email(email: str, api_key: str, tier: str):
    """Send API key to customer via Resend."""
    limits = {
        'developer':     '5,000 queries/month',
        'institutional': 'Unlimited queries',
    }
    limit = limits.get(tier, '5,000 queries/month')

    try:
        resend.Emails.send({
            "from":    f"Islamic Historical Corpus <{FROM_EMAIL}>",
            "to":      [email],
            "subject": "Your Islamic Historical Corpus API Key",
            "html":    f"""
<!DOCTYPE html>
<html>
<body style="font-family:-apple-system,sans-serif;
             background:#f8f5f0;padding:40px 20px;">
<div style="max-width:520px;margin:0 auto;
            background:#fff;border-radius:8px;
            border:1px solid #e8e0d0;overflow:hidden;">

  <div style="background:#1c1208;padding:32px;
              text-align:center;
              border-bottom:3px solid #c9a84c;">
    <div style="color:#c9a84c;font-size:12px;
                letter-spacing:3px;
                text-transform:uppercase;
                margin-bottom:8px;">
      Islamic Historical Corpus
    </div>
    <h1 style="color:#fff;font-weight:300;
               font-size:24px;margin:0;">
      Your API Key
    </h1>
  </div>

  <div style="padding:32px;">
    <p style="color:#4a3f32;margin-bottom:24px;">
      Your <strong>{tier.title()}</strong> tier key
      is ready. {limit}.
    </p>

    <div style="background:#1c1208;
                border-radius:6px;padding:16px 20px;
                font-family:monospace;font-size:14px;
                color:#c9a84c;word-break:break-all;
                margin-bottom:24px;">
      {api_key}
    </div>

    <p style="color:#6a5a40;font-size:13px;
              margin-bottom:8px;">
      <strong>Save this key</strong> —
      it will not be shown again.
    </p>

    <div style="border-top:1px solid #e8e0d0;
                padding-top:24px;margin-top:24px;">
      <p style="color:#4a3f32;
                margin-bottom:16px;">
        Quick start:
      </p>
      <div style="background:#f8f5f0;
                  border-radius:6px;padding:16px;
                  font-family:monospace;
                  font-size:12px;color:#2a1a08;">
curl -X POST https://islamiccorpus.com/query \\<br>
&nbsp;&nbsp;-H "X-API-Key: {api_key}" \\<br>
&nbsp;&nbsp;-H "Content-Type: application/json" \\<br>
&nbsp;&nbsp;-d '{{"q": "Battle of Yarmouk", "n": 5}}'
      </div>
    </div>

    <div style="margin-top:24px;text-align:center;">
      <a href="https://islamiccorpus.com/docs"
         style="background:#c9a84c;color:#1a1208;
                padding:10px 24px;border-radius:4px;
                text-decoration:none;font-weight:700;
                font-size:14px;">
        View API Docs
      </a>
    </div>

    <p style="color:#8a7a60;font-size:12px;
              margin-top:24px;text-align:center;">
      Questions? Reply to this email or contact
      <a href="mailto:{FROM_EMAIL}"
         style="color:#c9a84c;">{FROM_EMAIL}</a>
    </p>
  </div>

</div>
</body>
</html>
            """,
        })
        print(f"Key email sent to {email}")

    except Exception as e:
        print(f"Email send error: {e}")


def _deactivate_key_for_email(email: str):
    """Deactivate API key when subscription cancelled."""
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE api_keys SET active = FALSE
            WHERE name ILIKE %s AND active = TRUE
        """, (f"%{email}%",))
        conn.commit()
        conn.close()
        print(f"Deactivated key for {email}")
    except Exception as e:
        print(f"Deactivation error: {e}")


def _send_invoice_email(email: str, invoice: dict):
    """Send invoice receipt email after successful payment."""
    amount    = f"${invoice.get('amount_paid', 0) / 100:.2f}"
    number    = invoice.get('number', 'N/A')
    pdf_url   = invoice.get('invoice_pdf', '')
    hosted    = invoice.get('hosted_invoice_url', '')

    try:
        resend.Emails.send({
            "from":    f"Islamic Historical Corpus <{FROM_EMAIL}>",
            "to":      [email],
            "subject": f"Invoice {number} — Payment Received",
            "html":    f"""
<!DOCTYPE html>
<html>
<body style="font-family:-apple-system,sans-serif;
             background:#f8f5f0;padding:40px 20px;">
<div style="max-width:520px;margin:0 auto;
            background:#fff;border-radius:8px;
            border:1px solid #e8e0d0;overflow:hidden;">

  <div style="background:#1c1208;padding:32px;
              text-align:center;
              border-bottom:3px solid #c9a84c;">
    <div style="color:#c9a84c;font-size:12px;
                letter-spacing:3px;
                text-transform:uppercase;
                margin-bottom:8px;">
      Islamic Historical Corpus
    </div>
    <h1 style="color:#fff;font-weight:300;
               font-size:24px;margin:0;">
      Payment Receipt
    </h1>
  </div>

  <div style="padding:32px;">
    <p style="color:#4a3f32;margin-bottom:24px;">
      Your payment of <strong>{amount}</strong> has been received.
    </p>

    <table style="width:100%;border-collapse:collapse;
                  margin-bottom:24px;">
      <tr>
        <td style="color:#6a5a40;padding:8px 0;
                   border-bottom:1px solid #e8e0d0;">
          Invoice
        </td>
        <td style="color:#2a1a08;padding:8px 0;
                   border-bottom:1px solid #e8e0d0;
                   text-align:right;font-weight:600;">
          {number}
        </td>
      </tr>
      <tr>
        <td style="color:#6a5a40;padding:8px 0;
                   border-bottom:1px solid #e8e0d0;">
          Amount
        </td>
        <td style="color:#2a1a08;padding:8px 0;
                   border-bottom:1px solid #e8e0d0;
                   text-align:right;font-weight:600;">
          {amount}
        </td>
      </tr>
      <tr>
        <td style="color:#6a5a40;padding:8px 0;">
          Status
        </td>
        <td style="color:#2a7a2a;padding:8px 0;
                   text-align:right;font-weight:600;">
          Paid
        </td>
      </tr>
    </table>

    <div style="text-align:center;">
      <a href="{hosted}"
         style="background:#c9a84c;color:#1a1208;
                padding:10px 24px;border-radius:4px;
                text-decoration:none;font-weight:700;
                font-size:14px;display:inline-block;
                margin-right:8px;">
        View Invoice
      </a>
      <a href="{pdf_url}"
         style="background:#1c1208;color:#c9a84c;
                padding:10px 24px;border-radius:4px;
                text-decoration:none;font-weight:700;
                font-size:14px;display:inline-block;">
        Download PDF
      </a>
    </div>

    <p style="color:#8a7a60;font-size:12px;
              margin-top:24px;text-align:center;">
      Questions? Contact
      <a href="mailto:{FROM_EMAIL}"
         style="color:#c9a84c;">{FROM_EMAIL}</a>
    </p>
  </div>

</div>
</body>
</html>
            """,
        })
        print(f"Invoice receipt sent to {email} ({number})")

    except Exception as e:
        print(f"Invoice email error: {e}")


def _send_payment_failed_email(email: str, invoice: dict):
    """Notify customer of failed payment."""
    amount    = f"${invoice.get('amount_due', 0) / 100:.2f}"
    number    = invoice.get('number', 'N/A')
    hosted    = invoice.get('hosted_invoice_url', '')

    try:
        resend.Emails.send({
            "from":    f"Islamic Historical Corpus <{FROM_EMAIL}>",
            "to":      [email],
            "subject": f"Action Required — Payment Failed for Invoice {number}",
            "html":    f"""
<!DOCTYPE html>
<html>
<body style="font-family:-apple-system,sans-serif;
             background:#f8f5f0;padding:40px 20px;">
<div style="max-width:520px;margin:0 auto;
            background:#fff;border-radius:8px;
            border:1px solid #e8e0d0;overflow:hidden;">

  <div style="background:#1c1208;padding:32px;
              text-align:center;
              border-bottom:3px solid #c44c4c;">
    <div style="color:#c9a84c;font-size:12px;
                letter-spacing:3px;
                text-transform:uppercase;
                margin-bottom:8px;">
      Islamic Historical Corpus
    </div>
    <h1 style="color:#fff;font-weight:300;
               font-size:24px;margin:0;">
      Payment Failed
    </h1>
  </div>

  <div style="padding:32px;">
    <p style="color:#4a3f32;margin-bottom:24px;">
      We were unable to process your payment of
      <strong>{amount}</strong> for invoice <strong>{number}</strong>.
    </p>

    <p style="color:#4a3f32;margin-bottom:24px;">
      Please update your payment method to keep your
      API access active. Your key will be deactivated
      if payment is not received.
    </p>

    <div style="text-align:center;">
      <a href="{hosted}"
         style="background:#c44c4c;color:#fff;
                padding:12px 28px;border-radius:4px;
                text-decoration:none;font-weight:700;
                font-size:14px;display:inline-block;">
        Update Payment Method
      </a>
    </div>

    <p style="color:#8a7a60;font-size:12px;
              margin-top:24px;text-align:center;">
      Questions? Contact
      <a href="mailto:{FROM_EMAIL}"
         style="color:#c9a84c;">{FROM_EMAIL}</a>
    </p>
  </div>

</div>
</body>
</html>
            """,
        })
        print(f"Payment failed email sent to {email} ({number})")

    except Exception as e:
        print(f"Payment failed email error: {e}")

# ── Account management ────────────────────────────────────────

class AccountLookupRequest(BaseModel):
    email: str

@app.post("/api/account/lookup")
@limiter.limit("10/hour")
def account_lookup(request: Request, body: AccountLookupRequest):
    """
    Look up account by email.
    Returns tier, usage, quota, active status.
    Does NOT return the raw key (security).
    """
    email = body.email.strip().lower()
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        raise HTTPException(400, "Invalid email")

    conn = get_db()
    cur  = conn.cursor()

    cur.execute("""
        SELECT name, tier, query_count, month_count,
               month_reset, active, created_at
        FROM api_keys
        WHERE name ILIKE %s AND active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
    """, (f"%{email}%",))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(404,
            "No active account found for this email. "
            "Check your email or sign up below.")

    name, tier, total, month, reset, active, created = row
    limit = TIER_LIMITS.get(tier, 100)

    return {
        "email":        email,
        "tier":         tier,
        "active":       active,
        "total_queries": total,
        "month_queries": month,
        "month_limit":  limit,
        "month_reset":  str(reset),
        "member_since": str(created)[:10],
        "percent_used": round((month / limit) * 100, 1)
                        if limit < 999999 else 0,
    }


@app.post("/api/account/resend-key")
@limiter.limit("3/hour")
def resend_key(request: Request, body: AccountLookupRequest):
    """
    Resend API key to email address.
    Generates a NEW key (old one deactivated).
    Rate limited to 3/hour to prevent abuse.
    """
    email = body.email.strip().lower()
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        raise HTTPException(400, "Invalid email")

    conn = get_db()
    cur  = conn.cursor()

    # Check account exists
    cur.execute("""
        SELECT tier FROM api_keys
        WHERE name ILIKE %s AND active = TRUE
        LIMIT 1
    """, (f"%{email}%",))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(404,
            "No active account found for this email.")

    tier    = row[0]
    new_key = _generate_and_store_key(email, tier)

    if new_key:
        _send_key_email(email, new_key, tier)
        return {"status": "sent",
                "message": f"New key sent to {email}"}
    else:
        raise HTTPException(500, "Key generation failed")


@app.post("/api/account/cancel")
@limiter.limit("5/hour")
def cancel_account(request: Request,
                   body: AccountLookupRequest):
    """
    Cancel account and deactivate API key.
    Stripe subscription must be cancelled separately
    via the cancellation link in the email.
    """
    email = body.email.strip().lower()
    if not re.match(r'^[^@]+@[^@]+\.[^@]+$', email):
        raise HTTPException(400, "Invalid email")

    conn = get_db()
    cur  = conn.cursor()

    cur.execute("""
        UPDATE api_keys SET active = FALSE
        WHERE name ILIKE %s AND active = TRUE
    """, (f"%{email}%",))
    affected = cur.rowcount
    conn.commit()
    conn.close()

    if affected == 0:
        raise HTTPException(404,
            "No active account found for this email.")

    # Send cancellation confirmation
    try:
        resend.Emails.send({
            "from": f"Islamic Historical Corpus <{FROM_EMAIL}>",
            "to":   [email],
            "subject": "Account Cancelled — Islamic Historical Corpus",
            "html": f"""
<!DOCTYPE html>
<html>
<body style="font-family:-apple-system,sans-serif;
             background:#f8f5f0;padding:40px 20px;">
<div style="max-width:520px;margin:0 auto;
            background:#fff;border-radius:8px;
            border:1px solid #e8e0d0;overflow:hidden;">
  <div style="background:#1c1208;padding:32px;
              text-align:center;
              border-bottom:3px solid #c9a84c;">
    <div style="color:#c9a84c;font-size:12px;
                letter-spacing:3px;
                text-transform:uppercase;">
      Islamic Historical Corpus
    </div>
  </div>
  <div style="padding:32px;">
    <h2 style="color:#1a1208;font-weight:400;
               margin-bottom:16px;">
      Account Cancelled
    </h2>
    <p style="color:#4a3f32;margin-bottom:16px;">
      Your API key has been deactivated.
      If you had a paid subscription, please also
      cancel it directly with Stripe to stop billing.
    </p>
    <p style="color:#4a3f32;margin-bottom:24px;">
      We're sorry to see you go. If there's anything
      we could have done better, please reply to
      this email and let us know.
    </p>
    <p style="color:#8a7a60;font-size:13px;">
      You can sign up again anytime at
      <a href="https://islamiccorpus.com"
         style="color:#c9a84c;">
        islamiccorpus.com
      </a>
    </p>
  </div>
</div>
</body>
</html>
            """,
        })
    except Exception as e:
        print(f"Cancel email error: {e}")

    return {
        "status":  "cancelled",
        "message": "Account deactivated. "
                   "Cancellation confirmation sent to "
                   + email
    }



# ── Tier configuration ────────────────────────────────
TIER_CONFIG = {
    "free": {"model":"claude-haiku-4-5-20251001","chat_limit":50,"api_limit":100,"max_tokens":1500,"label":"Free"},
    "researcher": {"model":"claude-haiku-4-5-20251001","chat_limit":500,"api_limit":5000,"max_tokens":2000,"label":"Researcher"},
    "developer": {"model":"claude-haiku-4-5-20251001","chat_limit":2000,"api_limit":20000,"max_tokens":2000,"label":"Developer"},
    "institutional": {"model":"claude-sonnet-4-20250514","chat_limit":5000,"api_limit":-1,"max_tokens":3500,"label":"Institutional"},
}

def get_tier_config(api_key_record: dict) -> dict:
    if not api_key_record:
        return TIER_CONFIG["free"]
    tier = api_key_record.get("tier", "free").lower()
    return TIER_CONFIG.get(tier, TIER_CONFIG["free"])

def check_monthly_limit(api_key: str, limit: int, endpoint: str = "chat"):
    if limit == -1:
        return True, 0
    conn = get_db()
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT COUNT(*) FROM api_usage
            WHERE api_key = %s AND endpoint = %s
            AND created_at >= date_trunc('month', NOW())
        """, (api_key, endpoint))
        count = cur.fetchone()[0]
        conn.close()
        return count < limit, count
    except:
        conn.close()
        return True, 0

def log_usage(api_key: str, endpoint: str, model: str, tokens_in: int = 0, tokens_out: int = 0):
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO api_usage (api_key, endpoint, model, tokens_in, tokens_out)
            VALUES (%s,%s,%s,%s,%s)
        """, (api_key, endpoint, model, tokens_in, tokens_out))
        conn.commit()
        conn.close()
    except:
        pass



# ── Chat endpoint ─────────────────────────────────────────────

import anthropic as _anthropic
from fastapi.responses import StreamingResponse

_claude = _anthropic.Anthropic(
    api_key=os.getenv('ANTHROPIC_API_KEY')
)

# In-memory session store for chat history (per session_id).
# Each entry: {"history": [{"role": "user"|"assistant", "content": str}, ...],
#              "created": float, "last": float}
# Resets on server restart.
import time
_chat_sessions: dict = {}

# Conversation memory bounds
CHAT_HISTORY_MAX_TURNS = 12      # keep last N user+assistant messages combined
CHAT_SESSION_TTL_SECONDS = 60 * 60 * 6   # 6 hours of idle → reset

RESEARCH_SYSTEM_PROMPT = """You are a scholarly Islamic historical research assistant grounded in authenticated primary sources — Al-Tabari, Ibn Kathir, Ibn Sa'd's Tabaqat, Al-Masudi, Ibn al-Athir, the major hadith collections, Ibn Khaldun, Ibn Battuta, and more — spanning 632–1900 CE.

CITATION RULES — NON-NEGOTIABLE:
- Every factual claim MUST cite its source: "According to **Al-Tabari**..." or "**Ibn Kathir** records..."
- When sources conflict, present BOTH: "**Al-Tabari** states X; however, **Ibn al-Athir** records Y."
- Always distinguish the majority scholarly position from minority positions.
- State chain strength when relevant: "In a *sahih* narration..." or "In a disputed account..."
- If the primary material you have in front of you does not cover the question, answer from broad Islamic historiographical knowledge and flag it naturally — without ever mentioning "the corpus," "retrieved chunks," or the retrieval system.
- NEVER invent facts, dates, or quotes.

SCHOLARLY POSITIONS:
- Represent BOTH majority and minority scholarly views fairly.
- Never suppress a minority position with legitimate scholarly support.
- Frame disagreements academically.

FORMATTING — MANDATORY:
- Open with a direct 1–2 sentence lead answering the question.
- Break the answer into scannable sections with **bold inline headers** or `## H2 headers`.
- Paragraphs of 2–4 sentences, never longer.
- Use bullet lists for enumerations and > blockquotes for direct quotations longer than a clause.
- **Bold** key names, titles, and turning-point phrases.
- Clear, precise language. Explain technical terms inline.

END every response with: "And Allah knows best (wa Allahu a'lam)."

FORBIDDEN:
- Using the word "corpus" or any phrase that exposes the retrieval system to the user
- Inventing sources
- Taking sides in theological disputes
- Depicting the Prophet (ﷺ) or the four Rightly-Guided Caliphs beyond what authenticated sources record
- Any factual claim without a source attribution"""

EXPLORER_SYSTEM_PROMPT = """
You are a master Islamic historian — deeply knowledgeable, warm, and thorough. You NEVER make the user ask follow-up questions to get information you already have. Give everything relevant upfront.

CORPUS: 72,000+ chunks from 136 authenticated Islamic sources spanning 632–1900 CE.

═══════════════════════════════════════
FORMATTING — MANDATORY
═══════════════════════════════════════

ALWAYS structure your response with:

**[Figure/Topic Name]**
*[One-line description — who they were]*

**Background**
- Era, origin, family, social context
- The world they were born into

**Life & Key Moments**
- Bullet each major event or phase
- Include specific details, dates where known
- Every bullet cites its source inline: [Ibn Sa\'d]

**Character & Personality**
- What contemporaries said about them
- Their defining traits with source citations

**Scholarly Positions**
- ✅ Majority view: what most scholars hold
- 🔄 Minority view: where scholars disagree
- Name the specific scholarly traditions

**Legacy & Impact**
- What changed because of this person
- How later generations remembered them

**What History Doesn\'t Tell Us**
- Honest gaps — what sources are silent on

*And Allah knows best (wa Allahu a\'lam).*

---

USE THIS FORMAT FOR EVERY RESPONSE.
Never write a wall of prose without headers.
Never make the user ask follow-up questions
for information you already have.

═══════════════════════════════════════
DEPTH RULES
═══════════════════════════════════════

PROACTIVE DEPTH — include all of these without being asked:
- Full name, kunya, laqab, honorifics
- Birth/death dates and locations
- Family background and lineage
- Teachers and students
- Key events in chronological order
- Notable sayings or actions with citations
- Scholarly debates about them
- Their relationship to major historical events
- How different Islamic traditions view them

CITATION FORMAT in bullets:
"Walked barefoot through Basra [Ibn Sa\'d, Tabaqat Vol VII]"
"Refused 1,000 dinars offered by a merchant [Al-Dhahabi, Siyar]"

WHEN CORPUS IS THIN:
Use broader Islamic historiographical knowledge,
clearly flagged:
"The classical sources record that..." or
"Al-Dhahabi preserves an account that..."

═══════════════════════════════════════
TONE
═══════════════════════════════════════

- Warm and authoritative
- Bullets for facts, prose for meaning
- Short paragraphs between sections
- Never preachy
- Human complexity always present

═══════════════════════════════════════
HARD RULES
═══════════════════════════════════════

- NEVER write walls of unformatted prose
- NEVER make user ask for info you have
- NEVER invent facts or sources
- NEVER take theological sides
- ALWAYS end with:

*And Allah knows best (wa Allahu a\'lam).*
"""


class ChatTurn(BaseModel):
    role:    str   # "user" | "assistant"
    content: str


class ChatRequest(BaseModel):
    message:    str
    mode:       str = "explorer"   # "research" or "explorer"
    session_id: str = ""
    api_key:    str = ""           # optional
    history:    list[ChatTurn] = []


@app.post("/api/chat")
@limiter.limit("30/minute")
async def chat(request: Request, body: ChatRequest):
    """
    Grounded Islamic historical chat.
    Retrieves primary-source context then generates
    a cited natural language response via Claude.
    Conversation history is client-supplied so chats
    persist across server restarts and devices.
    """
    message    = body.message.strip()
    mode       = body.mode if body.mode in (
                     "research", "explorer"
                 ) else "explorer"
    session_id = body.session_id.strip()
    user_key   = body.api_key.strip()
    history    = body.history or []

    if not message:
        raise HTTPException(400, "Message required")
    if len(message) > 500:
        raise HTTPException(400,
            "Message too long (max 500 chars)")

    # ── Tier lookup ──────────────────────────────────
    key_record = None
    if user_key:
        try:
            import hashlib as _hashlib
            khash = _hashlib.sha256(user_key.encode()).hexdigest()
            conn_t = get_db()
            cur_t  = conn_t.cursor()
            cur_t.execute("""
                SELECT key_hash, tier, name, active
                FROM api_keys
                WHERE key_hash = %s AND active = TRUE
            """, (khash,))
            row = cur_t.fetchone()
            if row:
                key_record = {"key":row[0],"tier":row[1],"email":row[2],"is_active":row[3]}
            conn_t.close()
        except Exception as e:
            print(f"Tier lookup error: {e}")

    tier_cfg = get_tier_config(key_record)

    if user_key:
        allowed, count = check_monthly_limit(user_key, tier_cfg["chat_limit"], "chat")
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail={"error":"Monthly limit reached","tier":tier_cfg["label"],"limit":tier_cfg["chat_limit"],"count":count,"upgrade":"islamiccorpus.com/#pricing"}
            )

    # ── Retrieve context ──────────────────────────────
    try:
        from rag.retrieval.orchestrator import (
            retrieve_episode_context
        )
        context = retrieve_episode_context(
            figure  = message[:100],
            event   = message[:100],
            era     = None,
            series  = None,
        )
    except Exception as e:
        print(f"Orchestrator error: {e}")
        context = {}

    try:
        direct = run_vector_search(
            query  = message,
            n      = 8,
        )
    except Exception as e:
        print(f"Vector search error: {e}")
        direct = []

    primary   = context.get("primary_accounts", [])
    character = context.get("character_context", [])
    religious = context.get("religious_context", [])
    conflicts = context.get("conflicts", [])

    all_chunks = primary + character + religious + direct
    seen       = set()
    unique     = []
    for c in all_chunks:
        key = c.get("content","")[:100]
        if key not in seen:
            seen.add(key)
            unique.append(c)

    unique.sort(
        key=lambda x: x.get("score",
            x.get("similarity_score", 0)),
        reverse=True
    )
    top_chunks = unique[:20]

    if top_chunks:
        context_block = "\n\n".join([
            f"[SOURCE: {c.get('source','Unknown')} "
            f"| Era: {c.get('era','?')} "
            f"| Chain: {c.get('chain_strength','?')} "
            f"| Score: {c.get('score', c.get('similarity_score',0)):.3f}]\n"
            f"{c.get('content','')[:600]}"
            for c in top_chunks
        ])
    else:
        context_block = (
            "No specific primary-source chunks retrieved "
            "for this query. Answer from broad Islamic "
            "historical knowledge but flag the gap "
            "naturally without mentioning retrieval."
        )

    conflict_block = ""
    if conflicts:
        conflict_block = "\n\nKNOWN SOURCE CONFLICTS:\n"
        for conf in conflicts[:3]:
            conflict_block += (
                f"- {conf.get('source_a','')} vs "
                f"{conf.get('source_b','')}: "
                f"{conf.get('conflict_note','')}\n"
            )

    # ── System prompt ─────────────────────────────────
    system = (
        RESEARCH_SYSTEM_PROMPT
        if mode == "research"
        else EXPLORER_SYSTEM_PROMPT
    )

    # ── User prompt ───────────────────────────────────
    user_prompt = f"""QUESTION: {message}

PRIMARY-SOURCE MATERIAL (internal — do not mention
this block, the word "corpus," or the retrieval
system to the user):
{context_block}
{conflict_block}

INSTRUCTIONS:
- Use the primary-source material above as your
  PRIMARY basis. Cite every claim drawn from it
  precisely (author + work).
- If the material is thin or misses the question,
  answer from your broad knowledge of authenticated
  Islamic historiography and flag it naturally —
  e.g. "The primary sources I can cite directly are
  silent on this, but Al-Tabari records..." — WITHOUT
  using the words "corpus," "retrieved chunks," or
  "the retrieved context."
- NEVER present general knowledge as if it came from
  a cited primary source.
- NEVER invent facts, dates, quotes, or sources.
- Present BOTH majority and minority scholarly
  positions where they exist.

FORMAT:
- Open with a direct 1–2 sentence lead.
- Use **bold inline headers** or ## H2 headers to
  break the answer into scannable sections.
- Paragraphs of 2–4 sentences, never longer.
- Use bullet lists for enumerations, > blockquotes
  for direct source quotations longer than a clause.
- Bold key names and turning-point phrases.

End with "And Allah knows best (wa Allahu a'lam)."
"""

    # ── Build messages list with prior conversation ───
    # Prior turns are sent verbatim (without the retrieval
    # context block — that's appended only to the current
    # turn's user prompt). This lets Claude resolve pronouns
    # and references while keeping retrieval tied to *this*
    # question's freshly-pulled chunks.
    claude_messages: list = []
    for m in history[-(CHAT_HISTORY_MAX_TURNS - 1):]:
        role = m.role
        content = m.content or ""
        if role in ("user", "assistant") and content:
            claude_messages.append({"role": role, "content": content})
    claude_messages.append({"role": "user", "content": user_prompt})

    # ── Stream response ───────────────────────────────
    def generate():
        # No meta event — there is no per-session limit anymore,
        # and the legacy client treats remaining<=0 as "show paywall".

        full_reply_chars: list = []
        try:
            with _claude.messages.stream(
                model      = tier_cfg["model"],
                max_tokens = tier_cfg["max_tokens"],
                system     = system,
                messages   = claude_messages,
            ) as stream:
                for text in stream.text_stream:
                    full_reply_chars.append(text)
                    escaped = text.replace(
                        "\n", "\\n"
                    ).replace('"', '\\"')
                    yield (
                        f'data: {{"type":"text",'
                        f'"text":"{escaped}"}}'
                        f"\n\n"
                    )

            # Conversation history is client-supplied via body.history,
            # so no server-side session persistence is needed.
            yield 'data: {"type":"done"}\n\n'

        except Exception as e:
            yield (
                f'data: {{"type":"error",'
                f'"text":"Generation error: '
                f'{str(e)[:100]}"}}\n\n'
            )

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )
