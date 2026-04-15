"""
Islam Stories — Chunk Classifier
Classifies every chunk in `documents` → `chunk_metadata` using Claude Haiku.

Links each chunk to:
  - figure_ids (which figures are mentioned)
  - event_id (which event, if any)
  - account_type (eyewitness/transmitted/later_compilation/commentary)
  - chain_strength (sahih/hasan/daif/unknown/scholarly)
  - conflict_flag / conflict_note
  - noise_flag (irrelevant content)

Run:  python -m rag.knowledge.classify_chunks [--batch-size 10] [--concurrency 5] [--limit 100] [--dry-run]
"""

import os
import sys
import json
import time
import argparse
import psycopg2
import psycopg2.extras
import anthropic
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
MODEL = "claude-haiku-4-5-20251001"

# Cost tracking (Haiku 4.5 pricing)
INPUT_COST_PER_MTOK = 0.80
CACHE_READ_COST_PER_MTOK = 0.08  # 90% discount on cached input
CACHE_WRITE_COST_PER_MTOK = 1.00
OUTPUT_COST_PER_MTOK = 4.00


def load_reference_data():
    """Load figures and events for the classification prompt."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT id, name, name_variants FROM figures ORDER BY id")
    figures = cur.fetchall()

    cur.execute("SELECT id, name, name_variants FROM events ORDER BY id")
    events = cur.fetchall()

    conn.close()

    # Build compact reference strings
    fig_lines = []
    for f in figures:
        variants = ", ".join(f["name_variants"]) if f["name_variants"] else ""
        line = f"  {f['id']}: {f['name']}"
        if variants:
            line += f" ({variants})"
        fig_lines.append(line)

    evt_lines = []
    for e in events:
        variants = ", ".join(e["name_variants"]) if e["name_variants"] else ""
        line = f"  {e['id']}: {e['name']}"
        if variants:
            line += f" ({variants})"
        evt_lines.append(line)

    return "\n".join(fig_lines), "\n".join(evt_lines), figures, events


def build_system_prompt(fig_ref: str, evt_ref: str) -> str:
    return f"""You classify Islamic historical text chunks. For each chunk, identify:

1. **figure_ids**: List of figure IDs mentioned or discussed. Use IDs from the reference list. Empty list [] if none.
2. **event_id**: Single event ID if the chunk describes a specific event. null if none.
3. **account_type**: One of: eyewitness, transmitted, later_compilation, commentary
   - eyewitness: First-person or direct witness account
   - transmitted: Hadith-style chain of narration (isnad)
   - later_compilation: Later historian compiling accounts
   - commentary: Scholarly analysis or tafsir
4. **chain_strength**: One of: sahih, hasan, daif, unknown, scholarly
   - sahih: Authentic hadith collection (Bukhari, Muslim) or well-attested historical account
   - hasan: Good/acceptable chain
   - daif: Weak chain or single narrator
   - unknown: Chain not determinable
   - scholarly: Modern scholarly analysis
5. **conflict_flag**: true if this chunk contradicts or challenges another known account
6. **conflict_note**: Brief note if conflict_flag is true, else null
7. **noise_flag**: true if chunk has no historical narrative value (e.g., pure index, table of contents, blank filler)

FIGURES REFERENCE:
{fig_ref}

EVENTS REFERENCE:
{evt_ref}

Respond with a JSON array, one object per chunk. Use short keys: id (chunk_id), f (figure_ids array), e (event_id or null), t (account_type first letter: e/t/l/c), s (chain_strength first letter: s/h/d/u/x for scholarly), cf (conflict_flag 0/1), cn (conflict_note or null), n (noise_flag 0/1).

Example: [{{"id":5,"f":[15,3],"e":9,"t":"l","s":"u","cf":0,"cn":null,"n":0}}]

Return ONLY the JSON array."""


def build_batch_prompt(chunks: list[dict]) -> str:
    parts = []
    for c in chunks:
        source_hint = c.get("source", "unknown")
        era_hint = c.get("era", "unknown")
        text = c["content"][:500]  # cap to control token count
        parts.append(
            f"--- CHUNK {c['id']} [source: {source_hint}, era: {era_hint}] ---\n{text}"
        )
    return "\n\n".join(parts)


def classify_batch(client, system_prompt: str, chunks: list[dict], max_retries=3) -> tuple[list[dict], int, int]:
    """Classify a batch of chunks. Returns (results, input_tokens, output_tokens).
    Retries on rate limit errors with exponential backoff."""
    user_prompt = build_batch_prompt(chunks)

    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4000,
                system=[{
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_prompt}],
            )
            break
        except Exception as e:
            if "rate_limit" in str(e) and attempt < max_retries - 1:
                wait = 2 ** (attempt + 1) * 5  # 10s, 20s, 40s
                time.sleep(wait)
                continue
            raise

    text = response.content[0].text.strip()
    input_tok = response.usage.input_tokens
    output_tok = response.usage.output_tokens
    cache_read = getattr(response.usage, "cache_read_input_tokens", 0) or 0
    cache_create = getattr(response.usage, "cache_creation_input_tokens", 0) or 0

    # Parse JSON — handle markdown code blocks
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    try:
        results = json.loads(text)
    except json.JSONDecodeError:
        # Try to salvage partial JSON
        print(f"  WARN: JSON parse failed for batch starting at chunk {chunks[0]['id']}")
        results = []

    return results, input_tok, output_tok, cache_read, cache_create


def get_unclassified_chunk_ids(cur) -> list[int]:
    """Get document IDs not yet in chunk_metadata."""
    cur.execute("""
        SELECT d.id FROM documents d
        LEFT JOIN chunk_metadata cm ON cm.chunk_id = d.id
        WHERE cm.id IS NULL
        ORDER BY d.id
    """)
    return [r["id"] for r in cur.fetchall()]


def fetch_chunks(cur, chunk_ids: list[int]) -> list[dict]:
    """Fetch chunk content for given IDs."""
    cur.execute("""
        SELECT id, content, source, era FROM documents
        WHERE id = ANY(%s)
        ORDER BY id
    """, (chunk_ids,))
    return [dict(r) for r in cur.fetchall()]


ACCOUNT_TYPE_MAP = {"e": "eyewitness", "t": "transmitted", "l": "later_compilation", "c": "commentary"}
CHAIN_MAP = {"s": "sahih", "h": "hasan", "d": "daif", "u": "unknown", "x": "scholarly"}


def insert_results(cur, results: list[dict]):
    """Insert classification results into chunk_metadata. Handles both compact and full keys."""
    for r in results:
        # Support compact keys (id/f/e/t/s/cf/cn/n) and full keys
        chunk_id = r.get("id") or r.get("chunk_id")
        if not chunk_id:
            continue

        figure_ids = r.get("f") or r.get("figure_ids") or []
        event_id = r.get("e") or r.get("event_id")
        account_raw = r.get("t") or r.get("account_type") or "u"
        chain_raw = r.get("s") or r.get("chain_strength") or "u"

        account_type = ACCOUNT_TYPE_MAP.get(account_raw, account_raw)
        chain_strength = CHAIN_MAP.get(chain_raw, chain_raw)

        conflict_flag = bool(r.get("cf") or r.get("conflict_flag") or False)
        conflict_note = r.get("cn") or r.get("conflict_note")
        noise_flag = bool(r.get("n") or r.get("noise_flag") or False)

        cur.execute("""
            INSERT INTO chunk_metadata (chunk_id, figure_ids, event_id, account_type,
                                        chain_strength, conflict_flag, conflict_note, noise_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            chunk_id, figure_ids, event_id, account_type,
            chain_strength, conflict_flag, conflict_note, noise_flag,
        ))


def run_classifier(batch_size=10, concurrency=5, limit=None, dry_run=False):
    print("=" * 60)
    print("CHUNK CLASSIFIER")
    print(f"Model: {MODEL} | Batch: {batch_size} | Concurrency: {concurrency}")
    if limit:
        print(f"Limit: {limit} chunks")
    if dry_run:
        print("DRY RUN — no DB writes")
    print("=" * 60)

    # Load reference data
    print("\nLoading reference data...")
    fig_ref, evt_ref, _, _ = load_reference_data()
    system_prompt = build_system_prompt(fig_ref, evt_ref)
    sys_tokens_est = len(system_prompt) // 3  # rough estimate
    print(f"  System prompt: ~{sys_tokens_est} tokens")

    # Get unclassified chunks
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    unclassified_ids = get_unclassified_chunk_ids(cur)
    total = len(unclassified_ids)
    print(f"  Unclassified chunks: {total}")

    if limit:
        unclassified_ids = unclassified_ids[:limit]
        print(f"  Processing: {len(unclassified_ids)}")

    if not unclassified_ids:
        print("\nAll chunks already classified!")
        conn.close()
        return

    # Create batches
    batches = []
    for i in range(0, len(unclassified_ids), batch_size):
        batch_ids = unclassified_ids[i:i + batch_size]
        batches.append(batch_ids)

    print(f"  Batches: {len(batches)}")

    # Estimated cost
    avg_input = sys_tokens_est + batch_size * 300  # ~300 tokens per chunk
    avg_output = batch_size * 50  # ~50 tokens per result
    est_cost = (
        len(batches) * avg_input * INPUT_COST_PER_MTOK / 1_000_000 +
        len(batches) * avg_output * OUTPUT_COST_PER_MTOK / 1_000_000
    )
    print(f"  Estimated cost: ${est_cost:.2f}")

    if dry_run:
        print("\nDry run complete.")
        conn.close()
        return

    # Initialize API client
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    # Process batches
    total_input_tok = 0
    total_output_tok = 0
    total_cache_read = 0
    total_cache_create = 0
    total_classified = 0
    total_errors = 0
    start_time = time.time()

    # Use a write connection
    write_conn = psycopg2.connect(DB_URL)
    write_conn.autocommit = True
    write_cur = write_conn.cursor()

    def process_batch(batch_idx, batch_ids):
        read_conn = psycopg2.connect(DB_URL)
        read_cur = read_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        chunks = fetch_chunks(read_cur, batch_ids)
        read_conn.close()

        results, in_tok, out_tok, c_read, c_create = classify_batch(client, system_prompt, chunks)
        return batch_idx, results, in_tok, out_tok, c_read, c_create

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {}
        batch_queue = list(enumerate(batches))
        active = 0

        # Submit initial batch of work
        while batch_queue and active < concurrency:
            batch_idx, batch_ids = batch_queue.pop(0)
            f = executor.submit(process_batch, batch_idx, batch_ids)
            futures[f] = batch_idx
            active += 1

        while futures:
            for f in as_completed(futures):
                batch_idx = futures.pop(f)
                active -= 1

                try:
                    _, results, in_tok, out_tok, c_read, c_create = f.result()
                    total_input_tok += in_tok
                    total_output_tok += out_tok
                    total_cache_read += c_read
                    total_cache_create += c_create

                    if results:
                        insert_results(write_cur, results)
                        total_classified += len(results)
                    else:
                        total_errors += 1

                except Exception as e:
                    print(f"\n  ERROR batch {batch_idx}: {e}")
                    total_errors += 1

                # Progress
                done = batch_idx + 1
                elapsed = time.time() - start_time
                uncached_input = total_input_tok - total_cache_read - total_cache_create
                cost = (
                    uncached_input * INPUT_COST_PER_MTOK / 1_000_000 +
                    total_cache_read * CACHE_READ_COST_PER_MTOK / 1_000_000 +
                    total_cache_create * CACHE_WRITE_COST_PER_MTOK / 1_000_000 +
                    total_output_tok * OUTPUT_COST_PER_MTOK / 1_000_000
                )
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(batches) - done) / rate if rate > 0 else 0

                if done % 50 == 0 or done == len(batches):
                    print(
                        f"  [{done}/{len(batches)}] "
                        f"classified={total_classified} errors={total_errors} "
                        f"cost=${cost:.2f} "
                        f"rate={rate:.1f} batch/s "
                        f"ETA={eta/60:.1f}min"
                    )

                # Submit more work
                while batch_queue and active < concurrency:
                    next_idx, next_ids = batch_queue.pop(0)
                    nf = executor.submit(process_batch, next_idx, next_ids)
                    futures[nf] = next_idx
                    active += 1

                break  # restart as_completed after processing

    # Final stats
    elapsed = time.time() - start_time
    uncached_input = total_input_tok - total_cache_read - total_cache_create
    final_cost = (
        uncached_input * INPUT_COST_PER_MTOK / 1_000_000 +
        total_cache_read * CACHE_READ_COST_PER_MTOK / 1_000_000 +
        total_cache_create * CACHE_WRITE_COST_PER_MTOK / 1_000_000 +
        total_output_tok * OUTPUT_COST_PER_MTOK / 1_000_000
    )

    write_cur.close()
    write_conn.close()
    conn.close()

    print("\n" + "=" * 60)
    print("CLASSIFICATION COMPLETE")
    print(f"  Chunks classified: {total_classified}")
    print(f"  Errors: {total_errors}")
    print(f"  Input tokens:  {total_input_tok:,}")
    print(f"  Output tokens: {total_output_tok:,}")
    print(f"  Total cost:    ${final_cost:.2f}")
    print(f"  Time:          {elapsed/60:.1f} minutes")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Classify document chunks via Haiku")
    parser.add_argument("--batch-size", type=int, default=10, help="Chunks per API call")
    parser.add_argument("--concurrency", type=int, default=5, help="Parallel API calls")
    parser.add_argument("--limit", type=int, default=None, help="Max chunks to process")
    parser.add_argument("--dry-run", action="store_true", help="Estimate cost without running")
    args = parser.parse_args()

    run_classifier(
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        limit=args.limit,
        dry_run=args.dry_run,
    )
