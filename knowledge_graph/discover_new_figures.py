"""
Islam Stories — Discover New Figures
Scans document chunks to find historical figures NOT yet in the
knowledge graph. Validates candidates, then inserts them into the
figures table.

Two-stage pipeline:
  Stage 1 (Haiku):  Extract named figures from chunk batches,
                     filter against known names → candidate list.
  Stage 2 (Sonnet): Validate top candidates, assign tier/era/metadata,
                     insert into DB + run backfill for chunk linking.

Run:  python -m rag.knowledge.discover_new_figures [--limit 5000] [--min-mentions 3] [--dry-run] [--concurrency 5]
"""

import os
import sys
import json
import time
import re
import argparse
import psycopg2
import psycopg2.extras
import anthropic
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-haiku-4-5-20251001"  # switched from Sonnet to Haiku for cost savings

# Cost tracking
HAIKU_INPUT_PER_MTOK = 0.80
HAIKU_OUTPUT_PER_MTOK = 4.00
SONNET_INPUT_PER_MTOK = 3.00
SONNET_OUTPUT_PER_MTOK = 15.00
CACHE_READ_DISCOUNT = 0.1  # 90% discount


# ═══════════════════════════════════════════════════════════════════════
# STAGE 1: Extract figure names from chunks via Haiku
# ═══════════════════════════════════════════════════════════════════════

def load_known_names():
    """Load all known figure names + variants for filtering."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("SELECT id, name, name_variants FROM figures ORDER BY id")
    figures = cur.fetchall()
    conn.close()

    known = set()
    name_to_id = {}
    for fig in figures:
        n = fig["name"].lower().strip()
        known.add(n)
        name_to_id[n] = fig["id"]
        if fig["name_variants"]:
            for v in fig["name_variants"]:
                vl = v.lower().strip()
                known.add(vl)
                name_to_id[vl] = fig["id"]

    return known, name_to_id, figures


EXTRACTION_SYSTEM = """You are an expert in Islamic history. Your task: extract the names of ALL historical figures mentioned in text chunks.

Rules:
- Return ONLY personal names of real historical people (not places, tribes, dynasties, or abstract groups).
- Include both major and minor figures — commanders, scholars, poets, governors, wives, narrators.
- Use the most common English transliteration for each name.
- Include any honorifics or patronymics that help identify them (e.g., "Abu Sufyan ibn Harb" not just "Abu Sufyan").
- Do NOT include: Allah, God, angels, jinn, unnamed people ("a man", "his wife"), tribal names without a person.
- Do NOT include: modern authors, translators, or editors.
- Deduplicate: if the same person appears with different spellings, pick the fullest form.

Respond with a JSON array of strings. Example: ["Khalid ibn al-Walid", "Abu Sufyan ibn Harb", "Hind bint Utba"]
Return ONLY the JSON array."""


def build_extraction_prompt(chunks):
    parts = []
    for c in chunks:
        text = c["content"][:600]  # cap per chunk
        parts.append(f"--- CHUNK {c['id']} [{c.get('source', '')[:60]}] ---\n{text}")
    return "\n\n".join(parts)


def extract_figures_batch(client, chunks, max_retries=3):
    """Call Haiku to extract figure names from a batch of chunks."""
    user_prompt = build_extraction_prompt(chunks)

    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=HAIKU,
                max_tokens=2000,
                system=[{
                    "type": "text",
                    "text": EXTRACTION_SYSTEM,
                    "cache_control": {"type": "ephemeral"},
                }],
                messages=[{"role": "user", "content": user_prompt}],
            )
            break
        except Exception as e:
            if "rate_limit" in str(e).lower() and attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1) * 5)
                continue
            raise

    text = response.content[0].text.strip()
    in_tok = response.usage.input_tokens
    out_tok = response.usage.output_tokens
    cache_read = getattr(response.usage, "cache_read_input_tokens", 0) or 0

    # Parse JSON
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    try:
        names = json.loads(text)
        if not isinstance(names, list):
            names = []
    except json.JSONDecodeError:
        names = []

    return names, in_tok, out_tok, cache_read


def is_known(name, known_set):
    """Check if a name matches any known figure (fuzzy)."""
    nl = name.lower().strip()
    if nl in known_set:
        return True
    # Check if any known name is a substring or vice versa
    for k in known_set:
        if len(k) > 4 and (k in nl or nl in k):
            return True
    return False


# Noise filter — common false positives
NOISE_NAMES = {
    "allah", "god", "muhammad", "prophet", "prophet muhammad",
    "messenger of allah", "the prophet", "his messenger",
    "iblis", "satan", "gabriel", "jibril", "adam", "eve",
    "moses", "musa", "jesus", "isa", "abraham", "ibrahim",
    "noah", "nuh", "solomon", "sulayman", "david", "dawud",
    "joseph", "yusuf", "mary", "maryam", "aaron", "harun",
    "the narrator", "a man", "a woman", "his wife", "his son",
}


def normalize_name(name):
    """Light normalization for deduplication."""
    # Remove common prefixes/suffixes that vary
    n = name.strip()
    # Remove parenthetical notes
    n = re.sub(r'\s*\(.*?\)\s*', ' ', n).strip()
    return n


# ═══════════════════════════════════════════════════════════════════════
# STAGE 2: Validate and enrich candidates via Sonnet
# ═══════════════════════════════════════════════════════════════════════

VALIDATION_SYSTEM = """You are the lead researcher for Islam Stories, a cinematic Islamic civilizational universe.
You are validating newly discovered historical figures to add to the knowledge graph.

For each candidate, determine:
1. Is this a REAL, distinct historical person (not a duplicate of someone already known)?
2. What is their full name and common variants?
3. Sensitivity tier:
   - S: Prophet, Caliphs, Prophet's wives — never depicted
   - A: Major companions, Ahl al-Bayt, mothers of believers — depict with extreme care
   - B: Fully depictable historical figures — commanders, scholars, governors
   - C: Controversial or negative figures — document actions, acknowledge disputes
4. Era(s): rashidun, umayyad, abbasid, crusades, mongol, ottoman, south_asia, andalusia, africa, persia, resistance_colonial
5. What are they known for (1 sentence)?
6. Generation: sahabi, tabi_i, tabi_al_tabi_in, later
7. Birth/death dates (approximate CE)
8. Death circumstance: battle, plague, martyrdom, natural, executed, assassinated, unknown
9. A dramatic question their life poses

Respond with a JSON array. For each valid figure:
{
  "name": "Full Name",
  "variants": ["Variant1", "Variant2"],
  "tier": "B",
  "era": ["rashidun"],
  "known_for": "Brief description",
  "generation": "sahabi",
  "birth_death": "590-642 CE",
  "death_circumstance": "battle",
  "dramatic_question": "The question their life poses",
  "reject_reason": null
}

For figures to REJECT (duplicates, non-persons, uncertain identity), set:
  "reject_reason": "reason for rejection"

Return ONLY the JSON array."""


def validate_candidates(client, candidates, known_figures):
    """Use Sonnet to validate and enrich discovered candidates."""
    known_list = "\n".join(
        f"  - {f['name']}" + (f" ({', '.join(f['name_variants'])})" if f['name_variants'] else "")
        for f in known_figures
    )

    candidate_list = "\n".join(
        f"  {i+1}. {name} (mentioned {count}x across chunks)"
        for i, (name, count) in enumerate(candidates)
    )

    prompt = f"""ALREADY KNOWN FIGURES (do NOT re-add these or their aliases):
{known_list}

CANDIDATE FIGURES TO VALIDATE:
{candidate_list}

For each candidate:
- If it's a variant/alias of a known figure above → REJECT with reason "duplicate of [known name]"
- If it's not a real historical person → REJECT
- If identity is too uncertain → REJECT
- Otherwise → validate and provide full metadata

Be generous with inclusion — minor figures (hadith narrators, governors, battle participants) are valuable.
Reject ONLY clear duplicates or non-persons."""

    response = client.messages.create(
        model=SONNET,
        max_tokens=8000,
        system=[{
            "type": "text",
            "text": VALIDATION_SYSTEM,
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()
    in_tok = response.usage.input_tokens
    out_tok = response.usage.output_tokens

    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    try:
        results = json.loads(text)
    except json.JSONDecodeError:
        print(f"  WARN: Sonnet JSON parse failed")
        results = []

    return results, in_tok, out_tok


# ═══════════════════════════════════════════════════════════════════════
# STAGE 3: Insert validated figures into DB
# ═══════════════════════════════════════════════════════════════════════

VALID_ERAS = {
    "rashidun", "umayyad", "abbasid", "crusades", "mongol",
    "ottoman", "south_asia", "andalusia", "africa", "persia",
    "resistance_colonial", "central_asia", "east_africa",
    "persia_safavid", "scholars", "southeast_asia", "china",
}

VALID_GENERATIONS = {"sahabi", "tabi_i", "tabi_al_tabi_in", "later"}

VALID_DEATH = {
    "battle", "plague", "martyrdom", "natural",
    "executed", "assassinated", "unknown",
}


def insert_figures(figures, dry_run=False):
    """Insert validated figures into the DB."""
    if not figures:
        return 0

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    inserted = 0

    for fig in figures:
        if fig.get("reject_reason"):
            continue

        name = fig.get("name", "").strip()
        if not name:
            continue

        variants = fig.get("variants") or []
        tier = fig.get("tier", "B")
        if tier not in ("S", "A", "B", "C"):
            tier = "B"

        eras = fig.get("era") or []
        eras = [e for e in eras if e in VALID_ERAS]

        generation = fig.get("generation")
        if generation not in VALID_GENERATIONS:
            generation = "later"

        death = fig.get("death_circumstance")
        if death not in VALID_DEATH:
            death = "unknown"

        birth_death = fig.get("birth_death", "unknown")
        known_for = fig.get("known_for", "")
        dramatic_q = fig.get("dramatic_question", "")

        if dry_run:
            print(f"    [DRY] Would insert: {name} (Tier {tier}, {', '.join(eras)})")
            inserted += 1
            continue

        try:
            cur.execute("""
                INSERT INTO figures (
                    name, name_variants, sensitivity_tier, era, series,
                    birth_death, dramatic_question, generation,
                    known_for, death_circumstance
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
            """, (
                name, variants, tier, eras, [],
                birth_death, dramatic_q, generation,
                known_for, death,
            ))
            result = cur.fetchone()
            if result:
                inserted += 1
        except Exception as e:
            print(f"    ERROR inserting {name}: {e}")
            conn.rollback()
            continue

    if not dry_run:
        conn.commit()

    cur.close()
    conn.close()
    return inserted


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def run_discovery(limit=5000, min_mentions=3, concurrency=5, dry_run=False,
                  batch_size=15, validation_batch=40):
    print("=" * 60)
    print("DISCOVER NEW FIGURES")
    print(f"Haiku: {HAIKU} | Sonnet: {SONNET}")
    print(f"Limit: {limit} chunks | Min mentions: {min_mentions}")
    print(f"Batch: {batch_size} | Concurrency: {concurrency}")
    if dry_run:
        print("DRY RUN — no DB writes")
    print("=" * 60)

    # Load known figures
    print("\nLoading known figures...")
    known_set, name_to_id, known_figures = load_known_names()
    print(f"  {len(known_figures)} figures, {len(known_set)} known names/variants")

    # Fetch chunks to scan (prioritize classified chunks with content)
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    cur.execute("""
        SELECT d.id, d.content, d.source, d.era
        FROM documents d
        JOIN chunk_metadata cm ON cm.chunk_id = d.id
        WHERE cm.noise_flag = false
          AND d.content IS NOT NULL
          AND LENGTH(d.content) > 100
        ORDER BY random()
        LIMIT %s
    """, (limit,))
    chunks = [dict(r) for r in cur.fetchall()]
    conn.close()

    print(f"  Chunks to scan: {len(chunks)}")

    if not chunks:
        print("No chunks to process!")
        return

    # ── Stage 1: Extract names via Haiku ──────────────────────────
    print(f"\n{'─' * 60}")
    print("STAGE 1: Extract figure names (Haiku)")
    print(f"{'─' * 60}")

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    # Create batches
    batches = []
    for i in range(0, len(chunks), batch_size):
        batches.append(chunks[i:i + batch_size])

    all_names = []
    name_sources = defaultdict(set)  # name -> set of source eras
    total_in = 0
    total_out = 0
    total_cache = 0
    errors = 0
    start_time = time.time()

    def process_batch(batch_idx, batch):
        return batch_idx, extract_figures_batch(client, batch)

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {}
        queue = list(enumerate(batches))
        active = 0

        while queue and active < concurrency:
            idx, batch = queue.pop(0)
            f = executor.submit(process_batch, idx, batch)
            futures[f] = idx
            active += 1

        while futures:
            for f in as_completed(futures):
                idx = futures.pop(f)
                active -= 1

                try:
                    _, (names, in_tok, out_tok, cache_read) = f.result()
                    total_in += in_tok
                    total_out += out_tok
                    total_cache += cache_read

                    for name in names:
                        norm = normalize_name(name)
                        if not norm or len(norm) < 3:
                            continue
                        if norm.lower() in NOISE_NAMES:
                            continue
                        all_names.append(norm)

                        # Track which era this name appears in
                        for c in batches[idx]:
                            if c.get("era"):
                                name_sources[norm.lower()].add(c["era"])

                except Exception as e:
                    print(f"  ERROR batch {idx}: {e}")
                    errors += 1

                done = idx + 1
                if done % 20 == 0 or done == len(batches):
                    elapsed = time.time() - start_time
                    cost = (
                        (total_in - total_cache) * HAIKU_INPUT_PER_MTOK / 1_000_000 +
                        total_cache * HAIKU_INPUT_PER_MTOK * CACHE_READ_DISCOUNT / 1_000_000 +
                        total_out * HAIKU_OUTPUT_PER_MTOK / 1_000_000
                    )
                    print(
                        f"  [{done}/{len(batches)}] "
                        f"names={len(all_names)} errors={errors} "
                        f"cost=${cost:.2f} {elapsed:.0f}s"
                    )

                while queue and active < concurrency:
                    next_idx, next_batch = queue.pop(0)
                    nf = executor.submit(process_batch, next_idx, next_batch)
                    futures[nf] = next_idx
                    active += 1

                break  # restart as_completed

    # ── Aggregate and filter ──────────────────────────────────────
    print(f"\n{'─' * 60}")
    print("FILTERING")
    print(f"{'─' * 60}")

    # Count mentions (case-insensitive dedup)
    name_counter = Counter()
    canonical = {}  # lowercase -> best capitalized form
    for name in all_names:
        nl = name.lower().strip()
        name_counter[nl] += 1
        # Keep the longest capitalized form
        if nl not in canonical or len(name) > len(canonical[nl]):
            canonical[nl] = name

    total_unique = len(name_counter)
    print(f"  Total unique names extracted: {total_unique}")

    # Filter out known figures
    unknown = {}
    for nl, count in name_counter.items():
        if not is_known(canonical[nl], known_set):
            unknown[nl] = count

    print(f"  After removing known figures: {len(unknown)}")

    # Filter by minimum mentions
    candidates = {nl: c for nl, c in unknown.items() if c >= min_mentions}
    print(f"  After min_mentions >= {min_mentions}: {len(candidates)}")

    if not candidates:
        print("\nNo new candidates found!")
        return

    # Sort by mention count
    sorted_candidates = sorted(candidates.items(), key=lambda x: -x[1])

    print(f"\n  Top 20 candidates:")
    for nl, count in sorted_candidates[:20]:
        eras = ", ".join(sorted(name_sources.get(nl, set())))
        print(f"    {canonical[nl]:45s} ({count}x) [{eras}]")

    # ── Stage 2: Validate via Sonnet ──────────────────────────────
    print(f"\n{'─' * 60}")
    print("STAGE 2: Validate candidates (Sonnet)")
    print(f"{'─' * 60}")

    # Process in validation batches
    candidate_pairs = [(canonical[nl], count) for nl, count in sorted_candidates]
    validated = []
    rejected = 0
    sonnet_in = 0
    sonnet_out = 0

    for i in range(0, len(candidate_pairs), validation_batch):
        batch = candidate_pairs[i:i + validation_batch]
        batch_num = i // validation_batch + 1
        total_batches = (len(candidate_pairs) + validation_batch - 1) // validation_batch
        print(f"\n  Validation batch {batch_num}/{total_batches} ({len(batch)} candidates)...")

        try:
            results, in_tok, out_tok = validate_candidates(client, batch, known_figures)
            sonnet_in += in_tok
            sonnet_out += out_tok

            for fig in results:
                if fig.get("reject_reason"):
                    rejected += 1
                else:
                    validated.append(fig)

            print(f"    Validated: {sum(1 for r in results if not r.get('reject_reason'))}, "
                  f"Rejected: {sum(1 for r in results if r.get('reject_reason'))}")

        except Exception as e:
            print(f"    ERROR: {e}")
            if "rate_limit" in str(e).lower():
                time.sleep(30)
                try:
                    results, in_tok, out_tok = validate_candidates(client, batch, known_figures)
                    sonnet_in += in_tok
                    sonnet_out += out_tok
                    for fig in results:
                        if fig.get("reject_reason"):
                            rejected += 1
                        else:
                            validated.append(fig)
                except Exception as e2:
                    print(f"    RETRY ERROR: {e2}")

    print(f"\n  Total validated: {len(validated)}")
    print(f"  Total rejected: {rejected}")

    # ── Stage 3: Insert into DB ──────────────────────────────────
    print(f"\n{'─' * 60}")
    print("STAGE 3: Insert new figures")
    print(f"{'─' * 60}")

    inserted = insert_figures(validated, dry_run=dry_run)

    # ── Final report ──────────────────────────────────────────────
    elapsed = time.time() - start_time
    haiku_cost = (
        (total_in - total_cache) * HAIKU_INPUT_PER_MTOK / 1_000_000 +
        total_cache * HAIKU_INPUT_PER_MTOK * CACHE_READ_DISCOUNT / 1_000_000 +
        total_out * HAIKU_OUTPUT_PER_MTOK / 1_000_000
    )
    sonnet_cost = (
        sonnet_in * SONNET_INPUT_PER_MTOK / 1_000_000 +
        sonnet_out * SONNET_OUTPUT_PER_MTOK / 1_000_000
    )

    print(f"\n{'=' * 60}")
    print("DISCOVERY COMPLETE")
    print(f"  Chunks scanned:      {len(chunks)}")
    print(f"  Unique names found:  {total_unique}")
    print(f"  Unknown candidates:  {len(unknown)}")
    print(f"  Above threshold:     {len(candidates)}")
    print(f"  Validated:           {len(validated)}")
    print(f"  Rejected:            {rejected}")
    print(f"  Inserted into DB:    {inserted}")
    print(f"  Haiku cost:          ${haiku_cost:.2f}")
    print(f"  Sonnet cost:         ${sonnet_cost:.2f}")
    print(f"  Total cost:          ${haiku_cost + sonnet_cost:.2f}")
    print(f"  Time:                {elapsed/60:.1f} minutes")
    print("=" * 60)

    if validated:
        print("\nNewly added figures:")
        for fig in validated:
            if not fig.get("reject_reason"):
                print(f"  + {fig['name']} (Tier {fig.get('tier', '?')}, {', '.join(fig.get('era', []))})")
                print(f"    {fig.get('known_for', '')}")

    return validated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discover new historical figures from document chunks")
    parser.add_argument("--limit", type=int, default=5000, help="Max chunks to scan")
    parser.add_argument("--min-mentions", type=int, default=3, help="Min mentions to consider a candidate")
    parser.add_argument("--concurrency", type=int, default=5, help="Parallel Haiku calls")
    parser.add_argument("--batch-size", type=int, default=15, help="Chunks per Haiku call")
    parser.add_argument("--validation-batch", type=int, default=40, help="Candidates per Sonnet call")
    parser.add_argument("--dry-run", action="store_true", help="Report without writing to DB")
    args = parser.parse_args()

    run_discovery(
        limit=args.limit,
        min_mentions=args.min_mentions,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        validation_batch=args.validation_batch,
        dry_run=args.dry_run,
    )
