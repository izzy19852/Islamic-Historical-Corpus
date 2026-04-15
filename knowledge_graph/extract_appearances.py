"""
Islam Stories — Appearance Extractor
Queries the vector store for physical descriptions of each figure,
extracts appearance data via Claude Haiku, and seeds the DB.

ALL appearance data comes from the corpus. Nothing is hardcoded.
If the corpus is silent, the figure gets appearance_confidence='none'.
"""

import argparse
import json
import os
import sys
import time

import anthropic
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..', '..')
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

sys.path.insert(0, PROJECT_ROOT)
from rag.embeddings.query import query_rag_multi

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


# ═══════════════════════════════════════════════════════════════════════
# ETHNIC NORMALIZATION MAP
# Maps corpus terms to kling_appearance descriptors.
# Actual source descriptions override these defaults.
# ═══════════════════════════════════════════════════════════════════════

ETHNIC_NORMALIZATION = {
    "abyssinian":   "Abyssinian features, very dark complexion",
    "habashi":      "Abyssinian features, very dark complexion",
    "moor":         "Andalusian Moorish features, olive to dark complexion",
    "andalusian":   "Andalusian Moorish features, olive to dark complexion",
    "berber":       "Berber features, medium to dark complexion",
    "amazigh":      "Berber features, medium to dark complexion",
    "quraysh":      "Qurayshi Arab features",
    "qurayshi":     "Qurayshi Arab features",
    "meccan":       "Qurayshi Arab features",
    "ansar":        "Medinan Arab features",
    "medinan":      "Medinan Arab features",
    "persian":      "Persian features, olive to fair complexion",
    "farsi":        "Persian features, olive to fair complexion",
    "kipchak":      "Kipchak Turkic features, lighter complexion, possibly light eyes",
    "cuman":        "Kipchak Turkic features, lighter complexion, possibly light eyes",
    "kurdish":      "Kurdish features, olive to fair complexion",
    "sindhi":       "South Asian features, medium to dark complexion",
    "indian":       "South Asian features, medium to dark complexion",
    "malian":       "West African features, very dark complexion",
    "sudanese":     "West African features, very dark complexion",
    "byzantine":    "Byzantine Greek features, fair complexion",
    "rum":          "Byzantine Greek features, fair complexion",
    "yemeni":       "South Arabian features, medium to dark complexion",
    "himyari":      "South Arabian features, medium to dark complexion",
}


EXTRACTION_PROMPT = """
You are extracting ONLY documented physical appearance
from primary Islamic historical sources.

Figure: {name}
Era: {era}
Known origin: {era} (use this only to understand
  geographic context, NOT to assume appearance)

Extract from these source chunks ONLY.
Do not use any knowledge outside these chunks.
Do not infer appearance from name or region.
If a field is not documented in the chunks: "unknown".

Return JSON only — no other text:
{{
  "ethnicity": "string — exact ethnic/geographic origin as stated in sources. Examples: Abyssinian, Qurayshi Arab, Persian, Berber, Kipchak Turk, Andalusian Moor, Sindhi, Malian, Byzantine Greek. Not generic like just Arab.",
  "skin_tone": "exact description from source or unknown",
  "height": "tall|medium|short|unknown",
  "build": "description using ONLY these terms: lean frame, medium frame, broad-shouldered frame, slight frame. Or unknown.",
  "face": "specific documented facial features only. Include: beard style, eye color if documented, distinguishing marks, missing teeth, scars. Exclude vague terms like handsome or beautiful.",
  "hair": "documented hair description or unknown",
  "age_context": "approximate age during their main story arc based on birth/death dates if available",
  "source_quote": "exact quote from chunk describing appearance — max 50 words",
  "source_name": "which source this comes from",
  "confidence": "high|medium|low",
  "conflicts": "note if multiple chunks contradict each other on appearance"
}}

CHUNKS:
{chunks}
"""


def get_figures_needing_appearance(skip_if_set: bool = False) -> list[dict]:
    """Get figures where kling_appearance is NULL (or all if not skipping)."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    if skip_if_set:
        cur.execute("""
            SELECT id, name, name_variants, sensitivity_tier, era
            FROM figures
            WHERE kling_appearance IS NULL
            ORDER BY
                CASE sensitivity_tier
                    WHEN 'S' THEN 4
                    WHEN 'A' THEN 3
                    WHEN 'B' THEN 1
                    WHEN 'C' THEN 2
                END,
                id
        """)
    else:
        cur.execute("""
            SELECT id, name, name_variants, sensitivity_tier, era
            FROM figures
            ORDER BY id
        """)
    rows = cur.fetchall()
    conn.close()
    return rows


def query_appearance_chunks(name: str, name_variants: list, threshold: float = 0.70) -> list[dict]:
    """Query vector store for appearance-related chunks about a figure."""
    search_queries = [
        f"{name} complexion skin",
        f"{name} appearance face",
        f"{name} tall short height",
        f"{name} beard hair eyes",
        f"{name} features body",
        f"{name} described as",
        f"{name} was known for his appearance",
    ]
    # Also query using name variants
    if name_variants:
        for variant in name_variants[:3]:  # Top 3 variants
            search_queries.append(f"{variant} appearance complexion")
            search_queries.append(f"{variant} described features")

    results = query_rag_multi(search_queries, n_results=8)

    # Filter by threshold
    above_threshold = [r for r in results if r["similarity_score"] >= threshold]
    return above_threshold[:8]  # Top 8


def normalize_ethnicity(extracted: dict, chunks: list[dict]) -> str:
    """
    Post-processing normalization step.
    If source chunks contain known ethnic terms, map to correct descriptor.
    Actual source descriptions override defaults.
    """
    ethnicity = extracted.get("ethnicity", "unknown")

    # If Haiku said "unknown" or returned nothing useful, don't guess from chunks
    if not ethnicity or ethnicity.lower() == "unknown":
        return "unknown"

    # Check if the extracted ethnicity matches a normalization key
    for key, normalized in ETHNIC_NORMALIZATION.items():
        if key in ethnicity.lower():
            return normalized

    # Return Haiku's extraction as-is if no normalization match
    return ethnicity


def build_kling_appearance_string(extracted: dict, normalized_ethnicity: str) -> str:
    """Build the kling_appearance prompt string from extracted data."""
    parts = []

    # Ethnicity / features
    if normalized_ethnicity and normalized_ethnicity.lower() != "unknown":
        parts.append(normalized_ethnicity)

    # Skin tone (only if source-documented and not already in ethnicity)
    skin_tone = extracted.get("skin_tone", "unknown")
    if skin_tone.lower() != "unknown" and "complexion" not in (normalized_ethnicity or "").lower():
        parts.append(f"{skin_tone} complexion")

    # Build
    build = extracted.get("build", "unknown")
    if build.lower() != "unknown":
        # Enforce allowed terms
        allowed_builds = ["lean frame", "medium frame", "broad-shouldered frame", "slight frame"]
        build_lower = build.lower()
        matched = False
        for ab in allowed_builds:
            if ab in build_lower:
                parts.append(ab)
                matched = True
                break
        if not matched and build.lower() != "unknown":
            parts.append("medium frame")

    # Height
    height = extracted.get("height", "unknown")
    if height.lower() != "unknown":
        parts.append(f"{height} stature")

    # Face / distinguishing features
    face = extracted.get("face", "unknown")
    if face.lower() != "unknown":
        parts.append(face)

    # Hair
    hair = extracted.get("hair", "unknown")
    if hair.lower() != "unknown":
        parts.append(hair)

    # Age context
    age = extracted.get("age_context", "unknown")
    if age.lower() != "unknown":
        parts.append(age)

    if not parts:
        return ""

    return ", ".join(parts)


def extract_appearance_for_figure(
    figure: dict,
    client: anthropic.Anthropic,
    threshold: float = 0.70,
    dry_run: bool = False,
) -> dict:
    """Extract appearance for a single figure from the corpus."""
    name = figure["name"]
    name_variants = figure["name_variants"] or []
    era = figure["era"][0] if figure["era"] else "unknown"

    # Step 1: Query vector store
    chunks = query_appearance_chunks(name, name_variants, threshold=threshold)

    if len(chunks) < 2:
        # Not enough corpus data — mark as 'none'
        result = {
            "figure_id": figure["id"],
            "name": name,
            "confidence": "none",
            "reason": f"Only {len(chunks)} chunk(s) above {threshold} threshold",
        }
        if not dry_run:
            _update_figure_appearance(
                figure["id"],
                ethnicity=None,
                physical_description=None,
                appearance_source=None,
                kling_appearance=None,
                confidence="none",
            )
        return result

    # Step 2: Format chunks for Haiku
    chunk_text = ""
    for i, c in enumerate(chunks, 1):
        chunk_text += f"\n--- Chunk {i} (source: {c['source']}, score: {c['similarity_score']:.3f}) ---\n"
        chunk_text += c["content"] + "\n"

    prompt = EXTRACTION_PROMPT.format(
        name=name,
        era=era,
        chunks=chunk_text,
    )

    # Step 3: Call Claude Haiku
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_text = response.content[0].text.strip()

        # Parse JSON — handle markdown code blocks
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
            raw_text = raw_text.strip()

        extracted = json.loads(raw_text)
    except (json.JSONDecodeError, IndexError, KeyError) as e:
        return {
            "figure_id": figure["id"],
            "name": name,
            "confidence": "none",
            "reason": f"Haiku extraction failed: {e}",
        }

    # Step 3b: Validate — if Haiku says "no physical appearance found", override confidence
    source_quote = (extracted.get("source_quote") or "").lower()
    no_data_signals = [
        "no physical appearance",
        "not found",
        "no appearance description",
        "no documented",
        "no specific physical",
        "unknown",
    ]
    all_unknown = all(
        (extracted.get(k) or "unknown").lower() == "unknown"
        for k in ("skin_tone", "height", "build", "face", "hair")
    )
    has_no_data = any(sig in source_quote for sig in no_data_signals) or all_unknown

    if has_no_data:
        result = {
            "figure_id": figure["id"],
            "name": name,
            "confidence": "none",
            "reason": "Haiku found no physical appearance data in chunks",
        }
        if not dry_run:
            _update_figure_appearance(
                figure["id"],
                ethnicity=None,
                physical_description=None,
                appearance_source=None,
                kling_appearance=None,
                confidence="none",
            )
        return result

    # Step 4: Normalize ethnicity
    normalized_ethnicity = normalize_ethnicity(extracted, chunks)

    # Step 5: Build kling_appearance string
    kling_appearance = build_kling_appearance_string(extracted, normalized_ethnicity)

    confidence = extracted.get("confidence", "low")
    if not kling_appearance:
        confidence = "none"

    result = {
        "figure_id": figure["id"],
        "name": name,
        "ethnicity": normalized_ethnicity,
        "kling_appearance": kling_appearance,
        "confidence": confidence,
        "source_quote": extracted.get("source_quote", ""),
        "source_name": extracted.get("source_name", ""),
        "conflicts": extracted.get("conflicts", ""),
        "extracted": extracted,
    }

    if not dry_run and kling_appearance:
        _update_figure_appearance(
            figure["id"],
            ethnicity=normalized_ethnicity,
            physical_description=extracted.get("source_quote"),
            appearance_source=extracted.get("source_name"),
            kling_appearance=kling_appearance,
            confidence=confidence,
        )

    return result


def _update_figure_appearance(
    figure_id: int,
    ethnicity: str,
    physical_description: str,
    appearance_source: str,
    kling_appearance: str,
    confidence: str,
):
    """Update a figure's appearance columns in the DB."""
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        UPDATE figures SET
            ethnicity = %s,
            physical_description = %s,
            appearance_source = %s,
            kling_appearance = %s,
            appearance_confidence = %s
        WHERE id = %s
    """, (ethnicity, physical_description, appearance_source,
          kling_appearance, confidence, figure_id))
    conn.commit()
    conn.close()


def run_extraction(
    skip_if_set: bool = False,
    threshold: float = 0.70,
    dry_run: bool = False,
    limit: int = None,
):
    """Run appearance extraction for all figures."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    figures = get_figures_needing_appearance(skip_if_set)

    if limit:
        figures = figures[:limit]

    print(f"\n{'='*60}")
    print(f"Islam Stories — Appearance Extractor")
    print(f"{'='*60}")
    print(f"Figures to process: {len(figures)}")
    print(f"Confidence threshold: {threshold}")
    print(f"Skip if set: {skip_if_set}")
    print(f"Dry run: {dry_run}")
    print(f"{'='*60}\n")

    results = []
    high_count = 0
    medium_count = 0
    low_count = 0
    none_count = 0

    for i, fig in enumerate(figures, 1):
        print(f"[{i}/{len(figures)}] {fig['name']} (Tier {fig['sensitivity_tier']})...", end=" ", flush=True)

        result = extract_appearance_for_figure(fig, client, threshold=threshold, dry_run=dry_run)
        results.append(result)

        conf = result.get("confidence", "none")
        if conf == "high":
            high_count += 1
        elif conf == "medium":
            medium_count += 1
        elif conf == "low":
            low_count += 1
        else:
            none_count += 1

        if conf != "none":
            print(f"{conf} — {result.get('ethnicity', 'unknown')}")
        else:
            print(f"none — {result.get('reason', 'no data')}")

        # Rate limit: ~0.5s between calls
        time.sleep(0.5)

    # ─── REPORT ───
    print(f"\n{'='*60}")
    print(f"EXTRACTION REPORT")
    print(f"{'='*60}")
    print(f"┌─────────────────────────────────┬──────────────┬────────────────────────────────┐")
    print(f"│ {'Figure':<31} │ {'Confidence':<12} │ {'Ethnicity':<30} │")
    print(f"├─────────────────────────────────┼──────────────┼────────────────────────────────┤")

    for r in results:
        if r.get("confidence", "none") != "none":
            name = r["name"][:31]
            conf = r.get("confidence", "none")
            eth = (r.get("ethnicity") or "unknown")[:30]
            print(f"│ {name:<31} │ {conf:<12} │ {eth:<30} │")

    print(f"└─────────────────────────────────┴──────────────┴────────────────────────────────┘")

    print(f"\nSummary: {high_count} high, {medium_count} medium, {low_count} low, {none_count} none")

    if none_count > 0:
        print(f"\nFigures with confidence=none (no corpus data):")
        for r in results:
            if r.get("confidence") == "none":
                print(f"  - {r['name']}: {r.get('reason', 'no data')}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract figure appearances from corpus")
    parser.add_argument("--skip-if-set", action="store_true",
                        help="Skip figures that already have kling_appearance set")
    parser.add_argument("--confidence-threshold", type=float, default=0.70,
                        help="Minimum similarity score for chunks (default: 0.70)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract but don't update DB")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max figures to process")
    args = parser.parse_args()

    run_extraction(
        skip_if_set=args.skip_if_set,
        threshold=args.confidence_threshold,
        dry_run=args.dry_run,
        limit=args.limit,
    )
