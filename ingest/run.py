#!/usr/bin/env python3
"""
Islam Stories — Unified ingestion entry point.

Replaces 15+ separate ingest scripts with one command:

    # Ingest everything (skips already-ingested sources)
    python -m ingest.run

    # Ingest specific categories
    python -m ingest.run --category tabari classical persia

    # Ingest a single source by short_name
    python -m ingest.run --source ibn-kathir-prophets

    # Ingest hadith collections from API
    python -m ingest.run --hadith

    # Ingest Quran translations from API
    python -m ingest.run --quran

    # List all categories and source counts
    python -m ingest.run --list

    # Dry run — show what would be downloaded
    python -m ingest.run --dry-run
"""

import argparse
import os
import sys
import time
import requests
from pathlib import Path

from ingest.core import (
    get_db_connection, get_voyage_client, get_ingested_sources,
    already_ingested, extract_figures, embed_batch, insert_chunks,
    register_source, print_corpus_summary,
    clean_djvu_text, clean_pdf_text, strip_gutenberg, extract_pdf_text,
    chunk_text, SOURCES_DIR, VOYAGE_BATCH_SIZE,
)
from ingest.archive_resolver import (
    resolve_and_download, try_identifiers, resolve_text,
)
from ingest.sources import (
    get_all_sources, get_source_by_short_name, list_categories,
    HADITH_COLLECTIONS, FAWAZ_BASE, QURAN_TRANSLATIONS,
    ALL_CATEGORIES,
)


# ═══════════════════════════════════════════════════════════════════
# ARCHIVE.ORG SOURCE INGESTION
# ═══════════════════════════════════════════════════════════════════

def ingest_archive_source(src, cur, vo, ingested):
    """Download, clean, chunk, embed, insert a single Archive.org source."""
    source_name = src["source"]
    short_name = src["short_name"]
    language = src.get("language", "english")

    if short_name in ingested:
        print(f"  SKIP (already ingested): {short_name}")
        return 0

    if already_ingested(cur, source_name, min_chunks=10):
        print(f"  SKIP (in DB): {source_name[:55]}")
        return 0

    # Determine download directory
    category = src.get("category", "general")
    dest_dir = SOURCES_DIR / category
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Download the text
    text = None
    path = None

    if "url" in src:
        # Direct URL (e.g. Gutenberg)
        print(f"  Downloading from direct URL...")
        try:
            r = requests.get(src["url"], timeout=120,
                             headers={"User-Agent": "Mozilla/5.0 (research/academic)"})
            r.raise_for_status()
            text = r.text
            if src.get("format") == "gutenberg_txt":
                text = strip_gutenberg(text)
        except Exception as e:
            print(f"  Download failed: {e}")
            return 0

    elif "identifiers" in src:
        # Archive.org — try multiple identifiers
        identifiers = src["identifiers"]

        # If there's a filename_hint, try direct download first
        if "filename_hint" in src:
            from ingest.archive_resolver import DOWNLOAD_BASE, HEADERS
            from urllib.parse import quote
            ident = identifiers[0]
            hint = src["filename_hint"]
            direct_url = f"{DOWNLOAD_BASE}/{ident}/{quote(hint)}"
            local_path = dest_dir / f"{short_name}{os.path.splitext(hint)[1]}"

            if not local_path.exists() or local_path.stat().st_size == 0:
                try:
                    print(f"  Trying direct: {hint}")
                    r = requests.get(direct_url, timeout=180, stream=True, headers=HEADERS)
                    r.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in r.iter_content(8192):
                            f.write(chunk)
                    path = local_path
                except Exception:
                    print(f"  Direct download failed, trying resolver...")

        # Fallback: use resolver
        if not path or not path.exists() or path.stat().st_size == 0:
            prefer_text = src.get("format") != "pdf"
            path = try_identifiers(identifiers, dest_dir, prefer_text=prefer_text, label=short_name)

        if not path:
            print(f"  FAILED: no download succeeded for {short_name}")
            return 0

        # Read text
        if path.suffix.lower() == '.txt':
            text = path.read_text(encoding='utf-8', errors='replace')
        elif path.suffix.lower() == '.pdf':
            print(f"  Extracting text from PDF...")
            text = extract_pdf_text(path)
            if not text or len(text.split()) < 100:
                print(f"  PDF appears image-based ({len((text or '').split())} words)")
                return 0
        else:
            print(f"  Unsupported format: {path.suffix}")
            return 0

    elif "identifier" in src:
        # Single identifier (Tabari volumes)
        ident = src["identifier"]
        hint = src.get("filename_hint")
        prefer_text = src.get("format") != "pdf"
        path = resolve_and_download(ident, dest_dir, label=short_name, prefer_text=prefer_text)

        if not path:
            print(f"  FAILED: download failed for {short_name}")
            return 0

        if path.suffix.lower() == '.txt':
            text = path.read_text(encoding='utf-8', errors='replace')
        elif path.suffix.lower() == '.pdf':
            print(f"  Extracting text from PDF...")
            text = extract_pdf_text(path)
            if not text or len(text.split()) < 100:
                print(f"  PDF appears image-based ({len((text or '').split())} words)")
                return 0

    if not text or len(text.strip()) < 500:
        print(f"  SKIP — too little text ({len(text or '')} chars)")
        return 0

    # Clean text
    if language in ("arabic", "persian"):
        text = clean_djvu_text(text, multilingual=True)
    else:
        text = clean_djvu_text(text)

    if len(text) < 1000:
        print(f"  SKIP — too short after cleaning ({len(text)} chars)")
        return 0

    # Chunk
    chunks_raw = chunk_text(text)
    if not chunks_raw:
        print(f"  No chunks produced")
        return 0

    print(f"  {len(chunks_raw):,} chunks from {len(text):,} chars")

    # Build chunk dicts
    era = src.get("era")
    if isinstance(era, list):
        era = era[0] if era else None

    chunks = []
    for idx, c in enumerate(chunks_raw):
        chunks.append({
            "content": c,
            "source": source_name,
            "source_type": src.get("source_type", "primary_arabic"),
            "era": era,
            "figures": extract_figures(c),
            "chunk_index": idx,
        })

    # Pick embedding model
    model = "voyage-2" if language == "english" else "voyage-multilingual-2"
    batch_size = 64 if language != "english" else VOYAGE_BATCH_SIZE

    # Embed and insert
    inserted = insert_chunks(cur, vo, chunks, model=model, batch_size=batch_size, label=short_name)

    # Register source
    era_coverage = src.get("era")
    if isinstance(era_coverage, str):
        era_coverage = [era_coverage]
    register_source(
        cur, source_name, short_name, src.get("source_type", "primary_arabic"),
        language=language, translator=src.get("translator"),
        era_coverage=era_coverage, reliability=src.get("reliability"),
        chunk_count=inserted,
    )

    print(f"  {short_name}: {inserted:,} chunks inserted")
    return inserted


# ═══════════════════════════════════════════════════════════════════
# HADITH INGESTION (API)
# ═══════════════════════════════════════════════════════════════════

def ingest_hadith(cur, vo, ingested):
    """Ingest hadith collections from Fawazahmed0 CDN."""
    print(f"\n{'='*60}")
    print("HADITH INGESTION (Fawazahmed0 API)")
    print(f"{'='*60}")

    total = 0
    for collection_key, collection_name in HADITH_COLLECTIONS.items():
        if collection_key in ingested:
            print(f"  {collection_name}: already ingested")
            continue

        print(f"\n  Fetching: {collection_name}")
        url = f"{FAWAZ_BASE}/{collection_key}.min.json"

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            hadiths = resp.json().get("hadiths", [])
        except Exception as e:
            print(f"  FAILED: {e}")
            continue

        print(f"  {len(hadiths)} hadiths fetched")
        chunks = []
        for idx, h in enumerate(hadiths):
            text = h.get("text", "").strip()
            if not text or len(text) < 30:
                continue
            num = h.get("hadithnumber", idx + 1)
            content = f"[{collection_name}] Hadith {num}: {text}"
            chunks.append({
                "content": content,
                "source": collection_name,
                "source_type": "hadith",
                "era": "rashidun",
                "figures": extract_figures(content),
                "chunk_index": idx,
            })

        n = insert_chunks(cur, vo, chunks, label=collection_key)
        register_source(cur, collection_name, collection_key, "hadith",
                        reliability="sahih", chunk_count=n)
        ingested.add(collection_key)
        total += n
        print(f"  {collection_name}: {n} inserted")

    return total


# ═══════════════════════════════════════════════════════════════════
# QURAN INGESTION (API)
# ═══════════════════════════════════════════════════════════════════

def ingest_quran(cur, vo, ingested):
    """Ingest Quran translations from AlQuran.cloud API."""
    print(f"\n{'='*60}")
    print("QURAN TRANSLATIONS (AlQuran.cloud)")
    print(f"{'='*60}")

    total = 0
    for edition, source_name in QURAN_TRANSLATIONS.items():
        if edition in ingested:
            print(f"  {source_name}: already ingested")
            continue

        print(f"\n  Fetching: {source_name}")
        try:
            resp = requests.get(f"https://api.alquran.cloud/v1/quran/{edition}", timeout=60)
            resp.raise_for_status()
            surahs = resp.json().get("data", {}).get("surahs", [])
        except Exception as e:
            print(f"  FAILED: {e}")
            continue

        chunks = []
        cidx = 0
        for surah in surahs:
            ayahs = surah.get("ayahs", [])
            snum = surah.get("number", "?")
            for i in range(0, len(ayahs), 5):
                group = ayahs[i:i + 5]
                lines = [f"Quran {snum}:{a.get('numberInSurah', '?')} — {a.get('text', '').strip()}"
                         for a in group if a.get("text", "").strip()]
                if not lines:
                    continue
                content = "\n".join(lines)
                chunks.append({
                    "content": content,
                    "source": source_name,
                    "source_type": "quran",
                    "era": "rashidun",
                    "figures": extract_figures(content),
                    "chunk_index": cidx,
                })
                cidx += 1

        n = insert_chunks(cur, vo, chunks, label=edition)
        register_source(cur, source_name, edition, "quran",
                        reliability="sahih", chunk_count=n)
        ingested.add(edition)
        total += n
        print(f"  {source_name}: {n} inserted")

    return total


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Islam Stories — Unified Ingestion Pipeline")
    parser.add_argument("--category", nargs="+", help="Categories to ingest (tabari, classical, persia, etc)")
    parser.add_argument("--source", help="Ingest a single source by short_name")
    parser.add_argument("--hadith", action="store_true", help="Ingest hadith collections from API")
    parser.add_argument("--quran", action="store_true", help="Ingest Quran translations from API")
    parser.add_argument("--list", action="store_true", help="List all categories")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    args = parser.parse_args()

    if args.list:
        print("Available categories:")
        list_categories()
        print(f"\nAPI sources:")
        print(f"  hadith              {len(HADITH_COLLECTIONS):>3} collections")
        print(f"  quran               {len(QURAN_TRANSLATIONS):>3} translations")
        return

    # Determine what to ingest
    if args.source:
        src = get_source_by_short_name(args.source)
        if not src:
            print(f"Unknown source: {args.source}")
            sys.exit(1)
        sources = [src]
    elif args.category:
        sources = get_all_sources(categories=args.category)
    elif not args.hadith and not args.quran:
        # Default: ingest all archive sources + hadith + quran
        sources = get_all_sources()
        args.hadith = True
        args.quran = True
    else:
        sources = []

    if args.dry_run:
        print(f"\nDRY RUN — {len(sources)} archive sources would be processed:")
        for src in sources:
            idents = src.get("identifiers", [src.get("identifier", "N/A")])
            print(f"  [{src.get('category', '?'):12s}] {src['short_name']:30s} {idents[0]}")
        if args.hadith:
            print(f"\n  + {len(HADITH_COLLECTIONS)} hadith collections")
        if args.quran:
            print(f"  + {len(QURAN_TRANSLATIONS)} Quran translations")
        return

    # Connect
    conn = get_db_connection()
    cur = conn.cursor()
    vo = get_voyage_client()
    ingested = get_ingested_sources(cur)

    t0 = time.time()
    total_new = 0
    failed = []

    # Archive sources
    if sources:
        print(f"\n{'='*60}")
        print(f"ARCHIVE SOURCES — {len(sources)} to process")
        print(f"{'='*60}")

        for i, src in enumerate(sources, 1):
            print(f"\n[{i}/{len(sources)}] {src['source'][:55]}")
            try:
                n = ingest_archive_source(src, cur, vo, ingested)
                total_new += n
                if n > 0:
                    ingested.add(src["short_name"])
            except Exception as e:
                print(f"  ERROR: {e}")
                failed.append(src["short_name"])
                conn.rollback()
            time.sleep(0.5)

    # API sources
    if args.hadith:
        total_new += ingest_hadith(cur, vo, ingested)

    if args.quran:
        total_new += ingest_quran(cur, vo, ingested)

    elapsed = time.time() - t0

    # Summary
    print(f"\n{'='*60}")
    print(f"INGESTION COMPLETE — {total_new:,} new chunks in {elapsed:.0f}s")
    if failed:
        print(f"  Failed: {', '.join(failed)}")
    print(f"{'='*60}")

    print_corpus_summary(cur)
    conn.close()


if __name__ == "__main__":
    main()
