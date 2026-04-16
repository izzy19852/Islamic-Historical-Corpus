#!/usr/bin/env python3
"""
Islam Stories — OCR pipeline for scanned PDFs.

Uses ocrmypdf with Tesseract language packs to add text layers to scanned PDFs.
Can also be used standalone to OCR downloaded Archive.org files.

Usage:
    # OCR all pending files
    python -m ingest.ocr

    # OCR only English-language files
    python -m ingest.ocr --lang eng

    # OCR a specific file
    python -m ingest.ocr --file /path/to/scan.pdf --lang fas

    # List pending OCR jobs
    python -m ingest.ocr --list
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from ingest.core import SOURCES_DIR

OCR_OUTPUT_DIR = SOURCES_DIR / "ocr_output"

# (input_relative_path, output_name, tesseract_langs, approx_pages)
DEFAULT_JOBS = [
    # English scans
    ("persia/Iskandar Beg Munshi - History of Shah Abbas Vol 2.pdf",
     "Iskandar Beg Munshi - Shah Abbas Vol 2 (OCR).pdf", "eng", 860),
    ("caucasus/Baddeley - Russian Conquest of the Caucasus.pdf",
     "Baddeley - Russian Conquest of the Caucasus (OCR).pdf", "eng", 609),
    ("persia/Juvaini - History of the World Conqueror Vol I.pdf",
     "Juvaini - History of World Conqueror Vol I (OCR).pdf", "eng", 424),
    ("persia/Sykes - A History of Persia Vol II.pdf",
     "Sykes - History of Persia Vol II (OCR).pdf", "eng", 670),
    ("timur/Clavijo - Embassy to Tamerlane 1403-1406.pdf",
     "Clavijo - Embassy to Tamerlane (OCR).pdf", "eng", 420),
    ("timur/Clavijo - Narrative of Embassy to Timour.pdf",
     "Clavijo - Narrative Embassy Timour (OCR).pdf", "eng", 273),

    # Russian scans
    ("caucasus/Qarakhi - Chronicle (Russian).pdf",
     "Qarakhi - Chronicle (OCR).pdf", "rus", 340),

    # Arabic scans
    ("timur/Ibn Arabshah - Ajaib al-Maqdur (Arabic).pdf",
     "Ibn Arabshah - Ajaib al-Maqdur (OCR).pdf", "ara", 260),

    # Persian/Farsi scans
    ("persia/Nizami Ganjavi - Khamsa (Persian).pdf",
     "Nizami - Khamsa (OCR).pdf", "fas", 600),
    ("persia/Rashid al-Din - Jami al-Twarikh Vol 2.pdf",
     "Rashid al-Din - Jami al-Twarikh Vol 2 (OCR).pdf", "fas", 771),
    ("persia/Iskandar Beg - Tarikh-i Alam Ara-yi Abbasi (Persian).pdf",
     "Iskandar Beg - Alam Ara Abbasi (OCR).pdf", "fas", 745),
    ("persia/Wassaf - Tarikh-e Wassaf (Farsi).pdf",
     "Wassaf - Tarikh-e Wassaf (OCR).pdf", "fas", 708),
    ("persia/Wassaf - Tajziyat al-Amsar (manuscript).PDF",
     "Wassaf - Tajziyat al-Amsar (OCR).pdf", "fas+ara", 289),
    ("timur/Ibn Arabshah - Life of Timur (Farsi).pdf",
     "Ibn Arabshah - Life of Timur Farsi (OCR).pdf", "fas", 379),
]


def run_ocr(input_path, output_path, langs, timeout=1800):
    """Run ocrmypdf on a single file. Returns True on success."""
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        print(f"  MISSING: {input_path}")
        return False

    if output_path.exists() and output_path.stat().st_size > 0:
        print(f"  SKIP (exists): {output_path.name}")
        return True

    output_path.parent.mkdir(parents=True, exist_ok=True)
    size_mb = input_path.stat().st_size / (1024 * 1024)
    print(f"  Input: {size_mb:.0f} MB, lang={langs}")

    cmd = [
        "ocrmypdf",
        "--language", langs,
        "--jobs", "2",
        "--skip-text",
        "--optimize", "1",
        "--output-type", "pdf",
        str(input_path),
        str(output_path),
    ]

    start = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        elapsed = time.time() - start

        if result.returncode == 0:
            out_mb = output_path.stat().st_size / (1024 * 1024)
            print(f"  DONE: {out_mb:.0f} MB in {elapsed:.0f}s")
            return True
        elif result.returncode == 6:
            # Already has text layer — copy as-is
            print(f"  Already has text layer — copying ({elapsed:.0f}s)")
            shutil.copy2(input_path, output_path)
            return True
        else:
            print(f"  FAILED (rc={result.returncode}): {result.stderr[:200]}")
            return False

    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT after {timeout}s")
        if output_path.exists():
            output_path.unlink()
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="OCR scanned PDFs")
    parser.add_argument("--lang", help="Filter by language (eng, fas, ara, rus)")
    parser.add_argument("--file", help="OCR a specific file")
    parser.add_argument("--output", help="Output path (with --file)")
    parser.add_argument("--list", action="store_true", help="List pending OCR jobs")
    parser.add_argument("--base-dir", help="Base directory for input files",
                        default=str(SOURCES_DIR))
    args = parser.parse_args()

    base = Path(args.base_dir)

    if args.file:
        langs = args.lang or "eng"
        output = args.output or args.file.replace(".pdf", " (OCR).pdf").replace(".PDF", " (OCR).pdf")
        print(f"[OCR] {args.file}")
        ok = run_ocr(args.file, output, langs)
        sys.exit(0 if ok else 1)

    jobs = DEFAULT_JOBS
    if args.lang:
        jobs = [(i, o, l, p) for i, o, l, p in jobs if args.lang in l]

    if args.list:
        print(f"{'='*60}")
        print(f"OCR JOBS — {len(jobs)} files")
        print(f"{'='*60}")
        for input_rel, output_name, langs, pages in jobs:
            input_path = base / input_rel
            output_path = OCR_OUTPUT_DIR / output_name
            status = "DONE" if output_path.exists() else ("READY" if input_path.exists() else "MISSING")
            print(f"  [{status:7s}] [{langs:7s}] ~{pages:>4d}pp  {output_name[:50]}")
        return

    print(f"{'='*70}")
    print(f"  OCR PIPELINE — {len(jobs)} files")
    print(f"{'='*70}")

    success, failed = 0, 0
    for input_rel, output_name, langs, pages in jobs:
        input_path = base / input_rel
        output_path = OCR_OUTPUT_DIR / output_name

        print(f"\n[OCR] {output_name}")
        ok = run_ocr(input_path, output_path, langs)
        if ok:
            success += 1
        else:
            failed += 1

    print(f"\n{'='*70}")
    print(f"  OCR COMPLETE: {success} success, {failed} failed")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
