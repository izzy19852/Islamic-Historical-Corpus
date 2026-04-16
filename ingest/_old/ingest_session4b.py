"""
Islam Stories RAG — Session 4B: Central Asia, Persia, South Asia (Mughal), Bosnia
Downloads djvu.txt / PDF sources, chunks, embeds voyage-2, inserts with metadata.
"""

import os
import re
import sys
import time
import requests
import psycopg2
import psycopg2.extras
import voyageai
import yaml
from collections import Counter
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
SOURCES_DIR = os.path.join(os.path.dirname(__file__), '..', 'sources')
VOYAGE_BATCH_SIZE = 128

sys.path.insert(0, os.path.dirname(__file__))
from ingest_texts import chunk_text, extract_figures, embed_batch, get_ingested_sources

try:
    import fitz  # PyMuPDF
except ImportError:
    sys.exit("pip install pymupdf")


# ─── READABLE TEXT FILTER ─────────────────────────────────────────

def detect_repeated_headers(lines, threshold=30, max_len=100):
    short_lines = [l.strip() for l in lines if 5 < len(l.strip()) < max_len]
    counts = Counter(short_lines)
    return {line for line, count in counts.items() if count >= threshold}


def clean_djvu_text(raw_text):
    """Clean djvu.txt OCR output: remove noise, headers, page markers, short junk lines."""
    lines = raw_text.split('\n')
    repeated = detect_repeated_headers(lines)
    cleaned = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if cleaned and cleaned[-1] != '':
                cleaned.append('')
            continue
        # Skip page numbers (purely numeric)
        if re.match(r'^\d+$', stripped):
            continue
        # Skip Roman numeral page numbers
        if re.match(r'^[ivxlcdm]+$', stripped, re.IGNORECASE) and len(stripped) < 10:
            continue
        # Skip very short lines (OCR noise, headers)
        if len(stripped) < 15:
            continue
        # Skip repeated headers/footers
        if stripped in repeated:
            continue
        # Skip lines that are mostly non-alpha (OCR garbage)
        alpha_ratio = sum(1 for c in stripped if c.isalpha()) / max(len(stripped), 1)
        if alpha_ratio < 0.5 and len(stripped) < 80:
            continue
        cleaned.append(stripped)

    text = '\n'.join(cleaned)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]{3,}', '  ', text)
    return text.strip()


def clean_pdf_text(raw_text):
    """Clean extracted PDF text."""
    lines = raw_text.split('\n')
    repeated = detect_repeated_headers(lines)
    cleaned = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            if cleaned and cleaned[-1] != '':
                cleaned.append('')
            continue
        if re.match(r'^\d+$', stripped):
            continue
        if re.match(r'^[ivxlcdm]+$', stripped, re.IGNORECASE) and len(stripped) < 10:
            continue
        if len(stripped) < 15:
            continue
        if stripped in repeated:
            continue
        cleaned.append(stripped)

    text = '\n'.join(cleaned)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]{3,}', '  ', text)
    return text.strip()


def extract_pdf_text(pdf_path):
    """Extract and clean text from PDF using PyMuPDF."""
    doc = fitz.open(pdf_path)
    all_text = []
    for page in doc:
        text = page.get_text("text")
        if text:
            all_text.append(text)
    doc.close()
    return clean_pdf_text('\n'.join(all_text))


# ─── DOWNLOAD HELPERS ─────────────────────────────────────────────

def download_text(url, timeout=120):
    """Download plain text (djvu.txt)."""
    print(f"    Downloading: {url[:80]}...")
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    text = resp.text
    words = len(text.split())
    print(f"    Raw: {words:,} words")
    return text


def download_pdf_file(url, dest_path, timeout=120):
    """Download PDF to disk."""
    print(f"    Downloading PDF: {url[:80]}...")
    resp = requests.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()
    with open(dest_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    print(f"    Saved: {size_mb:.1f} MB")
    return True


# ─── INSERT HELPER ────────────────────────────────────────────────

def insert_chunks(cur, vo, chunks, label=""):
    total = len(chunks)
    inserted = 0
    for i in range(0, total, VOYAGE_BATCH_SIZE):
        batch = chunks[i:i + VOYAGE_BATCH_SIZE]
        texts = [c["content"] for c in batch]
        try:
            embeddings = embed_batch(vo, texts)
        except Exception as e:
            print(f"    Skipping batch {i}: {e}")
            continue
        for c, emb in zip(batch, embeddings):
            cur.execute("""
                INSERT INTO documents (content, embedding, source, source_type, era, figures, chunk_index, word_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (c["content"], str(emb), c["source"], c["source_type"],
                  c.get("era"), c.get("figures"), c.get("chunk_index"),
                  len(c["content"].split())))
            inserted += 1
        cur.connection.commit()
        print(f"    [{label}] Embedded {min(i + len(batch), total)}/{total}")
        time.sleep(0.5)
    return inserted


def register_source(cur, src, n_chunks):
    era_list = src["era"] if isinstance(src["era"], list) else [src["era"]]
    cur.execute("""
        INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (src["source"], src["short_name"], src["source_type"], "english",
          src.get("translator"), era_list, src.get("reliability", "scholarly"), n_chunks))
    cur.connection.commit()


# ─── SOURCE DEFINITIONS ──────────────────────────────────────────

SOURCES = [
    # ── Central Asia ──
    {
        "short_name": "unesco-timurid",
        "source": "UNESCO Silk Road: Timurid States in the 15th-16th centuries",
        "url": "https://en.unesco.org/silkroad/sites/default/files/knowledge-bank-article/vol_IVa%20silk%20road_the%20timurid%20states%20in%20the%20fifteenth%20and%20sixteenth%20centuries.pdf",
        "format": "pdf",
        "source_type": "scholarly_western",
        "era": "central_asia",
        "translator": None,
        "reliability": "scholarly",
        "subdir": "central_asia",
    },
    {
        "short_name": "unesco-central-asia-v4",
        "source": "UNESCO History of Civilizations of Central Asia Vol 4",
        "url": "https://www.kroraina.com/vojnikov/ist_sr_a_4_1.pdf",
        "format": "pdf",
        "source_type": "scholarly_western",
        "era": "central_asia",
        "translator": None,
        "reliability": "scholarly",
        "subdir": "central_asia",
    },
    {
        "short_name": "burnes-bukhara",
        "source": "Burnes, Travels into Bukhara (1834) — British envoy eyewitness",
        "url": "https://www.gutenberg.org/files/58074/58074-0.txt",
        "format": "djvu",
        "source_type": "scholarly_western",
        "era": "central_asia",
        "translator": None,
        "reliability": "scholarly",
        "subdir": "central_asia",
    },
    {
        "short_name": "yemelianova-central-asia",
        "source": "Yemelianova, Muslims of Central Asia — Chapter 1 (Edinburgh UP)",
        "url": "https://edinburghuniversitypress.com/media/resources/9781474416344_Muslims_of_Central_Asia_Chapter1.pdf",
        "format": "pdf",
        "source_type": "scholarly_western",
        "era": "central_asia",
        "translator": None,
        "reliability": "scholarly",
        "subdir": "central_asia",
    },

    # ── Persia / Safavid ──
    {
        "short_name": "sykes-persia-v1",
        "source": "Sykes, History of Persia Vol 1 (1915)",
        "url": "https://archive.org/download/historyofpersia01sykeuoft/historyofpersia01sykeuoft_djvu.txt",
        "format": "djvu",
        "source_type": "scholarly_western",
        "era": "persia_safavid",
        "translator": None,
        "reliability": "scholarly",
        "subdir": "persia",
    },
    {
        "short_name": "sykes-persia-v2",
        "source": "Sykes, History of Persia Vol 2 (1915)",
        "url": "https://archive.org/download/historyofpersia02sykeuoft/historyofpersia02sykeuoft_djvu.txt",
        "format": "djvu",
        "source_type": "scholarly_western",
        "era": "persia_safavid",
        "translator": None,
        "reliability": "scholarly",
        "subdir": "persia",
    },
    {
        "short_name": "rumi-masnavi-v1",
        "source": "Rumi, Masnavi-i Ma'navi Vol 1 (Whinfield translation)",
        "url": "https://archive.org/download/in.ernet.dli.2015.65659/2015.65659.Teachings-Of-Rumi-The-Masnavi_djvu.txt",
        "format": "djvu",
        "source_type": "primary_arabic",
        "era": "abbasid",
        "translator": "E.H. Whinfield",
        "reliability": "scholarly",
        "subdir": "persia",
    },

    # ── South Asia Extra (Mughal) ──
    {
        "short_name": "akbarnama-v1",
        "source": "Abul Fazl, Akbarnama Vol 1 (official Mughal history)",
        "url": "https://archive.org/download/in.ernet.dli.2015.189288/2015.189288.The-Akbarnama-Of-Abul-Fazl--Vol-1_djvu.txt",
        "format": "djvu",
        "source_type": "primary_arabic",
        "era": "south_asia",
        "translator": "H. Beveridge",
        "reliability": "scholarly",
        "subdir": "south_asia_extra",
    },
    {
        "short_name": "akbarnama-v2",
        "source": "Abul Fazl, Akbarnama Vol 2",
        "url": "https://archive.org/download/in.ernet.dli.2015.55649/2015.55649.The-Akbarnama-Of-Abul-Fazl--Vol-2_djvu.txt",
        "format": "djvu",
        "source_type": "primary_arabic",
        "era": "south_asia",
        "translator": "H. Beveridge",
        "reliability": "scholarly",
        "subdir": "south_asia_extra",
    },
    {
        "short_name": "ain-i-akbari-v1",
        "source": "Abul Fazl, Ain-i-Akbari Vol 1 (Mughal administrative record)",
        "url": "https://archive.org/download/in.ernet.dli.2015.32526/2015.32526.Ain-i-akbari--Vol-1_djvu.txt",
        "format": "djvu",
        "source_type": "primary_arabic",
        "era": "south_asia",
        "translator": "H. Blochmann",
        "reliability": "scholarly",
        "subdir": "south_asia_extra",
    },
    {
        "short_name": "firishta-v1",
        "source": "Firishta, History of the Rise of Mahomedan Power in India Vol 1",
        "url": "https://archive.org/download/history-of-the-rise-of-the-mahomedan-power-in-india-vol.-1/History%20Of%20The%20Rise%20Of%20The%20Mahomedan%20Power%20In%20India%2C%20Vol.%201_djvu.txt",
        "format": "djvu",
        "source_type": "primary_arabic",
        "era": "south_asia",
        "translator": "John Briggs",
        "reliability": "scholarly",
        "subdir": "south_asia_extra",
    },
    {
        "short_name": "firishta-v2",
        "source": "Firishta, History of the Rise of Mahomedan Power in India Vol 2",
        "url": "https://archive.org/download/historyofriseofm02firi/historyofriseofm02firi_djvu.txt",
        "format": "djvu",
        "source_type": "primary_arabic",
        "era": "south_asia",
        "translator": "John Briggs",
        "reliability": "scholarly",
        "subdir": "south_asia_extra",
    },

    # ── Bosnia ──
    {
        "short_name": "krijestorac-bosniak",
        "source": "Krijestorac, First Nationalism Then Identity: Bosnian Muslims (2022)",
        "url": "https://library.oapen.org/bitstream/handle/20.500.12657/58604/9780472902880.pdf",
        "format": "pdf",
        "source_type": "scholarly_western",
        "era": "ottoman",
        "translator": None,
        "reliability": "scholarly",
        "subdir": "central_asia",  # reuse dir
    },
    {
        "short_name": "metu-bosniak-thesis",
        "source": "Formation of the Bosniak Nation (METU thesis 2014)",
        "url": "https://etd.lib.metu.edu.tr/upload/12616805/index.pdf",
        "format": "pdf",
        "source_type": "scholarly_western",
        "era": "ottoman",
        "translator": None,
        "reliability": "scholarly",
        "subdir": "central_asia",
    },
]


# ─── KNOWN FIGURES (extended for these regions) ──────────────────

EXTRA_FIGURES = [
    "Timur", "Tamerlane", "Ulugh Beg", "Shah Ismail", "Shah Abbas",
    "Babur", "Akbar", "Jahangir", "Shah Jahan", "Aurangzeb",
    "Humayun", "Sher Shah", "Rumi", "Hafiz", "Firdausi",
    "Nizam al-Mulk", "Sultan Mahmud", "Abul Fazl", "Biruni",
]
EXTRA_PATTERNS = [(fig, re.compile(re.escape(fig), re.IGNORECASE)) for fig in EXTRA_FIGURES]


def extract_figures_extended(text):
    """Extract figures including Central Asian / Mughal / Persian figures."""
    base = extract_figures(text) or []
    extra = [fig for fig, pat in EXTRA_PATTERNS if pat.search(text)]
    combined = list(set(base + extra))
    return combined if combined else None


# ─── MAIN INGESTION ──────────────────────────────────────────────

def ingest_source(src, cur, vo, ingested):
    short = src["short_name"]
    if short in ingested:
        print(f"\n  [{short}] Already ingested — skipping")
        return {"short_name": short, "chunks": 0, "skipped": True}

    print(f"\n  [{short}] {src['source']}")

    subdir = os.path.join(SOURCES_DIR, src.get("subdir", ""))
    os.makedirs(subdir, exist_ok=True)

    try:
        if src["format"] == "djvu":
            raw_text = download_text(src["url"])
            text = clean_djvu_text(raw_text)
            # Save locally
            txt_path = os.path.join(subdir, f"{short}.txt")
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(text)
        elif src["format"] == "pdf":
            pdf_path = os.path.join(subdir, f"{short}.pdf")
            if not os.path.exists(pdf_path):
                download_pdf_file(src["url"], pdf_path)
            text = extract_pdf_text(pdf_path)
            # Save extracted text
            txt_path = os.path.join(subdir, f"{short}.txt")
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(text)
            # Clean up PDF
            os.remove(pdf_path)
        else:
            print(f"    Unknown format: {src['format']}")
            return {"short_name": short, "chunks": 0, "error": "unknown format"}
    except Exception as e:
        print(f"    DOWNLOAD FAILED: {e}")
        return {"short_name": short, "chunks": 0, "error": str(e)}

    words = len(text.split())
    print(f"    Cleaned text: {words:,} words")

    if words < 500:
        print(f"    Too short ({words} words) — skipping")
        return {"short_name": short, "chunks": 0, "error": "too short"}

    # Chunk
    text_chunks = chunk_text(text)
    print(f"    Chunks: {len(text_chunks)}")

    era_str = src["era"]

    chunks = []
    for idx, c in enumerate(text_chunks):
        chunks.append({
            "content": c,
            "source": src["source"],
            "source_type": src["source_type"],
            "era": era_str,
            "figures": extract_figures_extended(c),
            "chunk_index": idx,
        })

    n = insert_chunks(cur, vo, chunks, short)
    register_source(cur, src, n)
    ingested.add(short)

    # Write YAML sidecar
    yaml_path = os.path.join(subdir, f"{short}.yaml")
    with open(yaml_path, 'w') as f:
        yaml.dump({
            "source": src["source"],
            "short_name": short,
            "source_type": src["source_type"],
            "era": src["era"],
            "translator": src.get("translator"),
            "reliability": src.get("reliability"),
            "word_count": words,
            "chunk_count": n,
        }, f, default_flow_style=False)

    print(f"    DONE: {n} chunks, {words:,} words")
    return {"short_name": short, "chunks": n, "words": words}


# ─── VALIDATION ──────────────────────────────────────────────────

def validate():
    """Run validation queries and print results."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'embeddings'))
    from query import query_rag

    queries = [
        ("Samarkand Bukhara Timurid Islamic civilization Silk Road", "central_asia"),
        ("Safavid Persia Shah Ismail Shia Islam", "persia_safavid"),
        ("Rumi Sufi poetry mysticism", "abbasid"),
        ("Akbar Mughal India religious tolerance Din-i-Ilahi", "south_asia"),
        ("Delhi Sultanate India Muslim rulers history", "south_asia"),
        ("Bosnian Muslims Ottoman Austro-Hungarian identity", "ottoman"),
    ]

    print(f"\n{'='*60}")
    print("VALIDATION QUERIES")
    print(f"{'='*60}")

    all_pass = True
    for query_text, era in queries:
        results = query_rag(query_text, era=era, n_results=3)
        print(f"\n  Query: \"{query_text}\" (era={era})")
        if not results:
            print("    NO RESULTS")
            all_pass = False
            continue
        for i, r in enumerate(results, 1):
            score = r["similarity_score"]
            src = r["source"][:60]
            status = "OK" if score >= 0.65 else "LOW"
            print(f"    {i}. [{status}] {score:.4f} — {src}")
            if score < 0.65:
                all_pass = False

    print(f"\n  {'ALL PASS' if all_pass else 'SOME BELOW 0.65 THRESHOLD'}")
    return all_pass


# ─── MAIN ─────────────────────────────────────────────────────────

def main():
    if not DB_URL or not VOYAGE_API_KEY:
        print("ERROR: ISLAM_STORIES_DB_URL or VOYAGE_API_KEY not set")
        sys.exit(1)

    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    ingested = get_ingested_sources(cur)

    t0 = time.time()
    all_stats = []

    for src in SOURCES:
        stats = ingest_source(src, cur, vo, ingested)
        all_stats.append(stats)

    elapsed = time.time() - t0

    # Summary
    print(f"\n{'='*60}")
    print(f"SESSION 4B INGESTION COMPLETE — {elapsed:.0f}s")
    print(f"{'='*60}")
    total_new = 0
    for s in all_stats:
        if s.get("skipped"):
            print(f"  {s['short_name']:30s} SKIPPED (already in DB)")
        elif s.get("error"):
            print(f"  {s['short_name']:30s} FAILED: {s['error']}")
        else:
            n = s.get("chunks", 0)
            w = s.get("words", 0)
            print(f"  {s['short_name']:30s} {n:>5} chunks  {w:>8,} words")
            total_new += n

    print(f"\n  New chunks inserted: {total_new}")

    # Total DB stats
    cur.execute("SELECT COUNT(*), SUM(word_count) FROM documents;")
    total_docs, total_words = cur.fetchone()
    print(f"  Total in DB: {total_docs} chunks, {total_words:,} words")

    conn.close()

    # Validation
    validate()


if __name__ == "__main__":
    main()
