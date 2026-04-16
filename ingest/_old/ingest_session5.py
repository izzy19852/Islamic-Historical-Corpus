"""
Islam Stories RAG — Session 5: Sahaba Sources (Tabaqat + Waqidi + Azami)
Downloads djvu.txt sources, chunks, embeds voyage-2, inserts with metadata.
All sources: era=rashidun, source_type=primary_arabic (except Azami=scholarly_western)
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
        if re.match(r'^\d+$', stripped):
            continue
        if re.match(r'^[ivxlcdm]+$', stripped, re.IGNORECASE) and len(stripped) < 10:
            continue
        if len(stripped) < 15:
            continue
        if stripped in repeated:
            continue
        # >40% alpha filter (readable_text)
        alpha_ratio = sum(1 for c in stripped if c.isalpha()) / max(len(stripped), 1)
        if alpha_ratio < 0.4 and len(stripped) < 80:
            continue
        cleaned.append(stripped)

    text = '\n'.join(cleaned)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]{3,}', '  ', text)
    return text.strip()


# ─── DOWNLOAD / INSERT HELPERS ────────────────────────────────────

def download_text(url, timeout=120):
    """Download plain text (djvu.txt)."""
    print(f"    Downloading: {url[:90]}...")
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    text = resp.text
    words = len(text.split())
    print(f"    Raw: {words:,} words")
    return text


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


# ─── EXTENDED FIGURE EXTRACTION ──────────────────────────────────

EXTRA_FIGURES = [
    "Nusayba", "Umm Amara", "Musab ibn Umayr", "Khawla", "Rufayda",
    "Julaybib", "Abu Dharr", "Sumayya", "Hasan al-Basri", "Said ibn Jubayr",
    "Khadija bint Khuwaylid", "Sawda", "Hafsa", "Zaynab", "Umm Salama",
    "Asma bint Abu Bakr", "Fatima bint Muhammad", "Hind bint Utba",
    "Abu Musa", "Abdullah ibn Masud", "Anas ibn Malik",
    "Sad ibn Abi Waqqas", "Zubayr ibn al-Awwam", "Talha ibn Ubaydullah",
    "Abd al-Rahman ibn Awf", "Abu Ubayda ibn al-Jarrah",
    "Ammar ibn Yasir", "Abu Dujana", "Safiyya bint Abd al-Muttalib",
]
EXTRA_PATTERNS = [(fig, re.compile(re.escape(fig), re.IGNORECASE)) for fig in EXTRA_FIGURES]


def extract_figures_extended(text):
    base = extract_figures(text) or []
    extra = [fig for fig, pat in EXTRA_PATTERNS if pat.search(text)]
    combined = list(set(base + extra))
    return combined if combined else None


# ─── SOURCE DEFINITIONS ──────────────────────────────────────────

SOURCES = [
    # ── Tabaqat (Bewley translation) ──
    {
        "short_name": "tabaqat-viii-women",
        "source": "Ibn Sa'd, Tabaqat Vol VIII: Women of Madina (Bewley)",
        "url": "https://archive.org/download/kitab-at-tabaqat-al-kabir/Kitab_at_Tabaqat_al_Kabir_Volume_VIII_The_Women_of_Madi_Muhammad_djvu.txt",
        "source_type": "primary_arabic",
        "era": "rashidun",
        "translator": "Aisha Bewley",
        "reliability": "scholarly",
        "subdir": "tabaqat",
    },
    {
        "short_name": "tabaqat-iii-badr",
        "source": "Ibn Sa'd, Tabaqat Vol III: Companions of Badr (Bewley)",
        "url": "https://archive.org/download/kitab-at-tabaqat-al-kabir/Kitab_at_Tabaqat_al_Kabir_Volume_III_The_Companions_of_Badr_2013_djvu.txt",
        "source_type": "primary_arabic",
        "era": "rashidun",
        "translator": "Aisha Bewley",
        "reliability": "scholarly",
        "subdir": "tabaqat",
    },
    {
        "short_name": "tabaqat-i-men-pt1",
        "source": "Ibn Sa'd, Tabaqat Vol I: Men of Madina Pt 1 (Bewley)",
        "url": "https://archive.org/download/kitab-at-tabaqat-al-kabir/Kitab_at_Tabaqat_al_Kabir_Volume_I_The_Men_of_Madina_Muhammad_Ibn_djvu.txt",
        "source_type": "primary_arabic",
        "era": "rashidun",
        "translator": "Aisha Bewley",
        "reliability": "scholarly",
        "subdir": "tabaqat",
    },
    {
        "short_name": "tabaqat-ii-men-pt2",
        "source": "Ibn Sa'd, Tabaqat Vol II: Men of Madina Pt 2 (Bewley)",
        "url": "https://archive.org/download/kitab-at-tabaqat-al-kabir/Kitab_at_Tabaqat_al_Kabir_Volume_II_The_Men_of_Madina_Muhammad_Ibn_djvu.txt",
        "source_type": "primary_arabic",
        "era": "rashidun",
        "translator": "Aisha Bewley",
        "reliability": "scholarly",
        "subdir": "tabaqat",
    },
    {
        "short_name": "tabaqat-vi-kufa",
        "source": "Ibn Sa'd, Tabaqat Vol VI: Scholars of Kufa (Bewley)",
        "url": "https://archive.org/download/kitab-at-tabaqat-al-kabir/Kitab_at_Tabaqat_al_Kabir_Volume_VI_The_Scholars_of_Kufa_Muhammad_djvu.txt",
        "source_type": "primary_arabic",
        "era": "rashidun",
        "translator": "Aisha Bewley",
        "reliability": "scholarly",
        "subdir": "tabaqat",
    },
    {
        "short_name": "tabaqat-vii-basra",
        "source": "Ibn Sa'd, Tabaqat Vol VII: Men of Madina Basra/Baghdad (Bewley)",
        "url": "https://archive.org/download/01-kitab-at-tabaqat-al-kabir-the-men-of-madina-by-muhammad-ibn-sad-english-trans/01%20Kitab%20at-Tabaqat%20al-Kabir%20The%20Men%20of%20Madina%20By%20Muhammad%20Ibn%20Sad%2C%20English%20translation%20by%20Aisha%20Bewley%201997_djvu.txt",
        "source_type": "primary_arabic",
        "era": "rashidun",
        "translator": "Aisha Bewley",
        "reliability": "scholarly",
        "subdir": "tabaqat",
    },
    {
        "short_name": "tabaqat-12-prophet",
        "source": "Ibn Sa'd, Tabaqat Vols 1-2: Prophet Era (Haq/Ghazanfar)",
        "url": "https://archive.org/download/TabaqatIbnSaadVol12English/IbnSaad_djvu.txt",
        "source_type": "primary_arabic",
        "era": "rashidun",
        "translator": "S. Moinul Haq / H.K. Ghazanfar",
        "reliability": "scholarly",
        "subdir": "tabaqat",
    },

    # ── Al-Waqidi ──
    {
        "short_name": "waqidi-maghazi",
        "source": "Al-Waqidi, Kitab al-Maghazi (Faizer translation 2011)",
        # Direct server URL (Unicode filename requires it)
        "url": "https://ia601302.us.archive.org/17/items/the-life-of-prophet-waqidis-kitab-al-maghazi-routledge-2011_202401/The%20life%20of%20prophet-W%C4%81qid%C4%AB%E2%80%99s%20Kit%C4%81b%20al-magh%C4%81z%C4%AB-Routledge%20%282011%29_djvu.txt",
        "source_type": "primary_arabic",
        "era": "rashidun",
        "translator": "Rizwi Faizer",
        "reliability": "scholarly",
        "reliability_note": "Acceptable for battle narrative, weak for legal hadith",
        "subdir": "maghazi",
    },

    # ── Al-Azami ──
    {
        "short_name": "azami-hadith-lit",
        "source": "Al-Azami, Studies in Early Hadith Literature",
        "url": "https://archive.org/download/hadithsunnah/StudiesInEarlyHadithLiteratureByShaykhMuhammadMustafaAlAzami_djvu.txt",
        "source_type": "scholarly_western",
        "era": "rashidun",
        "translator": None,
        "reliability": "scholarly",
        "subdir": "tabaqat",
    },
]


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
        raw_text = download_text(src["url"])
        text = clean_djvu_text(raw_text)

        # Save locally
        txt_path = os.path.join(subdir, f"{short}.txt")
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(text)
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
    print(f"SESSION 5 STEP 1 — SAHABA SOURCES INGESTION COMPLETE — {elapsed:.0f}s")
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

    cur.execute("SELECT COUNT(*), SUM(word_count) FROM documents;")
    total_docs, total_words = cur.fetchone()
    print(f"  Total in DB: {total_docs} chunks, {total_words:,} words")

    conn.close()


if __name__ == "__main__":
    main()
