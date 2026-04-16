"""
Islam Stories RAG — Ingest remaining Tabari volumes + Ibn Hisham.
Downloads PDF, extracts text, chunks, embeds, inserts, deletes PDF.
Skips any source already in the DB.
"""

import os
import sys
import time
import yaml
import requests
import psycopg2
import voyageai
from tqdm import tqdm
from dotenv import load_dotenv

try:
    import fitz
except ImportError:
    sys.exit("pip install pymupdf")

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

SOURCES_DIR = os.path.join(os.path.dirname(__file__), '..', 'sources')
DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")

sys.path.insert(0, os.path.dirname(__file__))
from download_sources import download_file, extract_pdf_text, write_yaml_sidecar
from ingest_texts import (chunk_text, extract_figures, embed_batch,
                          get_ingested_sources, VOYAGE_BATCH_SIZE)

IA_TABARI = "https://archive.org/download/the-history-of-al-tabari"

# Volume metadata: vol_num -> (era, translator, description)
TABARI_META = {
    14: (["rashidun"], "Rex Smith", "Al-Tabari Vol 14: Conquest of Iran 641-643 CE"),
    16: (["rashidun"], "Humphreys", "Al-Tabari Vol 16: Community Divided, Uthman and Ali 657 CE"),
    18: (["umayyad"], "Michael Fishbein", "Al-Tabari Vol 18: Between Civil Wars, Muawiyah 661-680 CE"),
    22: (["umayyad"], "Everett Rowson", "Al-Tabari Vol 22: Marwanid Restoration 693-701 CE"),
    23: (["umayyad"], "Martin Hinds", "Al-Tabari Vol 23: Zenith of the Marwanid House 701-715 CE"),
    26: (["umayyad"], "Khalid Blankinship", "Al-Tabari Vol 26: Waning of the Umayyad Caliphate 738-744 CE"),
    28: (["abbasid"], "Jane McAuliffe", "Al-Tabari Vol 28: Abbasid Authority Affirmed 750-763 CE"),
    29: (["abbasid"], "Hugh Kennedy", "Al-Tabari Vol 29: Al-Mansur and al-Mahdi 763-786 CE"),
    31: (["abbasid"], "C.E. Bosworth", "Al-Tabari Vol 31: War Between Brothers 809-813 CE"),
    32: (["abbasid"], "C.E. Bosworth", "Al-Tabari Vol 32: Reunification of the Abbasid Caliphate 813-833 CE"),
    33: (["abbasid"], "C.E. Bosworth", "Al-Tabari Vol 33: Storm and Stress, al-Mutasim 833-842 CE"),
    36: (["abbasid"], "David Waines", "Al-Tabari Vol 36: Revolt of the Zanj 869-879 CE"),
    38: (["abbasid"], "Franz Rosenthal", "Al-Tabari Vol 38: Return of the Caliphate to Baghdad 892-902 CE"),
}

# Ibn Hisham — may be image-based, will test
EXTRA_SOURCES = [
    {
        "name": "ibn-hisham-sira",
        "short_name": "ibn-hisham",
        "url": "https://archive.org/download/seerat-ibn-e-hisham-english-translation-2nd-edition/Seerat%20Ibn%20e%20Hisham%20-%20English%20Translation%20(2nd%20Edition).pdf",
        "source_type": "primary_arabic",
        "era": ["rashidun"],
        "translator": "various",
        "reliability": "scholarly",
        "description": "Ibn Hisham Sira: Biography of the Prophet, early companions, battles",
    },
]


def ingest_one(source_meta: dict, cur, vo, ingested: set) -> int:
    """Download, extract, chunk, embed, insert one source. Delete PDF after."""
    name = source_meta["name"]
    short_name = source_meta["short_name"]
    url = source_meta["url"]

    if short_name in ingested:
        print(f"  {short_name}: already ingested, skipping")
        return 0

    pdf_path = os.path.join(SOURCES_DIR, f"{name}.pdf")
    txt_path = os.path.join(SOURCES_DIR, f"{name}.txt")

    # Download if needed
    if not os.path.exists(txt_path) or os.path.getsize(txt_path) < 1000:
        if not os.path.exists(pdf_path):
            print(f"  Downloading {name}...")
            if not download_file(url, pdf_path):
                print(f"  FAILED download: {name}")
                return 0

        print(f"  Extracting text...")
        text = extract_pdf_text(pdf_path)
        if len(text.split()) < 100:
            print(f"  IMAGE-BASED PDF ({len(text.split())} words), skipping")
            os.remove(pdf_path)
            return 0

        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(text)
        write_yaml_sidecar(source_meta, txt_path, len(text.split()))

        # Delete PDF to save disk
        os.remove(pdf_path)
        print(f"  Extracted: {len(text.split()):,} words, PDF deleted")
    else:
        text = open(txt_path).read()
        print(f"  Text exists: {len(text.split()):,} words")

    # Chunk
    chunks = chunk_text(text)
    if not chunks:
        print(f"  No chunks produced")
        return 0

    # Embed and insert
    source = source_meta["description"]
    source_type = source_meta["source_type"]
    era = source_meta["era"]
    era_str = era[0] if isinstance(era, list) else era
    era_list = era if isinstance(era, list) else [era]
    translator = source_meta.get("translator")
    reliability = source_meta.get("reliability", "scholarly")

    inserted = 0
    for batch_start in range(0, len(chunks), VOYAGE_BATCH_SIZE):
        batch = chunks[batch_start:batch_start + VOYAGE_BATCH_SIZE]
        try:
            embeddings = embed_batch(vo, batch)
        except Exception as e:
            print(f"  Embedding failed: {e}")
            continue
        for idx, (chunk, emb) in enumerate(zip(batch, embeddings)):
            figures = extract_figures(chunk)
            cur.execute("""
                INSERT INTO documents (content, embedding, source, source_type, era, figures, chunk_index, word_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (chunk, str(emb), source, source_type, era_str, figures,
                  batch_start + idx, len(chunk.split())))
            inserted += 1
        cur.connection.commit()
        done = min(batch_start + len(batch), len(chunks))
        print(f"    Embedded {done}/{len(chunks)} chunks")
        time.sleep(0.5)

    # Register source
    cur.execute("""
        INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (source, short_name, source_type, 'english', translator, era_list, reliability, inserted))
    cur.connection.commit()
    print(f"  {short_name}: {inserted} chunks ingested")
    return inserted


def main():
    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    ingested = get_ingested_sources(cur)

    total_new = 0
    t0 = time.time()

    # Tabari volumes
    for vol_num in sorted(TABARI_META.keys()):
        era, translator, desc = TABARI_META[vol_num]
        meta = {
            "name": f"al-tabari-vol{vol_num}",
            "short_name": f"al-tabari-v{vol_num}",
            "url": f"{IA_TABARI}/Tabari_Volume_{vol_num}.pdf",
            "source_type": "primary_arabic",
            "era": era,
            "translator": translator,
            "reliability": "scholarly",
            "description": desc,
        }
        print(f"\n--- Vol {vol_num} ---")
        n = ingest_one(meta, cur, vo, ingested)
        total_new += n
        ingested.add(meta["short_name"])

    # Extra sources
    for src in EXTRA_SOURCES:
        print(f"\n--- {src['name']} ---")
        n = ingest_one(src, cur, vo, ingested)
        total_new += n
        ingested.add(src["short_name"])

    elapsed = time.time() - t0

    # Final counts
    cur.execute("SELECT source_type, COUNT(*), COUNT(DISTINCT source) FROM documents GROUP BY source_type;")
    print(f"\n{'='*60}")
    print(f"COMPLETE — {total_new} new chunks in {elapsed:.0f}s")
    print(f"{'='*60}")
    for r in cur.fetchall():
        print(f"  {r[0]:20s} {r[1]:>6} chunks  {r[2]:>3} sources")

    cur.execute("SELECT COUNT(*) FROM documents;")
    print(f"\n  TOTAL: {cur.fetchone()[0]} documents")
    conn.close()


if __name__ == "__main__":
    main()
