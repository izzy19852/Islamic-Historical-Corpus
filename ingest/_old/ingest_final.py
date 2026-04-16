"""
Islam Stories RAG — Final ingestion: remaining hadith, quran translations, djvu.txt sources.
"""

import os
import re
import sys
import time
import requests
import psycopg2
import voyageai
import yaml
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
SOURCES_DIR = os.path.join(os.path.dirname(__file__), '..', 'sources')
VOYAGE_BATCH_SIZE = 128

sys.path.insert(0, os.path.dirname(__file__))
from ingest_texts import chunk_text, extract_figures, embed_batch, get_ingested_sources

KNOWN_FIGURES = [
    "Khalid ibn Walid", "Abu Bakr", "Umar ibn Khattab",
    "Uthman", "Ali ibn Abi Talib", "Aisha", "Fatima",
    "Abu Ubayda", "Amr ibn al-As", "Salman al-Farsi",
    "Bilal", "Ibn Abbas", "Abu Hurairah", "Muawiyah",
    "Husayn", "Hassan", "Tariq ibn Ziyad", "Umar II",
    "Al-Hajjaj", "Abd al-Rahman", "Saladin", "Baybars",
    "Ibn Khaldun", "Ibn Battuta", "Ibn Sina", "Ibn Rushd",
    "Al-Ghazali", "Mansa Musa", "Muhammad al-Fatih",
    "Tipu Sultan", "Omar al-Mukhtar",
    "Prophet", "Messenger of Allah", "Abu Bakr al-Siddiq",
    "Umar", "Ali", "Khadija", "Hamza", "Abu Talib",
    "Abu Sufyan", "Zayd", "Usama",
]
FIGURE_PATTERNS = [(fig, re.compile(re.escape(fig), re.IGNORECASE)) for fig in KNOWN_FIGURES]


def _extract_figures(text):
    found = [fig for fig, pat in FIGURE_PATTERNS if pat.search(text)]
    return found if found else None


def insert_chunks(cur, vo, chunks, batch_label=""):
    total = len(chunks)
    inserted = 0
    for i in range(0, total, VOYAGE_BATCH_SIZE):
        batch = chunks[i:i + VOYAGE_BATCH_SIZE]
        texts = [c["content"] for c in batch]
        try:
            embeddings = embed_batch(vo, texts)
        except Exception as e:
            print(f"  Skipping batch {i}: {e}")
            continue
        for c, emb in zip(batch, embeddings):
            cur.execute("""
                INSERT INTO documents (content, embedding, source, source_type, era, figures, chunk_index, word_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (c["content"], str(emb), c["source"], c["source_type"],
                  c.get("era"), c.get("figures"), c.get("chunk_index"),
                  len(c["content"].split())))
            inserted += 1
        if inserted % 500 == 0 or i + len(batch) >= total:
            cur.connection.commit()
        print(f"  {batch_label} Embedded {min(i + len(batch), total)}/{total}")
        time.sleep(0.5)
    return inserted


# ─── PART 1: REMAINING HADITH ─────────────────────────────────────

FAWAZ_BASE = "https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions"
REMAINING_HADITH = {
    "eng-nasai": "Sunan an-Nasai",
    "eng-nawawi40": "Nawawi 40 Hadith",
    "eng-riyadussalihin": "Riyad as-Salihin",
    "eng-bulughalmaram": "Bulugh al-Maram",
}


def ingest_remaining_hadith(cur, vo, ingested):
    print("\n=== REMAINING HADITH ===")
    total = 0
    for key, name in REMAINING_HADITH.items():
        if key in ingested:
            print(f"  {name}: already ingested")
            continue
        print(f"\n  Fetching: {name}")
        try:
            resp = requests.get(f"{FAWAZ_BASE}/{key}.min.json", timeout=60)
            resp.raise_for_status()
            hadiths = resp.json().get("hadiths", [])
        except Exception as e:
            print(f"  FAILED: {e}")
            continue

        chunks = []
        for idx, h in enumerate(hadiths):
            text = h.get("text", "").strip()
            if not text or len(text) < 30:
                continue
            num = h.get("hadithnumber", idx + 1)
            content = f"[{name}] Hadith {num}: {text}"
            chunks.append({
                "content": content, "source": name,
                "source_type": "hadith", "era": "rashidun",
                "figures": _extract_figures(content), "chunk_index": idx,
            })

        print(f"  Chunks: {len(chunks)}")
        n = insert_chunks(cur, vo, chunks, name)
        cur.execute("INSERT INTO sources (name, short_name, source_type, language, reliability, chunk_count) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (name, key, "hadith", "english", "sahih", n))
        cur.connection.commit()
        total += n
        ingested.add(key)
        print(f"  {name}: {n} inserted")
    return total


# ─── PART 2: QURAN TRANSLATIONS ───────────────────────────────────

QURAN_TRANSLATIONS = {
    "en.pickthall": "Holy Quran (Pickthall translation)",
    "en.yusufali": "Holy Quran (Yusuf Ali translation)",
}


def ingest_quran_translations(cur, vo, ingested):
    print("\n=== QURAN TRANSLATIONS ===")
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
                lines = [f"Quran {snum}:{a.get('numberInSurah','?')} — {a.get('text','').strip()}"
                         for a in group if a.get("text", "").strip()]
                if not lines:
                    continue
                content = "\n".join(lines)
                chunks.append({
                    "content": content, "source": source_name,
                    "source_type": "quran", "era": "rashidun",
                    "figures": _extract_figures(content), "chunk_index": cidx,
                })
                cidx += 1

        print(f"  Chunks: {len(chunks)}")
        n = insert_chunks(cur, vo, chunks, edition)
        cur.execute("INSERT INTO sources (name, short_name, source_type, language, reliability, chunk_count) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (source_name, edition, "quran", "english", "sahih", n))
        cur.connection.commit()
        total += n
        ingested.add(edition)
        print(f"  {source_name}: {n} inserted")
    return total


# ─── PART 3: DJVU.TXT SOURCES ─────────────────────────────────────

DJVU_SOURCES = [
    {
        "name": "ibn-ishaq-guillaume",
        "short_name": "ibn-ishaq",
        "url": "https://archive.org/download/GuillaumeATheLifeOfMuhammad/Guillaume%2C%20A%20-%20The%20Life%20of%20Muhammad_djvu.txt",
        "fallback_url": "https://archive.org/download/TheLifeOfMohammedGuillaume/The_Life_Of_Mohammed_Guillaume_djvu.txt",
        "source_type": "primary_arabic",
        "era": ["rashidun"],
        "translator": "A. Guillaume",
        "reliability": "scholarly",
        "description": "Ibn Ishaq/Guillaume, The Life of Muhammad: Earliest biography, campaigns, companions",
    },
    {
        "name": "ibn-hisham-sira",
        "short_name": "ibn-hisham",
        "url": "https://archive.org/download/seerat-ibn-e-hisham-english-translation-2nd-edition/Seerat%20Ibn%20e%20Hisham%20-%20English%20Translation%20(2nd%20Edition)_djvu.txt",
        "fallback_url": None,
        "source_type": "primary_arabic",
        "era": ["rashidun"],
        "translator": "various",
        "reliability": "scholarly",
        "description": "Ibn Hisham Sira: Biography of the Prophet, early companions, battles",
    },
    {
        "name": "baha-ad-din-saladin-richards",
        "short_name": "saladin-richards",
        "url": "https://archive.org/download/rareexcellenthis00dsri/rareexcellenthis00dsri_djvu.txt",
        "fallback_url": None,
        "source_type": "primary_arabic",
        "era": ["crusades"],
        "translator": "D.S. Richards",
        "reliability": "scholarly",
        "description": "Baha ad-Din ibn Shaddad, The Rare and Excellent History of Saladin (Richards translation)",
    },
]


def download_text(url, fallback_url=None):
    """Download plain text file, with optional fallback."""
    for u in [url, fallback_url]:
        if not u:
            continue
        try:
            resp = requests.get(u, timeout=60)
            resp.raise_for_status()
            text = resp.text
            if len(text.split()) > 500:
                return text
            print(f"  Too short ({len(text.split())} words), trying fallback...")
        except Exception as e:
            print(f"  Download failed ({u[:60]}...): {e}")
    return None


def ingest_djvu_sources(cur, vo, ingested):
    print("\n=== DJVU.TXT SOURCES ===")
    total = 0
    for src in DJVU_SOURCES:
        short_name = src["short_name"]
        if short_name in ingested:
            print(f"\n  {short_name}: already ingested")
            continue

        print(f"\n  Downloading: {src['name']}")
        text = download_text(src["url"], src.get("fallback_url"))
        if not text:
            print(f"  FAILED: no text retrieved")
            continue

        words = len(text.split())
        print(f"  Downloaded: {words:,} words")

        # Save locally
        txt_path = os.path.join(SOURCES_DIR, f"{src['name']}.txt")
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(text)

        # Write YAML sidecar
        yaml_path = txt_path.replace('.txt', '.yaml')
        with open(yaml_path, 'w') as f:
            yaml.dump({
                "source": src["description"],
                "short_name": short_name,
                "source_type": src["source_type"],
                "era": src["era"],
                "translator": src["translator"],
                "reliability": src["reliability"],
                "word_count": words,
            }, f, default_flow_style=False)

        # Chunk, embed, insert
        chunks_text = chunk_text(text)
        if not chunks_text:
            print(f"  No chunks produced")
            continue

        era = src["era"]
        era_str = era[0] if isinstance(era, list) else era
        era_list = era if isinstance(era, list) else [era]

        chunks = []
        for idx, c in enumerate(chunks_text):
            chunks.append({
                "content": c, "source": src["description"],
                "source_type": src["source_type"], "era": era_str,
                "figures": _extract_figures(c), "chunk_index": idx,
            })

        n = insert_chunks(cur, vo, chunks, short_name)
        cur.execute("""
            INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (src["description"], short_name, src["source_type"], "english",
              src["translator"], era_list, src["reliability"], n))
        cur.connection.commit()
        total += n
        ingested.add(short_name)
        print(f"  {short_name}: {n} chunks ingested")
    return total


# ─── MAIN ──────────────────────────────────────────────────────────

def main():
    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    ingested = get_ingested_sources(cur)

    t0 = time.time()
    h = ingest_remaining_hadith(cur, vo, ingested)
    q = ingest_quran_translations(cur, vo, ingested)
    d = ingest_djvu_sources(cur, vo, ingested)
    elapsed = time.time() - t0

    cur.execute("SELECT source_type, COUNT(*), COUNT(DISTINCT source) FROM documents GROUP BY source_type ORDER BY COUNT(*) DESC;")
    print(f"\n{'='*60}")
    print(f"COMPLETE — {h + q + d} new chunks in {elapsed:.0f}s")
    print(f"{'='*60}")
    for r in cur.fetchall():
        print(f"  {r[0]:20s} {r[1]:>6} chunks  {r[2]:>3} sources")
    cur.execute("SELECT COUNT(*), SUM(word_count) FROM documents;")
    total_docs, total_words = cur.fetchone()
    print(f"\n  TOTAL: {total_docs} documents, {total_words:,} words")
    conn.close()


if __name__ == "__main__":
    main()
