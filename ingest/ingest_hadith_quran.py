"""
Islam Stories RAG — Ingest Hadith (Fawazahmed0) + Quran translations (AlQuran.cloud)
"""

import os
import re
import sys
import time
import requests
import psycopg2
import voyageai
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")

VOYAGE_BATCH_SIZE = 128
DB_COMMIT_BATCH = 500

# Reuse figure list
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


def extract_figures(text):
    found = [fig for fig, pat in FIGURE_PATTERNS if pat.search(text)]
    return found if found else None


def embed_batch(vo, texts, input_type="document"):
    for attempt in range(5):
        try:
            result = vo.embed(texts, model="voyage-2", input_type=input_type)
            return result.embeddings
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "reduced rate" in err:
                wait = 25 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif attempt < 4:
                time.sleep(2 ** (attempt + 1))
            else:
                raise


def insert_chunks(cur, vo, chunks):
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
        if inserted % DB_COMMIT_BATCH == 0 or i + len(batch) >= total:
            cur.connection.commit()
        print(f"  Embedded {min(i + len(batch), total)}/{total}")
        time.sleep(0.5)
    return inserted


# ─── HADITH INGESTION ──────────────────────────────────────────────

HADITH_COLLECTIONS = {
    "eng-bukhari": "Sahih al-Bukhari",
    "eng-muslim": "Sahih Muslim",
    "eng-abudawud": "Sunan Abu Dawud",
    "eng-ibnmajah": "Sunan Ibn Majah",
    "eng-tirmidhi": "Jami at-Tirmidhi",
    "eng-nasai": "Sunan an-Nasai",
    "eng-nawawi40": "Nawawi 40 Hadith",
    "eng-riyadussalihin": "Riyad as-Salihin",
    "eng-bulughalmaram": "Bulugh al-Maram",
}

FAWAZ_BASE = "https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions"


def get_grade(grades):
    """Extract chain strength from grades array."""
    if not grades:
        return "unknown"
    for g in grades:
        name = g.get("name", "").lower()
        grade = g.get("grade", "").lower()
        if "sahih" in grade:
            return "sahih"
        if "hasan" in grade:
            return "hasan"
        if "daif" in grade or "da'if" in grade:
            return "daif"
    return "unknown"


def ingest_hadith(cur, vo):
    print("\n=== HADITH INGESTION (Fawazahmed0 API) ===")
    t0 = time.time()
    total_inserted = 0

    for collection_key, collection_name in HADITH_COLLECTIONS.items():
        print(f"\n  Fetching: {collection_name} ({collection_key})")
        url = f"{FAWAZ_BASE}/{collection_key}.min.json"

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  FAILED to fetch {collection_key}: {e}")
            continue

        hadiths = data.get("hadiths", [])
        print(f"  Hadiths fetched: {len(hadiths)}")

        if not hadiths:
            continue

        chunks = []
        for idx, h in enumerate(hadiths):
            text = h.get("text", "").strip()
            if not text or len(text) < 30:
                continue

            hadith_num = h.get("hadithnumber", idx + 1)
            content = f"[{collection_name}] Hadith {hadith_num}: {text}"
            figures = extract_figures(content)

            chunks.append({
                "content": content,
                "source": collection_name,
                "source_type": "hadith",
                "era": "rashidun",
                "figures": figures,
                "chunk_index": idx,
            })

        print(f"  Valid chunks: {len(chunks)}")
        inserted = insert_chunks(cur, vo, chunks)
        total_inserted += inserted

        # Register source
        cur.execute("""
            INSERT INTO sources (name, short_name, source_type, language, reliability, chunk_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (collection_name, collection_key, "hadith", "english", "sahih", inserted))
        cur.connection.commit()
        print(f"  {collection_name}: {inserted} inserted")

    elapsed = time.time() - t0
    print(f"\nHadith total: {total_inserted} chunks in {elapsed:.1f}s")
    return total_inserted


# ─── QURAN TRANSLATIONS ───────────────────────────────────────────

QURAN_TRANSLATIONS = {
    "en.pickthall": "Holy Quran (Pickthall translation)",
    "en.yusufali": "Holy Quran (Yusuf Ali translation)",
}
AYAHS_PER_CHUNK = 5


def ingest_quran_translations(cur, vo):
    print("\n=== QURAN TRANSLATIONS (AlQuran.cloud) ===")
    t0 = time.time()
    total_inserted = 0

    for edition, source_name in QURAN_TRANSLATIONS.items():
        print(f"\n  Fetching: {source_name}")

        # Check if already ingested
        cur.execute("SELECT COUNT(*) FROM sources WHERE short_name = %s", (edition,))
        if cur.fetchone()[0] > 0:
            print(f"  Already ingested, skipping")
            continue

        try:
            resp = requests.get(f"https://api.alquran.cloud/v1/quran/{edition}", timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  FAILED: {e}")
            continue

        surahs = data.get("data", {}).get("surahs", [])
        print(f"  Surahs fetched: {len(surahs)}")

        chunks = []
        chunk_idx = 0
        for surah in surahs:
            ayahs = surah.get("ayahs", [])
            surah_num = surah.get("number", "?")

            for i in range(0, len(ayahs), AYAHS_PER_CHUNK):
                group = ayahs[i:i + AYAHS_PER_CHUNK]
                lines = []
                for a in group:
                    num = a.get("numberInSurah", "?")
                    text = a.get("text", "").strip()
                    if text:
                        lines.append(f"Quran {surah_num}:{num} — {text}")
                if not lines:
                    continue

                content = "\n".join(lines)
                figures = extract_figures(content)
                chunks.append({
                    "content": content,
                    "source": source_name,
                    "source_type": "quran",
                    "era": "rashidun",
                    "figures": figures,
                    "chunk_index": chunk_idx,
                })
                chunk_idx += 1

        print(f"  Chunks: {len(chunks)}")
        inserted = insert_chunks(cur, vo, chunks)
        total_inserted += inserted

        cur.execute("""
            INSERT INTO sources (name, short_name, source_type, language, reliability, chunk_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (source_name, edition, "quran", "english", "sahih", inserted))
        cur.connection.commit()
        print(f"  {source_name}: {inserted} inserted")

    elapsed = time.time() - t0
    print(f"\nQuran translations total: {total_inserted} chunks in {elapsed:.1f}s")
    return total_inserted


# ─── MAIN ──────────────────────────────────────────────────────────

def main():
    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    hadith_count = ingest_hadith(cur, vo)
    quran_count = ingest_quran_translations(cur, vo)

    cur.execute("SELECT source_type, COUNT(*), COUNT(DISTINCT source) FROM documents GROUP BY source_type;")
    print(f"\n{'='*50}")
    print("FINAL COUNTS")
    print(f"{'='*50}")
    for r in cur.fetchall():
        print(f"  {r[0]:20s} {r[1]:>6} chunks  {r[2]:>3} sources")

    conn.close()
    print(f"\nNew: {hadith_count} hadith + {quran_count} quran chunks")


if __name__ == "__main__":
    main()
