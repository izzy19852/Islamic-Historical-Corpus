"""
Islam Stories RAG — Ingest from Quran.com and Sunnah.com APIs
"""

import os
import re
import sys
import time
import json
import requests
import psycopg2
import psycopg2.extras
import voyageai
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
SUNNAH_API_SECRET = os.getenv("SUNNAH_API_SECRET", "")

VOYAGE_BATCH_SIZE = 128
DB_COMMIT_BATCH = 500

# Known Islamic figures for extraction
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
    # Additional common names/variants
    "Prophet", "Messenger of Allah", "Abu Bakr al-Siddiq",
    "Umar", "Ali", "Khadija", "Hamza", "Abu Talib",
    "Abu Sufyan", "Zayd", "Usama",
]

# Pre-compile patterns for figure extraction (case-insensitive)
FIGURE_PATTERNS = [(fig, re.compile(re.escape(fig), re.IGNORECASE)) for fig in KNOWN_FIGURES]


def extract_figures(text: str) -> list[str]:
    """Extract known Islamic figure names from text."""
    found = []
    for fig_name, pattern in FIGURE_PATTERNS:
        if pattern.search(text):
            found.append(fig_name)
    return found if found else None


def strip_html(text: str) -> str:
    """Remove HTML tags from translation text."""
    return re.sub(r'<[^>]+>', '', text).strip()


def embed_batch(vo: voyageai.Client, texts: list[str], input_type: str = "document") -> list[list[float]]:
    """Embed a batch of texts using Voyage AI with retry and rate-limit handling."""
    for attempt in range(5):
        try:
            result = vo.embed(texts, model="voyage-2", input_type=input_type)
            return result.embeddings
        except Exception as e:
            err_str = str(e)
            if "rate limit" in err_str.lower() or "reduced rate" in err_str.lower():
                wait = 25 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
            elif attempt < 4:
                wait = 2 ** (attempt + 1)
                print(f"  Voyage API error: {e}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Voyage API failed after 5 attempts: {e}")
                raise


def insert_chunks(cur, chunks: list[dict], vo: voyageai.Client):
    """Embed and insert chunks into the documents table."""
    total = len(chunks)
    inserted = 0

    for batch_start in range(0, total, VOYAGE_BATCH_SIZE):
        batch = chunks[batch_start:batch_start + VOYAGE_BATCH_SIZE]
        texts = [c["content"] for c in batch]

        try:
            embeddings = embed_batch(vo, texts)
        except Exception as e:
            print(f"  Skipping batch {batch_start}-{batch_start+len(batch)}: {e}")
            continue

        for chunk, emb in zip(batch, embeddings):
            try:
                cur.execute("""
                    INSERT INTO documents (content, embedding, source, source_type, era, figures, chunk_index, word_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    chunk["content"],
                    str(emb),
                    chunk["source"],
                    chunk["source_type"],
                    chunk.get("era"),
                    chunk.get("figures"),
                    chunk.get("chunk_index"),
                    len(chunk["content"].split()),
                ))
                inserted += 1
            except Exception as e:
                print(f"  DB insert error: {e}")

        # Commit periodically
        if inserted % DB_COMMIT_BATCH == 0 or batch_start + len(batch) >= total:
            cur.connection.commit()

        print(f"  Embedded {min(batch_start + len(batch), total)}/{total} chunks")

        # Brief pause between batches
        time.sleep(0.5)

    return inserted


# ─── QURAN.COM INGESTION ───────────────────────────────────────────

QURAN_BASE = "https://api.quran.com/api/v4"
QURAN_TRANSLATION_ID = 85  # Abdel Haleem (Asad not available)
QURAN_AYAHS_PER_CHUNK = 5


def fetch_quran_chapters() -> list[dict]:
    """Fetch list of all 114 surahs."""
    resp = requests.get(f"{QURAN_BASE}/chapters", timeout=30)
    resp.raise_for_status()
    return resp.json()["chapters"]


def fetch_quran_verses(chapter_id: int) -> list[dict]:
    """Fetch all verses for a chapter with translation."""
    verses = []
    page = 1
    while True:
        resp = requests.get(
            f"{QURAN_BASE}/verses/by_chapter/{chapter_id}",
            params={
                "translations": QURAN_TRANSLATION_ID,
                "per_page": 50,
                "page": page,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        verses.extend(data["verses"])
        pagination = data.get("pagination", {})
        if pagination.get("next_page") is None:
            break
        page = pagination["next_page"]
    return verses


def ingest_quran(cur, vo):
    """Ingest Quran translations grouped into 5-ayah chunks."""
    print("\n=== Ingesting Quran (Abdel Haleem translation) ===")
    t0 = time.time()

    chapters = fetch_quran_chapters()
    all_chunks = []
    chunk_idx = 0

    for ch in chapters:
        ch_id = ch["id"]
        ch_name = ch["name_simple"]
        verses = fetch_quran_verses(ch_id)

        # Group verses into chunks of QURAN_AYAHS_PER_CHUNK
        for i in range(0, len(verses), QURAN_AYAHS_PER_CHUNK):
            group = verses[i:i + QURAN_AYAHS_PER_CHUNK]
            lines = []
            for v in group:
                verse_key = v["verse_key"]
                trans_text = ""
                for t in v.get("translations", []):
                    trans_text = strip_html(t.get("text", ""))
                    break
                if trans_text:
                    lines.append(f"Quran {verse_key} — {trans_text}")

            if not lines:
                continue

            content = "\n".join(lines)
            figures = extract_figures(content)

            all_chunks.append({
                "content": content,
                "source": "Holy Quran (Abdel Haleem translation)",
                "source_type": "quran",
                "era": "rashidun",
                "figures": figures,
                "chunk_index": chunk_idx,
            })
            chunk_idx += 1

        if ch_id % 10 == 0:
            print(f"  Fetched {ch_id}/114 surahs ({ch_name})...")

    print(f"  Total Quran chunks: {len(all_chunks)}")
    inserted = insert_chunks(cur, all_chunks, vo)
    elapsed = time.time() - t0
    print(f"  Quran ingestion complete: {inserted} chunks in {elapsed:.1f}s")

    # Register source
    cur.execute("""
        INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        "Holy Quran (Abdel Haleem translation)", "quran-haleem",
        "quran", "english", "M.A.S. Abdel Haleem",
        ["rashidun"], "sahih", inserted,
    ))
    cur.connection.commit()
    return inserted


# ─── SUNNAH.COM INGESTION ──────────────────────────────────────────

SUNNAH_BASE = "https://api.sunnah.com/v1"
SUNNAH_COLLECTIONS = {
    "bukhari": "Sahih al-Bukhari",
    "muslim": "Sahih Muslim",
    "abudawud": "Sunan Abu Dawud",
    "ibnmajah": "Sunan Ibn Majah",
    "tirmidhi": "Jami at-Tirmidhi",
}


def fetch_hadiths(collection: str, page: int, limit: int = 50) -> dict:
    """Fetch a page of hadiths from Sunnah.com API."""
    resp = requests.get(
        f"{SUNNAH_BASE}/collections/{collection}/hadiths",
        params={"page": page, "limit": limit},
        headers={"x-aws-secret": SUNNAH_API_SECRET},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def ingest_sunnah(cur, vo):
    """Ingest hadith collections from Sunnah.com."""
    if not SUNNAH_API_SECRET or SUNNAH_API_SECRET == "YOUR_SUNNAH_API_SECRET_HERE":
        print("\n=== Skipping Sunnah.com — no API key configured ===")
        print("  Set SUNNAH_API_SECRET in .env to enable hadith ingestion.")
        return 0

    print("\n=== Ingesting Hadith collections from Sunnah.com ===")
    t0 = time.time()
    total_inserted = 0

    for collection_key, collection_name in SUNNAH_COLLECTIONS.items():
        print(f"\n  Collection: {collection_name}")
        all_chunks = []
        page = 1
        chunk_idx = 0

        while True:
            try:
                data = fetch_hadiths(collection_key, page)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    break
                print(f"  HTTP error on page {page}: {e}")
                break
            except Exception as e:
                print(f"  Error fetching page {page}: {e}")
                break

            hadiths = data.get("data", [])
            if not hadiths:
                break

            for h in hadiths:
                # Get English text
                english_text = ""
                for lang_data in h.get("hadith", []):
                    if lang_data.get("lang") == "en":
                        english_text = lang_data.get("body", "")
                        break

                if not english_text or len(english_text.strip()) < 20:
                    continue

                english_text = strip_html(english_text)
                hadith_num = h.get("hadithNumber", "?")
                book_num = h.get("bookNumber", "?")

                content = f"[{collection_name}] Book {book_num}, Hadith {hadith_num}: {english_text}"
                figures = extract_figures(content)

                all_chunks.append({
                    "content": content,
                    "source": collection_name,
                    "source_type": "hadith",
                    "era": "rashidun",
                    "figures": figures,
                    "chunk_index": chunk_idx,
                })
                chunk_idx += 1

            total_pages = data.get("total", 0) // data.get("limit", 50) + 1
            if page % 10 == 0:
                print(f"    Page {page}/{total_pages}...")

            if data.get("next") is None and page >= total_pages:
                break
            page += 1
            time.sleep(0.2)  # Rate limiting

        print(f"    Chunks from {collection_name}: {len(all_chunks)}")
        inserted = insert_chunks(cur, all_chunks, vo)
        total_inserted += inserted

        # Register source
        cur.execute("""
            INSERT INTO sources (name, short_name, source_type, language, era_coverage, reliability, chunk_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            collection_name, collection_key,
            "hadith", "english",
            ["rashidun"], "sahih", inserted,
        ))
        cur.connection.commit()

    elapsed = time.time() - t0
    print(f"\n  Sunnah ingestion complete: {total_inserted} hadiths in {elapsed:.1f}s")
    return total_inserted


# ─── MAIN ──────────────────────────────────────────────────────────

def main():
    if not DB_URL:
        print("ERROR: ISLAM_STORIES_DB_URL not set")
        sys.exit(1)
    if not VOYAGE_API_KEY:
        print("ERROR: VOYAGE_API_KEY not set")
        sys.exit(1)

    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    t_start = time.time()

    quran_count = ingest_quran(cur, vo)
    sunnah_count = ingest_sunnah(cur, vo)

    # Final counts
    cur.execute("SELECT COUNT(*) FROM documents;")
    total_docs = cur.fetchone()[0]

    cur.execute("SELECT source, COUNT(*) FROM documents GROUP BY source ORDER BY COUNT(*) DESC;")
    per_source = cur.fetchall()

    conn.close()

    elapsed = time.time() - t_start
    print(f"\n{'='*50}")
    print(f"INGESTION COMPLETE")
    print(f"{'='*50}")
    print(f"Quran chunks:  {quran_count}")
    print(f"Sunnah chunks: {sunnah_count}")
    print(f"Total rows in documents table: {total_docs}")
    print(f"Time taken: {elapsed:.1f}s")
    print(f"\nPer source:")
    for source, count in per_source:
        print(f"  {source}: {count}")


if __name__ == "__main__":
    main()
