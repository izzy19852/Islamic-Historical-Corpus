"""
Islam Stories RAG — Ingest text files from ~/islam-stories/rag/sources/
Designed to be re-run as new sources are added (skips already-ingested files).
"""

import os
import re
import sys
import time
import yaml
import psycopg2
import voyageai
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
SOURCES_DIR = os.path.join(os.path.dirname(__file__), '..', 'sources')

VOYAGE_BATCH_SIZE = 128
TARGET_CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_CHUNK_WORDS = 100

# Reuse figure extraction from ingest_apis
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


def extract_figures(text: str) -> list[str] | None:
    found = [fig for fig, pat in FIGURE_PATTERNS if pat.search(text)]
    return found if found else None


def parse_frontmatter(text: str) -> tuple[dict, str]:
    """Extract YAML frontmatter and return (metadata, body)."""
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n', text, re.DOTALL)
    if match:
        try:
            meta = yaml.safe_load(match.group(1))
            body = text[match.end():]
            return meta or {}, body
        except yaml.YAMLError:
            pass
    return {}, text


def split_into_sentences(text: str) -> list[str]:
    """Split text into sentences at period/question/exclamation boundaries."""
    sentences = re.split(r'(?<=[.!?])\s+', text)
    return [s.strip() for s in sentences if s.strip()]


def chunk_text(text: str, target_words=TARGET_CHUNK_WORDS,
               overlap_words=OVERLAP_WORDS, min_words=MIN_CHUNK_WORDS) -> list[str]:
    """Split text into overlapping chunks at sentence boundaries."""
    sentences = split_into_sentences(text)
    chunks = []
    current_sentences = []
    current_word_count = 0

    for sentence in sentences:
        word_count = len(sentence.split())
        current_sentences.append(sentence)
        current_word_count += word_count

        if current_word_count >= target_words:
            chunk_text = ' '.join(current_sentences)
            if len(chunk_text.split()) >= min_words:
                chunks.append(chunk_text)

            # Calculate overlap: keep last sentences up to overlap_words
            overlap_sentences = []
            overlap_count = 0
            for s in reversed(current_sentences):
                s_words = len(s.split())
                if overlap_count + s_words > overlap_words:
                    break
                overlap_sentences.insert(0, s)
                overlap_count += s_words

            current_sentences = overlap_sentences
            current_word_count = overlap_count

    # Final chunk
    if current_sentences:
        chunk_text = ' '.join(current_sentences)
        if len(chunk_text.split()) >= min_words:
            chunks.append(chunk_text)

    return chunks


def embed_batch(vo: voyageai.Client, texts: list[str], input_type="document") -> list[list[float]]:
    for attempt in range(5):
        try:
            result = vo.embed(texts, model="voyage-2", input_type=input_type)
            return result.embeddings
        except Exception as e:
            err_str = str(e).lower()
            if "rate limit" in err_str or "reduced rate" in err_str:
                wait = 25 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif attempt < 4:
                wait = 2 ** (attempt + 1)
                print(f"  Voyage error: {e}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def get_ingested_sources(cur) -> set[str]:
    """Get set of short_names already ingested."""
    cur.execute("SELECT short_name FROM sources;")
    return {row[0] for row in cur.fetchall()}


def ingest_file(filepath: str, cur, vo, ingested: set[str]) -> dict:
    """Ingest a single text file. Returns stats dict."""
    filename = os.path.basename(filepath)
    print(f"\n  Processing: {filename}")

    with open(filepath, 'r', encoding='utf-8') as f:
        raw_text = f.read()

    # Check for YAML sidecar file first, then inline frontmatter
    yaml_sidecar = filepath.replace('.txt', '.yaml')
    if os.path.exists(yaml_sidecar):
        with open(yaml_sidecar, 'r') as yf:
            meta = yaml.safe_load(yf) or {}
        body = raw_text  # No frontmatter to strip
    else:
        meta, body = parse_frontmatter(raw_text)

    source = meta.get('source', filename.replace('.txt', '').replace('_', ' ').title())
    short_name = meta.get('short_name', filename.replace('.txt', '').lower())
    source_type = meta.get('source_type', 'secondary')
    era = meta.get('era', None)
    translator = meta.get('translator', None)
    reliability = meta.get('reliability', None)

    # Handle era as list or string
    era_list = era if isinstance(era, list) else ([era] if era else [])
    era_str = era_list[0] if era_list else None

    # Check if already ingested
    if short_name in ingested:
        print(f"    Skipping {short_name} — already ingested")
        return {"filename": filename, "chunks": 0, "words": 0, "figures": [], "skipped": True}

    # Chunk the text
    chunks = chunk_text(body)
    if not chunks:
        print(f"    No chunks produced from {filename}")
        return {"filename": filename, "chunks": 0, "words": 0, "figures": []}

    total_words = sum(len(c.split()) for c in chunks)
    all_figures = set()

    # Embed and insert in batches
    inserted = 0
    for batch_start in range(0, len(chunks), VOYAGE_BATCH_SIZE):
        batch = chunks[batch_start:batch_start + VOYAGE_BATCH_SIZE]

        try:
            embeddings = embed_batch(vo, batch)
        except Exception as e:
            print(f"    Embedding failed for batch: {e}")
            continue

        for idx, (chunk, emb) in enumerate(zip(batch, embeddings)):
            figures = extract_figures(chunk)
            if figures:
                all_figures.update(figures)

            cur.execute("""
                INSERT INTO documents (content, embedding, source, source_type, era, figures, chunk_index, word_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                chunk, str(emb), source, source_type, era_str,
                figures, batch_start + idx, len(chunk.split()),
            ))
            inserted += 1

        cur.connection.commit()
        print(f"    Embedded {min(batch_start + len(batch), len(chunks))}/{len(chunks)} chunks")
        time.sleep(0.5)

    # Register source
    cur.execute("""
        INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (source, short_name, source_type, 'english', translator, era_list or None, reliability, inserted))
    cur.connection.commit()

    stats = {
        "filename": filename,
        "chunks": inserted,
        "words": total_words,
        "figures": sorted(all_figures),
    }
    print(f"    Done: {inserted} chunks, {total_words} words, figures: {sorted(all_figures)}")
    return stats


def main():
    if not DB_URL or not VOYAGE_API_KEY:
        print("ERROR: ISLAM_STORIES_DB_URL or VOYAGE_API_KEY not set")
        sys.exit(1)

    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    ingested = get_ingested_sources(cur)

    # Accept optional directory argument
    sources_dir = sys.argv[1] if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]) else SOURCES_DIR

    # Find text files in sources directory
    text_files = sorted([
        os.path.join(sources_dir, f)
        for f in os.listdir(sources_dir)
        if f.endswith(('.txt', '.md')) and not f.startswith('.')
    ]) if os.path.isdir(sources_dir) else []

    if not text_files:
        print("No text files found in rag/sources/")
        print("Place .txt files there (optionally with YAML frontmatter) and re-run.")
        conn.close()
        return

    print(f"Found {len(text_files)} text file(s) to process")
    t0 = time.time()
    all_stats = []

    for filepath in text_files:
        stats = ingest_file(filepath, cur, vo, ingested)
        all_stats.append(stats)

    elapsed = time.time() - t0

    # Summary
    print(f"\n{'='*50}")
    print("TEXT INGESTION COMPLETE")
    print(f"{'='*50}")
    for s in all_stats:
        status = "SKIPPED" if s.get("skipped") else f"{s['chunks']} chunks, {s['words']} words"
        print(f"  {s['filename']}: {status}")
        if s.get('figures'):
            print(f"    Figures: {', '.join(s['figures'])}")
    print(f"\nTime: {elapsed:.1f}s")

    cur.execute("SELECT COUNT(*) FROM documents;")
    print(f"Total documents in DB: {cur.fetchone()[0]}")
    conn.close()


if __name__ == "__main__":
    main()
