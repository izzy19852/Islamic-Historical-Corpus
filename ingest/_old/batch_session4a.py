"""
Session 4A — Batch download + ingest for Andalusia, Ottoman, Caucasus, Resistance.
Downloads djvu.txt from Internet Archive, filters for readable text (>40% alpha),
writes YAML sidecars, then ingests via the standard pipeline.
"""
import os
import re
import sys
import time
import yaml
import psycopg2
import voyageai
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
SOURCES_BASE = os.path.join(os.path.dirname(__file__), '..', 'sources')

VOYAGE_BATCH_SIZE = 128
TARGET_CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_CHUNK_WORDS = 100

# ── Sources to download ──────────────────────────────────────────

SOURCES = [
    # Andalusia
    {
        "url": "https://archive.org/download/historyofmohamme01makkuoft/historyofmohamme01makkuoft_djvu.txt",
        "short_name": "al-maqqari-v1",
        "source": "Al-Maqqari, History of Mohammedan Dynasties in Spain Vol 1",
        "source_type": "primary_arabic",
        "era": "andalusia",
        "subdir": "andalusia",
    },
    {
        "url": "https://archive.org/download/historyofmohamme02makkuoft/historyofmohamme02makkuoft_djvu.txt",
        "short_name": "al-maqqari-v2",
        "source": "Al-Maqqari, History of Mohammedan Dynasties in Spain Vol 2",
        "source_type": "primary_arabic",
        "era": "andalusia",
        "subdir": "andalusia",
    },
    {
        "url": "https://archive.org/download/ringthedovetreat00ibnhuoft/ringthedovetreat00ibnhuoft_djvu.txt",
        "short_name": "ibn-hazm-ring-dove",
        "source": "Ibn Hazm, The Ring of the Dove (Cordoba, 11th century)",
        "source_type": "primary_arabic",
        "era": "andalusia",
        "subdir": "andalusia",
    },
    {
        "url": "https://archive.org/download/moriscosofspain00leah/moriscosofspain00leah_djvu.txt",
        "short_name": "lea-moriscos",
        "source": "Lea, The Moriscos of Spain (1901)",
        "source_type": "scholarly_western",
        "era": "andalusia",
        "subdir": "andalusia",
    },
    # Ottoman Extra
    {
        "url": "https://archive.org/download/historyofmehmedt00krit/historyofmehmedt00krit_djvu.txt",
        "short_name": "kritovoulos-mehmed",
        "source": "Kritovoulos, History of Mehmed the Conqueror (eyewitness, 1467)",
        "source_type": "primary_arabic",
        "era": "ottoman",
        "subdir": "ottoman_extra",
    },
    {
        "url": "https://archive.org/download/riseofottomanem00witt/riseofottomanem00witt_djvu.txt",
        "short_name": "wittek-ottoman-rise",
        "source": "Wittek, The Rise of the Ottoman Empire (1938)",
        "source_type": "scholarly_western",
        "era": "ottoman",
        "subdir": "ottoman_extra",
    },
    # Caucasus
    {
        "url": "https://archive.org/download/russianconquestof00badd/russianconquestof00badd_djvu.txt",
        "short_name": "baddeley-caucasus",
        "source": "Baddeley, The Russian Conquest of the Caucasus (1908)",
        "source_type": "scholarly_western",
        "era": "resistance_colonial",
        "subdir": "caucasus",
    },
    # Resistance / Colonial
    {
        "url": "https://archive.org/download/lifeofabdelkader00chur/lifeofabdelkader00chur_djvu.txt",
        "short_name": "churchill-abd-al-qadir",
        "source": "Churchill, The Life of Abdel-Kader (1867)",
        "source_type": "scholarly_western",
        "era": "resistance_colonial",
        "subdir": "resistance",
    },
    {
        "url": "https://archive.org/download/arabicthoughtinl0000hour/arabicthoughtinl0000hour_djvu.txt",
        "short_name": "hourani-arabic-thought",
        "source": "Hourani, Arabic Thought in the Liberal Age 1798-1939",
        "source_type": "scholarly_western",
        "era": "resistance_colonial",
        "subdir": "resistance",
    },
    {
        "url": "https://archive.org/download/reconstructionof00iqba/reconstructionof00iqba_djvu.txt",
        "short_name": "iqbal-reconstruction",
        "source": "Iqbal, The Reconstruction of Religious Thought in Islam",
        "source_type": "primary_arabic",
        "era": "resistance_colonial",
        "subdir": "resistance",
    },
    {
        "url": "https://archive.org/download/indianmusalmans00huntgoog/indianmusalmans00huntgoog_djvu.txt",
        "short_name": "hunter-indian-musalmans",
        "source": "Hunter, The Indian Musalmans (1871) — colonial perspective",
        "source_type": "scholarly_western",
        "era": "resistance_colonial",
        "subdir": "resistance",
        "extra_meta": {"bias_warning": "Written from British colonial perspective; use with critical awareness"},
    },
]

# ── Figure extraction ───────────────────────────────────────────��─

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
    "Abd al-Qadir", "Imam Shamil", "Mehmed", "Iqbal",
    "Ibn Hazm", "Abduh", "Afghani",
]
FIGURE_PATTERNS = [(fig, re.compile(re.escape(fig), re.IGNORECASE)) for fig in KNOWN_FIGURES]


def extract_figures(text):
    found = [fig for fig, pat in FIGURE_PATTERNS if pat.search(text)]
    return found if found else None


# ── Readable text filter ──────────────────────────────────────────

def is_readable(text, threshold=0.40):
    """Return True if >threshold of characters are alphabetic (filters OCR garbage)."""
    if not text:
        return False
    alpha = sum(1 for c in text if c.isalpha())
    total = len(text)
    ratio = alpha / total if total else 0
    return ratio > threshold


# ── Chunking ──────────────────────────────────────────────────────

def chunk_text(text, target=TARGET_CHUNK_WORDS, overlap=OVERLAP_WORDS, min_w=MIN_CHUNK_WORDS):
    """Sentence-boundary chunking with word-window fallback for non-punctuated text."""
    sentences = re.split(r'(?<=[.!?])\s+', text)
    sentences = [s.strip() for s in sentences if s.strip()]

    # If text has very few sentence breaks, fall back to word-window chunking
    if len(sentences) <= 3 and len(text.split()) > target:
        words = text.split()
        chunks = []
        i = 0
        while i < len(words):
            chunk_words = words[i:i + target]
            if len(chunk_words) >= min_w:
                chunks.append(' '.join(chunk_words))
            i += target - overlap
        return chunks

    chunks = []
    current = []
    current_wc = 0
    for sent in sentences:
        wc = len(sent.split())
        current.append(sent)
        current_wc += wc
        if current_wc >= target:
            ct = ' '.join(current)
            if len(ct.split()) >= min_w:
                chunks.append(ct)
            # overlap
            overlap_sents = []
            oc = 0
            for s in reversed(current):
                sw = len(s.split())
                if oc + sw > overlap:
                    break
                overlap_sents.insert(0, s)
                oc += sw
            current = overlap_sents
            current_wc = oc
    if current:
        ct = ' '.join(current)
        if len(ct.split()) >= min_w:
            chunks.append(ct)
    return chunks


# ── Embedding ─────────────────────────────────────────────────────

def embed_batch(vo, texts, input_type="document"):
    for attempt in range(5):
        try:
            result = vo.embed(texts, model="voyage-2", input_type=input_type)
            return result.embeddings
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "reduced rate" in err:
                wait = 25 * (attempt + 1)
                print(f"      Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif attempt < 4:
                wait = 2 ** (attempt + 1)
                print(f"      Voyage error: {e}, retry in {wait}s...")
                time.sleep(wait)
            else:
                raise


# ── Download ──────────────────────────────────────────────────────

def download_text(url, dest_path):
    print(f"    Downloading...")
    resp = requests.get(url, timeout=120)
    if resp.status_code != 200:
        print(f"    FAILED: HTTP {resp.status_code}")
        return False
    with open(dest_path, 'w', encoding='utf-8') as f:
        f.write(resp.text)
    wc = len(resp.text.split())
    print(f"    Downloaded: {wc:,} words")
    return True


# ── Main pipeline ─────────────────────────────────────────────────

def main():
    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Get already-ingested sources
    cur.execute("SELECT short_name FROM sources")
    ingested = {r[0] for r in cur.fetchall()}

    results = []
    t0 = time.time()

    for src in SOURCES:
        sn = src['short_name']
        print(f"\n{'='*60}")
        print(f"  [{sn}]")

        if sn in ingested:
            print(f"    Already in DB — skipping")
            results.append((sn, 0, "SKIPPED_EXISTS"))
            continue

        dest_dir = os.path.join(SOURCES_BASE, src['subdir'])
        os.makedirs(dest_dir, exist_ok=True)
        txt_path = os.path.join(dest_dir, f"{sn}.txt")
        yaml_path = os.path.join(dest_dir, f"{sn}.yaml")

        # Download if not on disk
        if not os.path.exists(txt_path):
            if not download_text(src['url'], txt_path):
                results.append((sn, 0, "DOWNLOAD_FAILED"))
                continue

        # Read text
        with open(txt_path, 'r', encoding='utf-8') as f:
            text = f.read()

        # Readable text filter
        if not is_readable(text):
            alpha_ratio = sum(1 for c in text if c.isalpha()) / max(len(text), 1)
            print(f"    FAILED readable filter: {alpha_ratio:.1%} alpha (need >40%)")
            os.remove(txt_path)
            results.append((sn, 0, "FAILED_READABLE_FILTER"))
            continue

        word_count = len(text.split())
        alpha_ratio = sum(1 for c in text if c.isalpha()) / max(len(text), 1)
        print(f"    {word_count:,} words, {alpha_ratio:.0%} alpha — PASS")

        # Chunk
        chunks = chunk_text(text)
        if not chunks:
            print(f"    No chunks produced")
            results.append((sn, 0, "NO_CHUNKS"))
            continue

        total_words = sum(len(c.split()) for c in chunks)
        all_figures = set()

        # Embed + insert
        inserted = 0
        for batch_start in range(0, len(chunks), VOYAGE_BATCH_SIZE):
            batch = chunks[batch_start:batch_start + VOYAGE_BATCH_SIZE]
            try:
                embeddings = embed_batch(vo, batch)
            except Exception as e:
                print(f"    Embedding failed: {e}")
                continue

            for idx, (chunk, emb) in enumerate(zip(batch, embeddings)):
                figures = extract_figures(chunk)
                if figures:
                    all_figures.update(figures)
                cur.execute("""
                    INSERT INTO documents (content, embedding, source, source_type, era, figures, chunk_index, word_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    chunk, str(emb), src['source'], src['source_type'], src['era'],
                    figures, batch_start + idx, len(chunk.split()),
                ))
                inserted += 1
            conn.commit()
            print(f"    Embedded {min(batch_start + len(batch), len(chunks))}/{len(chunks)} chunks")
            time.sleep(0.5)

        # Register source
        era_list = [src['era']]
        cur.execute("""
            INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (src['source'], sn, src['source_type'], 'english', None, era_list, 'scholarly', inserted))
        conn.commit()

        # Write YAML sidecar
        meta = {
            "source": src['source'],
            "short_name": sn,
            "source_type": src['source_type'],
            "era": era_list,
            "reliability": "scholarly",
            "word_count": total_words,
        }
        if src.get('extra_meta'):
            meta.update(src['extra_meta'])
        with open(yaml_path, 'w') as f:
            yaml.dump(meta, f, default_flow_style=False)

        # Delete text file after ingestion
        os.remove(txt_path)

        fig_str = f", figures: {sorted(all_figures)}" if all_figures else ""
        print(f"    Done: {inserted} chunks, {total_words:,} words{fig_str}")
        results.append((sn, inserted, "OK"))

    elapsed = time.time() - t0

    # Summary
    print(f"\n{'='*60}")
    print("SESSION 4A — BATCH INGESTION COMPLETE")
    print(f"{'='*60}")
    for name, chunks, status in results:
        print(f"  {name:<30} {chunks:>5} chunks  {status}")

    cur.execute("SELECT COUNT(*) FROM documents")
    print(f"\nTotal documents in DB: {cur.fetchone()[0]}")
    print(f"Time: {elapsed:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
