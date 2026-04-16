"""
Islam Stories — Shared ingestion utilities.

All ingest scripts import from here instead of re-implementing the same
clean/chunk/embed/insert logic. One place to update figures, chunking
parameters, embedding model, or DB schema.
"""

import os
import re
import time
import subprocess
from collections import Counter
from pathlib import Path

import requests
import psycopg2
import voyageai
from dotenv import load_dotenv

# ── Environment ──────────────────────────────────────────────────────

_ENV_PATH = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(_ENV_PATH)

DB_URL = os.getenv('ISLAM_STORIES_DB_URL')
VOYAGE_API_KEY = os.getenv('VOYAGE_API_KEY')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
SOURCES_DIR = Path(os.path.dirname(__file__)).parent / "sources"

# ── Chunking parameters ─────────────────────────────────────────────

TARGET_CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_CHUNK_WORDS = 100
VOYAGE_BATCH_SIZE = 128
DB_COMMIT_BATCH = 500

# ── Known Islamic figures (superset from all scripts) ────────────────

KNOWN_FIGURES = [
    # Rashidun Companions
    "Abu Bakr", "Abu Bakr al-Siddiq", "Umar ibn al-Khattab", "Umar ibn Khattab",
    "Uthman ibn Affan", "Ali ibn Abi Talib",
    "Aisha", "Khadijah", "Khadija", "Fatimah", "Fatima",
    "Khalid ibn Walid", "Bilal ibn Rabah", "Bilal",
    "Salman al-Farisi", "Salman al-Farsi",
    "Abu Hurairah", "Ibn Abbas", "Ibn Umar", "Anas ibn Malik",
    "Abu Musa al-Ashari", "Muadh ibn Jabal", "Zayd ibn Thabit", "Nusayba",
    "Abu Dharr", "Amr ibn al-As", "Sad ibn Abi Waqqas", "Usamah ibn Zayd",
    "Hamza ibn Abd al-Muttalib", "Hamza",
    "Zubayr ibn al-Awwam", "Zubayr", "Talha ibn Ubaydullah", "Talha",
    "Abu Ubayda", "Abu Sufyan", "Abu Talib", "Zayd", "Usama",
    # Common short names
    "Umar", "Uthman", "Ali", "Husayn", "Hassan",
    "Prophet", "Messenger of Allah",
    # Umayyad / Abbasid
    "Muawiyah", "Husayn ibn Ali", "Hassan ibn Ali",
    "Tariq ibn Ziyad", "Umar ibn Abd al-Aziz", "Umar II",
    "Al-Hajjaj", "Abd al-Rahman",
    "Harun al-Rashid", "Al-Ma'mun",
    # Crusades / Medieval
    "Saladin", "Baybars", "Nur al-Din",
    "Ibn Khaldun", "Ibn Battuta", "Ibn Sina", "Ibn Rushd",
    "Al-Ghazali", "Ibn Jubayr", "Nawawi", "Ibn Hazm",
    # Persia / Caucasus / Timur
    "Firdausi", "Nizami", "Timur", "Tamerlane", "Genghis Khan", "Hulagu",
    "Shah Ismail", "Shah Abbas", "Nader Shah", "Imam Shamil",
    "Kazi Mullah", "Ghazi Muhammad", "Rustam", "Sohrab", "Khosrow",
    "Mahmud of Ghazni", "Rumi", "Hafiz", "Saadi",
    # Ottoman / Colonial / Modern
    "Muhammad al-Fatih", "Mehmed", "Mehmed II", "Constantine", "Constantinople",
    "Mansa Musa", "Tipu Sultan", "Omar al-Mukhtar",
    "Abd al-Qadir", "Abduh", "Afghani", "Iqbal",
]

# Deduplicate while preserving order
_seen = set()
_unique = []
for _f in KNOWN_FIGURES:
    if _f not in _seen:
        _seen.add(_f)
        _unique.append(_f)
KNOWN_FIGURES = _unique

FIGURE_PATTERNS = [(fig, re.compile(re.escape(fig), re.IGNORECASE)) for fig in KNOWN_FIGURES]

# Arabic letter set for multilingual text cleaning
_ARABIC_CHARS = 'ابتثجحخدذرزسشصضطظعغفقكلمنهويةءآأإؤئ'


# ═══════════════════════════════════════════════════════════════════
# FIGURE EXTRACTION
# ═══════════════════════════════════════════════════════════════════

def extract_figures(text):
    """Extract known Islamic figure names from text."""
    found = [fig for fig, pat in FIGURE_PATTERNS if pat.search(text)]
    return found if found else None


# ═══════════════════════════════════════════════════════════════════
# TEXT CLEANING
# ═══════════════════════════════════════════════════════════════════

def detect_repeated_headers(lines, threshold=30, max_len=100):
    """Detect header/footer strings that repeat on many pages."""
    short_lines = [l.strip() for l in lines if 5 < len(l.strip()) < max_len]
    counts = Counter(short_lines)
    return {line for line, count in counts.items() if count >= threshold}


def clean_djvu_text(text, alpha_threshold=0.40, multilingual=False):
    """Clean DjVu-extracted text: strip page numbers, OCR noise, form-feeds."""
    text = re.sub(r'\x0c', '\n', text)
    text = re.sub(r'(?m)^\s*\d{1,4}\s*$', '', text)

    lines = text.split('\n')
    repeated = detect_repeated_headers(lines)
    cleaned = []

    for line in lines:
        s = line.strip()
        if not s:
            if cleaned and cleaned[-1] != '':
                cleaned.append('')
            continue
        if s in repeated:
            continue
        # Roman numeral page numbers
        if re.match(r'^[ivxlcdm]+$', s, re.IGNORECASE) and len(s) < 10:
            continue
        # Very short lines (headers/footers/noise)
        if len(s) < 15:
            continue
        # Alpha ratio check
        if multilingual:
            alpha = sum(c.isalpha() or c in _ARABIC_CHARS for c in s)
        else:
            alpha = sum(c.isalpha() for c in s)
        if alpha / max(len(s), 1) > alpha_threshold:
            cleaned.append(line)

    text = '\n'.join(cleaned)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]{3,}', '  ', text)
    return text.strip()


def clean_pdf_text(raw_text):
    """Clean extracted PDF text: remove headers, footers, page numbers, noise."""
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


def strip_gutenberg(text):
    """Strip Project Gutenberg header/footer markers."""
    for marker in ["*** START OF", "***START OF"]:
        idx = text.find(marker)
        if idx != -1:
            nl = text.index('\n', idx)
            text = text[nl + 1:]
            break
    for marker in ["*** END OF", "***END OF"]:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx]
            break
    return text.strip()


def is_readable(text, threshold=0.40):
    """Return True if enough characters are alphabetic (filters OCR garbage)."""
    if not text:
        return False
    alpha = sum(1 for c in text if c.isalpha())
    return (alpha / len(text) if text else 0) > threshold


# ═══════════════════════════════════════════════════════════════════
# PDF TEXT EXTRACTION
# ═══════════════════════════════════════════════════════════════════

def extract_pdf_text(pdf_path):
    """Extract text from PDF. Tries PyMuPDF first, then pdftotext."""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(str(pdf_path))
        all_text = []
        for page in doc:
            text = page.get_text("text")
            if text:
                all_text.append(text)
        doc.close()
        raw = '\n'.join(all_text)
        if len(raw.split()) > 100:
            return clean_pdf_text(raw)
    except ImportError:
        pass
    except Exception:
        pass

    # Fallback: pdftotext CLI
    try:
        result = subprocess.run(
            ['pdftotext', '-layout', str(pdf_path), '-'],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode == 0 and len(result.stdout) > 1000:
            return clean_pdf_text(result.stdout)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    return ""


# ═══════════════════════════════════════════════════════════════════
# CHUNKING
# ═══════════════════════════════════════════════════════════════════

def chunk_text(text, target=TARGET_CHUNK_WORDS, overlap=OVERLAP_WORDS, min_words=MIN_CHUNK_WORDS):
    """
    Split text into overlapping chunks. Uses sentence boundaries when possible,
    falls back to word-window chunking for non-punctuated text (Arabic, OCR, etc).
    """
    sentences = re.split(r'(?<=[.!?])\s+', text)
    sentences = [s.strip() for s in sentences if s.strip()]

    # If text has very few sentence breaks, fall back to word-window
    if len(sentences) <= 3 and len(text.split()) > target:
        return _chunk_by_words(text, target, overlap, min_words)

    chunks = []
    current = []
    current_wc = 0

    for sent in sentences:
        wc = len(sent.split())
        current.append(sent)
        current_wc += wc

        if current_wc >= target:
            chunk = ' '.join(current)
            if len(chunk.split()) >= min_words:
                chunks.append(chunk)

            # Keep overlap sentences
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

    # Final chunk
    if current:
        chunk = ' '.join(current)
        if len(chunk.split()) >= min_words:
            chunks.append(chunk)

    return chunks


def _chunk_by_words(text, target, overlap, min_words):
    """Simple word-window chunker for text without sentence boundaries."""
    words = text.split()
    chunks = []
    i = 0
    while i < len(words):
        chunk_words = words[i:i + target]
        if len(chunk_words) >= min_words:
            chunks.append(' '.join(chunk_words))
        i += target - overlap
    return chunks


# ═══════════════════════════════════════════════════════════════════
# EMBEDDING
# ═══════════════════════════════════════════════════════════════════

def get_voyage_client():
    """Create a Voyage AI client."""
    return voyageai.Client(api_key=VOYAGE_API_KEY)


def embed_batch(vo, texts, model="voyage-2", input_type="document"):
    """Embed a batch of texts with exponential backoff and batch-splitting."""
    for attempt in range(5):
        try:
            result = vo.embed(texts, model=model, input_type=input_type)
            return result.embeddings
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "reduced rate" in err:
                wait = 25 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif "max allowed tokens" in err and len(texts) > 1:
                # Split batch in half and retry
                mid = len(texts) // 2
                left = embed_batch(vo, texts[:mid], model, input_type)
                right = embed_batch(vo, texts[mid:], model, input_type)
                return left + right
            elif attempt < 4:
                wait = 2 ** (attempt + 1)
                print(f"    Voyage error: {e}, retry in {wait}s...")
                time.sleep(wait)
            else:
                raise


# ═══════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════

def get_db_connection():
    """Create a new database connection."""
    return psycopg2.connect(DB_URL)


def already_ingested(cur, source_name, min_chunks=50):
    """Check if a source is already in the documents table."""
    cur.execute("SELECT COUNT(*) FROM documents WHERE source = %s", (source_name,))
    return cur.fetchone()[0] >= min_chunks


def get_ingested_sources(cur):
    """Get set of short_names already in the sources table."""
    cur.execute("SELECT short_name FROM sources")
    return {row[0] for row in cur.fetchall()}


def insert_chunks(cur, vo, chunks, model="voyage-2", batch_size=None, label=""):
    """
    Embed and insert a list of chunk dicts into the documents table.

    Each chunk dict should have:
        content, source, source_type, era, figures (optional), chunk_index (optional)
    """
    if batch_size is None:
        batch_size = VOYAGE_BATCH_SIZE

    total = len(chunks)
    inserted = 0

    for i in range(0, total, batch_size):
        batch = chunks[i:i + batch_size]
        texts = [c["content"] for c in batch]

        try:
            embeddings = embed_batch(vo, texts, model=model)
        except Exception as e:
            print(f"    Skipping batch {i}: {e}")
            continue

        for c, emb in zip(batch, embeddings):
            cur.execute("""
                INSERT INTO documents
                    (content, embedding, source, source_type, era, figures, chunk_index, word_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                c["content"], str(emb), c["source"], c["source_type"],
                c.get("era"), c.get("figures"), c.get("chunk_index"),
                len(c["content"].split()),
            ))
            inserted += 1

        if inserted % DB_COMMIT_BATCH == 0 or i + len(batch) >= total:
            cur.connection.commit()

        done = min(i + len(batch), total)
        print(f"    {label} {done}/{total} chunks embedded", flush=True)
        time.sleep(0.3)

    return inserted


def register_source(cur, name, short_name, source_type, language="english",
                    translator=None, era_coverage=None, reliability=None, chunk_count=0):
    """Register a source in the sources table (idempotent)."""
    cur.execute("SELECT 1 FROM sources WHERE short_name = %s", (short_name,))
    if cur.fetchone():
        return
    cur.execute("""
        INSERT INTO sources (name, short_name, source_type, language, translator,
                             era_coverage, reliability, chunk_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (name, short_name, source_type, language, translator,
          era_coverage, reliability, chunk_count))
    cur.connection.commit()


def print_corpus_summary(cur):
    """Print a summary of what's in the database."""
    cur.execute("""
        SELECT source_type, COUNT(*), COUNT(DISTINCT source)
        FROM documents GROUP BY source_type ORDER BY COUNT(*) DESC
    """)
    print(f"\n{'='*60}")
    print("CORPUS SUMMARY")
    print(f"{'='*60}")
    for row in cur.fetchall():
        print(f"  {row[0]:20s} {row[1]:>6} chunks  {row[2]:>3} sources")

    cur.execute("SELECT COUNT(*), COALESCE(SUM(word_count), 0) FROM documents")
    total_docs, total_words = cur.fetchone()
    cur.execute("SELECT COUNT(*) FROM sources")
    total_sources = cur.fetchone()[0]
    print(f"\n  TOTAL: {total_docs:,} documents, {total_words:,} words, {total_sources} sources")
    print(f"{'='*60}")
