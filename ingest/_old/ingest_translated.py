"""
Ingest foreign-language sources via Claude translation.
Downloads French (Ibn Hazm) and Greek/Latin (Kritovoulos) texts from Internet Archive,
translates chunk-by-chunk with Claude Haiku, embeds with Voyage, inserts to pgvector.
"""
import os
import re
import sys
import time
import yaml
import psycopg2
import voyageai
import requests
import anthropic
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SOURCES_BASE = os.path.join(os.path.dirname(__file__), '..', 'sources')

VOYAGE_BATCH_SIZE = 64  # conservative to avoid token limits
TARGET_CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_CHUNK_WORDS = 100

# ── Figure extraction ────────────────────────────────────────────

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
    "Constantine", "Constantinople", "Mehmed II",
]
FIGURE_PATTERNS = [(fig, re.compile(re.escape(fig), re.IGNORECASE)) for fig in KNOWN_FIGURES]


def extract_figures(text):
    found = [fig for fig, pat in FIGURE_PATTERNS if pat.search(text)]
    return found if found else None


# ── Text quality filter ──────────────────────────────────────────

def is_readable(text, threshold=0.40):
    if not text:
        return False
    alpha = sum(1 for c in text if c.isalpha())
    total = len(text)
    return (alpha / total if total else 0) > threshold


# ── Chunking ─────────────────────────────────────────────────────

def chunk_text(text, target=TARGET_CHUNK_WORDS, overlap=OVERLAP_WORDS, min_w=MIN_CHUNK_WORDS):
    sentences = re.split(r'(?<=[.!?])\s+', text)
    sentences = [s.strip() for s in sentences if s.strip()]

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


# ── Embedding ────────────────────────────────────────────────────

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
            elif "max allowed tokens" in err:
                # split batch in half
                if len(texts) > 1:
                    mid = len(texts) // 2
                    left = embed_batch(vo, texts[:mid], input_type)
                    right = embed_batch(vo, texts[mid:], input_type)
                    return left + right
                else:
                    raise
            elif attempt < 4:
                wait = 2 ** (attempt + 1)
                print(f"      Voyage error: {e}, retry in {wait}s...")
                time.sleep(wait)
            else:
                raise


# ── Translation ──────────────────────────────────────────────────

def translate_chunk(client, text, source_lang, context_note):
    """Translate a chunk of text using Claude Haiku."""
    for attempt in range(3):
        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4096,
                messages=[{
                    "role": "user",
                    "content": f"""Translate the following {source_lang} text into clear, scholarly English.
This is from a historical source: {context_note}
Preserve historical names, places, and terms. If a passage is unclear or garbled (OCR artifacts), translate what you can and skip unintelligible portions.
Do NOT add commentary or notes — output only the English translation.

---
{text}
---"""
                }]
            )
            return response.content[0].text
        except anthropic.RateLimitError:
            wait = 30 * (attempt + 1)
            print(f"      Claude rate limited, waiting {wait}s...")
            time.sleep(wait)
        except Exception as e:
            if attempt < 2:
                wait = 5 * (attempt + 1)
                print(f"      Claude error: {e}, retry in {wait}s...")
                time.sleep(wait)
            else:
                print(f"      Translation failed after 3 attempts: {e}")
                return None
    return None


def translate_chunks_batch(client, chunks, source_lang, context_note):
    """Translate a list of chunks, showing progress."""
    translated = []
    failed = 0
    for i, chunk in enumerate(chunks):
        result = translate_chunk(client, chunk, source_lang, context_note)
        if result:
            translated.append(result)
        else:
            failed += 1
        if (i + 1) % 10 == 0 or i == len(chunks) - 1:
            print(f"    Translated {i+1}/{len(chunks)} chunks ({failed} failed)")
        time.sleep(0.3)  # gentle pacing
    return translated, failed


# ── Source definitions ───────────────────────────────────────────

SOURCES = [
    {
        "url": "https://archive.org/download/TawqAlHamamahByIbnHazmFrench/Tawq%20al-hamamah%20by%20Ibn%20Hazm%20%28French%29_djvu.txt",
        "short_name": "ibn-hazm-ring-dove",
        "source": "Ibn Hazm, The Ring of the Dove (Tawq al-Hamamah, 11th century Cordoba)",
        "source_type": "primary_arabic",
        "era": "andalusia",
        "subdir": "andalusia",
        "source_lang": "French",
        "context_note": "Ibn Hazm's 'The Ring of the Dove' (Tawq al-Hamamah), a treatise on love written in 11th century Cordoba, Al-Andalus. French translation from the Arabic.",
        "translator": "claude-haiku-4-5 (from French)",
    },
    {
        "url": "https://archive.org/download/cuafragmentahist05mull/cuafragmentahist05mull_djvu.txt",
        "short_name": "kritovoulos-mehmed",
        "source": "Kritovoulos, History of Mehmed the Conqueror (eyewitness account, 1467)",
        "source_type": "primary_greek",
        "era": "ottoman",
        "subdir": "ottoman_extra",
        "source_lang": "Greek and Latin",
        "context_note": "Kritovoulos of Imbros, 'History of Mehmed the Conqueror' — an eyewitness Greek account of Mehmed II's conquests including the Fall of Constantinople (1453). Text from Fragmenta Historicorum Graecorum vol. 5 (Müller ed.), mixed Greek original with Latin scholarly notes.",
        "translator": "claude-haiku-4-5 (from Greek/Latin)",
        # Special handling: extract just the Critobulus section
        "extract_section": True,
        "section_start": 321,
        "section_end": 1486505,
    },
]


# ── Critobulus text cleaning ─────────────────────────────────────

def clean_critobulus(text):
    """
    Clean the Critobulus section from FHG vol 5.
    Remove editor prefaces, indices, page numbers, and OCR noise.
    Keep the narrative Greek/Latin text.
    """
    lines = text.split('\n')
    cleaned = []
    in_preface = True
    preface_end_markers = ['ΚΡΙΤΟΒΟΥΛΟΥ', 'ΒΙΒΛΟΣ', 'MECHEMETIS']

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Skip very short lines (page numbers, headers)
        if len(stripped) < 10 and re.match(r'^[\dxivXIV\s\.\-]+$', stripped):
            continue

        # Skip editor footnote markers
        if re.match(r'^\(\d+\)', stripped):
            continue

        # Skip lines that are just numbers or punctuation
        if re.match(r'^[\d\s\.\,\;\:\-\(\)]+$', stripped):
            continue

        # End of preface detection
        if in_preface:
            for marker in preface_end_markers:
                if marker in stripped:
                    in_preface = False
                    break
            if in_preface:
                continue

        cleaned.append(stripped)

    text = '\n'.join(cleaned)
    # Remove excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# ── Main pipeline ────────────────────────────────────────────────

def main():
    claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("SELECT short_name FROM sources")
    ingested = {r[0] for r in cur.fetchall()}

    results = []
    t0 = time.time()

    for src in SOURCES:
        sn = src['short_name']
        print(f"\n{'='*60}")
        print(f"  [{sn}] ({src['source_lang']})")

        if sn in ingested:
            print(f"    Already in DB — skipping")
            results.append((sn, 0, "SKIPPED_EXISTS"))
            continue

        dest_dir = os.path.join(SOURCES_BASE, src['subdir'])
        os.makedirs(dest_dir, exist_ok=True)
        translated_path = os.path.join(dest_dir, f"{sn}-translated.txt")
        yaml_path = os.path.join(dest_dir, f"{sn}.yaml")

        # Check if we already have a translated file from a prior run
        if os.path.exists(translated_path):
            print(f"    Found existing translation file, using it")
            with open(translated_path, 'r', encoding='utf-8') as f:
                translated_text = f.read()
        else:
            # Download source text
            print(f"    Downloading {src['source_lang']} source...")
            try:
                resp = requests.get(src['url'], timeout=180)
                if resp.status_code != 200:
                    print(f"    FAILED: HTTP {resp.status_code}")
                    results.append((sn, 0, f"DOWNLOAD_FAILED_{resp.status_code}"))
                    continue
                raw_text = resp.text
            except Exception as e:
                print(f"    FAILED: {e}")
                results.append((sn, 0, "DOWNLOAD_EXCEPTION"))
                continue

            # Extract section if needed (Critobulus)
            if src.get('extract_section'):
                raw_text = raw_text[src['section_start']:src['section_end']]
                raw_text = clean_critobulus(raw_text)
                print(f"    Extracted section: {len(raw_text.split()):,} words")

            src_word_count = len(raw_text.split())
            print(f"    Source text: {src_word_count:,} words")

            # Chunk the SOURCE text for translation (larger chunks = fewer API calls)
            # Use 800-word chunks for translation to reduce API calls
            source_chunks = chunk_text(raw_text, target=800, overlap=0, min_w=50)
            print(f"    Source chunks for translation: {len(source_chunks)}")

            # Translate
            print(f"    Translating {len(source_chunks)} chunks with Claude Haiku...")
            translated_chunks, failed = translate_chunks_batch(
                claude, source_chunks, src['source_lang'], src['context_note']
            )
            print(f"    Translation complete: {len(translated_chunks)} OK, {failed} failed")

            if not translated_chunks:
                print(f"    No translations produced — skipping")
                results.append((sn, 0, "TRANSLATION_FAILED"))
                continue

            # Join all translated chunks
            translated_text = '\n\n'.join(translated_chunks)

            # Save translated text for recovery
            with open(translated_path, 'w', encoding='utf-8') as f:
                f.write(translated_text)
            print(f"    Saved translation: {len(translated_text.split()):,} words")

        # Now chunk the TRANSLATED (English) text for embedding
        eng_chunks = chunk_text(translated_text)
        if not eng_chunks:
            print(f"    No chunks from translated text")
            results.append((sn, 0, "NO_CHUNKS"))
            continue

        total_words = sum(len(c.split()) for c in eng_chunks)
        all_figures = set()
        print(f"    English chunks for embedding: {len(eng_chunks)}, {total_words:,} words")

        # Embed + insert
        inserted = 0
        for batch_start in range(0, len(eng_chunks), VOYAGE_BATCH_SIZE):
            batch = eng_chunks[batch_start:batch_start + VOYAGE_BATCH_SIZE]
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
            print(f"    Embedded {min(batch_start + len(batch), len(eng_chunks))}/{len(eng_chunks)} chunks")
            time.sleep(0.5)

        # Register source
        era_list = [src['era']]
        cur.execute("""
            INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (src['source'], sn, src['source_type'], 'english', src['translator'], era_list, 'scholarly', inserted))
        conn.commit()

        # Write YAML sidecar
        meta = {
            "source": src['source'],
            "short_name": sn,
            "source_type": src['source_type'],
            "era": era_list,
            "reliability": "scholarly",
            "original_language": src['source_lang'],
            "translator": src['translator'],
            "word_count": total_words,
            "note": f"AI-translated from {src['source_lang']} via Claude Haiku. Original from Internet Archive.",
        }
        with open(yaml_path, 'w') as f:
            yaml.dump(meta, f, default_flow_style=False)

        # Clean up translated text file (keep for auditability)
        # os.remove(translated_path)  # uncomment to delete after ingestion

        fig_str = f", figures: {sorted(all_figures)}" if all_figures else ""
        print(f"    Done: {inserted} chunks, {total_words:,} words{fig_str}")
        results.append((sn, inserted, "OK"))

    elapsed = time.time() - t0

    # Summary
    print(f"\n{'='*60}")
    print("TRANSLATED SOURCE INGESTION — COMPLETE")
    print(f"{'='*60}")
    for name, chunks, status in results:
        print(f"  {name:<30} {chunks:>5} chunks  {status}")

    ok_count = sum(1 for _, _, s in results if s == 'OK')
    total_new = sum(c for _, c, s in results if s == 'OK')
    print(f"\nIngested: {ok_count} sources, {total_new:,} new chunks")

    cur.execute("SELECT COUNT(*) FROM documents")
    print(f"Total documents in DB: {cur.fetchone()[0]:,}")
    print(f"Time: {elapsed:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
