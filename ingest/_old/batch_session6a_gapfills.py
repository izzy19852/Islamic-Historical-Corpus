"""
Session 6A — Gap Fill Ingestion.
Downloads from Internet Archive (if up) and alternative sources.
Skip-aware: already-ingested sources are skipped.
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
SOURCES_BASE = os.path.join(os.path.dirname(__file__), '..', 'sources', 'gap_fills')

VOYAGE_BATCH_SIZE = 128
TARGET_CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_CHUNK_WORDS = 100
HEADERS = {"User-Agent": "Mozilla/5.0 (IslamStories RAG research project)"}

# ── Sources ─────────────────────────────────────────────────────

SOURCES = [
    # Ibn Sina biography
    {
        "urls": [
            "https://archive.org/download/avicennahislifea00gohlrich/avicennahislifea00gohlrich_djvu.txt",
        ],
        "short_name": "gohlman-ibn-sina",
        "source": "Gohlman, The Life of Ibn Sina (Avicenna)",
        "source_type": "primary_arabic",
        "era": "abbasid",
        "fmt": "djvu",
    },
    # Al-Ghazali autobiography
    {
        "urls": [
            "https://archive.org/download/al-ghazali-munqidh/al-ghazali-munqidh_djvu.txt",
            "https://archive.org/download/DeliberanceFromError/Deliberance%20from%20Error_djvu.txt",
            "https://archive.org/download/AlGhazalisMunqidhMinAdDalalDeliberanceFromError/Al-Ghazali%27s%20Munqidh%20min%20ad-Dalal%20%28Deliberance%20from%20Error%29_djvu.txt",
        ],
        "short_name": "ghazali-munqidh",
        "source": "Al-Ghazali, Deliverance from Error (al-Munqidh min al-Dalal)",
        "source_type": "primary_arabic",
        "era": "abbasid",
        "fmt": "djvu",
    },
    # Tipu Sultan biography
    {
        "urls": [
            "https://archive.org/download/lifeoftippusulta00beat/lifeoftippusulta00beat_djvu.txt",
        ],
        "short_name": "beaton-tipu-sultan",
        "source": "Beaton, Life of Tippoo Sultan",
        "source_type": "scholarly_western",
        "era": "south_asia",
        "fmt": "djvu",
    },
    # Iqbal — try IA first, then Iqbal Cyber Library
    {
        "urls": [
            "https://archive.org/download/reconstructionof00iqba/reconstructionof00iqba_djvu.txt",
        ],
        "short_name": "iqbal-reconstruction",
        "source": "Iqbal, The Reconstruction of Religious Thought in Islam",
        "source_type": "primary_arabic",
        "era": "resistance_colonial",
        "fmt": "djvu",
        "fallback_scrape": "iqbal_cyber_library",
    },
]

# ── Figure extraction ───────────────────────────────────────────

KNOWN_FIGURES = [
    "Khalid ibn Walid", "Abu Bakr", "Umar ibn Khattab",
    "Uthman", "Ali ibn Abi Talib", "Aisha", "Fatima",
    "Abu Ubayda", "Amr ibn al-As", "Salman al-Farsi",
    "Bilal", "Ibn Abbas", "Abu Hurairah", "Muawiyah",
    "Husayn", "Hassan", "Tariq ibn Ziyad", "Umar II",
    "Al-Hajjaj", "Abd al-Rahman", "Saladin", "Baybars",
    "Ibn Khaldun", "Ibn Battuta", "Ibn Sina", "Ibn Rushd",
    "Al-Ghazali", "Mansa Musa", "Muhammad al-Fatih",
    "Tipu Sultan", "Omar al-Mukhtar", "Avicenna",
    "Prophet", "Messenger of Allah", "Abu Bakr al-Siddiq",
    "Umar", "Ali", "Khadija", "Hamza", "Abu Talib",
    "Abu Sufyan", "Zayd", "Usama",
    "Abd al-Qadir", "Imam Shamil", "Mehmed", "Iqbal",
    "Ibn Hazm", "Abduh", "Afghani", "Tipu",
    "Dan Fodio", "Usman", "Sokoto",
]
FIGURE_PATTERNS = [(fig, re.compile(re.escape(fig), re.IGNORECASE)) for fig in KNOWN_FIGURES]


def extract_figures(text):
    found = [fig for fig, pat in FIGURE_PATTERNS if pat.search(text)]
    return found if found else None


def is_readable(text, threshold=0.40):
    if not text:
        return False
    alpha = sum(1 for c in text if c.isalpha())
    total = len(text)
    return (alpha / total if total else 0) > threshold


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


def download_text(url, timeout=120):
    """Try downloading text from URL. Returns text or None."""
    try:
        resp = requests.get(url, timeout=timeout, headers=HEADERS)
        if resp.status_code == 200:
            return resp.text
        print(f"    HTTP {resp.status_code} from {url[:80]}")
        return None
    except Exception as e:
        print(f"    Download error: {e}")
        return None


def download_pdf_text(url, dest_path, timeout=180):
    """Download PDF, extract text with PyMuPDF, return text or None."""
    try:
        import fitz
        resp = requests.get(url, timeout=timeout, headers=HEADERS)
        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code} from {url[:80]}")
            return None
        with open(dest_path, 'wb') as f:
            f.write(resp.content)
        doc = fitz.open(dest_path)
        all_text = []
        for page in doc:
            text = page.get_text("text")
            if text:
                all_text.append(text)
        doc.close()
        os.remove(dest_path)
        raw = '\n'.join(all_text)
        # Clean
        lines = raw.split('\n')
        from collections import Counter
        line_counts = Counter(line.strip() for line in lines if line.strip())
        repeated = {line for line, count in line_counts.items() if count > 30}
        cleaned = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                cleaned.append('')
                continue
            if stripped in repeated:
                continue
            if re.match(r'^[\dxivXIV]+$', stripped):
                continue
            cleaned.append(line)
        text = '\n'.join(cleaned)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()
    except Exception as e:
        print(f"    PDF extraction error: {e}")
        return None


def scrape_iqbal_cyber_library():
    """Scrape Iqbal's Reconstruction from Iqbal Cyber Library."""
    print("    Trying Iqbal Cyber Library fallback...")
    from bs4 import BeautifulSoup
    # The book has multiple chapters — scrape the index page first
    base_url = "https://iqbalcyberlibrary.net/en/645.html"
    try:
        resp = requests.get(base_url, timeout=60, headers=HEADERS)
        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code}")
            return None
        soup = BeautifulSoup(resp.text, 'html.parser')
        # Find chapter links
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/en/' in href and href.endswith('.html') and href != base_url:
                full = href if href.startswith('http') else f"https://iqbalcyberlibrary.net{href}"
                if full not in links:
                    links.append(full)

        all_text = []
        # Get text from main page
        main_content = soup.find('div', class_=re.compile(r'content|entry|post|article', re.I))
        if main_content:
            all_text.append(main_content.get_text(separator='\n', strip=True))
        else:
            for tag in soup.find_all(['script', 'style', 'nav', 'footer', 'header']):
                tag.decompose()
            all_text.append(soup.get_text(separator='\n', strip=True))

        # Follow chapter links (limit to 20)
        for link in links[:20]:
            time.sleep(1)
            try:
                r2 = requests.get(link, timeout=30, headers=HEADERS)
                if r2.status_code == 200:
                    s2 = BeautifulSoup(r2.text, 'html.parser')
                    for tag in s2.find_all(['script', 'style', 'nav', 'footer', 'header']):
                        tag.decompose()
                    content = s2.find('div', class_=re.compile(r'content|entry|post|article', re.I))
                    if content:
                        all_text.append(content.get_text(separator='\n', strip=True))
                    else:
                        all_text.append(s2.get_text(separator='\n', strip=True))
            except Exception:
                continue

        combined = '\n\n'.join(all_text)
        wc = len(combined.split())
        print(f"    Scraped {wc:,} words from {len(all_text)} pages")
        return combined if wc > 500 else None
    except Exception as e:
        print(f"    Scrape error: {e}")
        return None


def ingest_source(vo, cur, conn, src, text):
    """Chunk, embed, and insert a source into the DB."""
    sn = src['short_name']

    if not is_readable(text):
        alpha = sum(1 for c in text if c.isalpha()) / max(len(text), 1)
        print(f"    FAILED readable filter: {alpha:.1%} alpha")
        return 0, "FAILED_READABLE_FILTER"

    word_count = len(text.split())
    alpha = sum(1 for c in text if c.isalpha()) / max(len(text), 1)
    print(f"    {word_count:,} words, {alpha:.0%} alpha — PASS")

    chunks = chunk_text(text)
    if not chunks:
        print(f"    No chunks produced")
        return 0, "NO_CHUNKS"

    total_words = sum(len(c.split()) for c in chunks)
    all_figures = set()
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
    cur.execute("""
        INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (src['source'], sn, src['source_type'], 'english', None, [src['era']], 'scholarly', inserted))
    conn.commit()

    # Write YAML sidecar
    yaml_path = os.path.join(SOURCES_BASE, f"{sn}.yaml")
    meta = {
        "source": src['source'],
        "short_name": sn,
        "source_type": src['source_type'],
        "era": [src['era']],
        "reliability": "scholarly",
        "word_count": total_words,
    }
    with open(yaml_path, 'w') as f:
        yaml.dump(meta, f, default_flow_style=False)

    fig_str = f", figures: {sorted(all_figures)}" if all_figures else ""
    print(f"    Done: {inserted} chunks, {total_words:,} words{fig_str}")
    return inserted, "OK"


def main():
    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("SELECT short_name FROM sources")
    ingested = {r[0] for r in cur.fetchall()}

    os.makedirs(SOURCES_BASE, exist_ok=True)
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

        text = None

        # Try each URL
        for url in src['urls']:
            print(f"    Trying {url[:80]}...")
            if url.endswith('.pdf'):
                pdf_path = os.path.join(SOURCES_BASE, f"{sn}.pdf")
                text = download_pdf_text(url, pdf_path)
            else:
                text = download_text(url)
            if text and len(text.split()) > 50:
                print(f"    Got {len(text.split()):,} words from URL")
                break
            text = None

        # Try fallback scraper
        if not text and src.get('fallback_scrape') == 'iqbal_cyber_library':
            text = scrape_iqbal_cyber_library()

        if not text or len(text.split()) < 50:
            print(f"    No text obtained — SKIPPING")
            results.append((sn, 0, "NO_TEXT"))
            continue

        inserted, status = ingest_source(vo, cur, conn, src, text)
        results.append((sn, inserted, status))

    elapsed = time.time() - t0

    print(f"\n{'='*60}")
    print("SESSION 6A — GAP FILL INGESTION COMPLETE")
    print(f"{'='*60}")
    for name, chunks, status in results:
        print(f"  {name:<35} {chunks:>5} chunks  {status}")

    cur.execute("SELECT COUNT(*) FROM documents")
    print(f"\nTotal documents in DB: {cur.fetchone()[0]:,}")
    print(f"Time: {elapsed:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
