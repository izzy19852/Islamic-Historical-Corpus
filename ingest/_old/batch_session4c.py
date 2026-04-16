"""
Session 4C — East Africa, Southeast Asia, China, Caribbean, North Africa.
Downloads PDFs (PyMuPDF), DJVU text (archive.org), HTML (BeautifulSoup).
Readable text filter, chunk (500w/50 overlap), embed voyage-2, insert to pgvector.
"""
import os
import re
import sys
import time
import yaml
import psycopg2
import voyageai
import requests
import fitz  # PyMuPDF
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
SOURCES_BASE = os.path.join(os.path.dirname(__file__), '..', 'sources')

VOYAGE_BATCH_SIZE = 128
TARGET_CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_CHUNK_WORDS = 100

# ── Sources ──────────────────────────────────────────────────────

SOURCES = [
    # ── East Africa ──────────────────────────────────────────────
    {
        "url": "https://idmsa.org/downloads/Africas-Islamic-Heritage-by-Suleman-Dangor.pdf",
        "short_name": "dangor-africa-islamic-heritage",
        "source": "Dangor, Africa's Islamic Heritage: Muslim Regimes in East Africa",
        "source_type": "scholarly_western",
        "era": "east_africa",
        "subdir": "east_africa",
        "fmt": "pdf",
    },
    {
        "url": "https://www.artsrn.ualberta.ca/amcdouga/Hist243/winter_2017/additional_rdgs/robinson_1.pdf",
        "short_name": "robinson-islamization-africa",
        "source": "Robinson, The Islamization of Africa — Swahili Coast chapter",
        "source_type": "scholarly_western",
        "era": "east_africa",
        "subdir": "east_africa",
        "fmt": "pdf",
    },
    # ── Southeast Asia ───────────────────────────────────────────
    {
        "url": "https://library.oapen.org/bitstream/id/fa8296dc-d9b5-432b-b200-22a40cf61314/1002111.pdf",
        "short_name": "making-islamic-heritage",
        "source": "The Making of Islamic Heritage (OAPEN open access)",
        "source_type": "scholarly_western",
        "era": "southeast_asia",
        "subdir": "southeast_asia",
        "fmt": "pdf",
    },
    {
        "url": "https://archive.org/download/achehnese01snouuoft/achehnese01snouuoft_djvu.txt",
        "short_name": "snouck-achehnese-v1",
        "source": "Snouck Hurgronje, The Achehnese Vol 1 (1906) — Aceh War Dutch scholar",
        "source_type": "scholarly_western",
        "era": "southeast_asia",
        "subdir": "southeast_asia",
        "fmt": "djvu",
    },
    {
        "url": "https://archive.org/download/historyofjava01raffiala/historyofjava01raffiala_djvu.txt",
        "short_name": "raffles-java-v1",
        "source": "Raffles, History of Java Vol 1 (1817)",
        "source_type": "scholarly_western",
        "era": "southeast_asia",
        "subdir": "southeast_asia",
        "fmt": "djvu",
    },
    # ── China ────────────────────────────────────────────────────
    {
        "url": "https://www.asianstudies.org/wp-content/uploads/islam-in-china.pdf",
        "short_name": "rossabi-islam-china",
        "source": "Morris Rossabi, Islam in China (Asian Studies overview)",
        "source_type": "scholarly_western",
        "era": "china",
        "subdir": "china",
        "fmt": "pdf",
    },
    # kfcris-earliest-muslims-china — skipped, image-only PDF (0 extractable words)
    {
        "url": "https://muslimheritage.com/uploads/Samarkand.pdf",
        "short_name": "muslim-heritage-samarkand",
        "source": "Muslim Heritage — Samarkand as Center of Islamic Civilization",
        "source_type": "scholarly_western",
        "era": "central_asia",
        "subdir": "china",
        "fmt": "pdf",
    },
    {
        "url": "https://archive.org/download/cathayandwayth01yule/cathayandwayth01yule_djvu.txt",
        "short_name": "yule-cathay-v1",
        "source": "Yule, Cathay and the Way Thither Vol 1 (1866) — medieval accounts of China",
        "source_type": "scholarly_western",
        "era": "china",
        "subdir": "china",
        "fmt": "djvu",
    },
    # ── Caribbean ────────────────────────────────────────────────
    {
        "url": "https://www.caribbeanmuslims.com/muslims-in-the-caribbean-a-hidden-history-across-a-thousand-years",
        "short_name": "caribbean-muslims-hidden-history",
        "source": "CaribbeanMuslims.com — Hidden History of Muslims in the Caribbean",
        "source_type": "scholarly_western",
        "era": "resistance_colonial",
        "subdir": "caribbean",
        "fmt": "html",
    },
    {
        "url": "https://www.caribbeanmuslims.com/history-of-muslims-in-trinidad",
        "short_name": "caribbean-muslims-trinidad",
        "source": "CaribbeanMuslims.com — History of Muslims in Trinidad",
        "source_type": "scholarly_western",
        "era": "resistance_colonial",
        "subdir": "caribbean",
        "fmt": "html",
    },
    {
        "url": "https://revues.acaref.net/wp-content/uploads/sites/3/2023/10/13-Cherif-Saloum-DIATTA.pdf",
        "short_name": "diatta-islam-slave-revolts",
        "source": "Diatta, Islam and Slave Revolts in the Caribbean (2023)",
        "source_type": "scholarly_western",
        "era": "resistance_colonial",
        "subdir": "caribbean",
        "fmt": "pdf",
    },
    # ── North Africa Extra ───────────────────────────────────────
    {
        "url": "https://archive.org/download/historyofberbersv1/historyofberbersv1_djvu.txt",
        "short_name": "ibn-khaldun-berbers-v1",
        "source": "Ibn Khaldun, History of the Berbers Vol 1",
        "source_type": "primary_arabic",
        "era": "africa",
        "subdir": "north_africa",
        "fmt": "djvu",
    },
    {
        "url": "https://archive.org/download/tarikhalsudan/tarikhalsudan_djvu.txt",
        "short_name": "tarikh-al-sudan",
        "source": "Tarikh al-Sudan (Abd al-Rahman al-Sadi, 17th century West African chronicle)",
        "source_type": "primary_arabic",
        "era": "africa",
        "subdir": "north_africa",
        "fmt": "djvu",
        "readable_threshold": 0.50,  # stricter filter for OCR issues
        "min_readable_chunks": 500,  # skip if <500 readable chunks
    },
]

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
    # Session 4C additions
    "Zheng He", "Mansa Musa", "Askia Muhammad",
    "Yusuf ibn Tashfin", "Ahmad Baba",
]
FIGURE_PATTERNS = [(fig, re.compile(re.escape(fig), re.IGNORECASE)) for fig in KNOWN_FIGURES]


def extract_figures(text):
    found = [fig for fig, pat in FIGURE_PATTERNS if pat.search(text)]
    return found if found else None


# ── Readable text filter ─────────────────────────────────────────

def is_readable(text, threshold=0.40):
    if not text:
        return False
    alpha = sum(1 for c in text if c.isalpha())
    total = len(text)
    ratio = alpha / total if total else 0
    return ratio > threshold


def alpha_ratio(text):
    if not text:
        return 0.0
    return sum(1 for c in text if c.isalpha()) / max(len(text), 1)


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
            elif attempt < 4:
                wait = 2 ** (attempt + 1)
                print(f"      Voyage error: {e}, retry in {wait}s...")
                time.sleep(wait)
            else:
                raise


# ── Download / extraction helpers ────────────────────────────────

def download_file(url, dest_path, timeout=120):
    print(f"    Downloading from {url[:80]}...")
    resp = requests.get(url, timeout=timeout, headers={
        "User-Agent": "Mozilla/5.0 (IslamStories RAG research project)"
    })
    if resp.status_code != 200:
        print(f"    FAILED: HTTP {resp.status_code}")
        return False
    if dest_path.endswith('.pdf'):
        with open(dest_path, 'wb') as f:
            f.write(resp.content)
    else:
        with open(dest_path, 'w', encoding='utf-8') as f:
            f.write(resp.text)
    size_kb = len(resp.content) / 1024
    print(f"    Downloaded: {size_kb:.0f} KB")
    return True


def clean_pdf_text(raw):
    """Clean extracted PDF text — remove headers/footers/page numbers."""
    lines = raw.split('\n')
    # Detect repeated headers/footers
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
        # Skip pure page numbers
        if re.match(r'^[\dxivXIV]+$', stripped):
            continue
        # Skip very short lines (likely headers/footers)
        if len(stripped) < 15 and not stripped.endswith('.'):
            continue
        cleaned.append(line)

    text = '\n'.join(cleaned)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf_text(pdf_path):
    """Extract and clean text from PDF using PyMuPDF."""
    doc = fitz.open(pdf_path)
    all_text = []
    for page in doc:
        text = page.get_text("text")
        if text:
            all_text.append(text)
    doc.close()
    raw = '\n'.join(all_text)
    return clean_pdf_text(raw)


def extract_html_text(url):
    """Fetch HTML page and extract main article text with BeautifulSoup."""
    print(f"    Scraping {url[:80]}...")
    resp = requests.get(url, timeout=60, headers={
        "User-Agent": "Mozilla/5.0 (IslamStories RAG research project)"
    })
    if resp.status_code != 200:
        print(f"    FAILED: HTTP {resp.status_code}")
        return None

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Remove non-content elements
    for tag in soup.find_all(['script', 'style', 'nav', 'footer', 'header', 'aside', 'iframe']):
        tag.decompose()

    # Try to find main content area
    main = soup.find('article') or soup.find('main') or soup.find('div', class_=re.compile(r'content|entry|post|article', re.I))
    if main:
        text = main.get_text(separator='\n', strip=True)
    else:
        text = soup.get_text(separator='\n', strip=True)

    # Clean up
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    text = '\n'.join(lines)
    return text


# ── Main pipeline ────────────────────────────────────────────────

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
        fmt = src['fmt']
        print(f"\n{'='*60}")
        print(f"  [{sn}] ({fmt})")

        if sn in ingested:
            print(f"    Already in DB — skipping")
            results.append((sn, 0, "SKIPPED_EXISTS"))
            continue

        dest_dir = os.path.join(SOURCES_BASE, src['subdir'])
        os.makedirs(dest_dir, exist_ok=True)

        # ── Get text based on format ──
        text = None

        if fmt == "pdf":
            pdf_path = os.path.join(dest_dir, f"{sn}.pdf")
            txt_path = os.path.join(dest_dir, f"{sn}.txt")
            if not os.path.exists(txt_path):
                if not os.path.exists(pdf_path):
                    if not download_file(src['url'], pdf_path, timeout=180):
                        results.append((sn, 0, "DOWNLOAD_FAILED"))
                        continue
                print(f"    Extracting PDF text...")
                text = extract_pdf_text(pdf_path)
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(text)
                os.remove(pdf_path)  # clean up PDF after extraction
            else:
                with open(txt_path, 'r', encoding='utf-8') as f:
                    text = f.read()

        elif fmt == "djvu":
            txt_path = os.path.join(dest_dir, f"{sn}.txt")
            if not os.path.exists(txt_path):
                if not download_file(src['url'], txt_path):
                    results.append((sn, 0, "DOWNLOAD_FAILED"))
                    continue
            with open(txt_path, 'r', encoding='utf-8') as f:
                text = f.read()

        elif fmt == "html":
            txt_path = os.path.join(dest_dir, f"{sn}.txt")
            if not os.path.exists(txt_path):
                text = extract_html_text(src['url'])
                if not text:
                    results.append((sn, 0, "SCRAPE_FAILED"))
                    continue
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(text)
            else:
                with open(txt_path, 'r', encoding='utf-8') as f:
                    text = f.read()

        if not text or len(text.split()) < 50:
            print(f"    Too short or empty ({len(text.split()) if text else 0} words)")
            results.append((sn, 0, "TOO_SHORT"))
            continue

        # Readable text filter
        threshold = src.get('readable_threshold', 0.40)
        ratio = alpha_ratio(text)
        if ratio <= threshold:
            print(f"    FAILED readable filter: {ratio:.1%} alpha (need >{threshold:.0%})")
            results.append((sn, 0, "FAILED_READABLE_FILTER"))
            continue

        word_count = len(text.split())
        print(f"    {word_count:,} words, {ratio:.0%} alpha — PASS")

        # Chunk
        chunks = chunk_text(text)
        if not chunks:
            print(f"    No chunks produced")
            results.append((sn, 0, "NO_CHUNKS"))
            continue

        # Special handling for Tarikh al-Sudan: filter each chunk for readability
        if src.get('min_readable_chunks'):
            readable_chunks = [c for c in chunks if is_readable(c, threshold)]
            print(f"    Readable chunks: {len(readable_chunks)}/{len(chunks)}")
            if len(readable_chunks) < src['min_readable_chunks']:
                print(f"    Only {len(readable_chunks)} readable chunks (need {src['min_readable_chunks']}) — SKIPPING")
                results.append((sn, 0, f"ONLY_{len(readable_chunks)}_READABLE_CHUNKS"))
                continue
            chunks = readable_chunks

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
        yaml_path = os.path.join(dest_dir, f"{sn}.yaml")
        meta = {
            "source": src['source'],
            "short_name": sn,
            "source_type": src['source_type'],
            "era": era_list,
            "reliability": "scholarly",
            "word_count": total_words,
        }
        with open(yaml_path, 'w') as f:
            yaml.dump(meta, f, default_flow_style=False)

        # Clean up text file
        if os.path.exists(txt_path):
            os.remove(txt_path)

        fig_str = f", figures: {sorted(all_figures)}" if all_figures else ""
        print(f"    Done: {inserted} chunks, {total_words:,} words{fig_str}")
        results.append((sn, inserted, "OK"))

    elapsed = time.time() - t0

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("SESSION 4C — BATCH INGESTION COMPLETE")
    print(f"{'='*60}")
    for name, chunks, status in results:
        print(f"  {name:<40} {chunks:>5} chunks  {status}")

    # ── Validation queries ───────────────────────────────────────
    print(f"\n{'='*60}")
    print("VALIDATION QUERIES")
    print(f"{'='*60}")

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'embeddings'))
    from query import query_rag

    queries = [
        ("Swahili coast East Africa Islam trade Kilwa Zanzibar", "east_africa"),
        ("Aceh sultanate Dutch resistance Indonesia Islam", "southeast_asia"),
        ("Zheng He Chinese Muslim admiral voyages", "china"),
        ("Muslim slaves Caribbean Jamaica Trinidad revolt", "resistance_colonial"),
        ("Berbers North Africa Ibn Khaldun Almoravid", "africa"),
    ]

    for q, era in queries:
        print(f"\n  Query: \"{q}\" (era={era})")
        try:
            results_q = query_rag(q, era=era, n_results=3)
            if not results_q:
                print(f"    No results found")
            for r in results_q:
                print(f"    [{r['similarity_score']:.3f}] {r['source'][:70]}")
        except Exception as e:
            print(f"    Error: {e}")

    # ── Final DB report ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print("FULL DB REPORT")
    print(f"{'='*60}")

    cur.execute("""
        SELECT era, COUNT(*) as chunks, COUNT(DISTINCT source) as sources
        FROM documents GROUP BY era ORDER BY chunks DESC
    """)
    print(f"\n  {'Era':<25} {'Chunks':>8} {'Sources':>8}")
    print(f"  {'-'*25} {'-'*8} {'-'*8}")
    for row in cur.fetchall():
        print(f"  {row[0] or 'NULL':<25} {row[1]:>8,} {row[2]:>8}")

    cur.execute("SELECT COUNT(*) as total_chunks, COUNT(DISTINCT source) as total_sources FROM documents")
    total = cur.fetchone()
    print(f"\n  TOTAL: {total[0]:,} chunks, {total[1]} sources")
    print(f"\nTime: {elapsed:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
