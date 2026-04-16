"""
Session 6 — Alternative Source Ingestion.
Since Internet Archive /download/ endpoint is returning 503,
this script ingests from alternative sources:
  - Ibn Sina: Afnan biography PDF (direct IA server URL worked)
  - Tipu Sultan: 3 scholarly PDFs + Britannica article
  - Omar al-Mukhtar: 4 web articles (to supplement existing 2 chunks)
"""
import os
import re
import sys
import time
import yaml
import psycopg2
import voyageai
import requests
import fitz
from collections import Counter
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
SOURCES_BASE = os.path.join(os.path.dirname(__file__), '..', 'sources')

VOYAGE_BATCH_SIZE = 128
TARGET_CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_CHUNK_WORDS = 100
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (IslamStories RAG)"}

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
    "Ibn Hazm", "Abduh", "Afghani", "Tipu", "Hyder Ali",
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
    return (alpha / max(len(text), 1)) > threshold


def chunk_text(text, target=TARGET_CHUNK_WORDS, overlap=OVERLAP_WORDS, min_w=MIN_CHUNK_WORDS):
    sentences = re.split(r'(?<=[.!?])\s+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    if len(sentences) <= 3 and len(text.split()) > target:
        words = text.split()
        chunks = []
        i = 0
        while i < len(words):
            cw = words[i:i + target]
            if len(cw) >= min_w:
                chunks.append(' '.join(cw))
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


def clean_pdf_text(raw):
    lines = raw.split('\n')
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


def extract_pdf_text(pdf_path):
    doc = fitz.open(pdf_path)
    all_text = []
    for page in doc:
        text = page.get_text("text")
        if text:
            all_text.append(text)
    doc.close()
    raw = '\n'.join(all_text)
    return clean_pdf_text(raw)


def fetch_html_text(url):
    from bs4 import BeautifulSoup
    resp = requests.get(url, timeout=60, headers=HEADERS)
    if resp.status_code != 200:
        print(f"    HTTP {resp.status_code} from {url[:80]}")
        return None
    soup = BeautifulSoup(resp.text, 'html.parser')
    for tag in soup.find_all(['script', 'style', 'nav', 'footer', 'header', 'aside', 'iframe']):
        tag.decompose()
    main = (soup.find('article') or soup.find('main') or
            soup.find('div', class_=re.compile(r'content|entry|post|article', re.I)))
    if main:
        text = main.get_text(separator='\n', strip=True)
    else:
        text = soup.get_text(separator='\n', strip=True)
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    return '\n'.join(lines)


def ingest_text(vo, cur, conn, short_name, source, source_type, era, text, subdir="gap_fills"):
    """Chunk, embed, insert a text source."""
    if not is_readable(text):
        alpha = sum(1 for c in text if c.isalpha()) / max(len(text), 1)
        print(f"    FAILED readable filter: {alpha:.1%}")
        return 0

    word_count = len(text.split())
    alpha = sum(1 for c in text if c.isalpha()) / max(len(text), 1)
    print(f"    {word_count:,} words, {alpha:.0%} alpha — PASS")

    chunks = chunk_text(text)
    if not chunks:
        print(f"    No chunks")
        return 0

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
            """, (chunk, str(emb), source, source_type, era, figures, batch_start + idx, len(chunk.split())))
            inserted += 1
        conn.commit()
        print(f"    Embedded {min(batch_start + len(batch), len(chunks))}/{len(chunks)} chunks")
        time.sleep(0.5)

    # Register source
    cur.execute("""
        INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (source, short_name, source_type, 'english', None, [era], 'scholarly', inserted))
    conn.commit()

    # YAML sidecar
    dest_dir = os.path.join(SOURCES_BASE, subdir)
    os.makedirs(dest_dir, exist_ok=True)
    yaml_path = os.path.join(dest_dir, f"{short_name}.yaml")
    meta = {
        "source": source,
        "short_name": short_name,
        "source_type": source_type,
        "era": [era],
        "reliability": "scholarly",
        "word_count": total_words,
    }
    with open(yaml_path, 'w') as f:
        yaml.dump(meta, f, default_flow_style=False)

    fig_str = f", figures: {sorted(all_figures)}" if all_figures else ""
    print(f"    Done: {inserted} chunks, {total_words:,} words{fig_str}")
    return inserted


def main():
    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("SELECT short_name FROM sources")
    ingested = {r[0] for r in cur.fetchall()}

    results = []
    t0 = time.time()

    # ═══════════════════════════════════════════════════════════════
    # 1. IBN SINA — Afnan biography (PDF already downloaded)
    # ═══════════════════════════════════════════════════════════════
    sn = "afnan-avicenna"
    print(f"\n{'='*60}")
    print(f"  [{sn}] Ibn Sina / Avicenna biography")
    if sn in ingested:
        print("    Already in DB — skipping")
        results.append((sn, 0, "SKIPPED"))
    else:
        pdf_path = "/tmp/avicenna_ibn_sina.pdf"
        if os.path.exists(pdf_path):
            text = extract_pdf_text(pdf_path)
            n = ingest_text(vo, cur, conn, sn,
                            "Afnan, Avicenna: His Life and Works (1958)",
                            "scholarly_western", "abbasid", text, "abbasid")
            results.append((sn, n, "OK" if n else "FAILED"))
        else:
            # Try downloading
            print("    Downloading from direct IA server...")
            resp = requests.get("https://ia601900.us.archive.org/2/items/avicenna_ibn_sina/avicenna_ibn_sina.pdf",
                                timeout=180, headers=HEADERS)
            if resp.status_code == 200:
                with open(pdf_path, 'wb') as f:
                    f.write(resp.content)
                text = extract_pdf_text(pdf_path)
                n = ingest_text(vo, cur, conn, sn,
                                "Afnan, Avicenna: His Life and Works (1958)",
                                "scholarly_western", "abbasid", text, "abbasid")
                results.append((sn, n, "OK" if n else "FAILED"))
            else:
                print(f"    Download failed: HTTP {resp.status_code}")
                results.append((sn, 0, "DOWNLOAD_FAILED"))

    # ═══════════════════════════════════════════════════════════════
    # 2. TIPU SULTAN — Multiple PDFs combined
    # ═══════════════════════════════════════════════════════════════
    sn = "tipu-sultan-compiled"
    print(f"\n{'='*60}")
    print(f"  [{sn}] Tipu Sultan compiled sources")
    if sn in ingested:
        print("    Already in DB — skipping")
        results.append((sn, 0, "SKIPPED"))
    else:
        all_text_parts = []

        # PDF sources
        pdf_sources = [
            ("/tmp/tipu_revisionism.pdf", "https://www.ijrti.org/papers/IJRTI2304025.pdf"),
            ("/tmp/tipu_ijmer.pdf", "http://s3-ap-southeast-1.amazonaws.com/ijmer/pdf/volume10/volume10-issue10(8)/16.pdf"),
            ("/tmp/tipu_library.pdf", "https://ijsshmr.com/v1i3/Doc/3.pdf"),
        ]
        for pdf_path, url in pdf_sources:
            if not os.path.exists(pdf_path):
                print(f"    Downloading {url[:60]}...")
                try:
                    resp = requests.get(url, timeout=120, headers=HEADERS)
                    if resp.status_code == 200:
                        with open(pdf_path, 'wb') as f:
                            f.write(resp.content)
                except Exception as e:
                    print(f"    Download failed: {e}")
            if os.path.exists(pdf_path):
                text = extract_pdf_text(pdf_path)
                if text and len(text.split()) > 100:
                    all_text_parts.append(text)
                    print(f"    PDF: {len(text.split()):,} words")

        # Britannica article
        print("    Fetching Britannica article...")
        try:
            brit_text = fetch_html_text("https://www.britannica.com/biography/Tipu-Sultan")
            if brit_text and len(brit_text.split()) > 100:
                all_text_parts.append(brit_text)
                print(f"    Britannica: {len(brit_text.split()):,} words")
        except Exception as e:
            print(f"    Britannica fetch failed: {e}")

        # Wikipedia
        print("    Fetching Wikipedia article...")
        try:
            wiki_text = fetch_html_text("https://en.wikipedia.org/wiki/Tipu_Sultan")
            if wiki_text and len(wiki_text.split()) > 500:
                all_text_parts.append(wiki_text)
                print(f"    Wikipedia: {len(wiki_text.split()):,} words")
        except Exception as e:
            print(f"    Wikipedia fetch failed: {e}")

        if all_text_parts:
            combined = '\n\n'.join(all_text_parts)
            n = ingest_text(vo, cur, conn, sn,
                            "Tipu Sultan: Compiled scholarly articles and biographical sources",
                            "scholarly_western", "south_asia", combined, "south_asia")
            results.append((sn, n, "OK" if n else "FAILED"))
        else:
            print("    No text obtained")
            results.append((sn, 0, "NO_TEXT"))

    # ═══════════════════════════════════════════════════════════════
    # 3. OMAR AL-MUKHTAR — Supplementary articles
    # ═══════════════════════════════════════════════════════════════
    sn = "omar-mukhtar-supplement"
    print(f"\n{'='*60}")
    print(f"  [{sn}] Omar al-Mukhtar supplementary sources")
    if sn in ingested:
        print("    Already in DB — skipping")
        results.append((sn, 0, "SKIPPED"))
    else:
        article_urls = [
            "https://fanack.com/libya/history-of-libya/italian-reconquest-of-libya-and-umar-al-mukhtar/",
            "https://libyanheritagehouse.org/omar-al-mukhtar-and-the-first-italian-invasion-of-libya",
            "https://twistislamophobia.org/en/2022/07/07/omar-al-mukhtar-the-fight-against-the-colonization-of-libya/",
            "https://jamestown.org/the-libyan-battle-for-the-heritage-of-omar-al-mukhtar-the-lion-of-the-desert/",
            "https://en.wikipedia.org/wiki/Omar_al-Mukhtar",
        ]
        all_text_parts = []
        for url in article_urls:
            print(f"    Fetching {url[:70]}...")
            try:
                text = fetch_html_text(url)
                if text and len(text.split()) > 200:
                    all_text_parts.append(text)
                    print(f"      {len(text.split()):,} words")
                else:
                    print(f"      Too short or empty")
            except Exception as e:
                print(f"      Error: {e}")
            time.sleep(1)

        if all_text_parts:
            combined = '\n\n'.join(all_text_parts)
            n = ingest_text(vo, cur, conn, sn,
                            "Omar al-Mukhtar: Libya resistance and Italian colonialism (compiled articles supplement)",
                            "scholarly_western", "resistance_colonial", combined, "resistance")
            results.append((sn, n, "OK" if n else "FAILED"))
        else:
            print("    No articles obtained")
            results.append((sn, 0, "NO_TEXT"))

    # ═══════════════════════════════════════════════════════════════
    # 4. BAYBARS — Supplementary (Wikipedia + Britannica)
    # ═══════════════════════════════════════════════════════════════
    sn = "baybars-supplement"
    print(f"\n{'='*60}")
    print(f"  [{sn}] Baybars supplementary sources")
    if sn in ingested:
        print("    Already in DB — skipping")
        results.append((sn, 0, "SKIPPED"))
    else:
        all_text_parts = []
        urls = [
            "https://en.wikipedia.org/wiki/Baybars",
            "https://www.britannica.com/biography/Baybars-I",
        ]
        for url in urls:
            print(f"    Fetching {url[:70]}...")
            try:
                text = fetch_html_text(url)
                if text and len(text.split()) > 200:
                    all_text_parts.append(text)
                    print(f"      {len(text.split()):,} words")
            except Exception as e:
                print(f"      Error: {e}")
            time.sleep(1)

        if all_text_parts:
            combined = '\n\n'.join(all_text_parts)
            n = ingest_text(vo, cur, conn, sn,
                            "Baybars I: Mamluk Sultan compiled biographical sources",
                            "scholarly_western", "mongol", combined, "mongol")
            results.append((sn, n, "OK" if n else "FAILED"))
        else:
            results.append((sn, 0, "NO_TEXT"))

    elapsed = time.time() - t0

    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("SESSION 6 — ALTERNATIVE SOURCE INGESTION COMPLETE")
    print(f"{'='*60}")
    for name, chunks, status in results:
        print(f"  {name:<35} {chunks:>5} chunks  {status}")

    cur.execute("SELECT COUNT(*) FROM documents")
    total_chunks = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sources")
    total_sources = cur.fetchone()[0]
    print(f"\nTotal: {total_chunks:,} chunks, {total_sources} sources")
    print(f"Time: {elapsed:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
