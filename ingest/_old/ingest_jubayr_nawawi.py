"""
Ingest expanded Ibn Jubayr and Nawawi sources.

Ibn Jubayr (32 chunks currently — stub):
  - Full Travels (Broadhurst 1952 translation) from Archive.org DjVu
  - Alt copy if first fails

Nawawi (42 chunks currently — only 40 Hadith):
  - Riyad al-Salihin (Gardens of the Righteous) — Archive.org DjVu
  - Kitab al-Adhkar (Book of Remembrances) — Archive.org DjVu (Arabic)
  - 40 Hadith already ingested, skip
"""

import os, re, time, requests, subprocess, psycopg2, voyageai
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv('ISLAM_STORIES_DB_URL')
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
vo = voyageai.Client(api_key=os.getenv('VOYAGE_API_KEY'))

SOURCES_DIR = Path(os.path.dirname(__file__)).parent / "sources" / "classical"
SOURCES_DIR.mkdir(parents=True, exist_ok=True)

KNOWN_FIGURES = [
    "Abu Bakr", "Umar ibn al-Khattab", "Uthman ibn Affan", "Ali ibn Abi Talib",
    "Aisha", "Khadijah", "Fatimah", "Khalid ibn Walid", "Bilal ibn Rabah",
    "Salman al-Farisi", "Abu Hurairah", "Ibn Abbas", "Ibn Umar", "Anas ibn Malik",
    "Muawiyah", "Husayn ibn Ali", "Saladin", "Baybars", "Harun al-Rashid",
    "Zubayr", "Talha", "Amr ibn al-As", "Sad ibn Abi Waqqas",
    "Nur al-Din", "Ibn Jubayr", "Nawawi",
]


def extract_figures(text):
    return [f for f in KNOWN_FIGURES if f.lower() in text.lower()]


def clean_djvu(text):
    """Clean DjVu-extracted text: strip page numbers, headers, OCR artifacts."""
    text = re.sub(r'\x0c', '\n', text)
    # Strip standalone page numbers
    text = re.sub(r'(?m)^\s*\d{1,4}\s*$', '', text)
    # Strip lines that are mostly non-alphanumeric (OCR noise)
    lines = []
    for line in text.split('\n'):
        s = line.strip()
        if not s:
            lines.append('')
            continue
        alpha = sum(c.isalpha() or c in 'ابتثجحخدذرزسشصضطظعغفقكلمنهويةءآأإؤئ' for c in s)
        if alpha / max(len(s), 1) > 0.35:
            lines.append(line)
    text = '\n'.join(lines)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text


def chunk_text(text, size=500, overlap=50):
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunk = ' '.join(words[i:i + size])
        if len(chunk.strip()) > 100:
            chunks.append(chunk)
        i += size - overlap
    return chunks


def already_ingested(source, threshold=50):
    cur.execute("SELECT COUNT(*) FROM documents WHERE source = %s", (source,))
    return cur.fetchone()[0] > threshold


def download_file(url, filepath, timeout=180):
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; IslamStoriesBot/1.0)',
    }
    r = requests.get(url, timeout=timeout, stream=True, headers=headers)
    r.raise_for_status()
    with open(filepath, 'wb') as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    return filepath


def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using pdftotext or PyPDF2."""
    try:
        result = subprocess.run(
            ['pdftotext', '-layout', str(pdf_path), '-'],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0 and len(result.stdout) > 1000:
            return result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(str(pdf_path))
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text
    except Exception:
        pass
    return ""


def embed_batch_safe(texts, model):
    """Embed with fallback to smaller batches on failure."""
    try:
        return vo.embed(texts, model=model, input_type="document").embeddings
    except Exception as e:
        print(f"    Embed error on batch of {len(texts)}: {e}")
        if len(texts) <= 4:
            raise
        # Split in half and retry
        mid = len(texts) // 2
        left = embed_batch_safe(texts[:mid], model)
        time.sleep(0.3)
        right = embed_batch_safe(texts[mid:], model)
        return left + right


def ingest_text(text, source_name, era, source_type, language="english"):
    """Chunk, embed, and insert text into documents table."""
    text = clean_djvu(text) if language == "arabic" else clean_djvu(text)
    if len(text) < 3000:
        print(f"    SKIP — too short ({len(text)} chars)")
        return 0

    chunks = chunk_text(text)
    print(f"    {len(chunks):,} chunks")

    model = "voyage-2" if language == "english" else "voyage-multilingual-2"

    batch_texts, batch_figs = [], []
    inserted = 0

    for chunk in chunks:
        batch_texts.append(chunk)
        batch_figs.append(extract_figures(chunk))

        if len(batch_texts) >= 64:
            embeddings = embed_batch_safe(batch_texts, model)
            for c, emb, fig in zip(batch_texts, embeddings, batch_figs):
                cur.execute("""
                    INSERT INTO documents (content, source, source_type, era, embedding, figures, word_count)
                    VALUES (%s,%s,%s,%s,%s::vector,%s,%s) ON CONFLICT DO NOTHING
                """, (c, source_name, source_type, era, emb, fig or None, len(c.split())))
            conn.commit()
            inserted += len(batch_texts)
            batch_texts, batch_figs = [], []
            time.sleep(0.3)

    if batch_texts:
        embeddings = embed_batch_safe(batch_texts, model)
        for c, emb, fig in zip(batch_texts, embeddings, batch_figs):
            cur.execute("""
                INSERT INTO documents (content, source, source_type, era, embedding, figures, word_count)
                VALUES (%s,%s,%s,%s,%s::vector,%s,%s) ON CONFLICT DO NOTHING
            """, (c, source_name, source_type, era, emb, fig or None, len(c.split())))
        conn.commit()
        inserted += len(batch_texts)

    print(f"    ✅ {inserted:,} chunks inserted")

    # Register in sources table
    cur.execute("SELECT 1 FROM sources WHERE name = %s", (source_name,))
    if not cur.fetchone():
        short = re.sub(r'[^a-z0-9_]', '_', source_name[:40].lower()).strip('_')
        cur.execute("""
            INSERT INTO sources (name, short_name, source_type, language, chunk_count)
            VALUES (%s, %s, %s, %s, %s)
        """, (source_name, short, source_type, language, inserted))
        conn.commit()

    return inserted


# ═══════════════════════════════════════════════════════════
# SOURCE DEFINITIONS
# ═══════════════════════════════════════════════════════════

IBN_JUBAYR_SOURCES = [
    {
        "url": "https://dbooks.bodleian.ox.ac.uk/books/PDFs/N12568637.pdf",
        "source": "Ibn Jubayr, The Travels of Ibn Jubayr (Broadhurst tr. 1952)",
        "era": "crusades",
        "filename": "ibn_jubayr_travels_bodleian.pdf",
        "language": "english",
        "is_pdf": True,
    },
    {
        "url": "https://archive.org/download/travelsofibnJubayr/travelsofibnJubayr_djvu.txt",
        "source": "Ibn Jubayr, The Travels of Ibn Jubayr (Broadhurst tr. 1952)",
        "era": "crusades",
        "filename": "ibn_jubayr_travels_broadhurst.txt",
        "language": "english",
        "is_pdf": False,
    },
]

NAWAWI_SOURCES = [
    {
        "url": "https://ahadith.co.uk/downloads/riyadus_saleheen.pdf",
        "source": "Imam Nawawi, Riyad al-Salihin (Gardens of the Righteous)",
        "era": "crusades",
        "filename": "nawawi_riyad_salihin.pdf",
        "language": "english",
        "is_pdf": True,
    },
    {
        "url": "https://islamfuture.wordpress.com/wp-content/uploads/2010/06/riyad-us-saliheen-gardens-of-the-righteous-vol-i-and-ii.pdf",
        "source": "Imam Nawawi, Riyad al-Salihin (Gardens of the Righteous, alt)",
        "era": "crusades",
        "filename": "nawawi_riyad_salihin_alt.pdf",
        "language": "english",
        "is_pdf": True,
    },
    {
        "url": "https://www.emaanlibrary.com/wp-content/uploads/2015/04/The-Book-Of-Remembrances-Kitab-Al-Adhkar-Part-1.pdf",
        "source": "Imam Nawawi, Kitab al-Adhkar (Book of Remembrances)",
        "era": "crusades",
        "filename": "nawawi_adhkar_en.pdf",
        "language": "english",
        "is_pdf": True,
    },
]


def run():
    print("=" * 60)
    print("IBN JUBAYR & NAWAWI — EXPANDED INGESTION")
    print("=" * 60)

    grand_total = 0

    # ── IBN JUBAYR ──────────────────────────────────────────
    print("\n── IBN JUBAYR: Rihla (Travels) ──")
    print("  Current: 32 chunks (stub). Need full Broadhurst translation.")

    jubayr_done = False
    for src in IBN_JUBAYR_SOURCES:
        if jubayr_done or already_ingested(src["source"]):
            if already_ingested(src["source"]):
                print(f"  ✅ Already ingested: {src['source'][:55]}")
            continue

        filepath = SOURCES_DIR / src["filename"]
        if not filepath.exists():
            print(f"  → Downloading: {src['url'][:70]}...")
            try:
                download_file(src["url"], filepath)
                size_mb = filepath.stat().st_size / 1024 / 1024
                print(f"    {size_mb:.1f} MB downloaded")
            except Exception as e:
                print(f"    FAILED: {e}")
                continue

        if src.get("is_pdf"):
            print("    Extracting text from PDF...")
            text = extract_text_from_pdf(filepath)
        else:
            text = filepath.read_text(encoding='utf-8', errors='replace')
        print(f"    Raw text: {len(text):,} chars")

        n = ingest_text(text, src["source"], src["era"], "primary_arabic", src["language"])
        if n > 0:
            grand_total += n
            jubayr_done = True
        time.sleep(1)

    # ── NAWAWI ──────────────────────────────────────────────
    print("\n── NAWAWI: Riyad al-Salihin + Kitab al-Adhkar ──")
    print("  Current: 42 chunks (40 Hadith only). Adding major works.")

    for src in NAWAWI_SOURCES:
        if already_ingested(src["source"]):
            print(f"  ✅ Already ingested: {src['source'][:55]}")
            continue

        filepath = SOURCES_DIR / src["filename"]
        if not filepath.exists():
            print(f"  → Downloading: {src['url'][:70]}...")
            try:
                download_file(src["url"], filepath)
                size_mb = filepath.stat().st_size / 1024 / 1024
                print(f"    {size_mb:.1f} MB downloaded")
            except Exception as e:
                print(f"    FAILED: {e}")
                continue

        if src.get("is_pdf"):
            print("    Extracting text from PDF...")
            text = extract_text_from_pdf(filepath)
        else:
            text = filepath.read_text(encoding='utf-8', errors='replace')
        print(f"    Raw text: {len(text):,} chars")

        n = ingest_text(text, src["source"], src["era"], "primary_arabic", src["language"])
        grand_total += n
        time.sleep(1)

    # ── SUMMARY ─────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"COMPLETE — {grand_total:,} new chunks added")

    # Verify final counts
    for name in ['jubayr', 'nawawi']:
        cur.execute(
            "SELECT COUNT(*), COUNT(DISTINCT source) FROM documents WHERE LOWER(source) ILIKE %s",
            (f"%{name}%",)
        )
        count, sources = cur.fetchone()
        print(f"  {name:20s} {count:>6,} chunks  {sources} sources")

    cur.execute("SELECT COUNT(*) FROM documents")
    print(f"\nTotal DB: {cur.fetchone()[0]:,} chunks")
    print("=" * 60)


if __name__ == "__main__":
    run()
