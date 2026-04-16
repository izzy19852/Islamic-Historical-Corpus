"""
Ingest remaining 4 classical sources:
1. Ibn al-Athir — Austin College PDF of Chronicle (Crusading Period)
2. Abu Nu'aym — Archive.org Hilyat al-Awliya (Arabic, Voyage multilingual)
3. Ibn Abd al-Barr — Archive.org Al-Isti'ab (Arabic, Voyage multilingual)
4. Al-Azraqi — Nabataea.net English PDF (Vol 1, partial translation)
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
]


def extract_figures(text):
    return [f for f in KNOWN_FIGURES if f.lower() in text.lower()]


def clean_text(text):
    text = re.sub(r'\x0c', '\n', text)
    text = re.sub(r'(?m)^\s*\d+\s*$', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    lines = []
    for line in text.split('\n'):
        s = line.strip()
        if not s:
            lines.append('')
            continue
        if sum(c.isalpha() or c in 'ابتثجحخدذرزسشصضطظعغفقكلمنهويةءآأإؤئ' for c in s) / max(len(s), 1) > 0.40:
            lines.append(line)
    return '\n'.join(lines)


def chunk_text(text, size=500, overlap=50):
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunk = ' '.join(words[i:i + size])
        if len(chunk.strip()) > 100:
            chunks.append(chunk)
        i += size - overlap
    return chunks


def already_ingested(source):
    cur.execute("SELECT COUNT(*) FROM documents WHERE source = %s", (source,))
    return cur.fetchone()[0] > 50


def download_file(url, filepath, timeout=180):
    """Download with proper headers for various hosts."""
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
    """Extract text from PDF using pdftotext or python."""
    try:
        result = subprocess.run(
            ['pdftotext', '-layout', str(pdf_path), '-'],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode == 0 and len(result.stdout) > 1000:
            return result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Fallback: try PyPDF2
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


def ingest_text(text, source_name, era, source_type, language="english"):
    """Chunk, embed, and insert text."""
    text = clean_text(text)
    if len(text) < 3000:
        print(f"    SKIP — too short ({len(text)} chars)")
        return 0

    chunks = chunk_text(text)
    print(f"    {len(chunks):,} chunks")

    # Use voyage-2 for English, voyage-multilingual-2 for Arabic
    model = "voyage-2" if language == "english" else "voyage-multilingual-2"

    batch_texts, batch_figs = [], []
    inserted = 0

    for chunk in chunks:
        batch_texts.append(chunk)
        batch_figs.append(extract_figures(chunk))

        if len(batch_texts) >= 64:
            try:
                embeddings = vo.embed(batch_texts, model=model, input_type="document").embeddings
            except Exception as e:
                print(f"    Embed error: {e}, trying smaller batch...")
                # Try smaller batch
                for j in range(0, len(batch_texts), 16):
                    small = batch_texts[j:j+16]
                    small_figs = batch_figs[j:j+16]
                    embs = vo.embed(small, model=model, input_type="document").embeddings
                    for c, emb, fig in zip(small, embs, small_figs):
                        cur.execute("""
                            INSERT INTO documents (content, source, source_type, era, embedding, figures, word_count)
                            VALUES (%s,%s,%s,%s,%s::vector,%s,%s) ON CONFLICT DO NOTHING
                        """, (c, source_name, source_type, era, emb, fig or None, len(c.split())))
                    conn.commit()
                    inserted += len(small)
                batch_texts, batch_figs = [], []
                time.sleep(0.5)
                continue

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
        embeddings = vo.embed(batch_texts, model=model, input_type="document").embeddings
        for c, emb, fig in zip(batch_texts, embeddings, batch_figs):
            cur.execute("""
                INSERT INTO documents (content, source, source_type, era, embedding, figures, word_count)
                VALUES (%s,%s,%s,%s,%s::vector,%s,%s) ON CONFLICT DO NOTHING
            """, (c, source_name, source_type, era, emb, fig or None, len(c.split())))
        conn.commit()
        inserted += len(batch_texts)

    print(f"    ✅ {inserted:,} chunks inserted")

    # Register source
    cur.execute("SELECT 1 FROM sources WHERE name = %s", (source_name,))
    if not cur.fetchone():
        short = re.sub(r'[^a-z0-9_]', '_', source_name[:40].lower()).strip('_')
        cur.execute("""
            INSERT INTO sources (name, short_name, source_type, language, chunk_count)
            VALUES (%s, %s, %s, %s, %s)
        """, (source_name, short, source_type, language, inserted))
        conn.commit()

    return inserted


def run():
    print("=" * 60)
    print("REMAINING CLASSICAL SOURCES")
    print("=" * 60)

    grand_total = 0

    # ── 1. Ibn al-Athir: Chronicle (English PDF from Austin College) ──
    source = "Ibn al-Athir, Al-Kamil fi'l-Tarikh (Chronicle of the Crusading Period)"
    print(f"\n── {source[:55]} ──")
    if not already_ingested(source):
        filepath = SOURCES_DIR / "ibn_athir_chronicle.pdf"
        if not filepath.exists():
            print("  → Downloading from Austin College...")
            try:
                download_file(
                    "http://artemis.austincollege.edu/acad/history/htooley/The%20Chronicle%20of%20Ibn%20Al-AthirBigFile.pdf",
                    filepath,
                )
                print(f"    {filepath.stat().st_size / 1024 / 1024:.1f} MB")
            except Exception as e:
                print(f"    FAILED: {e}")
                filepath = None

        if filepath and filepath.exists():
            text = extract_text_from_pdf(filepath)
            if text:
                n = ingest_text(text, source, "crusades", "primary_arabic", "english")
                grand_total += n
            else:
                print("    PDF text extraction failed")
    else:
        print(f"  ✅ Already ingested")

    # ── 2. Al-Azraqi: Akhbar Makkah Vol 1 (English PDF from Nabataea) ──
    source = "Al-Azraqi, Akhbar Makkah (History of Mecca, Vol 1 — Gibson tr.)"
    print(f"\n── {source[:55]} ──")
    if not already_ingested(source):
        filepath = SOURCES_DIR / "azraqi_makkah_v1.pdf"
        if not filepath.exists():
            print("  → Downloading from Nabataea...")
            try:
                download_file(
                    "https://nabataea.net/media/04shop/PDFS/Azraqi_Vol1.pdf",
                    filepath,
                )
                print(f"    {filepath.stat().st_size / 1024 / 1024:.1f} MB")
            except Exception as e:
                print(f"    FAILED: {e}")
                filepath = None

        if filepath and filepath.exists():
            text = extract_text_from_pdf(filepath)
            if text:
                n = ingest_text(text, source, "rashidun", "primary_arabic", "english")
                grand_total += n
            else:
                print("    PDF text extraction failed")
    else:
        print(f"  ✅ Already ingested")

    # ── 3. Abu Nu'aym: Hilyat al-Awliya (Arabic from Archive.org) ──
    source = "Abu Nu'aym al-Isfahani, Hilyat al-Awliya (Arabic)"
    print(f"\n── {source[:55]} ──")
    if not already_ingested(source):
        # The Arabic version is available as a large collection
        filepath = SOURCES_DIR / "abu_nuaym_hilya_ar.txt"
        if not filepath.exists():
            print("  → Downloading Arabic text from Archive.org...")
            try:
                # Try the DjVu of the Arabic collection
                download_file(
                    "https://archive.org/download/HilyatulAwliya/HilyatulAwliya_djvu.txt",
                    filepath,
                )
                print(f"    {filepath.stat().st_size / 1024 / 1024:.1f} MB")
            except Exception as e:
                print(f"    FAILED: {e}")
                filepath = None

        if filepath and filepath.exists():
            text = filepath.read_text(encoding='utf-8', errors='replace')
            n = ingest_text(text, source, "rashidun", "primary_arabic", "arabic")
            grand_total += n
    else:
        print(f"  ✅ Already ingested")

    # ── 4. Ibn Abd al-Barr: Al-Isti'ab (Arabic from Archive.org) ──
    source = "Ibn Abd al-Barr, Al-Isti'ab fi Ma'rifat al-Ashab (Arabic)"
    print(f"\n── {source[:55]} ──")
    if not already_ingested(source):
        filepath = SOURCES_DIR / "ibn_abd_barr_istiab_ar.txt"
        if not filepath.exists():
            print("  → Downloading Arabic text from Archive.org...")
            try:
                download_file(
                    "https://archive.org/download/alistiabfimarifa02ibnauoft/alistiabfimarifa02ibnauoft_djvu.txt",
                    filepath,
                )
                print(f"    {filepath.stat().st_size / 1024 / 1024:.1f} MB")
            except Exception as e:
                print(f"    FAILED: {e}")
                filepath = None

        if filepath and filepath.exists():
            text = filepath.read_text(encoding='utf-8', errors='replace')
            n = ingest_text(text, source, "rashidun", "primary_arabic", "arabic")
            grand_total += n
    else:
        print(f"  ✅ Already ingested")

    print(f"\n{'=' * 60}")
    print(f"COMPLETE — {grand_total:,} new chunks")
    cur.execute("SELECT COUNT(*) FROM documents")
    print(f"Total DB: {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM sources")
    print(f"Total sources: {cur.fetchone()[0]}")
    print("=" * 60)


if __name__ == "__main__":
    run()
