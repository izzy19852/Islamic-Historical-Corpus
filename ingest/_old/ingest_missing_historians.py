"""
Ingest 4 missing historian sources from Archive.org:
1. Ibn Khallikan, Wafayat al-Ayan Vol 3 & Vol 4
2. Ibn al-Jawzi, Al-Muntazam (Arabic, select volumes)
3. Amir Khusrau, Khazainul Futuh (Treasury of Victories)
4. Al-Suyuti, Husn al-Muhadara (Egypt/Cairo)
"""

import os, re, time, requests, psycopg2, voyageai
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_URL = os.getenv('ISLAM_STORIES_DB_URL')
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
vo = voyageai.Client(api_key=os.getenv('VOYAGE_API_KEY'))

SOURCES_DIR = Path(os.path.dirname(__file__)).parent / "sources" / "missing"
SOURCES_DIR.mkdir(parents=True, exist_ok=True)

KNOWN_FIGURES = [
    "Abu Bakr", "Umar ibn al-Khattab", "Uthman ibn Affan", "Ali ibn Abi Talib",
    "Aisha", "Khadijah", "Fatimah", "Khalid ibn Walid", "Bilal ibn Rabah",
    "Salman al-Farisi", "Abu Hurairah", "Ibn Abbas", "Ibn Umar", "Anas ibn Malik",
    "Muawiyah", "Husayn ibn Ali", "Saladin", "Baybars", "Harun al-Rashid",
    "Zubayr", "Talha", "Amr ibn al-As", "Sad ibn Abi Waqqas",
]

# Sources to ingest
SOURCES = [
    {
        "source": "Ibn Khallikan, Wafayat al-Ayan Vol 3 (Biographical Dictionary)",
        "url": "https://archive.org/download/WafayatAl-ayantheObituariesOfEminentMenByIbnKhallikan/Vol3Of4WafayatAl-ayantheObituariesOfEminentMenByIbnKhallikan_djvu.txt",
        "era": "medieval", "source_type": "biography", "language": "arabic",
    },
    {
        "source": "Ibn Khallikan, Wafayat al-Ayan Vol 4 (Biographical Dictionary)",
        "url": "https://archive.org/download/WafayatAl-ayantheObituariesOfEminentMenByIbnKhallikan/Vol4Of4WafayatAl-ayantheObituariesOfEminentMenByIbnKhallikan_djvu.txt",
        "era": "medieval", "source_type": "biography", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 1 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo01_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 2 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo02_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 3 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo03_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 4 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo04_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 5 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo05_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 6 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo06_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 7 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo07_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 8 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo08_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 9 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo09_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 10 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo10_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 11 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo11_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 12 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo12_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 13 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo13_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 14 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo14_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 15 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo15_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 16 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo16_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 17 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo17_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 18 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo18_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol 19 (Arabic)",
        "url": "https://archive.org/download/muntazim_tarikh_mlouk_oumm/mtmo19_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
    {
        "source": "Amir Khusrau, Khazainul Futuh (Treasury of Victories)",
        "url": "https://archive.org/download/dli.ernet.13638/13638-Khazainul%20Futuh_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "english",
    },
    {
        "source": "Amir Khusrau, Khazainul Futuh (Habib translation, Delhi)",
        "url": "https://archive.org/download/dli.ernet.335347/335347-The%20Khazainul%20Futuh%20Of%20Hazrat%20Amir%20Khusrau%20Of%20Delhi_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "english",
    },
    {
        "source": "Al-Suyuti, Husn al-Muhadara fi Tarikh Misr wal-Qahira (Arabic)",
        "url": "https://archive.org/download/HusnAlMuhadarah/Husn_djvu.txt",
        "era": "medieval", "source_type": "chronicle", "language": "arabic",
    },
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


def download_text(url, timeout=180):
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; IslamStoriesBot/1.0)'}
    r = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
    r.raise_for_status()
    return r.text, len(r.content)


def ingest_text(text, source_name, era, source_type, language="english"):
    text = clean_text(text)
    if len(text) < 500:
        print(f"    SKIP - too short ({len(text)} chars)")
        return 0

    chunks = chunk_text(text)
    model = "voyage-2" if language == "english" else "voyage-multilingual-2"

    batch_texts, batch_figs = [], []
    inserted = 0

    for chunk in chunks:
        batch_texts.append(chunk)
        batch_figs.append(extract_figures(chunk))

        batch_limit = 32 if language != "english" else 64
        if len(batch_texts) >= batch_limit:
            try:
                embeddings = vo.embed(batch_texts, model=model, input_type="document").embeddings
            except Exception as e:
                print(f"    Embed error: {e}, retrying smaller batch...")
                for j in range(0, len(batch_texts), 16):
                    small = batch_texts[j:j+16]
                    small_figs = batch_figs[j:j+16]
                    try:
                        embs = vo.embed(small, model=model, input_type="document").embeddings
                    except Exception as e2:
                        print(f"    FATAL embed error: {e2}")
                        break
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
            if inserted % 128 == 0 or inserted == len(batch_texts):
                print(f"    ...{inserted} chunks")
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

    return inserted


def main():
    total_new = 0
    skipped = 0
    failed = 0

    print(f"Processing {len(SOURCES)} sources...\n")

    for idx, src in enumerate(SOURCES, 1):
        name = src["source"]
        print(f"[{idx:2d}/{len(SOURCES)}] {name[:70]}")

        if already_ingested(name):
            print(f"  SKIP (exists): {name[:60]}")
            skipped += 1
            continue

        try:
            text, size = download_text(src["url"])
            print(f"    Downloaded: {size:,}B")
        except Exception as e:
            print(f"  FAIL download: {e}")
            failed += 1
            continue

        try:
            n = ingest_text(text, name, src["era"], src["source_type"], src["language"])
            print(f"  OK: {n} chunks inserted")
            total_new += n
        except Exception as e:
            print(f"  FAIL ingest: {e}")
            failed += 1
            conn.rollback()
            continue

    cur.execute("SELECT COUNT(*) FROM documents")
    total = cur.fetchone()[0]

    print(f"\n=== INGESTION COMPLETE ===")
    print(f"  New chunks inserted: {total_new:,}")
    print(f"  Sources added:       {len(SOURCES) - skipped - failed}")
    print(f"  Already present:     {skipped}")
    print(f"  Failed:              {failed}")
    print(f"  Total corpus:        {total:,} chunks")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
