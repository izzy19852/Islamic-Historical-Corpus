"""
Islam Stories — Classical Islamic Source Ingestion
Ingests authenticated classical Islamic historical sources from Archive.org.
Adapts to the actual DB schema (no metadata column).

Run:  python3 -m rag.ingest.ingest_classical_sources
"""

import os, re, time, requests, json, psycopg2, voyageai
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv('ISLAM_STORIES_DB_URL')
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
vo = voyageai.Client(api_key=os.getenv('VOYAGE_API_KEY'))

SOURCES_DIR = Path(os.path.dirname(__file__)).parent / "sources" / "classical"
SOURCES_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 500
CHUNK_OVERLAP = 50

# Known figures for figure extraction
KNOWN_FIGURES = [
    "Abu Bakr", "Umar ibn al-Khattab", "Uthman ibn Affan", "Ali ibn Abi Talib",
    "Aisha", "Khadijah", "Fatimah", "Khalid ibn Walid", "Bilal ibn Rabah",
    "Salman al-Farisi", "Abu Hurairah", "Ibn Abbas", "Ibn Umar", "Anas ibn Malik",
    "Abu Musa al-Ashari", "Muadh ibn Jabal", "Zayd ibn Thabit", "Nusayba",
    "Abu Dharr", "Amr ibn al-As", "Sad ibn Abi Waqqas", "Usamah ibn Zayd",
    "Hamza ibn Abd al-Muttalib", "Zubayr ibn al-Awwam", "Talha ibn Ubaydullah",
    "Muawiyah", "Husayn ibn Ali", "Hassan ibn Ali", "Saladin", "Baybars",
    "Harun al-Rashid", "Al-Ma'mun", "Tariq ibn Ziyad", "Umar ibn Abd al-Aziz",
]


def extract_figures(text):
    return [f for f in KNOWN_FIGURES if f.lower() in text.lower()]


def clean_djvu(text):
    text = re.sub(r'\x0c', '\n', text)
    text = re.sub(r'(?m)^\s*\d+\s*$', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    lines = []
    for line in text.split('\n'):
        s = line.strip()
        if not s:
            lines.append('')
            continue
        alpha = sum(c.isalpha() for c in s) / max(len(s), 1)
        if alpha > 0.50:
            lines.append(line)
    return '\n'.join(lines)


def chunk_text(text):
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunk = " ".join(words[i:i + CHUNK_SIZE])
        if len(chunk.strip()) > 100:
            chunks.append(chunk)
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks


def embed_batch(texts):
    return vo.embed(texts, model="voyage-2", input_type="document").embeddings


def already_ingested(source):
    cur.execute("SELECT COUNT(*) FROM documents WHERE source = %s", (source,))
    return cur.fetchone()[0] > 50


def ingest_source(url, source_name, era, filename, source_type="primary_arabic"):
    """Download, chunk, embed, and insert a single source."""
    if already_ingested(source_name):
        print(f"  ✅ Already ingested: {source_name[:60]}")
        return 0

    filepath = SOURCES_DIR / filename
    if not filepath.exists():
        print(f"  → Downloading {source_name[:55]}...")
        try:
            r = requests.get(url, timeout=120, stream=True)
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"    {size_mb:.1f} MB")
        except Exception as e:
            print(f"    DOWNLOAD FAILED: {e}")
            return -1

    try:
        text = filepath.read_text(encoding='utf-8', errors='replace')
        text = clean_djvu(text)
        if len(text) < 5000:
            print(f"    SKIP — too short ({len(text)} chars)")
            return -1
    except Exception as e:
        print(f"    READ FAILED: {e}")
        return -1

    chunks = chunk_text(text)
    print(f"    {len(chunks):,} chunks")

    batch_texts, batch_figs = [], []
    inserted = 0

    for chunk in chunks:
        figs = extract_figures(chunk)
        batch_texts.append(chunk)
        batch_figs.append(figs)

        if len(batch_texts) >= 64:
            embeddings = embed_batch(batch_texts)
            for c, emb, fig in zip(batch_texts, embeddings, batch_figs):
                cur.execute("""
                    INSERT INTO documents
                    (content, source, source_type, era, embedding, figures, word_count)
                    VALUES (%s, %s, %s, %s, %s::vector, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (c, source_name, source_type, era, emb,
                      fig or None, len(c.split())))
            conn.commit()
            inserted += len(batch_texts)
            batch_texts, batch_figs = [], []
            time.sleep(0.3)

    if batch_texts:
        embeddings = embed_batch(batch_texts)
        for c, emb, fig in zip(batch_texts, embeddings, batch_figs):
            cur.execute("""
                INSERT INTO documents
                (content, source, source_type, era, embedding, figures, word_count)
                VALUES (%s, %s, %s, %s, %s::vector, %s, %s)
                ON CONFLICT DO NOTHING
            """, (c, source_name, source_type, era, emb,
                  fig or None, len(c.split())))
        conn.commit()
        inserted += len(batch_texts)

    print(f"    ✅ {inserted:,} chunks inserted")
    return inserted


# ═══════════════════════════════════════════════════════════════════
# ALL CLASSICAL SOURCES TO INGEST
# ═══════════════════════════════════════════════════════════════════

ALL_SOURCES = [
    # ── Ibn Kathir: Al-Bidaya wa'l-Nihaya (7 English books) ──
    {
        "urls": [
            "https://archive.org/download/albidayaannihayaallinonepdf/6.%20Miracles%20and%20Merits%20of%20Rasulullah_djvu.txt",
        ],
        "source": "Ibn Kathir, Al-Bidaya wa'l-Nihaya — Miracles and Merits",
        "era": "rashidun",
        "filename": "ibn_kathir_miracles.txt",
    },
    {
        "urls": [
            "https://archive.org/download/IbnKathirEarlyDays_201703/_Ibn%20Kathir%20-%20Early%20days_djvu.txt",
        ],
        "source": "Ibn Kathir, Al-Bidaya wa'l-Nihaya — Early Days",
        "era": "rashidun",
        "filename": "ibn_kathir_early_days.txt",
    },
    {
        "urls": [
            "https://archive.org/download/BookOfTheEnd_ibnkathir/Book_Of_The_End_djvu.txt",
        ],
        "source": "Ibn Kathir, Al-Bidaya wa'l-Nihaya — Book of the End",
        "era": "rashidun",
        "filename": "ibn_kathir_end.txt",
    },
    # ── Ibn Kathir: Stories of the Prophets ──
    {
        "urls": [
            "https://archive.org/download/pdfy-FFZIzpkiBPA9qqDp/Stories+of+the+Prophets+by+Ibn+Kathir_djvu.txt",
            "https://archive.org/download/StoriesOfTheProphetsByIbnKathir_201312/Stories%20Of%20The%20Prophets%20By%20Ibn%20Kathir_djvu.txt",
        ],
        "source": "Ibn Kathir, Stories of the Prophets (Qisas al-Anbiya)",
        "era": "rashidun",
        "filename": "ibn_kathir_prophets.txt",
    },
    # ── Baladhuri: Futuh al-Buldan ──
    {
        "urls": [
            "https://archive.org/download/originsislamics00hittgoog/originsislamics00hittgoog_djvu.txt",
            "https://archive.org/download/originsofislamic00balarich/originsofislamic00balarich_djvu.txt",
        ],
        "source": "Al-Baladhuri, Futuh al-Buldan (Origins of the Islamic State)",
        "era": "rashidun",
        "filename": "baladhuri_futuh.txt",
    },
    # ── Al-Masudi: Meadows of Gold ──
    {
        "urls": [
            "https://archive.org/download/historicalencycl00masrich/historicalencycl00masrich_djvu.txt",
            "https://archive.org/download/elmasdshistoric00unkngoog/elmasdshistoric00unkngoog_djvu.txt",
        ],
        "source": "Al-Masudi, Muruj al-Dhahab (Meadows of Gold)",
        "era": "abbasid",
        "filename": "masudi_muruj.txt",
    },
    # ── Ibn al-Athir: Al-Kamil (Crusading Period selections) ──
    {
        "urls": [
            "https://archive.org/download/IbnAlAthirInCicilianMuslims/Chronicle_of_Ibn_al_Athir%20Part%203%20Intro_djvu.txt",
        ],
        "source": "Ibn al-Athir, Al-Kamil fi'l-Tarikh (Chronicle — Crusading Period)",
        "era": "crusades",
        "filename": "ibn_athir_crusades.txt",
    },
    # ── Ibn Jubayr: Travels ──
    {
        "urls": [
            "https://archive.org/download/travelsofibnjuba05ibnjuoft/travelsofibnjuba05ibnjuoft_djvu.txt",
            "https://archive.org/download/travelsibnjubay00goejgoog/travelsibnjubay00goejgoog_djvu.txt",
        ],
        "source": "Ibn Jubayr, Rihla (The Travels of Ibn Jubayr)",
        "era": "crusades",
        "filename": "ibn_jubayr_rihla.txt",
    },
    # ── Al-Masudi: Meadows of Gold (additional editions) ──
    {
        "urls": [
            "https://archive.org/download/meadowsgoldmine00masgoog/meadowsgoldmine00masgoog_djvu.txt",
        ],
        "source": "Al-Masudi, Muruj al-Dhahab (Meadows of Gold, Sprenger translation)",
        "era": "abbasid",
        "filename": "masudi_meadows_gold_sprenger.txt",
    },
    {
        "urls": [
            "https://archive.org/download/in.ernet.dli.2015.187564/2015.187564.Muruj-Al-Dhahab-Vol-1_djvu.txt",
        ],
        "source": "Al-Masudi, Muruj al-Dhahab Vol 1",
        "era": "abbasid",
        "filename": "masudi_vol1.txt",
    },
    {
        "urls": [
            "https://archive.org/download/in.ernet.dli.2015.187565/2015.187565.Muruj-Al-Dhahab-Vol-2_djvu.txt",
        ],
        "source": "Al-Masudi, Muruj al-Dhahab Vol 2",
        "era": "abbasid",
        "filename": "masudi_vol2.txt",
    },
    # ── Ibn al-Athir: Al-Kamil (additional editions) ──
    {
        "urls": [
            "https://archive.org/download/chronicleofcrus00nebegoog/chronicleofcrus00nebegoog_djvu.txt",
        ],
        "source": "Ibn al-Athir, Chronicle of the Crusades (Richards translation)",
        "era": "crusades",
        "filename": "ibn_athir_crusades_richards.txt",
    },
    {
        "urls": [
            "https://archive.org/download/theannalsofthese00ibna/theannalsofthese00ibna_djvu.txt",
        ],
        "source": "Ibn al-Athir, The Annals of the Seljuk Turks",
        "era": "abbasid",
        "filename": "ibn_athir_seljuk.txt",
    },
    {
        "urls": [
            "https://archive.org/download/al-kamil-fi-al-tarikh-ibn-al-athir/al-kamil-fi-al-tarikh-ibn-al-athir_djvu.txt",
        ],
        "source": "Ibn al-Athir, Al-Kamil fi al-Tarikh (The Complete History)",
        "era": "abbasid",
        "filename": "ibn_athir_kamil_full.txt",
    },
]


def run():
    print("=" * 60)
    print("CLASSICAL ISLAMIC SOURCES INGESTION")
    print(f"Sources to check: {len(ALL_SOURCES)}")
    print("=" * 60)

    grand_total = 0
    for src in ALL_SOURCES:
        source_name = src["source"]
        print(f"\n── {source_name[:60]} ──")

        inserted = -1
        for url in src["urls"]:
            inserted = ingest_source(url, source_name, src["era"],
                                     src["filename"], src.get("source_type", "primary_arabic"))
            if inserted >= 0:
                break  # success, skip alternate URLs
            print(f"    Trying alternate URL...")

        if inserted > 0:
            grand_total += inserted
        elif inserted == 0:
            pass  # already ingested
        else:
            print(f"    ❌ ALL URLs FAILED for {source_name[:40]}")

    # Register in sources table
    print(f"\nRegistering sources...")
    for src in ALL_SOURCES:
        cur.execute("SELECT 1 FROM sources WHERE name = %s", (src["source"],))
        if not cur.fetchone():
            short = src["filename"].replace(".txt", "")
            cur.execute("SELECT COUNT(*) FROM documents WHERE source = %s", (src["source"],))
            count = cur.fetchone()[0]
            if count > 0:
                cur.execute("""
                    INSERT INTO sources (name, short_name, source_type, language, chunk_count)
                    VALUES (%s, %s, %s, %s, %s)
                """, (src["source"], short, "primary_arabic", "english", count))
                print(f"  Registered: {src['source'][:50]} ({count} chunks)")
    conn.commit()

    print(f"\n{'=' * 60}")
    print(f"INGESTION COMPLETE")
    print(f"  New chunks inserted: {grand_total:,}")
    cur.execute("SELECT COUNT(*) FROM documents")
    print(f"  Total chunks in DB: {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM sources")
    print(f"  Total sources: {cur.fetchone()[0]}")
    print("=" * 60)


if __name__ == "__main__":
    run()
