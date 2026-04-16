"""
Ingest Persian history, Caucasus, and Timur sources from Archive.org.
Downloads djvu.txt files, chunks, embeds with Voyage, inserts into documents table.
"""

import os, re, time, requests, json, sys
import psycopg2, voyageai
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_URL = os.getenv('ISLAM_STORIES_DB_URL')
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
vo = voyageai.Client(api_key=os.getenv('VOYAGE_API_KEY'))

SOURCES_DIR = Path(os.path.dirname(__file__)).parent / "sources" / "persia_caucasus"
SOURCES_DIR.mkdir(parents=True, exist_ok=True)

KNOWN_FIGURES = [
    "Abu Bakr", "Umar ibn al-Khattab", "Uthman ibn Affan", "Ali ibn Abi Talib",
    "Aisha", "Khadijah", "Fatimah", "Khalid ibn Walid", "Bilal ibn Rabah",
    "Salman al-Farisi", "Abu Hurairah", "Ibn Abbas", "Ibn Umar", "Anas ibn Malik",
    "Muawiyah", "Husayn ibn Ali", "Saladin", "Baybars", "Harun al-Rashid",
    "Zubayr", "Talha", "Amr ibn al-As", "Sad ibn Abi Waqqas",
    # Persia / Caucasus / Timur figures
    "Firdausi", "Nizami", "Timur", "Tamerlane", "Genghis Khan", "Hulagu",
    "Shah Ismail", "Shah Abbas", "Nader Shah", "Imam Shamil",
    "Kazi Mullah", "Ghazi Muhammad", "Rustam", "Sohrab", "Khosrow",
    "Mahmud of Ghazni", "Rumi", "Hafiz", "Saadi",
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
        if sum(c.isalpha() for c in s) / max(len(s), 1) > 0.40:
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


def download_djvu(identifier, filename):
    filepath = SOURCES_DIR / f"{identifier}.txt"
    if filepath.exists() and filepath.stat().st_size > 1000:
        print(f"    Cached: {filepath}", flush=True)
        return filepath

    url = f"https://archive.org/download/{identifier}/{quote(filename)}"
    print(f"    Downloading: {url}", flush=True)
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; IslamStoriesBot/1.0)'}
    r = requests.get(url, timeout=180, stream=True, headers=headers)
    r.raise_for_status()
    with open(filepath, 'wb') as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    size_mb = filepath.stat().st_size / 1024 / 1024
    print(f"    Downloaded: {size_mb:.1f} MB", flush=True)
    return filepath


def embed_batch(texts, model="voyage-2"):
    for attempt in range(3):
        try:
            return vo.embed(texts, model=model, input_type="document").embeddings
        except Exception as e:
            print(f"    Embed error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(5 * (attempt + 1))
    raise Exception("Failed to embed after 3 attempts")


def ingest_text(text, source_name, era, source_type="historical"):
    text = clean_text(text)
    if len(text) < 3000:
        print(f"    SKIP — too short ({len(text)} chars)", flush=True)
        return 0

    chunks = chunk_text(text)
    print(f"    {len(chunks):,} chunks", flush=True)

    batch_texts, batch_figs = [], []
    inserted = 0

    for chunk in chunks:
        batch_texts.append(chunk)
        batch_figs.append(extract_figures(chunk))

        if len(batch_texts) >= 64:
            embeddings = embed_batch(batch_texts)
            for c, emb, fig in zip(batch_texts, embeddings, batch_figs):
                cur.execute("""
                    INSERT INTO documents (content, source, source_type, era, embedding, figures, word_count)
                    VALUES (%s,%s,%s,%s,%s::vector,%s,%s) ON CONFLICT DO NOTHING
                """, (c, source_name, source_type, era, emb, fig or None, len(c.split())))
            conn.commit()
            inserted += len(batch_texts)
            print(f"      {inserted:,}...", flush=True)
            batch_texts, batch_figs = [], []
            time.sleep(0.3)

    if batch_texts:
        embeddings = embed_batch(batch_texts)
        for c, emb, fig in zip(batch_texts, embeddings, batch_figs):
            cur.execute("""
                INSERT INTO documents (content, source, source_type, era, embedding, figures, word_count)
                VALUES (%s,%s,%s,%s,%s::vector,%s,%s) ON CONFLICT DO NOTHING
            """, (c, source_name, source_type, era, emb, fig or None, len(c.split())))
        conn.commit()
        inserted += len(batch_texts)

    print(f"    ✅ {inserted:,} chunks inserted", flush=True)
    return inserted


# ═══════════════════════════════════════════════════════
# SOURCE DEFINITIONS
# ═══════════════════════════════════════════════════════

SOURCES = [
    # ── PERSIAN HISTORY ───────────────────────────────
    {
        "source": "Al-Tabari, Tarikh al-Rusul wa al-Muluk (History of Prophets and Kings)",
        "era": "abbasid",
        "type": "historical",
        "identifier": "tarikh-al-tabari",
        "files": [f"Tabari_Volume_{i:02d}_djvu.txt" for i in range(1, 41)],
    },
    {
        "source": "Ata-Malik Juvaini, Tarikh-i-Jahan-Gusha (History of the World Conqueror)",
        "era": "mongol",
        "type": "historical",
        "identifier": "historyoftheworl011691mbp",
        "files": ["historyoftheworl011691mbp_djvu.txt"],
    },
    {
        "source": "Ata-Malik Juvaini, Tarikh-i-Jahan-Gusha Vol II",
        "era": "mongol",
        "type": "historical",
        "identifier": "historyoftheworl011648mbp",
        "files": ["historyoftheworl011648mbp_djvu.txt"],
    },
    {
        "source": "Firdausi, Shahnameh (The Persian Book of Kings)",
        "era": "pre-islamic",
        "type": "literary",
        "identifier": "shahnameh-the-persian-book-of-kings",
        "files": ["Shahnameh- The Persian Book of Kings_djvu.txt"],
    },
    {
        "source": "Edward Granville Browne, A Literary History of Persia",
        "era": "multi-era",
        "type": "secondary",
        "identifier": "volume-2-a-literary-history-of-persia",
        "files": ["A literary history of Persia Vol 1- Browne, Edward Granville, 1862-_djvu.txt"],
    },
    {
        "source": "Percy Sykes, A History of Persia Vol I",
        "era": "multi-era",
        "type": "secondary",
        "identifier": "historyofpersiasykesp.m.vol1_948_p",
        "files": ["History of Persia Sykes P.M. Vol 1_djvu.txt"],
    },
    {
        "source": "Percy Sykes, A History of Persia Vol II",
        "era": "multi-era",
        "type": "secondary",
        "identifier": "historyofpersiasykesp.m.vol2_790_s",
        "files": ["History of Persia Sykes P.M. Vol 2_djvu.txt"],
    },
    {
        "source": "Hamdullah Mustawfi, Nuzhat al-Qulub (Geographical Part)",
        "era": "mongol",
        "type": "geographical",
        "identifier": "TheGeographicalPartOFTheNuzhatAlQulub",
        "files": ["TheGeographicalPartOFTheNuzhatAlQulub_djvu.txt"],
    },
    {
        "source": "Ibn Isfandiyar, History of Tabaristan",
        "era": "abbasid",
        "type": "historical",
        "identifier": "abridgedtranslat00ibniuoft",
        "files": ["abridgedtranslat00ibniuoft_djvu.txt"],
    },
    {
        "source": "Khwandamir, Qanun-i-Humayuni",
        "era": "timurid",
        "type": "historical",
        "identifier": "Qanun-i-Humayun-Khwandamir",
        "files": ["216998_Qanun-I-Humayuni_djvu.txt"],
    },
    {
        "source": "Nizami Ganjavi, Leyla and Majnun",
        "era": "seljuk",
        "type": "literary",
        "identifier": "leyla-and-majnun",
        "files": ["Leyla and Majnun_djvu.txt"],
    },
    {
        "source": "Rashid al-Din Fazlullah, Introduction to the History of the Mongols",
        "era": "mongol",
        "type": "historical",
        "identifier": "introductionlh00blocuoft",
        "files": ["introductionlh00blocuoft_djvu.txt"],
    },

    # ── CAUCASUS ──────────────────────────────────────
    {
        "source": "John F. Baddeley, The Russian Conquest of the Caucasus",
        "era": "modern",
        "type": "secondary",
        "identifier": "cu31924028754616",
        "files": ["cu31924028754616_djvu.txt"],
    },
    {
        "source": "Moshe Gammer, Muslim Resistance to the Tsar: Shamil and the Conquest of Chechnia and Daghestan",
        "era": "modern",
        "type": "secondary",
        "identifier": "muslimresistance0000gamm",
        "files": ["muslimresistance0000gamm_djvu.txt"],
    },

    # ── TIMUR / TAMERLANE ─────────────────────────────
    {
        "source": "Ahmed Ibn Arabshah, Tamerlane or Timur the Great Amir",
        "era": "timurid",
        "type": "historical",
        "identifier": "TamerlaneOrTimurTheGreatAmir-AhmedIbnArabshah",
        "files": ["216572337-Tamerlane-or-Timur-the-Great-Amir-Ahmed-Ibn-Arabshah_djvu.txt"],
    },
    {
        "source": "Ruy Gonzalez de Clavijo, Embassy to Tamerlane 1403-1406",
        "era": "timurid",
        "type": "historical",
        "identifier": "narrativeembass00markgoog",
        "files": ["narrativeembass00markgoog_djvu.txt"],
    },
]


# ═══════════════════════════════════════════════════════
# MAIN INGEST LOOP
# ═══════════════════════════════════════════════════════

total_inserted = 0
total_skipped = 0

print("=" * 60, flush=True)
print("PERSIA / CAUCASUS / TIMUR INGEST", flush=True)
print(f"{len(SOURCES)} sources to process", flush=True)
print("=" * 60, flush=True)

for src in SOURCES:
    source_name = src["source"]
    print(f"\n{'─'*50}", flush=True)
    print(f"→ {source_name}", flush=True)

    if already_ingested(source_name):
        print(f"  SKIP — already ingested", flush=True)
        total_skipped += 1
        continue

    all_text = ""
    for filename in src["files"]:
        try:
            filepath = download_djvu(src["identifier"], filename)
            text = filepath.read_text(encoding='utf-8', errors='replace')
            all_text += text + "\n\n"
        except Exception as e:
            print(f"    ⚠️  Failed {filename}: {e}", flush=True)
            continue

    if not all_text.strip():
        print(f"  SKIP — no text downloaded", flush=True)
        continue

    print(f"  Raw text: {len(all_text):,} chars", flush=True)
    inserted = ingest_text(all_text, source_name, src["era"], src["type"])
    total_inserted += inserted
    time.sleep(2)

# ═══════════════════════════════════════════════════════
# FINAL REPORT
# ═══════════════════════════════════════════════════════

cur.execute("SELECT COUNT(*) FROM documents")
total_corpus = cur.fetchone()[0]

print(f"\n{'='*60}", flush=True)
print(f"INGEST COMPLETE", flush=True)
print(f"  New chunks:     {total_inserted:,}", flush=True)
print(f"  Skipped:        {total_skipped}", flush=True)
print(f"  Total corpus:   {total_corpus:,}", flush=True)
print(f"{'='*60}", flush=True)

# Show all sources with persia/caucasus/timur era tags
print("\nNew sources in corpus:", flush=True)
cur.execute("""
    SELECT source, COUNT(*) as chunks, era
    FROM documents
    WHERE source ILIKE ANY(ARRAY[
        '%Tabari%Tarikh%', '%Juvaini%', '%Shahnameh%',
        '%Browne%Persia%', '%Sykes%Persia%', '%Mustawfi%',
        '%Isfandiyar%', '%Khwandamir%', '%Nizami%',
        '%Rashid al-Din%', '%Baddeley%', '%Gammer%',
        '%Ibn Arabshah%', '%Clavijo%'
    ])
    GROUP BY source, era
    ORDER BY chunks DESC
""")
for row in cur.fetchall():
    print(f"  {row[1]:>6,}  [{row[2]:10s}]  {row[0][:55]}", flush=True)

conn.close()
