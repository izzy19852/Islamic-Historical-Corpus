"""
Ingest Riyad as-Salihin (~1,896 hadiths) and Bulugh al-Maram (~1,767 hadiths).
Sources: Jaguar16/open-hadith-data and AhmedBaset/hadith-json on GitHub.
"""

import os, json, time, zipfile, psycopg2, voyageai
from dotenv import load_dotenv

load_dotenv('/home/islam_abdallah_ia85/islam-stories/.env')
conn = psycopg2.connect(os.getenv('ISLAM_STORIES_DB_URL'))
cur  = conn.cursor()
vo   = voyageai.Client(api_key=os.getenv('VOYAGE_AI_API_KEY'))

KNOWN_FIGURES = [
    "Abu Bakr","Umar ibn al-Khattab","Uthman ibn Affan","Ali ibn Abi Talib",
    "Aisha","Khadijah","Fatimah","Khalid ibn Walid","Bilal ibn Rabah",
    "Salman al-Farisi","Abu Hurairah","Ibn Abbas","Ibn Umar","Anas ibn Malik",
    "Abu Musa al-Ashari","Muadh ibn Jabal","Zayd ibn Thabit","Nusayba",
    "Abu Dharr","Amr ibn al-As","Sad ibn Abi Waqqas","Usamah ibn Zayd",
    "Hamza ibn Abd al-Muttalib","Zubayr ibn al-Awwam","Talha ibn Ubaydullah",
]

def extract_figures(text):
    return [f for f in KNOWN_FIGURES if f.lower() in text.lower()]

def embed_batch(texts):
    result = vo.embed(texts, model="voyage-2", input_type="document")
    return result.embeddings

def source_exists(source, hadith_num):
    cur.execute(
        "SELECT 1 FROM documents WHERE source = %s AND content LIKE %s LIMIT 1",
        (source, f"% Hadith {hadith_num}:%")
    )
    return cur.fetchone() is not None

def flush_batch(batch_texts, batch_meta):
    if not batch_texts:
        return 0
    embeddings = embed_batch(batch_texts)
    for meta, emb in zip(batch_meta, embeddings):
        cur.execute("""
            INSERT INTO documents
            (content, source, source_type, era, embedding, figures, word_count)
            VALUES (%s,%s,%s,%s,%s::vector,%s,%s)
            ON CONFLICT DO NOTHING
        """, (
            meta["content"], meta["source"], meta["source_type"],
            meta["era"], emb,
            meta["figures"] or None,
            len(meta["content"].split()),
        ))
    conn.commit()
    return len(batch_texts)


# ─── RIYAD AS-SALIHIN ───────────────────────────────────────────────

print("=" * 60)
print("RIYAD AS-SALIHIN")
print("=" * 60)

zf = zipfile.ZipFile('/tmp/open-hadith-collections.zip')
riyad_data = json.loads(zf.read('riyadussalihin.json'))

total_riyad = 0
batch_texts, batch_meta = [], []

for book in riyad_data['books']:
    book_name = book.get('name_en', '')
    for h in book.get('hadiths', []):
        text_en = h.get('text_en', '').strip()
        if not text_en or len(text_en) < 30:
            continue

        hadith_num = h.get('hadith_number', '')
        source_str = "Riyad as-Salihin"

        if source_exists(source_str, hadith_num):
            continue

        # Build grade from text hints
        grade = h.get('grade_en', '') or ''
        grade_lower = grade.lower()
        if 'sahih' in grade_lower:
            chain = 'sahih'
        elif 'hasan' in grade_lower:
            chain = 'hasan'
        elif 'daif' in grade_lower or 'weak' in grade_lower:
            chain = 'daif'
        else:
            chain = 'unknown'

        content = f"Riyad as-Salihin Hadith {hadith_num}: {text_en}"
        figures = extract_figures(content)

        batch_texts.append(content)
        batch_meta.append({
            "content": content,
            "source": source_str,
            "source_type": "hadith",
            "era": "rashidun",
            "figures": figures,
        })

        if len(batch_texts) >= 128:
            total_riyad += flush_batch(batch_texts, batch_meta)
            print(f"  ... {total_riyad:,} inserted so far")
            batch_texts, batch_meta = [], []
            time.sleep(0.5)

total_riyad += flush_batch(batch_texts, batch_meta)
batch_texts, batch_meta = [], []
print(f"\n  Riyad as-Salihin COMPLETE: {total_riyad:,} chunks inserted")


# ─── BULUGH AL-MARAM ────────────────────────────────────────────────

print("\n" + "=" * 60)
print("BULUGH AL-MARAM")
print("=" * 60)

bulugh_data = json.load(open('/tmp/bulugh_almaram.json'))
hadiths_bulugh = bulugh_data.get('hadiths', [])
print(f"  Found {len(hadiths_bulugh):,} hadiths in JSON")

total_bulugh = 0
batch_texts, batch_meta = [], []

for h in hadiths_bulugh:
    eng = h.get('english', {})
    narrator = eng.get('narrator', '').strip()
    text = eng.get('text', '').strip()
    if not text or len(text) < 30:
        continue

    hadith_num = h.get('idInBook', '')
    source_str = "Bulugh al-Maram"

    if source_exists(source_str, hadith_num):
        continue

    full_text = f"{narrator} {text}".strip() if narrator else text
    content = f"Bulugh al-Maram Hadith {hadith_num}: {full_text}"
    figures = extract_figures(content)

    batch_texts.append(content)
    batch_meta.append({
        "content": content,
        "source": source_str,
        "source_type": "hadith",
        "era": "rashidun",
        "figures": figures,
    })

    if len(batch_texts) >= 128:
        total_bulugh += flush_batch(batch_texts, batch_meta)
        print(f"  ... {total_bulugh:,} inserted so far")
        batch_texts, batch_meta = [], []
        time.sleep(0.5)

total_bulugh += flush_batch(batch_texts, batch_meta)
print(f"\n  Bulugh al-Maram COMPLETE: {total_bulugh:,} chunks inserted")


# ─── SUMMARY ────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Riyad as-Salihin: {total_riyad:,} new chunks")
print(f"  Bulugh al-Maram:  {total_bulugh:,} new chunks")
print(f"  Total new:        {total_riyad + total_bulugh:,} chunks")

cur.execute("SELECT COUNT(*) FROM documents WHERE source_type='hadith'")
print(f"\n  Total hadith chunks in DB: {cur.fetchone()[0]:,}")
cur.execute("SELECT COUNT(*) FROM documents")
print(f"  Total chunks in DB:        {cur.fetchone()[0]:,}")
