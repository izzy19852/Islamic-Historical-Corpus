"""
Ingest all 9 Fawazahmed0 hadith collections.
CDN-hosted JSON — no key, no rate limit.
Expected output: ~25,000+ chunks
"""

import os, json, time, requests, psycopg2, voyageai
from dotenv import load_dotenv

load_dotenv('/home/islam_abdallah_ia85/islam-stories/.env')
conn = psycopg2.connect(os.getenv('ISLAM_STORIES_DB_URL'))
cur  = conn.cursor()
vo   = voyageai.Client(api_key=os.getenv('VOYAGE_AI_API_KEY'))

COLLECTIONS = {
    "eng-bukhari":        "Sahih al-Bukhari",
    "eng-muslim":         "Sahih Muslim",
    "eng-abudawud":       "Sunan Abu Dawud",
    "eng-tirmidhi":       "Jami at-Tirmidhi",
    "eng-nasai":          "Sunan an-Nasai",
    "eng-ibnmajah":       "Sunan Ibn Majah",
    "eng-nawawi40":       "Forty Hadith Nawawi",
    "eng-riyadussalihin": "Riyad as-Salihin",
    "eng-bulughalmaram":  "Bulugh al-Maram",
}

BASE_URL = "https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions/{}.min.json"

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

def chunk_already_exists(source, hadith_num):
    cur.execute(
        "SELECT 1 FROM documents WHERE source = %s AND content LIKE %s LIMIT 1",
        (source, f"% Hadith {hadith_num}:%")
    )
    return cur.fetchone() is not None

total_inserted = 0

for edition, collection_name in COLLECTIONS.items():
    print(f"\n→ Fetching {collection_name}...")
    url = BASE_URL.format(edition)

    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  FAILED: {e}")
        continue

    hadiths = data.get("hadiths", [])
    print(f"  Found {len(hadiths):,} hadiths")

    batch_texts, batch_meta = [], []

    for h in hadiths:
        text = h.get("text", "").strip()
        if not text or len(text) < 30:
            continue

        hadith_num = h.get("hadithnumber", "")
        source_str = f"{collection_name}"

        # Skip if already in DB
        if chunk_already_exists(source_str, hadith_num):
            continue

        # Determine chain_strength from grades
        grades = h.get("grades", [])
        grade_text = " ".join(g.get("grade","").lower() for g in grades)
        if "sahih" in grade_text:
            chain = "sahih"
        elif "hasan" in grade_text:
            chain = "hasan"
        elif "daif" in grade_text or "weak" in grade_text:
            chain = "daif"
        else:
            chain = "unknown"

        content = f"{collection_name} Hadith {hadith_num}: {text}"
        figures = extract_figures(content)

        batch_texts.append(content)
        batch_meta.append({
            "content":      content,
            "source":       source_str,
            "source_type":  "hadith",
            "era":          "rashidun",
            "chain_strength": chain,
            "figures":      figures,
            "hadith_num":   str(hadith_num),
        })

        if len(batch_texts) >= 128:
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
            total_inserted += len(batch_texts)
            print(f"  ... {total_inserted:,} inserted so far")
            batch_texts, batch_meta = [], []
            time.sleep(0.5)

    # Flush remainder
    if batch_texts:
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
        total_inserted += len(batch_texts)

    print(f"  ✅ {collection_name}: complete")
    time.sleep(2)  # polite pause between collections

print(f"\n=== SECTION A COMPLETE: {total_inserted:,} hadith chunks inserted ===")
cur.execute("SELECT COUNT(*) FROM documents WHERE source_type='hadith'")
print(f"Total hadith chunks in DB: {cur.fetchone()[0]:,}")
