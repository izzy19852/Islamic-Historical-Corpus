"""
Ingest Kritovoulos — translate Greek/Latin from FHG vol 5 via Claude Haiku, embed, insert.
"""
import os, re, sys, time, yaml, psycopg2, voyageai, requests, anthropic
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SOURCES_BASE = os.path.join(os.path.dirname(__file__), '..', 'sources')

sys.path.insert(0, os.path.dirname(__file__))
from batch_session4a import extract_figures

VOYAGE_BATCH_SIZE = 64
TARGET_CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_CHUNK_WORDS = 100


def embed_batch(vo, texts):
    for attempt in range(5):
        try:
            result = vo.embed(texts, model="voyage-2", input_type="document")
            return result.embeddings
        except Exception as e:
            err = str(e).lower()
            if "rate limit" in err or "reduced rate" in err:
                wait = 25 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
            elif "max allowed tokens" in err and len(texts) > 1:
                mid = len(texts) // 2
                left = embed_batch(vo, texts[:mid])
                right = embed_batch(vo, texts[mid:])
                return left + right
            elif attempt < 4:
                wait = 2 ** (attempt + 1)
                print(f"  Voyage error: {e}, retry in {wait}s...", flush=True)
                time.sleep(wait)
            else:
                raise


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


def main():
    print("=== Kritovoulos Translation + Ingestion ===", flush=True)

    claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    vo = voyageai.Client(api_key=VOYAGE_API_KEY)
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Check if already ingested
    cur.execute("SELECT short_name FROM sources WHERE short_name = 'kritovoulos-mehmed'")
    if cur.fetchone():
        print("Already in DB — skipping", flush=True)
        conn.close()
        return

    dest_dir = os.path.join(SOURCES_BASE, 'ottoman_extra')
    os.makedirs(dest_dir, exist_ok=True)
    translated_path = os.path.join(dest_dir, 'kritovoulos-mehmed-translated.txt')

    if os.path.exists(translated_path):
        print(f"Found existing translation file", flush=True)
        with open(translated_path, 'r', encoding='utf-8') as f:
            translated_text = f.read()
    else:
        # Download
        print("Downloading FHG vol 5 from Internet Archive...", flush=True)
        r = requests.get('https://archive.org/download/cuafragmentahist05mull/cuafragmentahist05mull_djvu.txt', timeout=180)
        if r.status_code != 200:
            print(f"FAILED: HTTP {r.status_code}", flush=True)
            conn.close()
            return

        # Extract Critobulus section
        raw = r.text[321:1486505]

        # Clean
        lines = raw.split('\n')
        cleaned = []
        in_preface = True
        for line in lines:
            s = line.strip()
            if not s:
                continue
            if len(s) < 10 and re.match(r'^[\dxivXIV\s\.\-]+$', s):
                continue
            if re.match(r'^\(\d+\)', s):
                continue
            if re.match(r'^[\d\s\.\,\;\:\-\(\)]+$', s):
                continue
            if in_preface:
                for marker in ['ΚΡΙΤΟΒΟΥΛΟΥ', 'ΒΙΒΛΟΣ', 'MECHEMETIS']:
                    if marker in s:
                        in_preface = False
                        break
                if in_preface:
                    continue
            cleaned.append(s)

        text = '\n'.join(cleaned)
        print(f"Cleaned source: {len(text.split()):,} words", flush=True)

        # Chunk source for translation (800 words, no overlap)
        words = text.split()
        src_chunks = []
        for i in range(0, len(words), 800):
            c = ' '.join(words[i:i + 800])
            if len(c.split()) >= 50:
                src_chunks.append(c)
        print(f"Source chunks for translation: {len(src_chunks)}", flush=True)

        # Translate
        translated_chunks = []
        failed = 0
        t0 = time.time()

        for i, chunk in enumerate(src_chunks):
            for attempt in range(3):
                try:
                    resp = claude.messages.create(
                        model="claude-haiku-4-5-20251001",
                        max_tokens=4096,
                        messages=[{"role": "user", "content": f"Translate the following Greek and Latin text into clear, scholarly English. This is from Kritovoulos of Imbros's 'History of Mehmed the Conqueror' (1467), an eyewitness account of Ottoman conquests including the Fall of Constantinople. Skip unintelligible OCR artifacts. Output only the English translation, no commentary.\n\n---\n{chunk}\n---"}]
                    )
                    translated_chunks.append(resp.content[0].text)
                    break
                except anthropic.RateLimitError:
                    wait = 30 * (attempt + 1)
                    print(f"  Rate limited at chunk {i+1}, waiting {wait}s...", flush=True)
                    time.sleep(wait)
                except Exception as e:
                    if attempt < 2:
                        wait = 5 * (attempt + 1)
                        print(f"  Error at chunk {i+1}: {e}, retry in {wait}s...", flush=True)
                        time.sleep(wait)
                    else:
                        print(f"  FAILED chunk {i+1}: {e}", flush=True)
                        failed += 1
            else:
                failed += 1

            if (i + 1) % 25 == 0 or i == len(src_chunks) - 1:
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed * 60
                remaining = (len(src_chunks) - i - 1) / rate if rate > 0 else 0
                print(f"  Translated {i+1}/{len(src_chunks)} ({failed} failed) — {rate:.0f} chunks/min, ~{remaining:.0f} min remaining", flush=True)

            time.sleep(0.3)

        print(f"Translation complete: {len(translated_chunks)} OK, {failed} failed in {time.time()-t0:.0f}s", flush=True)

        if not translated_chunks:
            print("No translations — aborting", flush=True)
            conn.close()
            return

        translated_text = '\n\n'.join(translated_chunks)
        with open(translated_path, 'w', encoding='utf-8') as f:
            f.write(translated_text)
        print(f"Saved translation: {len(translated_text.split()):,} words", flush=True)

    # Chunk translated English text for embedding
    eng_chunks = chunk_text(translated_text)
    total_words = sum(len(c.split()) for c in eng_chunks)
    print(f"English chunks for embedding: {len(eng_chunks)}, {total_words:,} words", flush=True)

    # Embed + insert
    inserted = 0
    all_figures = set()
    for batch_start in range(0, len(eng_chunks), VOYAGE_BATCH_SIZE):
        batch = eng_chunks[batch_start:batch_start + VOYAGE_BATCH_SIZE]
        try:
            embeddings = embed_batch(vo, batch)
        except Exception as e:
            print(f"  Embedding failed: {e}", flush=True)
            continue

        for idx, (chunk, emb) in enumerate(zip(batch, embeddings)):
            figures = extract_figures(chunk)
            if figures:
                all_figures.update(figures)
            cur.execute("""
                INSERT INTO documents (content, embedding, source, source_type, era, figures, chunk_index, word_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (chunk, str(emb),
                  "Kritovoulos, History of Mehmed the Conqueror (eyewitness account, 1467)",
                  "primary_greek", "ottoman",
                  figures, batch_start + idx, len(chunk.split())))
            inserted += 1
        conn.commit()
        print(f"  Embedded {min(batch_start + len(batch), len(eng_chunks))}/{len(eng_chunks)} chunks", flush=True)
        time.sleep(0.5)

    # Register source
    cur.execute("""
        INSERT INTO sources (name, short_name, source_type, language, translator, era_coverage, reliability, chunk_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, ("Kritovoulos, History of Mehmed the Conqueror (eyewitness account, 1467)",
          "kritovoulos-mehmed", "primary_greek", "english",
          "claude-haiku-4-5 (from Greek/Latin)", ["ottoman"], "scholarly", inserted))
    conn.commit()

    # YAML sidecar
    yaml_path = os.path.join(dest_dir, 'kritovoulos-mehmed.yaml')
    meta = {
        "source": "Kritovoulos, History of Mehmed the Conqueror (eyewitness account, 1467)",
        "short_name": "kritovoulos-mehmed",
        "source_type": "primary_greek",
        "era": ["ottoman"],
        "reliability": "scholarly",
        "original_language": "Greek and Latin",
        "translator": "claude-haiku-4-5 (from Greek/Latin)",
        "word_count": total_words,
        "note": "AI-translated from Greek/Latin (FHG vol 5, Müller ed.) via Claude Haiku.",
    }
    with open(yaml_path, 'w') as f:
        yaml.dump(meta, f, default_flow_style=False)

    fig_str = f", figures: {sorted(all_figures)}" if all_figures else ""
    print(f"\nDone: {inserted} chunks, {total_words:,} words{fig_str}", flush=True)

    cur.execute("SELECT COUNT(*) FROM documents")
    print(f"Total documents in DB: {cur.fetchone()[0]:,}", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
