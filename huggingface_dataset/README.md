---
license: cc-by-4.0
task_categories:
  - question-answering
  - text-retrieval
language:
  - en
  - ar
tags:
  - islamic-history
  - rag
  - hadith
  - arabic
  - digital-humanities
  - historical
  - religion
pretty_name: Islamic Historical Corpus — Source Manifest
size_categories:
  - n<1K
---

# Islamic Historical Corpus — Source Manifest

The public source manifest for the
**Islamic Historical Corpus** — the world's first
classified, AI-ready Islamic historical knowledge base.

## What This Dataset Contains

This dataset is the **public source manifest only** —
a catalog of the 216 primary sources ingested into the
corpus, with authentication tiers and chunk counts.
No copyrighted translation text is included.

| File | Records | Description |
|---|---|---|
| `sources_manifest.json` | 216 | Full corpus manifest with authentication tiers |

## The Full Corpus

The underlying corpus contains **133,789+ chunks** (24M+ words) across
**238+ sources** spanning **1,400 years** of Islamic history
(632–1900 CE).

Every chunk is:
- **Era-tagged** — rashidun, umayyad, abbasid, crusades,
  ottoman, south_asia, africa, andalusia, persia, timur,
  caucasus, mongol + 10 more
- **Source-attributed** — Al-Tabari, Ibn Kathir,
  Ibn Sa'd, all major hadith collections + 200 more
- **Chain-strength classified** — sahih / hasan / daif /
  scholarly / unknown
- **Authentication-flagged** — classical_islamic /
  primary_arabic / scholarly_western

Query the full corpus via the API:
**https://islamiccorpus.com**

## Sources Covered

### Classical Islamic Canon (fully authenticated)
- Al-Tabari — Tarikh al-Rusul (Vols 10–38)
- Ibn Kathir — Al-Bidaya wa'l-Nihaya
- Ibn Sa'd — Tabaqat al-Kabir (Vols I–VIII)
- Al-Masudi — Muruj al-Dhahab
- Ibn al-Athir — Al-Kamil fi al-Tarikh
- Ibn Jubayr — Rihla (eyewitness 1183–85 CE)
- Al-Baladhuri — Futuh al-Buldan
- Ibn Hisham / Ibn Ishaq — Sira
- Ibn Khaldun — Muqaddimah (Vols 1–3)

### Hadith Collections (all 9 canonical)
Bukhari · Muslim · Abu Dawud · Tirmidhi · Nasai ·
Ibn Majah · Nawawi 40 · Riyad as-Salihin ·
Bulugh al-Maram

### Persia / Caucasus / Timur
- Firdausi — Shahnameh
- Juvaini — History of the World Conqueror
- Rashid al-Din — Jami al-Twarikh
- Baddeley — Russian Conquest of the Caucasus
- Gammer — Muslim Resistance to the Tsar (Shamil)
- Ibn Arabshah — Life of Timur
- Clavijo — Embassy to Tamerlane

### + 190 more sources across 22 eras

## Use Cases

- **Islamic NLP research** — entity recognition,
  relation extraction, historical QA
- **AI grounding** — fact-check LLM outputs against
  authenticated Islamic sources
- **Digital humanities** — computational Islamic studies
- **Education technology** — Islamic history curriculum tools
- **Content creation** — authenticated source grounding
  for Islamic history content

## 🔌 Live API — No Setup Required

The full corpus is queryable right now at:

**[islamiccorpus.com](https://islamiccorpus.com)**

```bash
# Get an API key at islamiccorpus.com/account (Researcher tier and up)
# then query instantly:

curl -X POST https://islamiccorpus.com/query \
  -H "X-API-Key: your_key" \
  -H "Content-Type: application/json" \
  -d '{"q": "Khalid ibn Walid Battle of Yarmouk", "n": 5}'

# Or use the research endpoint for structured context:
curl -X POST https://islamiccorpus.com/research \
  -H "X-API-Key: your_key" \
  -H "Content-Type: application/json" \
  -d '{"figure": "Saladin", "event": "Fall of Jerusalem", "era": "crusades"}'
```

### Pricing
| Tier | API queries/month | Price |
|---|---|---|
| Free | — (chat only, 50 queries/month) | $0 |
| Researcher | 100 | $19/mo |
| Developer | 10,000 | $49/mo |
| Institutional | Unlimited | $149/mo |

### Try the Chat Interface
Ask questions in natural language at
**[islamiccorpus.com/chat](https://islamiccorpus.com/chat)**
— grounded in authenticated Islamic sources,
every answer cited.

### Python

```python
import requests

resp = requests.post(
    "https://islamiccorpus.com/query",
    headers={"X-API-Key": "your_key"},
    json={"q": "Khalid ibn Walid Battle of Yarmouk", "n": 5},
)
results = resp.json()
# Returns ranked chunks with source, era,
# chain_strength, similarity score
```

## Ethical Framework

This dataset is released under CC-BY 4.0.

The classical Islamic texts themselves belong to
their authors and to Islamic civilization. We charge
only for infrastructure access to the full corpus API —
not for the scholarship. The source manifest
(this dataset) is free.

See: https://islamiccorpus.com/#ethics

## Citation

```bibtex
@dataset{islamic_historical_corpus_2026,
  title     = {Islamic Historical Corpus —
               Source Manifest},
  author    = {Islamic Historical Corpus},
  year      = {2026},
  url       = {https://huggingface.co/datasets/
               izzy19852/islamic-historical-corpus},
  note      = {Public source manifest for the 128K+
               chunk Islamic historical RAG corpus.
               Full corpus API at islamiccorpus.com}
}
```

## Pipeline

The ingestion pipeline that built this corpus
is fully open source:

**https://github.com/izzy19852/Islamic-Historical-Corpus**

Anyone can reproduce the corpus by running the
pipeline against the same public domain sources.