# Islamic Historical Corpus

> 🕌 **Don't want to run this yourself?**
> The full corpus is live at
> **[islamiccorpus.com](https://islamiccorpus.com)**
> — free chat tier, no setup required.
> [Get an API key →](https://islamiccorpus.com/account)
> | [Try the chat →](https://islamiccorpus.com/chat)

---


The world's first structured, classified, AI-ready Islamic
historical knowledge base.

**133,789+ chunks | 238 sources | 22 eras | 1,400 years**

Every chunk is era-tagged, source-attributed, and
chain-strength classified against the Islamic scholarly
tradition (sahih / hasan / daif / scholarly).

---

## What's in the Corpus

### Primary Islamic Sources (Fully Authenticated)
| Source | Era | Coverage |
|---|---|---|
| Al-Tabari, Tarikh al-Rusul | Rashidun–Abbasid | Vols 10–38 |
| Ibn Kathir, Al-Bidaya wa'l-Nihaya | All eras | Key volumes |
| Ibn Sa'd, Tabaqat al-Kabir | Rashidun | Vols I–VIII |
| Al-Waqidi, Kitab al-Maghazi | Rashidun | Battle narratives |
| Al-Masudi, Muruj al-Dhahab | Abbasid | Full text |
| Ibn al-Athir, Al-Kamil fi al-Tarikh | Crusades/Mongol | Chronicle |
| Ibn Jubayr, Rihla | Crusades | Eyewitness travels |
| Al-Baladhuri, Futuh al-Buldan | Rashidun | Conquests |
| Ibn Hisham / Ibn Ishaq, Sira | Rashidun | Prophet era |
| Ibn Khaldun, Muqaddimah | All eras | Vols 1–3 |
| Ibn Battuta, Rihla | Africa/Asia | Vols 1–2 |

### Hadith Collections (All Major Canonical Collections)
Sahih Bukhari · Sahih Muslim · Abu Dawud · Tirmidhi ·
Nasai · Ibn Majah · Nawawi 40 · Riyad as-Salihin ·
Bulugh al-Maram

### Knowledge Graph
- **1839 figures** — sensitivity tier, dramatic function,
  era, known relationships, scholarly debates
- **32+ events** — year, era, causal chains, contested readings
- **Lineage chains** — biological, ideological, military patron
- **Source relationships** — corroborates / contradicts / supplements

---

## Use Cases

- **Islamic history research** — query 1,400 years of
  authenticated primary sources in natural language
- **AI applications** — ground LLMs in authenticated
  Islamic scholarship instead of Wikipedia
- **Education** — build Islamic studies tools with
  proper source attribution
- **Fact-checking** — verify claims against primary sources
  with chain strength metadata

---

## Quick Start

### Option 1: Use the API (no setup required)
```bash
curl -X POST https://islamiccorpus.com/query \
  -H "X-API-Key: your_key_here" \
  -H "Content-Type: application/json" \
  -d '{"q": "Khalid ibn Walid Battle of Yarmouk", "n": 5}'
```
API access starts at the Researcher tier ($19/mo, 100 queries/month). Sign up at **[islamiccorpus.com/account](https://islamiccorpus.com/account)**. Free chat access (50 queries/month) is available without an API key at **[islamiccorpus.com/chat](https://islamiccorpus.com/chat)**.

### Option 2: Run it yourself

```bash
# Prerequisites: Python 3.10+, PostgreSQL 14+ with pgvector

git clone https://github.com/izzy19852/islamic-historical-corpus
cd islamic-historical-corpus

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your DATABASE_URL, VOYAGE_AI_API_KEY, ANTHROPIC_API_KEY

# Create schema
psql $DATABASE_URL < schema/schema.sql

# Run ingestion (builds corpus from public sources)
python ingest/ingest_hadith_quran.py
python ingest/ingest_classical_sources.py
# ... see docs/ingestion_guide.md for full sequence

# Start API
uvicorn api.main:app --host 0.0.0.0 --port 8001
```

---

## Ethical Notes

This project charges **only for infrastructure access** —
not for the scholarship itself. The classical texts belong
to their authors and the Islamic scholarly tradition.

- All ingested sources are public domain or open access
- Modern copyrighted translations are not redistributed
- The knowledge graph metadata is original work (CC-BY)
- Pipeline code is MIT licensed

If this corpus is useful to your organization and you
cannot afford the API costs, reach out. We will work
something out.

---

## Corpus Authentication

Sources are classified into three tiers:

| Tier | Description | Examples |
|---|---|---|
| `classical_islamic` | Accepted by Islamic scholarly tradition | Tabari, Ibn Kathir, hadith collections |
| `primary_historical` | Contemporary accounts, non-canonical | Ibn Jubayr, Evliya Celebi |
| `scholarly_western` | Modern academic sources | OAPEN, university press |

Every chunk carries `chain_strength` metadata:
`sahih` · `hasan` · `daif` · `scholarly` · `unknown`

---

## Contributing

The pipeline is open source. Contributions welcome:

- New authenticated sources (open access / public domain only)
- Improved chunking strategies
- Additional knowledge graph entries
- Bug fixes

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) for guidelines.

---

## License

- **Code**: MIT
- **Knowledge graph metadata**: CC-BY 4.0
  (attribute: Islamic Historical Corpus)
- **Corpus text**: not redistributed —
  each source retains its original license

---

## Citation

If you use this in research:

```
Islamic Historical Corpus (2025).
Structured Islamic Historical Knowledge Base.
https://github.com/izzy19852/islamic-historical-corpus
```
