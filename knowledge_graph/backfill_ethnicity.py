"""
Islam Stories — Ethnicity Backfill
Persists inferred ethnicity to DB for all figures missing corpus-extracted ethnicity.
Uses the same inference logic as generate_cultural_profiles.py:
  1. Corpus-extracted ethnicity (already in DB)
  2. Name-based hints (known ethnic backgrounds)
  3. Era default (fallback)

Run:  python -m rag.knowledge.backfill_ethnicity [--dry-run]
"""

import os
import sys

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..', '..')
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


ERA_DEFAULT_ETHNICITY = {
    "rashidun":             "Qurayshi Arab",
    "umayyad":              "Arab",
    "abbasid":              "Arab-Persian",
    "andalusia":            "Andalusian Moor",
    "crusades":             "Arab-Kurdish",
    "mamluk":               "Mamluk (mixed Turkic-Arab)",
    "mongol":               "Central Asian",
    "ottoman":              "Ottoman Turkic",
    "south_asia":           "South Asian",
    "africa":               "West African",
    "resistance_colonial":  "varies",
    "persia":               "Persian",
}

ETHNICITY_HINTS = {
    "Berber":           ["Tariq ibn Ziyad", "Musa ibn Nusayr"],
    "Persian":          ["Salman al-Farsi", "Salman al-Farisi", "Abu Muslim al-Khurasani",
                         "Nizam al-Mulk", "Ibn Sina", "Al-Biruni", "Ferdowsi",
                         "Al-Khwarizmi", "Al-Tabari", "Firdawsi"],
    "Kurdish":          ["Saladin", "Nur ad-Din Zengi"],
    "Abyssinian":       ["Bilal ibn Rabah", "Wahshi ibn Harb", "Umm Ayman", "Najashi"],
    "Kipchak Turkic":   ["Baybars", "Qutuz"],
    "South Asian":      ["Tipu Sultan", "Muhammad Iqbal", "Aurangzeb",
                         "Shah Waliullah", "Ahmad Sirhindi", "Babur", "Akbar",
                         "Humayun", "Shah Jahan"],
    "Caucasian":        ["Imam Shamil"],
    "Algerian":         ["Emir Abdelkader", "Abd al-Qadir"],
    "Libyan":           ["Omar al-Mukhtar", "Omar Mukhtar"],
    "West African":     ["Mansa Musa", "Askia Muhammad", "Usman dan Fodio",
                         "Nana Asma'u"],
    "Roman/Byzantine":  ["Heraclius", "Constantine", "Justinian"],
    "Coptic":           ["Muqawqis", "al-Muqawqis"],
    "Mongol":           ["Genghis Khan", "Hulagu", "Timur", "Tamerlane"],
    "Qurayshi Arab":    ["Abu Bakr", "Umar ibn", "Uthman ibn", "Ali ibn Abi",
                         "Khalid ibn Walid", "Abu Sufyan", "Muawiyah"],
}


def infer_ethnicity(name: str, era: list, existing_ethnicity: str) -> str:
    """Infer ethnicity from corpus data, name hints, or era default."""
    if existing_ethnicity and existing_ethnicity.lower() not in ("unknown", ""):
        return existing_ethnicity

    # Name-based hints
    for eth_group, names in ETHNICITY_HINTS.items():
        for hint_name in names:
            if hint_name.lower() in name.lower() or name.lower() in hint_name.lower():
                return eth_group

    # Era default
    if era:
        return ERA_DEFAULT_ETHNICITY.get(era[0], "Arab")

    return "Arab"


def run(dry_run: bool = False):
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, era, ethnicity
        FROM figures
        WHERE ethnicity IS NULL OR ethnicity = '' OR ethnicity = 'unknown'
        ORDER BY id
    """)
    figures = cur.fetchall()

    print(f"Figures needing ethnicity: {len(figures)}")
    print(f"Dry run: {dry_run}")
    print()

    updated = 0
    for fig in figures:
        eth = infer_ethnicity(fig["name"], fig["era"] or [], fig["ethnicity"] or "")
        if eth and eth != (fig["ethnicity"] or ""):
            print(f"  {fig['name']}: {fig['ethnicity'] or 'NULL'} → {eth}")
            if not dry_run:
                cur.execute(
                    "UPDATE figures SET ethnicity = %s WHERE id = %s",
                    (eth, fig["id"]),
                )
            updated += 1

    if not dry_run:
        conn.commit()

    conn.close()

    print(f"\nUpdated: {updated}/{len(figures)}")
    return updated


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Backfill inferred ethnicity for all figures")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
