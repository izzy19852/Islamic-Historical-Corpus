"""
Islam Stories — Kling Appearance Backfill
Generates kling_appearance strings for figures that lack corpus-based
physical descriptions, using ethnicity + era to create accurate defaults.
Then rebuilds kling_full_character for all figures.

No API calls needed — pure template-based generation.

Run:  python -m rag.knowledge.backfill_kling_appearance [--dry-run]
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


# Ethnicity → default appearance template
# These are historically-grounded baseline appearances for each group
APPEARANCE_TEMPLATES = {
    "Qurayshi Arab": "Arab man, olive-brown complexion, dark eyes, strong jawline, black beard",
    "Arab": "Arab man, medium-brown complexion, dark eyes, dark hair, trimmed beard",
    "Arab-Persian": "Arab-Persian man, olive complexion, dark expressive eyes, dark hair, neat beard",
    "Arab-Kurdish": "Arab-Kurdish man, olive-tan complexion, prominent features, dark eyes, thick dark beard",
    "Persian": "Persian man, fair to olive complexion, refined features, dark eyes, groomed dark beard",
    "Andalusian Moor": "Moorish man, olive to dark complexion, sharp features, dark eyes, trimmed dark beard",
    "Berber": "Berber man, tan to olive complexion, angular features, dark eyes, dark beard",
    "Abyssinian": "Abyssinian man, very dark complexion, strong features, dark eyes, short dark hair",
    "Kipchak Turkic": "Turkic man, fair complexion, broad face, almond-shaped eyes, sparse beard",
    "Mamluk (mixed Turkic-Arab)": "Mamluk warrior, mixed Turkic-Arab features, medium complexion, intense eyes, dark beard",
    "Ottoman Turkic": "Ottoman Turkic man, olive to fair complexion, strong features, dark eyes, full dark beard",
    "South Asian": "South Asian man, brown complexion, refined features, dark eyes, dark beard",
    "Central Asian": "Central Asian man, broad face, fair to tan complexion, narrow eyes, sparse beard",
    "West African": "West African man, deep dark complexion, strong features, dark eyes, short hair",
    "Kurdish": "Kurdish man, olive complexion, strong features, dark eyes, thick dark beard",
    "Caucasian": "Caucasian mountaineer, fair complexion, sharp features, intense eyes, full dark beard",
    "Algerian": "Algerian man, olive to tan complexion, angular features, dark eyes, trimmed dark beard",
    "Libyan": "Libyan man, tan complexion, weathered features, dark eyes, grey-streaked beard",
    "Roman/Byzantine": "Byzantine man, fair to olive complexion, Roman features, light eyes, clean-shaven or trimmed beard",
    "Coptic": "Coptic Egyptian man, brown complexion, prominent features, dark eyes, short dark beard",
    "Mongol": "Mongol man, broad face, high cheekbones, narrow dark eyes, sparse facial hair",
    "Qibchaq Turk": "Qipchaq Turkic man, broad face, fair complexion, narrow eyes, sparse beard",
    "Kindi Arab": "Kindi Arab man, brown complexion, strong features, dark eyes, dark beard",
    "Mahhij Arab (from Yemen)": "Yemeni Arab man, dark brown complexion, sharp features, dark eyes, dark beard",
    "varies": "man, medium complexion, dark eyes, dark beard",
}

# Female figure detection keywords
FEMALE_INDICATORS = [
    "bint", "umm ", "umm_", "asma", "aisha", "khadijah", "fatima",
    "hafsa", "zaynab", "safiyya", "juwayri", "maymuna", "nusayba",
    "nana ", "rabia", "shajarat", "begim", "begum", "sultana",
    "haram", "habibah", "ruman", "hani",
]


def is_female(name: str) -> bool:
    name_lower = name.lower()
    return any(ind in name_lower for ind in FEMALE_INDICATORS)


def get_appearance(ethnicity: str, name: str) -> str:
    template = APPEARANCE_TEMPLATES.get(ethnicity, APPEARANCE_TEMPLATES.get("Arab"))
    if is_female(name):
        # Adjust for female figures — modest, dignified
        template = template.replace(" man,", " woman,").replace(" warrior,", " woman,")
        template = template.replace("beard", "hair covered by headscarf")
        template = template.replace("mountaineer", "woman")
    return template


def run(dry_run=False):
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()

    # Get figures without kling_appearance
    cur.execute("""
        SELECT id, name, ethnicity, sensitivity_tier, kling_appearance, kling_costume
        FROM figures
        WHERE (kling_appearance IS NULL OR kling_appearance = '')
          AND sensitivity_tier IN ('B', 'C')
        ORDER BY id
    """)
    figures = cur.fetchall()

    print("=" * 60)
    print("KLING APPEARANCE BACKFILL")
    print(f"Figures needing appearance: {len(figures)}")
    print(f"Dry run: {dry_run}")
    print("=" * 60)

    updated = 0
    for fig in figures:
        eth = fig["ethnicity"] or "Arab"
        appearance = get_appearance(eth, fig["name"])

        print(f"  {fig['name']}: {appearance[:60]}...")

        if not dry_run:
            cur.execute(
                "UPDATE figures SET kling_appearance = %s WHERE id = %s",
                (appearance, fig["id"]),
            )
        updated += 1

    # Now rebuild kling_full_character for ALL figures that have appearance or costume
    print(f"\nRebuilding kling_full_character...")
    cur.execute("""
        SELECT id, kling_appearance, kling_costume
        FROM figures
        WHERE kling_appearance IS NOT NULL OR kling_costume IS NOT NULL
    """)
    rows = cur.fetchall()

    full_updated = 0
    for row in rows:
        parts = []
        if row["kling_appearance"]:
            parts.append(row["kling_appearance"])
        if row["kling_costume"]:
            parts.append(row["kling_costume"])
        full = ", ".join(p for p in parts if p)

        if full and not dry_run:
            cur.execute(
                "UPDATE figures SET kling_full_character = %s WHERE id = %s",
                (full, row["id"]),
            )
            full_updated += 1

    if not dry_run:
        conn.commit()

    conn.close()

    print(f"\n{'=' * 60}")
    print("BACKFILL COMPLETE")
    print(f"  Appearances generated: {updated}")
    print(f"  kling_full_character rebuilt: {full_updated}")
    print("=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
