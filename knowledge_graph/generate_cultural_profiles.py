"""
Islam Stories — Cultural / Martial Profile Generator
Generates historically accurate dress, weapons, armor, and fighting style
descriptions for each figure, grouped by ethnicity + era + role.

Uses Claude Sonnet for nuanced historical knowledge.
Generates one profile per ethnic/era/role group, then specializes per figure.
Runs AFTER extract_appearances.py has populated ethnicity data.
"""

import argparse
import json
import os
import re
import sys
import time
import random

import anthropic
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..', '..')
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")


def _call_sonnet(client: anthropic.Anthropic, prompt: str, max_retries: int = 4) -> str:
    """Call Sonnet with exponential backoff retry on overloaded/rate errors."""
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text.strip()
        except (anthropic._exceptions.OverloadedError,
                anthropic._exceptions.RateLimitError,
                anthropic._exceptions.APIConnectionError) as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            if attempt < max_retries - 1:
                print(f"\n    API error ({e.__class__.__name__}), retrying in {wait:.0f}s...", end=" ", flush=True)
                time.sleep(wait)
            else:
                raise


def _parse_json_response(raw: str) -> dict:
    """Parse JSON from a Sonnet response, handling markdown code blocks."""
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    return json.loads(raw)


# ═══════════════════════════════════════════════════════════════════════
# ROLE INFERENCE FROM known_for + death_circumstance
# ═══════════════════════════════════════════════════════════════════════

ROLE_KEYWORDS = {
    "general": [
        "commander", "general", "conquest", "battle", "military",
        "undefeated", "cavalry", "army", "warrior", "soldier",
        "defeated", "fought", "led revolt", "siege",
    ],
    "ruler": [
        "caliph", "sultan", "emir", "king", "governor", "founded",
        "dynasty", "ruled", "reign", "throne", "empire", "prince",
        "vizier",
    ],
    "scholar": [
        "scholar", "philosopher", "wrote", "author", "theology",
        "jurist", "imam", "preacher", "taught", "student",
        "fiqh", "hadith", "commentary", "knowledge", "science",
        "medicine", "mathematics", "astronomy", "poetry", "poet",
        "mystic", "sufi", "traveler", "historian", "sociology",
    ],
    "companion": [
        "companion", "sahabi", "early convert", "emigrated",
        "pledged", "present at badr", "present at uhud",
        "first martyr", "freed slave",
    ],
    "merchant": [
        "merchant", "trade", "wealth", "commerce",
    ],
}


def infer_role(figure: dict) -> str:
    """Infer a figure's primary role from known_for and death_circumstance."""
    text = ((figure.get("known_for") or "") + " " +
            (figure.get("death_circumstance") or "")).lower()

    scores = {}
    for role, keywords in ROLE_KEYWORDS.items():
        scores[role] = sum(1 for kw in keywords if kw in text)

    # Battle death strongly implies warrior/general
    if figure.get("death_circumstance") in ("battle", "martyrdom"):
        scores["general"] = scores.get("general", 0) + 2

    if not any(scores.values()):
        # Default based on tier
        tier = figure.get("sensitivity_tier", "B")
        if tier == "S":
            return "companion"
        return "companion" if figure.get("generation") == "sahabi" else "scholar"

    return max(scores, key=scores.get)


# ═══════════════════════════════════════════════════════════════════════
# ETHNICITY INFERENCE FROM era + name + known_for
# When corpus extraction returned no ethnicity, we still need
# a cultural group for costume accuracy.
# ═══════════════════════════════════════════════════════════════════════

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
}

# Name-based hints for ethnicity when corpus is silent
ETHNICITY_HINTS = {
    "berber": ["Tariq ibn Ziyad", "Musa ibn Nusayr"],
    "persian": ["Salman al-Farsi", "Salman al-Farisi", "Abu Muslim al-Khurasani",
                "Nizam al-Mulk", "Ibn Sina", "Al-Biruni", "Ferdowsi"],
    "kurdish": ["Saladin", "Nur ad-Din Zengi"],
    "abyssinian": ["Bilal ibn Rabah", "Wahshi ibn Harb", "Umm Ayman"],
    "kipchak_turkic": ["Baybars", "Qutuz"],
    "south_asian": ["Tipu Sultan", "Muhammad Iqbal", "Aurangzeb",
                    "Shah Waliullah", "Ahmad Sirhindi"],
    "caucasian": ["Imam Shamil"],
    "algerian": ["Emir Abdelkader", "Abd al-Qadir"],
    "libyan": ["Omar al-Mukhtar"],
    "west_african": ["Mansa Musa", "Askia Muhammad", "Usman dan Fodio",
                     "Nana Asma'u"],
}


def infer_ethnicity(figure: dict) -> str:
    """Infer ethnicity from corpus data, name hints, or era default."""
    # 1. Use corpus-extracted ethnicity if available
    eth = figure.get("ethnicity")
    if eth and eth.lower() not in ("unknown", ""):
        return eth

    name = figure.get("name", "")

    # 2. Check name-based hints
    for eth_group, names in ETHNICITY_HINTS.items():
        for hint_name in names:
            if hint_name.lower() in name.lower() or name.lower() in hint_name.lower():
                return eth_group

    # 3. Fall back to era default
    eras = figure.get("era") or []
    if eras:
        return ERA_DEFAULT_ETHNICITY.get(eras[0], "Arab")

    return "Arab"


# ═══════════════════════════════════════════════════════════════════════
# CULTURAL PROFILE PROMPT
# ═══════════════════════════════════════════════════════════════════════

CULTURAL_PROFILE_PROMPT = """
You are a historical costume and material culture
consultant for an Islamic history documentary series.

Generate accurate visual descriptions for Kling AI
video generation. These must be:
- Historically accurate to the era and region
- Specific enough for an AI image model to render
- Free of anachronisms
- Culturally respectful

Figure: {name}
Era: {era} CE
Ethnicity/Origin: {ethnicity}
Role: {role} (general/scholar/merchant/warrior/ruler/companion)
Sensitivity tier: {tier}
Known for: {known_for}

Generate JSON only:
{{
  "dress": {{
    "base": "core garment description — fabric, cut, color range. Be specific: loose linen thawb, not just robe. Include head covering if appropriate.",
    "outer": "cloak, armor, or outer layer if applicable",
    "footwear": "historically accurate footwear",
    "colors": "documented or historically accurate color palette for this ethnicity/era/role. Arab warriors: undyed linen, deep indigo, earthy browns. Persian nobles: rich brocade, deep crimson. Abyssinian: varied. Never assume all Arabs wore white robes.",
    "distinguishing": "any rank markers, tribal markings, distinctive elements for this figure"
  }},
  "weapons": {{
    "primary": "primary weapon with specific description. Arab cavalry: straight double-edged sword dhul-fiqar style, not scimitar which came later. Abyssinian: javelin specialist. Persian: composite bow, curved sword.",
    "secondary": "secondary weapon if applicable",
    "carried_by": "how weapons are carried/worn",
    "era_note": "any important weapon accuracy notes"
  }},
  "armor": {{
    "type": "specific armor type for era and region. 7th century Arab: mail hauberk or lamellar, NOT plate armor which is medieval European. Helmet style if worn.",
    "shield": "shield type if applicable — Arab: round leather or wicker. Byzantine: large oval.",
    "era_note": "armor accuracy note"
  }},
  "fighting_style": {{
    "tradition": "martial tradition of their ethnic group",
    "specialty": "their documented or historically typical combat specialty",
    "visual_note": "how this looks on screen — stance, movement quality, weapon handling"
  }},
  "kling_costume": "single production-ready string combining all above for direct injection into Kling prompt. Max 60 words. Specific, visual, accurate. Example: loose undyed linen thawb with deep indigo outer cloak, leather belt, mail hauberk partially visible at collar, round leather shield on back, straight double-edged sword at hip, leather sandals, wrapped turban in earthy brown"
}}

ACCURACY REQUIREMENTS:
- 7th century Arab warriors did NOT wear white robes into battle — that is a modern stereotype
- Scimitars (curved sabers) became common AFTER the Mongol period — Rashidun era Arabs used straight double-edged swords
- Full plate armor is European medieval — wrong era
- Persian Sassanid influence on early Islamic dress was significant — acknowledge this for Persian figures
- Abyssinian dress differed significantly from Arab
- Moors/Andalusians had distinct dress blending Arab, Berber, and Iberian elements
- Rank and wealth shown through fabric quality and embroidery, not flashy Hollywood armor
- Scholars dressed differently from warriors — simpler garments, ink-stained fingers, book satchels
- For ruler figures, show authority through quality of fabric and turban, not through crown or throne
"""


# ═══════════════════════════════════════════════════════════════════════
# GROUP PROFILE PROMPT — one per ethnicity + era + role
# ═══════════════════════════════════════════════════════════════════════

GROUP_PROFILE_PROMPT = """
You are a historical costume and material culture consultant for an
Islamic history documentary series using AI video generation (Kling).

Generate a BASE cultural profile for this group. Individual figures
will be specialized from this template.

Group: {ethnicity} {role}s of the {era} era
Example figures in this group: {example_names}

Generate JSON only:
{{
  "dress_base": "core garment for this ethnic group, era, and role. Be specific about fabric, cut, color range. Never generic.",
  "dress_outer": "typical outer layer — cloak, robe, armor cover",
  "footwear": "historically accurate footwear for this group",
  "color_palette": "documented/accurate colors. NOT all-white. Specify fabric dyes available in this era/region.",
  "head_covering": "specific head covering style for this group/era",
  "weapon_primary": "standard primary weapon for this group, era, and role. Straight swords for pre-Mongol Arabs. Javelins for Abyssinians. Composite bows for Turkic cavalry.",
  "weapon_secondary": "common secondary weapon",
  "weapon_carry": "how weapons are worn/carried",
  "armor_type": "era-accurate armor. Mail for 7th century Arabs, lamellar for Turkic, NO European plate.",
  "shield_type": "era-accurate shield",
  "fighting_tradition": "martial tradition name and style",
  "fighting_visual": "how combat looks on screen — stance, movement, weapon handling",
  "kling_costume_template": "60-word max template. Use [RANK_MARKER] for elements that vary by individual rank. Example: loose [COLOR] linen thawb, [RANK_MARKER], leather belt with straight double-edged sword, mail hauberk under outer garment, round leather shield, wrapped [COLOR] turban, leather sandals"
}}

ACCURACY REQUIREMENTS:
- Pre-Mongol Arab warriors used straight double-edged swords, NOT scimitars
- 7th century Arabs did NOT wear white robes into battle
- Full plate armor is European medieval — never use for Islamic armies
- Abyssinian warriors were javelin specialists with distinct dress
- Kipchak/Turkic Mamluks had Central Asian cavalry equipment
- Berber warriors had distinct North African equipment
- Persian Sassanid military tradition differed from Arab
- Scholars: simple garments, no weapons unless also warriors
"""


def get_figures_needing_profiles() -> list[dict]:
    """Get all figures that need cultural profiles generated."""
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, name_variants, sensitivity_tier, era, ethnicity,
               known_for, death_circumstance, generation, kling_appearance,
               appearance_confidence, kling_costume
        FROM figures
        WHERE sensitivity_tier IN ('B', 'C')
          AND kling_costume IS NULL
        ORDER BY
            CASE sensitivity_tier WHEN 'B' THEN 1 WHEN 'C' THEN 2 END,
            id
    """)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def group_figures(figures: list[dict]) -> dict:
    """
    Group figures by inferred ethnicity + primary era + role.
    Returns dict of group_key -> list of figures.
    """
    groups = {}
    for fig in figures:
        eth = infer_ethnicity(fig)
        role = infer_role(fig)
        era = (fig.get("era") or ["unknown"])[0]
        key = f"{eth}|{era}|{role}"
        fig["_inferred_ethnicity"] = eth
        fig["_inferred_role"] = role
        fig["_group_key"] = key
        groups.setdefault(key, []).append(fig)
    return groups


def generate_group_profile(
    group_key: str,
    figures: list[dict],
    client: anthropic.Anthropic,
) -> dict | None:
    """Generate a base cultural profile for a group using Sonnet."""
    parts = group_key.split("|")
    ethnicity, era, role = parts[0], parts[1], parts[2]
    example_names = ", ".join(f["name"] for f in figures[:5])

    prompt = GROUP_PROFILE_PROMPT.format(
        ethnicity=ethnicity,
        era=era,
        role=role,
        example_names=example_names,
    )

    try:
        raw = _call_sonnet(client, prompt)
        return _parse_json_response(raw)
    except (json.JSONDecodeError, IndexError) as e:
        print(f"  WARNING: Group profile parse failed for {group_key}: {e}")
        return None
    except Exception as e:
        print(f"  WARNING: Group profile API failed for {group_key}: {e}")
        return None


def generate_individual_profile(
    figure: dict,
    group_profile: dict,
    client: anthropic.Anthropic,
) -> dict | None:
    """Generate an individual cultural profile, informed by the group base."""
    name = figure["name"]
    eth = figure["_inferred_ethnicity"]
    role = figure["_inferred_role"]
    era = (figure.get("era") or ["unknown"])[0]
    tier = figure["sensitivity_tier"]
    known_for = figure.get("known_for") or "historical figure"

    # Build the prompt with group context
    group_context = (
        f"\nBASE GROUP PROFILE (use as starting point, specialize for this figure):\n"
        f"Dress: {group_profile.get('dress_base', 'unknown')}\n"
        f"Outer: {group_profile.get('dress_outer', 'unknown')}\n"
        f"Weapon: {group_profile.get('weapon_primary', 'unknown')}\n"
        f"Armor: {group_profile.get('armor_type', 'unknown')}\n"
        f"Fighting style: {group_profile.get('fighting_tradition', 'unknown')}\n"
        f"Costume template: {group_profile.get('kling_costume_template', 'unknown')}\n"
    )

    full_prompt = CULTURAL_PROFILE_PROMPT.format(
        name=name,
        era=era,
        ethnicity=eth,
        role=role,
        tier=tier,
        known_for=known_for,
    ) + group_context

    try:
        raw = _call_sonnet(client, full_prompt)
        return _parse_json_response(raw)
    except (json.JSONDecodeError, IndexError) as e:
        print(f"  WARNING: Individual profile parse failed for {name}: {e}")
        return None
    except Exception as e:
        print(f"  WARNING: Individual profile API failed for {name}: {e}")
        return None


def build_full_character_string(figure_row: dict) -> str:
    """
    Combine kling_appearance + kling_costume into one complete
    character description for injection into any Kling character prompt.
    """
    parts = []
    appearance = figure_row.get("kling_appearance") or ""
    costume = figure_row.get("kling_costume") or ""

    if appearance:
        parts.append(appearance)
    if costume:
        parts.append(costume)

    return ", ".join(p for p in parts if p)


def update_figure_profile(figure_id: int, profile: dict):
    """Write cultural profile columns to DB for a figure."""
    dress = profile.get("dress", {})
    dress_desc = (
        f"{dress.get('base', '')}. {dress.get('outer', '')}. "
        f"Footwear: {dress.get('footwear', '')}. "
        f"Colors: {dress.get('colors', '')}. "
        f"{dress.get('distinguishing', '')}"
    ).strip()

    weapons = profile.get("weapons", {})
    weapons_desc = (
        f"Primary: {weapons.get('primary', '')}. "
        f"Secondary: {weapons.get('secondary', 'none')}. "
        f"Carried: {weapons.get('carried_by', '')}. "
        f"{weapons.get('era_note', '')}"
    ).strip()

    armor = profile.get("armor", {})
    armor_desc = (
        f"{armor.get('type', '')}. "
        f"Shield: {armor.get('shield', 'none')}. "
        f"{armor.get('era_note', '')}"
    ).strip()

    fighting = profile.get("fighting_style", {})
    fighting_desc = (
        f"{fighting.get('tradition', '')} — "
        f"{fighting.get('specialty', '')}. "
        f"Visual: {fighting.get('visual_note', '')}"
    ).strip()

    kling_costume = profile.get("kling_costume", "")

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute("""
        UPDATE figures SET
            dress_description = %s,
            weapons_description = %s,
            armor_description = %s,
            fighting_style = %s,
            kling_costume = %s
        WHERE id = %s
    """, (dress_desc, weapons_desc, armor_desc, fighting_desc,
          kling_costume, figure_id))
    conn.commit()
    conn.close()


def update_full_character_strings():
    """
    Build and save kling_full_character for all figures
    that have either kling_appearance or kling_costume set.
    """
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, kling_appearance, kling_costume
        FROM figures
        WHERE kling_appearance IS NOT NULL OR kling_costume IS NOT NULL
    """)
    rows = cur.fetchall()

    updated = 0
    for row in rows:
        full = build_full_character_string(row)
        if full:
            cur.execute(
                "UPDATE figures SET kling_full_character = %s WHERE id = %s",
                (full, row["id"]),
            )
            updated += 1

    conn.commit()
    conn.close()
    return updated


def run_generation(
    skip_if_set: bool = False,
    dry_run: bool = False,
    limit: int = None,
):
    """Run cultural profile generation for all Tier B/C figures."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    figures = get_figures_needing_profiles()
    if limit:
        figures = figures[:limit]

    print(f"\n{'='*60}")
    print(f"Islam Stories — Cultural Profile Generator")
    print(f"{'='*60}")
    print(f"Figures to process: {len(figures)}")
    print(f"Dry run: {dry_run}")
    print(f"{'='*60}\n")

    # Step 1: Group figures
    groups = group_figures(figures)
    print(f"Groups identified: {len(groups)}")
    for key, figs in sorted(groups.items(), key=lambda x: -len(x[1])):
        print(f"  {key}: {len(figs)} figures")
    print()

    # Step 2: Generate group base profiles
    group_profiles = {}
    print("Generating group base profiles...")
    for i, (key, figs) in enumerate(groups.items(), 1):
        print(f"  [{i}/{len(groups)}] {key}...", end=" ", flush=True)
        profile = generate_group_profile(key, figs, client)
        if profile:
            group_profiles[key] = profile
            template = profile.get("kling_costume_template", "")[:80]
            print(f"OK — {template}...")
        else:
            print("FAILED")
        time.sleep(0.3)

    print(f"\nGroup profiles generated: {len(group_profiles)}/{len(groups)}")
    print()

    # Step 3: Generate individual profiles
    print("Generating individual profiles...")
    success = 0
    failed = 0
    for i, fig in enumerate(figures, 1):
        group_key = fig["_group_key"]
        group_profile = group_profiles.get(group_key, {})

        print(f"  [{i}/{len(figures)}] {fig['name']} ({fig['_inferred_ethnicity']}/{fig['_inferred_role']})...",
              end=" ", flush=True)

        profile = generate_individual_profile(fig, group_profile, client)
        if profile:
            kling = profile.get("kling_costume", "")[:80]
            print(f"OK — {kling}...")
            if not dry_run:
                update_figure_profile(fig["id"], profile)
            success += 1
        else:
            print("FAILED")
            failed += 1

        time.sleep(0.3)

    # Step 4: Build combined kling_full_character strings
    if not dry_run:
        print(f"\nBuilding kling_full_character combined strings...")
        updated = update_full_character_strings()
        print(f"  Updated {updated} figures with combined appearance + costume strings.")

    # Report
    print(f"\n{'='*60}")
    print(f"CULTURAL PROFILE REPORT")
    print(f"{'='*60}")
    print(f"Total processed: {len(figures)}")
    print(f"Successful: {success}")
    print(f"Failed: {failed}")

    if not dry_run:
        # Show sample results
        conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        cur = conn.cursor()
        sample_names = [
            "Wahshi ibn Harb", "Khalid ibn Walid", "Bilal ibn Rabah",
            "Tariq ibn Ziyad", "Baybars", "Salman al-Farisi",
            "Saladin", "Ibn Battuta", "Omar al-Mukhtar",
        ]
        print(f"\nSample results:")
        print(f"{'─'*60}")
        for name in sample_names:
            cur.execute("""
                SELECT name, kling_costume, kling_full_character
                FROM figures
                WHERE name ILIKE %s OR %s = ANY(name_variants)
                LIMIT 1
            """, (f"%{name}%", name))
            row = cur.fetchone()
            if row and row.get("kling_costume"):
                print(f"{row['name']}:")
                print(f"  costume: {row['kling_costume'][:120]}")
                print(f"  full:    {(row['kling_full_character'] or '')[:120]}")
                print()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate cultural/martial profiles for figures")
    parser.add_argument("--skip-if-set", action="store_true",
                        help="Skip figures that already have kling_costume set")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate but don't update DB")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max figures to process")
    args = parser.parse_args()

    run_generation(
        skip_if_set=args.skip_if_set,
        dry_run=args.dry_run,
        limit=args.limit,
    )
