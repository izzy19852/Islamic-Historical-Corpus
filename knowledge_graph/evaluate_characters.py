"""
Islam Stories — Character Evaluation & Bible Readiness Audit
Runs all 4 gates on existing figures table:
  Gate 1: Deduplication (transliteration + variant overlap)
  Gate 2: Era + Series assignment validation
  Gate 3: Sensitivity tier correctness
  Gate 4: Dramatic function (screen vs corpus-only)

Output:
  docs/character_evaluation_report.md
  data/characters_approved_for_seed.json
  data/characters_needs_review.json

Run:  python -m rag.knowledge.evaluate_characters
"""

import os
import sys
import json
import re
import unicodedata
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DB_URL = os.getenv("ISLAM_STORIES_DB_URL")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# ═══════════════════════════════════════════════════════════════════════
# CANONICAL RULES — from sensitivity_guide.md + rashidun_character_guide
# ═══════════════════════════════════════════════════════════════════════

# Tier S: Prophet, Wives (Mothers of Believers), Four Rightly Guided Caliphs
TIER_S_CANONICAL = {
    "Prophet Muhammad (PBUH)",
    "Abu Bakr al-Siddiq",
    "Umar ibn al-Khattab",
    "Uthman ibn Affan",
    "Ali ibn Abi Talib",
    # Wives of the Prophet (Mothers of the Believers)
    "Khadijah bint Khuwaylid",
    "Aisha bint Abi Bakr",
    "Umm Salama",       # Hind bint Abi Umayya
    "Hafsa bint Umar",
    "Umm Habibah bint Abi Sufyan",
    "Safiyyah bint Huyayy",
    "Zaynab bint Jahsh",
    "Juwayria",         # Juwayriya bint al-Harith
    "Maymuna bint al-Harith",
    "Sawda bint Zam'a",
}

# Tier A: per sensitivity guide — senior companions, Ahl al-Bayt, close Prophet family
TIER_A_CANONICAL = {
    "Husayn ibn Ali",
    "Hassan ibn Ali",
    "Zaynab bint Ali",
    "Fatimah bint Muhammad",
    "Bilal ibn Rabah",
    "Salman al-Farisi",
    "Abu Dharr al-Ghifari",
    "Ammar ibn Yasir",
    "Abdullah ibn Masud",       # "Tier A (close companion, scholar)"
    "Hamza ibn Abd al-Muttalib",
    "Jafar ibn Abi Talib",
    "Sumayya bint Khabbat",
}

# Tier C: per sensitivity guide — contested figures
TIER_C_CANONICAL = {
    "Yazid ibn Muawiyah",
    "Al-Hajjaj ibn Yusuf",
    "Aurangzeb",
    "Nadir Shah",
    "Hulagu Khan",
    "Ibn al-Alqami",
    "Marwan ibn al-Hakam",  # "Tier B/C (complex figure)" per character guide
}

# Series map: which series a figure can appear in based on era + role
SERIES_MAP = {
    "rashidun":             "The Sword and the Succession",
    "umayyad":              "The Umayyad Paradox",
    "mongol":               "When Islam Stopped the Mongols",
    "mamluk":               "When Islam Stopped the Mongols",
    "andalusia":            "The Andalusian Arc",
    "abbasid":              "The Abbasid Golden Age and Rot",
    "crusades":             "Crusades: Islamic Perspective",
    "south_asia":           "South Asia",
    "persia":               "South Asia",
    "africa":               "African Islam",
    "ottoman":              "Ottoman Rise",
    "resistance_colonial":  "Resistance and Colonial Era",
}

# Figures that have screen time per the rashidun character guide
RASHIDUN_SCREEN_FIGURES = {
    "Khalid ibn Walid", "Abu Ubayda ibn al-Jarrah", "Amr ibn al-As",
    "Salman al-Farisi", "Abu Dharr al-Ghifari", "Muawiyah ibn Abi Sufyan",
    "Marwan ibn al-Hakam", "Ammar ibn Yasir", "Abdullah ibn Masud",
    "Nusayba bint Ka'ab", "Wahshi ibn Harb", "Aisha bint Abi Bakr",
    "Husayn ibn Ali", "Zaynab bint Ali", "Bilal ibn Rabah",
}

# Quranic prophets / pre-Islamic figures — valid in DB but should NOT have
# post-Prophet era assignments
QURANIC_PROPHETS = {
    "Lot", "Ishmael", "Shuayb", "Imran", "Ibrahim", "Musa", "Isa",
    "Nuh", "Yusuf", "Adam", "Dawud", "Sulayman", "Yunus", "Ayyub",
}


# ═══════════════════════════════════════════════════════════════════════
# HELPER: Normalize Arabic transliterations for comparison
# ═══════════════════════════════════════════════════════════════════════

def normalize_arabic(name: str) -> str:
    """Normalize Arabic transliteration for fuzzy dedup.

    Handles common transliteration variants:
    - Trailing h (ta marbuta): Salamah→Salama, Talhah→Talha
    - Prefix articles: az-, an-, al- all → al
    - Hamza/ayn: stripped
    - Digraphs: dh→d, th→t, kh→k, gh→g, sh→sh (kept)
    - Vowels: ay/ei→ai, ou/oo→u, ee→i
    - Joined words: ubaydullah→ubaid allah
    """
    n = name.lower().strip()
    # Strip diacritical marks
    n = unicodedata.normalize('NFD', n)
    n = ''.join(c for c in n if unicodedata.category(c) != 'Mn')
    # Common transliteration variants
    n = n.replace("'", "").replace("\u2018", "").replace("\u2019", "")
    n = n.replace("`", "").replace("\u02bb", "").replace("\u02bc", "")
    n = n.replace("-", " ").replace("_", " ")

    # Normalize prefix articles: az, an, ar, etc. → al
    n = re.sub(r'\b(az|an|ar|as|at|ad|al)\s+', 'al ', n)
    # Also handle attached prefix at start: "an-Nu'man" already handled by - → space

    # Common spelling variants
    n = re.sub(r'\bab[iu]\b', 'abu', n)
    n = re.sub(r'\bibn\b', 'ibn', n)
    n = re.sub(r'\bbint\b', 'bint', n)

    # Vowel normalizations
    n = n.replace('ay', 'ai').replace('ei', 'ai')
    n = n.replace('ou', 'u').replace('oo', 'u')
    n = n.replace('ee', 'i')

    # Digraphs
    n = n.replace('dh', 'd').replace('th', 't')
    n = n.replace('kh', 'k').replace('gh', 'g')

    # Strip trailing h from each word (ta marbuta: Salamah→Salama)
    # But preserve 'allah', 'dullah', short words
    words = n.split()
    normalized_words = []
    for w in words:
        if len(w) > 3 and w.endswith('h') and w not in ('allah', 'dullah'):
            w = w[:-1]
        normalized_words.append(w)
    n = ' '.join(normalized_words)

    # Split compound names: ubaydullah → ubaid allah, abdurrahman → abd al rahman
    n = re.sub(r'ubaidulla[h]?', 'ubaid allah', n)
    n = re.sub(r'abdurrahman', 'abd al rahman', n)

    # Strip leading article 'al' for consistency in matching
    # Keep 'al' in middle of names but normalize leading 'al '
    # (handled above by prefix normalization)

    # Remove double spaces
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def _name_tokens(name: str) -> set:
    """Get meaningful tokens from a name (skip 'ibn', 'bint', 'abu', 'al', 'bin')."""
    stop = {"ibn", "bint", "abu", "al", "bin", "umm", "abd"}
    return {t for t in normalize_arabic(name).split() if t not in stop and len(t) > 2}


def are_likely_same_person(name_a: str, name_b: str, variants_a: list, variants_b: list) -> bool:
    """Check if two figure entries likely refer to the same person.

    Rules (in order):
    0. Known-different pairs → no (prevents father/son, sibling, etc. false matches)
    1. Exact normalized name match → yes
    2. One normalized name is prefix of the other AND high token overlap → yes
    3. Any non-generic variant from A exactly matches one from B → yes
    4. Otherwise → no

    Deliberately strict to avoid false grouping of figures who share
    common name elements (e.g., 'Abd al-Rahman').
    """
    # Rule 0: Known-different pairs (sorted tuples for order-independent lookup)
    KNOWN_DIFFERENT = {
        ("Abdullah ibn Umar", "Salim ibn Abdullah"),
        ("Abdullah ibn Umar", "Salim ibn Abdullah ibn Umar"),
        ("Abdullah ibn Zayd al-Ansari", "Abdullah ibn al-Harith ibn Nawfal"),
        ("Imran ibn Husayn", "Imran"),
        ("Abu Ja'far al-Mansur", "Abu al-Abbas al-Saffah"),
        ("Zayd ibn Aslam", "Zayd ibn Harithah"),
        ("Al-Ashtar al-Nakha'i", "Ibrahim ibn al-Ashtar"),
        ("Talhah ibn Ubayd Allah", "Talha ibn Ubaydullah"),  # Actually same — handled by norm
        ("Sa'd ibn Abi Waqqas", "Amir ibn Sa'd"),
        ("Amr ibn al-As", "Abdullah ibn Amr ibn al-As"),
        ("Khalid ibn Yazid", "Yazid ibn Muawiyah"),
        ("Usman dan Fodio", "Nana Asma'u"),
        ("Abu Talib ibn Abd al-Muttalib", "Imran ibn Husayn"),
        ("Abu Salama ibn Abd al-Asad", "Abu Salamah ibn Abd al-Rahman"),
        ("Imran ibn Husayn", "Imran"),
        ("Anas ibn al-Nadr", "Unays"),
        ("Abu Musa al-Ash'ari", "Abdullah ibn Qays"),  # Actually same person
        ("Abu Sa'id al-Khudri", "Zaid ibn Thabit"),
        ("Asim ibn Thabit", "Sa'd ibn Mu'adh"),
        ("Asim ibn Thabit", "Zayd ibn Arqam"),
        ("Khadijah bint Khuwaylid", "Aisha bint Abi Bakr"),
        ("Zaynab bint Ali", "Zainab bint Muhammad"),
        ("Ibn Khaldun", "Abd al-Rahman I"),
        ("Ibn Khaldun", "Abd al-Rahman III"),
        ("Ibn Rushd", "Abd al-Malik ibn Marwan"),
        ("Abu Ayyub al-Ansari", "Sulayman ibn Yasar"),
        ("Ubayy ibn Ka'b", "Hisham ibn Urwa"),
        ("Abu Muslim al-Khurasani", "Salamah ibn al-Akwa"),
        ("Abu Muslim al-Khurasani", "Salama ibn al-Akwa"),
        ("Ibn Battuta", "Abdullah ibn al-Zubayr"),
        ("Shu'ayb ibn Harb", "Shuayb"),
    }
    pair = tuple(sorted([name_a, name_b]))
    if pair in KNOWN_DIFFERENT:
        return False
    norm_a = normalize_arabic(name_a)
    norm_b = normalize_arabic(name_b)

    # Rule 1: Exact normalized match (also try stripping leading 'al ')
    if norm_a == norm_b:
        return True
    strip_a = re.sub(r'^al ', '', norm_a)
    strip_b = re.sub(r'^al ', '', norm_b)
    if strip_a == strip_b:
        return True

    # Rule 2: Prefix/substring match with shared meaningful tokens
    # Exclude father/son (e.g., "Abdullah ibn Umar" vs "Salim ibn Abdullah ibn Umar")
    # They share tokens but are different people if the non-shared parts are different
    tokens_a = _name_tokens(name_a)
    tokens_b = _name_tokens(name_b)
    shared_tokens = tokens_a & tokens_b

    if (norm_a in norm_b or norm_b in norm_a) and len(shared_tokens) >= 2:
        shorter = min(norm_a, norm_b, key=len)
        longer = max(norm_a, norm_b, key=len)
        # Must share at least 80% of the shorter name's tokens to be a real duplicate
        # This avoids father/son matches where the son adds the father's full name
        if len(shorter) > 12 and len(shared_tokens) >= len(tokens_a & tokens_b):
            ratio = len(shared_tokens) / max(len(min(tokens_a, tokens_b, key=len)), 1)
            if ratio >= 0.8:
                return True

    # Rule 3: Exact variant cross-match (full string, not substring)
    # Exclude generic kunyas/titles shared by many different people
    GENERIC_KUNYAS = {
        normalize_arabic(k) for k in [
            "Abu Abdullah", "Abu Muhammad", "Abu al-Walid", "Abu al-Mundhir",
            "Umm al-Mu'minin", "Abu Muslim", "Abu Ayyub", "Abu al-Abbas",
            "Abu Khalid", "Abu Usamah", "Abu Usamah Zayd", "Abu al-Hasan",
            "Abu al-Qasim", "Abu Bakr", "Abu Amr", "Abu Isa", "Abu Yusuf",
            "Abu Ibrahim", "Abu al-Fadl", "Abu al-Husayn", "Abu Musa",
            "Abu al-Mundhir", "Abu al-Walid", "Abdullah ibn Muhammad",
        ]
    }

    all_a = set()
    for v in (variants_a or []):
        nv = normalize_arabic(v)
        if len(nv) > 8 and nv not in GENERIC_KUNYAS:
            all_a.add(nv)

    all_b = set()
    for v in (variants_b or []):
        nv = normalize_arabic(v)
        if len(nv) > 8 and nv not in GENERIC_KUNYAS:
            all_b.add(nv)

    if all_a and all_b and (all_a & all_b):
        return True

    return False


# ═══════════════════════════════════════════════════════════════════════
# GATE 1: DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════════

def gate1_deduplication(figures: list[dict]) -> list[dict]:
    """Find duplicate figure entries."""
    duplicates = []
    seen_groups = []  # list of sets of ids already grouped

    for i, fig_a in enumerate(figures):
        for fig_b in figures[i+1:]:
            # Skip if already grouped together
            already = False
            for group in seen_groups:
                if fig_a["id"] in group and fig_b["id"] in group:
                    already = True
                    break
            if already:
                continue

            if are_likely_same_person(
                fig_a["name"], fig_b["name"],
                fig_a.get("name_variants") or [],
                fig_b.get("name_variants") or []
            ):
                # Find or create group
                found_group = None
                for group in seen_groups:
                    if fig_a["id"] in group or fig_b["id"] in group:
                        group.add(fig_a["id"])
                        group.add(fig_b["id"])
                        found_group = group
                        break
                if not found_group:
                    seen_groups.append({fig_a["id"], fig_b["id"]})

                duplicates.append({
                    "id_a": fig_a["id"],
                    "name_a": fig_a["name"],
                    "tier_a": fig_a["sensitivity_tier"],
                    "id_b": fig_b["id"],
                    "name_b": fig_b["name"],
                    "tier_b": fig_b["sensitivity_tier"],
                    "tier_mismatch": fig_a["sensitivity_tier"] != fig_b["sensitivity_tier"],
                })

    # Consolidate into groups
    groups = []
    for group_ids in seen_groups:
        members = [f for f in figures if f["id"] in group_ids]
        # The canonical entry is the one with the lowest id (first seeded)
        members.sort(key=lambda f: f["id"])
        keep = members[0]
        remove = members[1:]
        tiers = set(m["sensitivity_tier"] for m in members)
        groups.append({
            "keep": {"id": keep["id"], "name": keep["name"], "tier": keep["sensitivity_tier"]},
            "remove": [{"id": r["id"], "name": r["name"], "tier": r["sensitivity_tier"]} for r in remove],
            "tier_conflict": len(tiers) > 1,
            "tiers_found": sorted(tiers),
        })

    return groups


# ═══════════════════════════════════════════════════════════════════════
# GATE 2: ERA + SERIES ASSIGNMENT
# ═══════════════════════════════════════════════════════════════════════

def gate2_era_series(figures: list[dict]) -> list[dict]:
    """Check era and series assignments."""
    issues = []
    for fig in figures:
        name = fig["name"]
        eras = fig.get("era") or []
        problems = []

        # No era assigned
        if not eras:
            problems.append("NO_ERA: No era assigned")

        # Quranic prophets with post-Prophet eras
        if name in QURANIC_PROPHETS:
            if eras and any(e not in ("pre_islamic", "prophetic") for e in eras):
                problems.append(f"PROPHET_ERA: Quranic prophet has post-Prophet era {eras}")

        # Check series mapping
        series = fig.get("series") or []
        if not series and eras:
            # Not necessarily a problem — corpus-only figures don't need series
            pass

        if problems:
            issues.append({
                "id": fig["id"],
                "name": name,
                "era": eras,
                "series": series,
                "problems": problems,
            })

    return issues


# ═══════════════════════════════════════════════════════════════════════
# GATE 3: SENSITIVITY TIER
# ═══════════════════════════════════════════════════════════════════════

def gate3_sensitivity_tier(figures: list[dict]) -> list[dict]:
    """Check tier assignments against canonical rules."""
    issues = []
    fig_by_name = {f["name"]: f for f in figures}

    for fig in figures:
        name = fig["name"]
        current_tier = fig["sensitivity_tier"]
        problems = []

        # Check Tier S figures
        if name in TIER_S_CANONICAL and current_tier != "S":
            problems.append(f"SHOULD_BE_S: '{name}' is a Tier S figure (Prophet/Wife/Caliph) but assigned {current_tier}")

        # Check Tier A figures
        if name in TIER_A_CANONICAL and current_tier not in ("S", "A"):
            problems.append(f"SHOULD_BE_A: '{name}' is a Tier A figure per sensitivity guide but assigned {current_tier}")

        # Check Tier C figures
        if name in TIER_C_CANONICAL and current_tier not in ("C",):
            # Marwan is B/C — flag only if higher than B
            if name == "Marwan ibn al-Hakam" and current_tier in ("B", "C"):
                pass  # acceptable
            else:
                problems.append(f"SHOULD_BE_C: '{name}' is contested (Tier C per guide) but assigned {current_tier}")

        # Sahabi with tier C who aren't in the canonical C list
        # Enemies of Islam (Abu Jahl, Musaylimah, etc.) are contemporaries
        # marked generation=sahabi but are correctly Tier C
        gen = fig.get("generation")
        KNOWN_OPPONENTS = {
            "Abu Jahl", "Musaylimah", "Al-Aswad al-Ansi", "Hind bint Utba",
            "Shaiba ibn Rabi'a", "Utba ibn Rabi'a", "Abu Lahab", "Rustam",
        }
        if gen == "sahabi" and current_tier == "C" and name not in TIER_C_CANONICAL and name not in KNOWN_OPPONENTS:
            problems.append(f"SAHABI_TIER_C: Companion assigned Tier C but not in canonical contested list or known opponents")

        # Wives of the Prophet should always be S
        if "bint" in name and current_tier != "S":
            # Check if they're actually a wife of the Prophet
            # Known wives: Khadijah, Aisha, Hafsa, Umm Salama, Zaynab bint Jahsh,
            # Umm Habibah, Safiyyah, Juwayriya, Maymuna, Sawda
            pass  # handled by TIER_S_CANONICAL above

        if problems:
            issues.append({
                "id": fig["id"],
                "name": name,
                "current_tier": current_tier,
                "generation": gen,
                "problems": problems,
            })

    return issues


# ═══════════════════════════════════════════════════════════════════════
# GATE 4: DRAMATIC FUNCTION
# ═══════════════════════════════════════════════════════════════════════

def gate4_dramatic_function(figures: list[dict], bible_files: set) -> list[dict]:
    """Classify figures as screen / corpus-only / bible-needed."""
    results = []
    for fig in figures:
        name = fig["name"]
        tier = fig["sensitivity_tier"]
        eras = fig.get("era") or []
        gen = fig.get("generation")

        # Normalize name for bible file matching
        bible_slug = name.lower().replace(" ", "_").replace("'", "").replace("'", "")
        bible_slug = re.sub(r'[()]', '', bible_slug)
        has_bible = bible_slug in bible_files or any(
            bible_slug.startswith(bf.split('.')[0][:15]) for bf in bible_files
        )

        # Determine function
        if tier == "S":
            function = "TIER_S_REFERENCE"  # Never on screen, always in DB
            needs_bible = False  # S figures don't get bibles — they're felt, not shown
        elif name in RASHIDUN_SCREEN_FIGURES:
            function = "SCREEN_CONFIRMED"
            needs_bible = True
        elif tier in ("A", "B") and gen in ("sahabi", "tabi_i") and eras:
            # Figures with era assignments who are companions/tabi'un
            # likely appear in narrative even if not explicitly listed
            function = "SCREEN_LIKELY"
            needs_bible = tier in ("A", "B")
        elif gen in ("tabi_al_tabi_in", "later") and tier == "B":
            function = "SCREEN_POSSIBLE"
            needs_bible = True
        else:
            function = "CORPUS_ONLY"
            needs_bible = False

        # Hadith transmitters with no dramatic question are likely corpus-only
        dramatic_q = fig.get("dramatic_question")
        if not dramatic_q and gen in ("tabi_i", "tabi_al_tabi_in"):
            if function not in ("SCREEN_CONFIRMED",):
                function = "CORPUS_LIKELY"
                needs_bible = False

        results.append({
            "id": fig["id"],
            "name": name,
            "tier": tier,
            "generation": gen,
            "era": eras,
            "function": function,
            "has_bible": has_bible,
            "needs_bible": needs_bible,
            "bible_gap": needs_bible and not has_bible,
        })

    return results


# ═══════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════

def load_figures():
    conn = psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, name_variants, sensitivity_tier, era, series,
               generation, dramatic_question, known_for,
               primary_hadith_count, death_circumstance
        FROM figures
        ORDER BY id
    """)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_bible_files():
    bible_dir = PROJECT_ROOT / "character_bible"
    files = set()
    if bible_dir.exists():
        for era_dir in bible_dir.iterdir():
            if era_dir.is_dir():
                for f in era_dir.glob("*.md"):
                    files.add(f.stem)
    return files


# ═══════════════════════════════════════════════════════════════════════
# GENERATE REPORT
# ═══════════════════════════════════════════════════════════════════════

def generate_report(figures, dup_groups, era_issues, tier_issues, dramatic_results):
    """Generate the human-readable evaluation report."""
    lines = []
    lines.append("# Character Evaluation & Bible Readiness Audit")
    lines.append(f"**Figures in DB:** {len(figures)}")
    lines.append(f"**Date:** Auto-generated by evaluate_characters.py")
    lines.append("")

    # ── Summary ──
    lines.append("## Summary")
    lines.append("")
    tier_counts = defaultdict(int)
    gen_counts = defaultdict(int)
    for f in figures:
        tier_counts[f["sensitivity_tier"]] += 1
        gen_counts[f.get("generation") or "unknown"] += 1

    lines.append("| Tier | Count |")
    lines.append("|------|-------|")
    for t in ("S", "A", "B", "C"):
        lines.append(f"| {t} | {tier_counts.get(t, 0)} |")
    lines.append("")

    lines.append("| Generation | Count |")
    lines.append("|-----------|-------|")
    for g in ("sahabi", "tabi_i", "tabi_al_tabi_in", "later", "unknown"):
        lines.append(f"| {g} | {gen_counts.get(g, 0)} |")
    lines.append("")

    # ── Gate 1: Duplicates ──
    lines.append("---")
    lines.append("## Gate 1: Deduplication")
    lines.append(f"**Duplicate groups found:** {len(dup_groups)}")
    lines.append("")

    if dup_groups:
        lines.append("### Action Required: Merge or Remove")
        lines.append("")
        for i, group in enumerate(dup_groups, 1):
            keep = group["keep"]
            remove = group["remove"]
            tier_note = " **TIER CONFLICT**" if group["tier_conflict"] else ""
            lines.append(f"**Group {i}:** KEEP `{keep['name']}` (id {keep['id']}, tier {keep['tier']}){tier_note}")
            for r in remove:
                lines.append(f"  - REMOVE `{r['name']}` (id {r['id']}, tier {r['tier']})")
            if group["tier_conflict"]:
                lines.append(f"  - Tiers found: {group['tiers_found']} — resolve before merging")
            lines.append("")

    # ── Gate 2: Era/Series ──
    lines.append("---")
    lines.append("## Gate 2: Era + Series Assignment")
    lines.append(f"**Issues found:** {len(era_issues)}")
    lines.append("")

    if era_issues:
        for issue in era_issues:
            lines.append(f"- **{issue['name']}** (id {issue['id']}): {'; '.join(issue['problems'])}")
        lines.append("")

    # ── Gate 3: Sensitivity Tier ──
    lines.append("---")
    lines.append("## Gate 3: Sensitivity Tier Violations")
    lines.append(f"**Issues found:** {len(tier_issues)}")
    lines.append("")

    if tier_issues:
        lines.append("### Tier Corrections Needed")
        lines.append("")
        lines.append("| ID | Name | Current | Should Be | Reason |")
        lines.append("|----|------|---------|-----------|--------|")
        for issue in tier_issues:
            for prob in issue["problems"]:
                # Extract the target tier from the problem message
                should = "?"
                if "SHOULD_BE_S" in prob:
                    should = "S"
                elif "SHOULD_BE_A" in prob:
                    should = "A"
                elif "SHOULD_BE_C" in prob:
                    should = "C"
                reason = prob.split(": ", 1)[1] if ": " in prob else prob
                lines.append(f"| {issue['id']} | {issue['name']} | {issue['current_tier']} | {should} | {reason} |")
        lines.append("")

    # ── Gate 4: Dramatic Function ──
    lines.append("---")
    lines.append("## Gate 4: Dramatic Function")
    lines.append("")

    func_counts = defaultdict(int)
    bible_gaps = []
    for dr in dramatic_results:
        func_counts[dr["function"]] += 1
        if dr["bible_gap"]:
            bible_gaps.append(dr)

    lines.append("| Function | Count |")
    lines.append("|----------|-------|")
    for func in ("SCREEN_CONFIRMED", "SCREEN_LIKELY", "SCREEN_POSSIBLE",
                 "CORPUS_LIKELY", "CORPUS_ONLY", "TIER_S_REFERENCE"):
        lines.append(f"| {func} | {func_counts.get(func, 0)} |")
    lines.append("")

    lines.append(f"### Bible Gaps: {len(bible_gaps)} figures need bibles but don't have one")
    lines.append("")
    if bible_gaps:
        lines.append("| ID | Name | Tier | Function | Era |")
        lines.append("|----|------|------|----------|-----|")
        for bg in sorted(bible_gaps, key=lambda x: x["tier"]):
            era_str = ", ".join(bg["era"]) if bg["era"] else "—"
            lines.append(f"| {bg['id']} | {bg['name']} | {bg['tier']} | {bg['function']} | {era_str} |")
        lines.append("")

    # ── Quranic Prophets ──
    quranic = [f for f in figures if f["name"] in QURANIC_PROPHETS]
    if quranic:
        lines.append("---")
        lines.append("## Note: Quranic Prophets in DB")
        lines.append("These are referenced in hadith chains but are not post-Prophet historical figures.")
        lines.append("They should remain in the DB (source material) but should NOT receive character bibles")
        lines.append("or screen assignments for any planned series.")
        lines.append("")
        for q in quranic:
            lines.append(f"- **{q['name']}** (id {q['id']}, tier {q['sensitivity_tier']}, era {q.get('era')})")
        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# GENERATE JSON OUTPUTS
# ═══════════════════════════════════════════════════════════════════════

def generate_approved_and_review(figures, dup_groups, tier_issues, dramatic_results):
    """Split figures into approved vs needs-review."""
    # IDs that need review
    review_ids = set()
    review_reasons = defaultdict(list)

    # Duplicates — all members of dup groups need review
    for group in dup_groups:
        for r in group["remove"]:
            review_ids.add(r["id"])
            review_reasons[r["id"]].append(f"DUPLICATE of {group['keep']['name']} (id {group['keep']['id']})")
        if group["tier_conflict"]:
            review_ids.add(group["keep"]["id"])
            review_reasons[group["keep"]["id"]].append(f"TIER_CONFLICT in dup group: {group['tiers_found']}")

    # Tier violations
    for issue in tier_issues:
        review_ids.add(issue["id"])
        for prob in issue["problems"]:
            review_reasons[issue["id"]].append(prob)

    # Build outputs
    approved = []
    needs_review = []

    dr_map = {dr["id"]: dr for dr in dramatic_results}

    for fig in figures:
        fid = fig["id"]
        dr = dr_map.get(fid, {})
        entry = {
            "id": fid,
            "name": fig["name"],
            "sensitivity_tier": fig["sensitivity_tier"],
            "era": fig.get("era") or [],
            "generation": fig.get("generation"),
            "dramatic_function": dr.get("function", "UNKNOWN"),
            "has_bible": dr.get("has_bible", False),
            "needs_bible": dr.get("needs_bible", False),
        }

        if fid in review_ids:
            entry["review_reasons"] = review_reasons[fid]
            needs_review.append(entry)
        else:
            approved.append(entry)

    return approved, needs_review


# ═══════════════════════════════════════════════════════════════════════
# CLAUDE API ENRICHMENT — Series, Season, Dramatic Function assignment
# ═══════════════════════════════════════════════════════════════════════

UNIVERSE_REFERENCE = """
PLANNED SERIES STRUCTURE:
  SPINE — "The Rightly Guided" (632–661 CE, 4 seasons):
    Season 1 "The Succession": Abu Bakr era, Ridda Wars, Musaylima
    Season 2 "The Expansion": Umar era, Syria/Persia/Egypt conquests, Yarmouk
    Season 3 "The Covenant": Uthman era, Marwan's rise, siege of Uthman
    Season 4 "The Fracture": Ali era, Camel, Siffin, Karbala setup

  SATELLITE SERIES (branch after spine):
    "The Sword of God": Khalid ibn Walid character arc across S1-S2
    "The Karbala Arc": Husayn ibn Ali, 680 CE
    "The Umayyad Paradox": 661-750 CE, Umayyad dynasty
    "The Andalusian Arc": Tariq ibn Ziyad, 711 CE onward
    "When Islam Stopped the Mongols": Baybars, Ain Jalut, 1260 CE
    "The Abbasid Golden Age": scholars, translation movement, 750-900 CE
    "African Islam": Mansa Musa, Timbuktu scholars
    "South Asia": Muhammad ibn Qasim, Delhi Sultanate, Mughals
    "Ottoman Rise": Constantinople, Suleiman

SENSITIVITY TIERS:
  S  = Prophet Muhammad (PBUH), his wives, and Four Rightly Guided Caliphs
       Never depicted in any form. Referenced through others only.
  A  = Senior Companions, Ahl al-Bayt, those close to the Prophet
       Depicted with extreme scholarly care. No invented dialogue.
  B  = Tabi'un (Successors) and prominent named historical figures
       Full depiction permitted. Dialogue may be written if sourced.
  C  = Contested figures (Yazid, Al-Hajjaj, etc.) or opponents of Islam
       Full depiction, document actions without moral judgment on their Islam

DRAMATIC FUNCTIONS:
  PROTAGONIST: Can anchor an episode as primary figure
  ENSEMBLE: Key recurring figure across multiple episodes
  SUPPORTING: Appears in specific scenes, not episode-anchoring
  ANTAGONIST: Primary dramatic opposition in their era
  CORPUS_ONLY: Source material value (hadith transmitter, scholar cited),
               no planned screen time — still seeded for attribution
"""

EVALUATION_PROMPT = """You are evaluating historical figures for the Islam Stories production bible.

For each figure, determine:
1. SERIES: Which planned series do they belong to? (array of strings, can be multiple)
2. SEASONS: Which specific seasons? (e.g. "Spine S1, Spine S2" or "Karbala Arc" or "N/A")
3. DRAMATIC_FUNCTION: PROTAGONIST / ENSEMBLE / SUPPORTING / ANTAGONIST / CORPUS_ONLY
4. REASON: One sentence explaining your assignment
5. FLAG: Any sensitivity concern or ambiguity, or empty string if none

IMPORTANT RULES:
- Hadith transmitters who are ONLY known for narrating hadith → CORPUS_ONLY
- Figures who appear in multiple eras (e.g. Muawiyah: rashidun + umayyad) → multiple series
- Tier S figures → always assign series but function is "TIER_S_REFERENCE"
- Unknown/obscure figures → CORPUS_ONLY with explanation

Respond ONLY as a JSON array. One object per figure with keys:
  name, SERIES, SEASONS, DRAMATIC_FUNCTION, REASON, FLAG

{figures_batch}"""


def enrich_with_claude(figures: list[dict], dup_remove_ids: set) -> dict:
    """Use Claude API to assign series/seasons/dramatic function to figures
    that lack series assignments. Returns dict of id → enrichment data."""
    import anthropic

    client = anthropic.Anthropic()
    enrichments = {}

    # Find figures that need enrichment: no series assigned, not a dup-to-remove
    candidates = [
        f for f in figures
        if (not f.get("series") or f["series"] == [])
        and f["id"] not in dup_remove_ids
    ]

    if not candidates:
        print("  No figures need enrichment — all have series assigned")
        return enrichments

    print(f"  {len(candidates)} figures need series/function enrichment")

    batch_size = 25
    for i in range(0, len(candidates), batch_size):
        batch = candidates[i:i+batch_size]
        batch_text = json.dumps([{
            "name": c["name"],
            "tier": c["sensitivity_tier"],
            "era": c.get("era") or [],
            "generation": c.get("generation"),
            "known_for": c.get("known_for", ""),
            "dramatic_question": c.get("dramatic_question", ""),
        } for c in batch], indent=2, ensure_ascii=False)

        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",  # switched from Sonnet for cost savings
                max_tokens=4000,
                system=UNIVERSE_REFERENCE,
                messages=[{
                    "role": "user",
                    "content": EVALUATION_PROMPT.format(figures_batch=batch_text)
                }]
            )

            text = response.content[0].text.strip()
            # Strip markdown fences if present
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            batch_results = json.loads(text)

            # Map results back to figure ids
            name_to_id = {c["name"]: c["id"] for c in batch}
            for result in batch_results:
                fig_name = result.get("name", "")
                fig_id = name_to_id.get(fig_name)
                if fig_id:
                    enrichments[fig_id] = {
                        "series": result.get("SERIES", []),
                        "seasons": result.get("SEASONS", ""),
                        "dramatic_function": result.get("DRAMATIC_FUNCTION", ""),
                        "reason": result.get("REASON", ""),
                        "flag": result.get("FLAG", ""),
                    }

            print(f"  Batch {i//batch_size + 1}/{(len(candidates)-1)//batch_size + 1}: "
                  f"{len(batch_results)} evaluated")

        except json.JSONDecodeError as e:
            print(f"  WARNING: Batch {i//batch_size + 1} parse error: {e}")
            print(f"  Raw: {response.content[0].text[:200]}...")
        except Exception as e:
            print(f"  WARNING: Batch {i//batch_size + 1} API error: {e}")

    return enrichments


# ═══════════════════════════════════════════════════════════════════════
# ENHANCED REPORT — includes Claude enrichment data
# ═══════════════════════════════════════════════════════════════════════

def generate_enrichment_report_section(enrichments: dict, figures: list[dict]) -> str:
    """Generate report section for Claude-enriched dramatic function assignments."""
    if not enrichments:
        return ""

    fig_map = {f["id"]: f for f in figures}
    lines = []
    lines.append("---")
    lines.append("## Gate 4b: Claude Dramatic Function Enrichment")
    lines.append(f"**Figures evaluated by Claude:** {len(enrichments)}")
    lines.append("")

    # Group by dramatic function
    by_function = defaultdict(list)
    flagged = []
    for fid, data in enrichments.items():
        fig = fig_map.get(fid, {})
        entry = {"id": fid, "name": fig.get("name", "?"), "tier": fig.get("sensitivity_tier", "?"), **data}
        by_function[data.get("dramatic_function", "UNKNOWN")].append(entry)
        if data.get("flag"):
            flagged.append(entry)

    for func in ("PROTAGONIST", "ENSEMBLE", "SUPPORTING", "ANTAGONIST",
                 "CORPUS_ONLY", "TIER_S_REFERENCE", "UNKNOWN"):
        entries = by_function.get(func, [])
        if not entries:
            continue
        lines.append(f"### {func} ({len(entries)} figures)")
        lines.append("")
        lines.append("| ID | Name | Tier | Series | Seasons | Reason |")
        lines.append("|----|------|------|--------|---------|--------|")
        for e in sorted(entries, key=lambda x: x["id"]):
            series_str = ", ".join(e.get("series", [])) if isinstance(e.get("series"), list) else str(e.get("series", ""))
            lines.append(f"| {e['id']} | {e['name']} | {e['tier']} | {series_str} | {e.get('seasons', '')} | {e.get('reason', '')} |")
        lines.append("")

    if flagged:
        lines.append("### Flagged by Claude — Needs Human Review")
        lines.append("")
        for f in flagged:
            lines.append(f"- **{f['name']}** (id {f['id']}): {f['flag']}")
        lines.append("")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Character Evaluation & Bible Readiness Audit")
    parser.add_argument("--enrich", action="store_true",
                        help="Use Claude API to enrich figures missing series/function data")
    args = parser.parse_args()

    print("=" * 60)
    print("CHARACTER EVALUATION & BIBLE READINESS AUDIT")
    if args.enrich:
        print("  (with Claude API enrichment)")
    print("=" * 60)

    # Load data
    print("\nLoading figures from DB...")
    figures = load_figures()
    print(f"  {len(figures)} figures loaded")

    print("Loading character bible files...")
    bible_files = load_bible_files()
    print(f"  {len(bible_files)} bible files found")

    # Gate 1
    print("\n── Gate 1: Deduplication ──")
    dup_groups = gate1_deduplication(figures)
    dup_remove_count = sum(len(g["remove"]) for g in dup_groups)
    dup_remove_ids = set()
    for g in dup_groups:
        for r in g["remove"]:
            dup_remove_ids.add(r["id"])
    tier_conflicts = sum(1 for g in dup_groups if g["tier_conflict"])
    print(f"  Duplicate groups: {len(dup_groups)}")
    print(f"  Figures to remove: {dup_remove_count}")
    print(f"  Tier conflicts:    {tier_conflicts}")

    # Gate 2
    print("\n── Gate 2: Era + Series Assignment ──")
    era_issues = gate2_era_series(figures)
    print(f"  Issues found: {len(era_issues)}")

    # Gate 3
    print("\n── Gate 3: Sensitivity Tier ──")
    tier_issues = gate3_sensitivity_tier(figures)
    print(f"  Violations found: {len(tier_issues)}")
    for issue in tier_issues:
        for prob in issue["problems"]:
            print(f"    {issue['name']}: {prob}")

    # Gate 4 (heuristic)
    print("\n── Gate 4: Dramatic Function (heuristic) ──")
    dramatic_results = gate4_dramatic_function(figures, bible_files)
    bible_gaps = [dr for dr in dramatic_results if dr["bible_gap"]]
    print(f"  Bible gaps: {len(bible_gaps)}")

    # Gate 4b (Claude enrichment, optional)
    enrichments = {}
    if args.enrich:
        print("\n── Gate 4b: Claude API Enrichment ──")
        enrichments = enrich_with_claude(figures, dup_remove_ids)
        print(f"  Total enriched: {len(enrichments)}")

        # Merge enrichments into dramatic_results
        dr_map = {dr["id"]: dr for dr in dramatic_results}
        for fid, enrich_data in enrichments.items():
            if fid in dr_map:
                dr_map[fid]["claude_function"] = enrich_data.get("dramatic_function", "")
                dr_map[fid]["claude_series"] = enrich_data.get("series", [])
                dr_map[fid]["claude_seasons"] = enrich_data.get("seasons", "")
                dr_map[fid]["claude_reason"] = enrich_data.get("reason", "")
                dr_map[fid]["claude_flag"] = enrich_data.get("flag", "")
                # Override heuristic if Claude provides a concrete function
                cf = enrich_data.get("dramatic_function", "")
                if cf and cf != "UNKNOWN":
                    dr_map[fid]["function"] = cf

        # Save enrichments to disk for reuse
        enrich_path = PROJECT_ROOT / "data" / "claude_enrichments.json"
        enrich_path.write_text(json.dumps(enrichments, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"  Saved: {enrich_path}")

    # Generate outputs
    print("\n── Generating outputs ──")

    # Report
    report = generate_report(figures, dup_groups, era_issues, tier_issues, dramatic_results)
    if enrichments:
        report += "\n" + generate_enrichment_report_section(enrichments, figures)
    report_path = PROJECT_ROOT / "docs" / "character_evaluation_report.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"  Report: {report_path}")

    # JSON outputs — include Claude enrichment data when available
    approved, needs_review = generate_approved_and_review(
        figures, dup_groups, tier_issues, dramatic_results
    )

    # Merge enrichment into JSON outputs
    if enrichments:
        for entry in approved + needs_review:
            fid = entry["id"]
            if fid in enrichments:
                entry["claude_series"] = enrichments[fid].get("series", [])
                entry["claude_seasons"] = enrichments[fid].get("seasons", "")
                entry["claude_function"] = enrichments[fid].get("dramatic_function", "")
                entry["claude_reason"] = enrichments[fid].get("reason", "")
                if enrichments[fid].get("flag"):
                    entry.setdefault("review_reasons", []).append(
                        f"CLAUDE_FLAG: {enrichments[fid]['flag']}")

        # Figures flagged by Claude that were previously approved → move to review
        still_approved = []
        for entry in approved:
            if entry.get("review_reasons"):
                needs_review.append(entry)
            else:
                still_approved.append(entry)
        approved = still_approved

    approved_path = PROJECT_ROOT / "data" / "characters_approved_for_seed.json"
    approved_path.write_text(json.dumps(approved, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Approved: {len(approved)} figures → {approved_path}")

    review_path = PROJECT_ROOT / "data" / "characters_needs_review.json"
    review_path.write_text(json.dumps(needs_review, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Review:   {len(needs_review)} figures → {review_path}")

    # Summary
    print(f"\n{'=' * 60}")
    print("AUDIT COMPLETE")
    print(f"  Total figures:     {len(figures)}")
    print(f"  Approved:          {len(approved)}")
    print(f"  Needs review:      {len(needs_review)}")
    print(f"  Duplicate groups:  {len(dup_groups)} ({dup_remove_count} to remove)")
    print(f"  Tier violations:   {len(tier_issues)}")
    print(f"  Bible gaps:        {len(bible_gaps)}")
    if enrichments:
        print(f"  Claude enriched:   {len(enrichments)}")
    print(f"{'=' * 60}")

    if needs_review:
        print(f"\n  Read: docs/character_evaluation_report.md")
        print(f"  Then: python -m rag.knowledge.apply_character_fixes")

    return len(needs_review) == 0  # True if everything is clean


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
