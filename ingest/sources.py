"""
Islam Stories — Master source registry.

Every source the project has ever ingested (or intends to ingest) is defined here.
Each entry has an Archive.org identifier OR a direct URL, plus metadata.

The `archive_resolver` uses the identifier to find a working download link
automatically — no more hardcoded URLs that break.

Categories:
    tabari          — Al-Tabari's History (40 volumes)
    classical       — Ibn Kathir, Baladhuri, Masudi, Ibn al-Athir, etc.
    hadith_api      — Hadith collections via Fawazahmed0 CDN
    quran_api       — Quran translations via AlQuran.cloud
    persia          — Persian history sources
    caucasus        — Caucasus / Shamil sources
    timur           — Timur / Tamerlane sources
    andalusia       — Al-Andalus sources
    ottoman         — Ottoman sources
    south_asia      — Mughal / Delhi Sultanate sources
    resistance      — Colonial resistance / reform sources
    sahaba          — Companion biography sources
    gap_fills       — Gap-fill sources (session 6)
    missing         — Missing historians (session latest)
"""

# ═══════════════════════════════════════════════════════════════════
# ARCHIVE.ORG IDENTIFIER-BASED SOURCES
# These use the metadata API to resolve download URLs automatically.
# ═══════════════════════════════════════════════════════════════════

IA_TABARI = "the-history-of-al-tabari"  # Archive.org collection identifier

TABARI_VOLUMES = {
    # vol_num: (era, translator, description)
    10: ("rashidun", "Fred Donner", "Conquest of Arabia, Riddah Wars 632-633 CE"),
    11: ("rashidun", "Khalid Blankinship", "Challenge to the Empires 633-635 CE"),
    12: ("rashidun", "Yohanan Friedmann", "Battle of Qadisiyyah, Conquest of Syria 635-637 CE"),
    13: ("rashidun", "Gautier Juynboll", "Conquest of Iraq and Egypt, Umar Caliphate"),
    14: ("rashidun", "Rex Smith", "Conquest of Iran 641-643 CE"),
    15: ("rashidun", "R. Stephen Humphreys", "Crisis of the Early Caliphate, Uthman"),
    16: ("rashidun", "Humphreys", "Community Divided, Uthman and Ali 657 CE"),
    17: ("rashidun", "G. Rex Smith", "First Civil War, Ali Caliphate, Fitna"),
    18: ("umayyad", "Michael Fishbein", "Between Civil Wars, Muawiyah 661-680 CE"),
    19: ("umayyad", "I. K. A. Howard", "Caliphate of Yazid, Karbala 680 CE"),
    20: ("umayyad", "G.R. Hawting", "Collapse of Sufyanid Authority, Marwanids 683-685 CE"),
    21: ("umayyad", "Michael Fishbein", "Victory of the Marwanids 685-693 CE"),
    22: ("umayyad", "Everett Rowson", "Marwanid Restoration 693-701 CE"),
    23: ("umayyad", "Martin Hinds", "Zenith of the Marwanid House 701-715 CE"),
    24: ("umayyad", "David Stephan Powers", "Empire in Transition, Sulayman, Umar, Yazid"),
    25: ("umayyad", "Khalid Blankinship", "End of Expansion, Caliphate of Hisham 724-738 CE"),
    26: ("umayyad", "Khalid Blankinship", "Waning of the Umayyad Caliphate 738-744 CE"),
    27: ("abbasid", "John Alden Williams", "Abbasid Revolution 743-750 CE"),
    28: ("abbasid", "Jane McAuliffe", "Abbasid Authority Affirmed 750-763 CE"),
    29: ("abbasid", "Hugh Kennedy", "Al-Mansur and al-Mahdi 763-786 CE"),
    30: ("abbasid", "C.E. Bosworth", "Abbasid Caliphate in Equilibrium, Harun al-Rashid"),
    31: ("abbasid", "C.E. Bosworth", "War Between Brothers 809-813 CE"),
    32: ("abbasid", "C.E. Bosworth", "Reunification of the Abbasid Caliphate 813-833 CE"),
    33: ("abbasid", "C.E. Bosworth", "Storm and Stress, al-Mutasim 833-842 CE"),
    34: ("abbasid", "Joel Kraemer", "Incipient Decline, al-Wathiq and al-Mutawakkil"),
    36: ("abbasid", "David Waines", "Revolt of the Zanj 869-879 CE"),
    38: ("abbasid", "Franz Rosenthal", "Return of the Caliphate to Baghdad 892-902 CE"),
}


def get_tabari_sources():
    """Generate source entries for all Tabari volumes."""
    sources = []
    for vol, (era, translator, desc) in sorted(TABARI_VOLUMES.items()):
        sources.append({
            "identifier": IA_TABARI,
            "filename_hint": f"Tabari_Volume_{vol}.pdf",
            "short_name": f"al-tabari-v{vol}",
            "source": f"Al-Tabari Vol {vol}: {desc}",
            "source_type": "primary_arabic",
            "era": era,
            "translator": translator,
            "reliability": "scholarly",
            "language": "english",
            "category": "tabari",
            "format": "pdf",
        })
    return sources


# ── Classical Islamic historians ─────────────────────────────────

CLASSICAL_SOURCES = [
    # Ibn Kathir
    {
        "identifiers": ["albidayaannihayaallinonepdf"],
        "filename_hint": "6. Miracles and Merits of Rasulullah_djvu.txt",
        "short_name": "ibn-kathir-miracles",
        "source": "Ibn Kathir, Al-Bidaya wa'l-Nihaya — Miracles and Merits",
        "source_type": "primary_arabic", "era": "rashidun",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["IbnKathirEarlyDays_201703"],
        "short_name": "ibn-kathir-early-days",
        "source": "Ibn Kathir, Al-Bidaya wa'l-Nihaya — Early Days",
        "source_type": "primary_arabic", "era": "rashidun",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["BookOfTheEnd_ibnkathir"],
        "short_name": "ibn-kathir-end",
        "source": "Ibn Kathir, Al-Bidaya wa'l-Nihaya — Book of the End",
        "source_type": "primary_arabic", "era": "rashidun",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["pdfy-FFZIzpkiBPA9qqDp", "StoriesOfTheProphetsByIbnKathir_201312"],
        "short_name": "ibn-kathir-prophets",
        "source": "Ibn Kathir, Stories of the Prophets (Qisas al-Anbiya)",
        "source_type": "primary_arabic", "era": "rashidun",
        "language": "english", "category": "classical",
    },
    # Baladhuri
    {
        "identifiers": ["originsislamics00hittgoog", "originsofislamic00balarich"],
        "short_name": "baladhuri-futuh",
        "source": "Al-Baladhuri, Futuh al-Buldan (Origins of the Islamic State)",
        "source_type": "primary_arabic", "era": "rashidun",
        "language": "english", "category": "classical",
    },
    # Al-Masudi
    {
        "identifiers": ["historicalencycl00masrich", "elmasdshistoric00unkngoog"],
        "short_name": "masudi-muruj",
        "source": "Al-Masudi, Muruj al-Dhahab (Meadows of Gold)",
        "source_type": "primary_arabic", "era": "abbasid",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["meadowsgoldmine00masgoog"],
        "short_name": "masudi-meadows-sprenger",
        "source": "Al-Masudi, Muruj al-Dhahab (Meadows of Gold, Sprenger translation)",
        "source_type": "primary_arabic", "era": "abbasid",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["in.ernet.dli.2015.187564"],
        "short_name": "masudi-vol1",
        "source": "Al-Masudi, Muruj al-Dhahab Vol 1",
        "source_type": "primary_arabic", "era": "abbasid",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["in.ernet.dli.2015.187565"],
        "short_name": "masudi-vol2",
        "source": "Al-Masudi, Muruj al-Dhahab Vol 2",
        "source_type": "primary_arabic", "era": "abbasid",
        "language": "english", "category": "classical",
    },
    # Ibn al-Athir
    {
        "identifiers": ["IbnAlAthirInCicilianMuslims"],
        "short_name": "ibn-athir-crusades",
        "source": "Ibn al-Athir, Al-Kamil fi'l-Tarikh (Chronicle — Crusading Period)",
        "source_type": "primary_arabic", "era": "crusades",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["chronicleofcrus00nebegoog"],
        "short_name": "ibn-athir-crusades-richards",
        "source": "Ibn al-Athir, Chronicle of the Crusades (Richards translation)",
        "source_type": "primary_arabic", "era": "crusades",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["theannalsofthese00ibna"],
        "short_name": "ibn-athir-seljuk",
        "source": "Ibn al-Athir, The Annals of the Seljuk Turks",
        "source_type": "primary_arabic", "era": "abbasid",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["al-kamil-fi-al-tarikh-ibn-al-athir"],
        "short_name": "ibn-athir-kamil-full",
        "source": "Ibn al-Athir, Al-Kamil fi al-Tarikh (The Complete History)",
        "source_type": "primary_arabic", "era": "abbasid",
        "language": "english", "category": "classical",
    },
    # Ibn Jubayr
    {
        "identifiers": ["travelsofibnjuba05ibnjuoft", "travelsibnjubay00goejgoog"],
        "short_name": "ibn-jubayr-rihla",
        "source": "Ibn Jubayr, Rihla (The Travels of Ibn Jubayr)",
        "source_type": "primary_arabic", "era": "crusades",
        "language": "english", "category": "classical",
    },
    # Ibn Ishaq / Ibn Hisham
    {
        "identifiers": ["GuillaumeATheLifeOfMuhammad", "TheLifeOfMohammedGuillaume"],
        "short_name": "ibn-ishaq",
        "source": "Ibn Ishaq/Guillaume, The Life of Muhammad",
        "source_type": "primary_arabic", "era": "rashidun",
        "language": "english", "category": "classical",
    },
    {
        "identifiers": ["seerat-ibn-e-hisham-english-translation-2nd-edition"],
        "short_name": "ibn-hisham",
        "source": "Ibn Hisham Sira: Biography of the Prophet",
        "source_type": "primary_arabic", "era": "rashidun",
        "language": "english", "category": "classical",
    },
    # Ibn Khaldun
    {
        "identifiers": ["MuqaddimahIbnKhaldun"],
        "filename_hint": "MuqVol1.pdf",
        "short_name": "ibn-khaldun-v1",
        "source": "Ibn Khaldun Muqaddimah Vol 1: Human civilization, asabiyyah theory",
        "source_type": "primary_arabic", "era": "abbasid",
        "translator": "Franz Rosenthal", "reliability": "scholarly",
        "language": "english", "category": "classical", "format": "pdf",
    },
    {
        "identifiers": ["MuqaddimahIbnKhaldun"],
        "filename_hint": "MuqVol2.pdf",
        "short_name": "ibn-khaldun-v2",
        "source": "Ibn Khaldun Muqaddimah Vol 2: Dynasties, caliphate, government",
        "source_type": "primary_arabic", "era": "abbasid",
        "translator": "Franz Rosenthal", "reliability": "scholarly",
        "language": "english", "category": "classical", "format": "pdf",
    },
    {
        "identifiers": ["MuqaddimahIbnKhaldun"],
        "filename_hint": "MuqVol3.pdf",
        "short_name": "ibn-khaldun-v3",
        "source": "Ibn Khaldun Muqaddimah Vol 3: Sciences, crafts, ways of making a living",
        "source_type": "primary_arabic", "era": "abbasid",
        "translator": "Franz Rosenthal", "reliability": "scholarly",
        "language": "english", "category": "classical", "format": "pdf",
    },
    # Saladin
    {
        "identifiers": ["rareexcellenthis00dsri"],
        "short_name": "saladin-richards",
        "source": "Baha ad-Din ibn Shaddad, The Rare and Excellent History of Saladin",
        "source_type": "primary_arabic", "era": "crusades",
        "translator": "D.S. Richards", "reliability": "scholarly",
        "language": "english", "category": "classical",
    },
    # Abu Nu'aym, Hilyat al-Awliya (Arabic, 11 volumes, Tesseract OCR)
    *[
        {
            "identifiers": ["HilyatAwliya"],
            "filename_hint": f"Hilyat_Awliya{vol:02d}_djvu.txt",
            "short_name": f"abu-nuaym-hilya-v{vol:02d}",
            "source": f"Abu Nu'aym al-Isfahani, Hilyat al-Awliya' wa Tabaqat al-Asfiya (Arabic, Vol {vol})",
            "source_type": "primary_arabic", "era": "rashidun",
            "language": "arabic", "category": "classical",
        }
        for vol in range(0, 11)
    ],
    # Al-Qushayri, Al-Risala (Knysh English translation, 2 volumes)
    *[
        {
            "identifiers": ["EpistleOnSufismAlRisalaAlQushayriyyaFiIlmAlTasawwuf1"],
            "filename_hint": hint,
            "short_name": f"qushayri-risala-knysh-v{i+1}",
            "source": f"Al-Qushayri, Al-Risala al-Qushayriyya (Knysh English translation, Vol {i+1})",
            "source_type": "primary_arabic", "era": "abbasid",
            "translator": "Alexander D. Knysh", "reliability": "scholarly",
            "language": "english", "category": "classical",
        }
        for i, hint in enumerate([
            "Epistle on Sufism - al Risala al Qushayriyya fi Ilm al Tasawwuf 1_djvu.txt",
            "Epistle on Sufism - al Risala al Qushayriyya fi Ilm al Tasawwuf 2_djvu.txt",
        ])
    ],
    # Ibn Abd al-Barr (Arabic)
    {
        "identifiers": ["alistiabfimarifa02ibnauoft"],
        "short_name": "ibn-abd-barr-istiab",
        "source": "Ibn Abd al-Barr, Al-Isti'ab fi Ma'rifat al-Ashab (Arabic)",
        "source_type": "primary_arabic", "era": "rashidun",
        "language": "arabic", "category": "classical",
    },
    # Baburnama (Gutenberg)
    {
        "url": "https://www.gutenberg.org/files/44608/44608-0.txt",
        "short_name": "baburnama",
        "source": "Baburnama: Memoirs of Babur, founder of Mughal Empire 1483-1530",
        "source_type": "primary_arabic", "era": "south_asia",
        "translator": "Annette Beveridge", "reliability": "scholarly",
        "language": "english", "category": "south_asia", "format": "gutenberg_txt",
    },
    # Ibn Battuta
    {
        "identifiers": ["in.ernet.dli.2015.62617"],
        "short_name": "ibn-battuta-asia",
        "source": "Ibn Battuta Travels in Asia and Africa 1325-1354",
        "source_type": "primary_arabic", "era": "mongol",
        "translator": "H.A.R. Gibb", "reliability": "scholarly",
        "language": "english", "category": "classical", "format": "pdf",
    },
]

# ── Missing historians (latest session) ─────────────────────────

MISSING_HISTORIANS = [
    # Ibn Khallikan
    {
        "identifiers": ["WafayatAl-ayantheObituariesOfEminentMenByIbnKhallikan"],
        "filename_hint": "Vol3Of4WafayatAl-ayantheObituariesOfEminentMenByIbnKhallikan_djvu.txt",
        "short_name": "ibn-khallikan-v3",
        "source": "Ibn Khallikan, Wafayat al-Ayan Vol 3 (Biographical Dictionary)",
        "source_type": "biography", "era": "medieval",
        "language": "arabic", "category": "missing",
    },
    {
        "identifiers": ["WafayatAl-ayantheObituariesOfEminentMenByIbnKhallikan"],
        "filename_hint": "Vol4Of4WafayatAl-ayantheObituariesOfEminentMenByIbnKhallikan_djvu.txt",
        "short_name": "ibn-khallikan-v4",
        "source": "Ibn Khallikan, Wafayat al-Ayan Vol 4 (Biographical Dictionary)",
        "source_type": "biography", "era": "medieval",
        "language": "arabic", "category": "missing",
    },
    # Ibn al-Jawzi (19 volumes)
    *[{
        "identifiers": ["muntazim_tarikh_mlouk_oumm"],
        "filename_hint": f"mtmo{vol:02d}_djvu.txt",
        "short_name": f"ibn-jawzi-muntazam-v{vol}",
        "source": f"Ibn al-Jawzi, Al-Muntazam fi Tarikh al-Muluk Vol {vol} (Arabic)",
        "source_type": "chronicle", "era": "medieval",
        "language": "arabic", "category": "missing",
    } for vol in range(1, 20)],
    # Amir Khusrau
    {
        "identifiers": ["dli.ernet.13638"],
        "short_name": "amir-khusrau-khazain",
        "source": "Amir Khusrau, Khazainul Futuh (Treasury of Victories)",
        "source_type": "chronicle", "era": "medieval",
        "language": "english", "category": "missing",
    },
    {
        "identifiers": ["dli.ernet.335347"],
        "short_name": "amir-khusrau-habib",
        "source": "Amir Khusrau, Khazainul Futuh (Habib translation, Delhi)",
        "source_type": "chronicle", "era": "medieval",
        "language": "english", "category": "missing",
    },
    # Al-Suyuti
    {
        "identifiers": ["HusnAlMuhadarah"],
        "short_name": "suyuti-husn",
        "source": "Al-Suyuti, Husn al-Muhadara fi Tarikh Misr wal-Qahira (Arabic)",
        "source_type": "chronicle", "era": "medieval",
        "language": "arabic", "category": "missing",
    },
]

# ── Persia / Caucasus / Timur ────────────────────────────────────

PERSIA_SOURCES = [
    {
        "identifiers": ["tarikh-al-tabari", "history-of-tabari-volume-1_202503"],
        "short_name": "tabari-complete-ar",
        "source": "Al-Tabari, Tarikh al-Rusul wa al-Muluk (History of Prophets and Kings)",
        "source_type": "historical", "era": "abbasid",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["historyoftheworl011691mbp"],
        "short_name": "juvaini-v1",
        "source": "Ata-Malik Juvaini, Tarikh-i-Jahan-Gusha (History of the World Conqueror)",
        "source_type": "historical", "era": "mongol",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["historyoftheworl011648mbp"],
        "short_name": "juvaini-v2",
        "source": "Ata-Malik Juvaini, Tarikh-i-Jahan-Gusha Vol II",
        "source_type": "historical", "era": "mongol",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["shahnameh-the-persian-book-of-kings"],
        "short_name": "shahnameh",
        "source": "Firdausi, Shahnameh (The Persian Book of Kings)",
        "source_type": "literary", "era": "pre-islamic",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["volume-2-a-literary-history-of-persia"],
        "short_name": "browne-persia",
        "source": "Edward Granville Browne, A Literary History of Persia",
        "source_type": "secondary", "era": "multi-era",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["historyofpersiasykesp.m.vol1_948_p"],
        "short_name": "sykes-persia-v1",
        "source": "Percy Sykes, A History of Persia Vol I",
        "source_type": "secondary", "era": "multi-era",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["historyofpersiasykesp.m.vol2_790_s", "in.ernet.dli.2015.81241"],
        "short_name": "sykes-persia-v2",
        "source": "Percy Sykes, A History of Persia Vol II",
        "source_type": "secondary", "era": "multi-era",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["TheGeographicalPartOFTheNuzhatAlQulub",
                        "MustawfiQazviniAlQulub1340GeographicalPartLeStrange1919"],
        "short_name": "mustawfi-nuzhat",
        "source": "Hamdullah Mustawfi, Nuzhat al-Qulub (Geographical Part)",
        "source_type": "geographical", "era": "mongol",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["abridgedtranslat00ibniuoft"],
        "short_name": "ibn-isfandiyar",
        "source": "Ibn Isfandiyar, History of Tabaristan",
        "source_type": "historical", "era": "abbasid",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["Qanun-i-Humayun-Khwandamir",
                        "qanunihumayuniorhumayunnamaofkhwandamirpersianskmbookno2511royalasiaticsociety"],
        "short_name": "khwandamir-qanun",
        "source": "Khwandamir, Qanun-i-Humayuni",
        "source_type": "historical", "era": "timurid",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["leyla-and-majnun"],
        "short_name": "nizami-layla",
        "source": "Nizami Ganjavi, Leyla and Majnun",
        "source_type": "literary", "era": "seljuk",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["introductionlh00blocuoft"],
        "short_name": "rashid-din-intro",
        "source": "Rashid al-Din Fazlullah, Introduction to the History of the Mongols",
        "source_type": "historical", "era": "mongol",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["Boyle1971RashidAlDin"],
        "short_name": "rashid-din-boyle",
        "source": "Rashid al-Din / Boyle - Successors of Genghis Khan",
        "source_type": "historical", "era": "mongol",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["monshi-shah-abbas-english", "in.ernet.dli.2015.38522"],
        "short_name": "iskandar-beg-english",
        "source": "Iskandar Beg Munshi - History of Shah Abbas (English)",
        "source_type": "historical", "era": "safavid",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["tadhkirat-al-muluk-3rd-ed-july-2025"],
        "short_name": "minorsky-tadhkirat",
        "source": "Minorsky - Tadhkirat al-Muluk (Safavid)",
        "source_type": "historical", "era": "safavid",
        "language": "english", "category": "persia",
    },
    # Persian-language sources (for OCR pipeline)
    {
        "identifiers": ["20220618_20220618_0440"],
        "short_name": "nizami-khamsa-fa",
        "source": "Nizami Ganjavi - Khamsa (Persian)",
        "source_type": "literary", "era": "seljuk",
        "language": "persian", "category": "persia",
    },
    {
        "identifiers": ["jami-al-twarikh-by-rashidi-volume-2"],
        "short_name": "rashid-din-jami-v2-fa",
        "source": "Rashid al-Din - Jami al-Twarikh Vol 2",
        "source_type": "historical", "era": "mongol",
        "language": "persian", "category": "persia",
    },
    {
        "identifiers": ["IskenderBeyTarikhIAlamAraYiAbbasi"],
        "short_name": "iskandar-beg-fa",
        "source": "Iskandar Beg - Tarikh-i Alam Ara-yi Abbasi (Persian)",
        "source_type": "historical", "era": "safavid",
        "language": "persian", "category": "persia",
    },
    {
        "identifiers": ["TarikhEWassafUl-Hazrat-AbdullahIbnFazlullahSharafuddinShiraziFarsi"],
        "short_name": "wassaf-fa",
        "source": "Wassaf - Tarikh-e Wassaf (Farsi)",
        "source_type": "historical", "era": "mongol",
        "language": "persian", "category": "persia",
    },
    {
        "identifiers": ["MS.PERS.65"],
        "short_name": "wassaf-manuscript",
        "source": "Wassaf - Tajziyat al-Amsar (manuscript)",
        "source_type": "historical", "era": "mongol",
        "language": "persian", "category": "persia",
    },
    {
        "identifiers": ["dli.ministry.05585"],
        "short_name": "mirkhwand-rauzat",
        "source": "Mirkhwand - Rauzat-us-Safa",
        "source_type": "historical", "era": "timurid",
        "language": "persian", "category": "persia",
    },
    {
        "identifiers": ["dli.ernet.508045"],
        "short_name": "rashid-din-jamia",
        "source": "Rashid al-Din - Jamia ul-Tawareekh",
        "source_type": "historical", "era": "mongol",
        "language": "persian", "category": "persia",
    },
    {
        "identifiers": ["haftpaykarmediev00niza"],
        "short_name": "nizami-haft-paykar",
        "source": "Nizami Ganjavi - Haft Paykar",
        "source_type": "literary", "era": "seljuk",
        "language": "english", "category": "persia",
    },
    {
        "identifiers": ["literaryhistoryo02brow"],
        "short_name": "browne-literary-history",
        "source": "Browne - A Literary History of Persia",
        "source_type": "secondary", "era": "multi-era",
        "language": "english", "category": "persia",
    },
]

CAUCASUS_SOURCES = [
    {
        "identifiers": ["cu31924028754616", "russianconquestof00badd"],
        "short_name": "baddeley-caucasus",
        "source": "John F. Baddeley, The Russian Conquest of the Caucasus",
        "source_type": "secondary", "era": "modern",
        "language": "english", "category": "caucasus",
    },
    {
        "identifiers": ["muslimresistance0000gamm"],
        "short_name": "gammer-shamil",
        "source": "Moshe Gammer, Muslim Resistance to the Tsar: Shamil",
        "source_type": "secondary", "era": "modern",
        "language": "english", "category": "caucasus",
    },
    {
        "identifiers": ["sabresofparadise0000unse"],
        "short_name": "blanch-sabres",
        "source": "Blanch - The Sabres of Paradise (Shamil)",
        "source_type": "secondary", "era": "modern",
        "language": "english", "category": "caucasus",
    },
    {
        "identifiers": ["hronikamuhammedatahiraalkarahi34"],
        "short_name": "qarakhi-chronicle",
        "source": "Qarakhi - Chronicle (Russian)",
        "source_type": "historical", "era": "modern",
        "language": "russian", "category": "caucasus",
    },
    {
        "identifiers": ["bournoutian-2004-2-karabagh-chronicles"],
        "short_name": "bournoutian-karabagh",
        "source": "Bournoutian - Two Chronicles on Karabagh",
        "source_type": "historical", "era": "modern",
        "language": "english", "category": "caucasus",
    },
]

TIMUR_SOURCES = [
    {
        "identifiers": ["TamerlaneOrTimurTheGreatAmir-AhmedIbnArabshah"],
        "short_name": "ibn-arabshah-timur",
        "source": "Ahmed Ibn Arabshah, Tamerlane or Timur the Great Amir",
        "source_type": "historical", "era": "timurid",
        "language": "english", "category": "timur",
    },
    {
        "identifiers": ["al_862"],
        "short_name": "ibn-arabshah-ajaib-ar",
        "source": "Ibn Arabshah - Ajaib al-Maqdur (Arabic)",
        "source_type": "historical", "era": "timurid",
        "language": "arabic", "category": "timur",
    },
    {
        "identifiers": ["ZindaganiEShigiftAavarETemur-IbnArabshahFarsiTarjuma"],
        "short_name": "ibn-arabshah-timur-fa",
        "source": "Ibn Arabshah - Life of Timur (Farsi)",
        "source_type": "historical", "era": "timurid",
        "language": "persian", "category": "timur",
    },
    {
        "identifiers": ["narrativeembass00markgoog", "b31354932"],
        "short_name": "clavijo-embassy",
        "source": "Clavijo - Embassy to Tamerlane 1403-1406",
        "source_type": "historical", "era": "timurid",
        "language": "english", "category": "timur",
    },
    {
        "identifiers": ["tamerlaneearthsh0000haro"],
        "short_name": "lamb-tamerlane",
        "source": "Harold Lamb - Tamerlane the Earth Shaker",
        "source_type": "secondary", "era": "timurid",
        "language": "english", "category": "timur",
    },
    {
        "identifiers": ["edgeofempireshis0000rayf"],
        "short_name": "rayfield-georgia",
        "source": "Rayfield - Edge of Empires History of Georgia",
        "source_type": "secondary", "era": "timurid",
        "language": "english", "category": "timur",
    },
]

# ── Andalusia / Ottoman / Resistance ─────────────────────────────

ANDALUSIA_SOURCES = [
    {
        "identifiers": ["historyofmohamme01makkuoft"],
        "short_name": "al-maqqari-v1",
        "source": "Al-Maqqari, History of Mohammedan Dynasties in Spain Vol 1",
        "source_type": "primary_arabic", "era": "andalusia",
        "language": "english", "category": "andalusia",
    },
    {
        "identifiers": ["historyofmohamme02makkuoft"],
        "short_name": "al-maqqari-v2",
        "source": "Al-Maqqari, History of Mohammedan Dynasties in Spain Vol 2",
        "source_type": "primary_arabic", "era": "andalusia",
        "language": "english", "category": "andalusia",
    },
    {
        "identifiers": ["ringthedovetreat00ibnhuoft"],
        "short_name": "ibn-hazm-ring-dove",
        "source": "Ibn Hazm, The Ring of the Dove (Cordoba, 11th century)",
        "source_type": "primary_arabic", "era": "andalusia",
        "language": "english", "category": "andalusia",
    },
    {
        "identifiers": ["moriscosofspain00leah"],
        "short_name": "lea-moriscos",
        "source": "Lea, The Moriscos of Spain (1901)",
        "source_type": "scholarly_western", "era": "andalusia",
        "language": "english", "category": "andalusia",
    },
]

OTTOMAN_SOURCES = [
    {
        "identifiers": ["historyofmehmedt00krit"],
        "short_name": "kritovoulos-mehmed",
        "source": "Kritovoulos, History of Mehmed the Conqueror (eyewitness, 1467)",
        "source_type": "primary_arabic", "era": "ottoman",
        "language": "english", "category": "ottoman",
    },
    {
        "identifiers": ["riseofottomanem00witt"],
        "short_name": "wittek-ottoman-rise",
        "source": "Wittek, The Rise of the Ottoman Empire (1938)",
        "source_type": "scholarly_western", "era": "ottoman",
        "language": "english", "category": "ottoman",
    },
]

RESISTANCE_SOURCES = [
    {
        "identifiers": ["lifeofabdelkader00chur"],
        "short_name": "churchill-abd-al-qadir",
        "source": "Churchill, The Life of Abdel-Kader (1867)",
        "source_type": "scholarly_western", "era": "resistance_colonial",
        "language": "english", "category": "resistance",
    },
    {
        "identifiers": ["arabicthoughtinl0000hour"],
        "short_name": "hourani-arabic-thought",
        "source": "Hourani, Arabic Thought in the Liberal Age 1798-1939",
        "source_type": "scholarly_western", "era": "resistance_colonial",
        "language": "english", "category": "resistance",
    },
    {
        "identifiers": ["reconstructionof00iqba"],
        "short_name": "iqbal-reconstruction",
        "source": "Iqbal, The Reconstruction of Religious Thought in Islam",
        "source_type": "primary_arabic", "era": "resistance_colonial",
        "language": "english", "category": "resistance",
    },
    {
        "identifiers": ["indianmusalmans00huntgoog"],
        "short_name": "hunter-indian-musalmans",
        "source": "Hunter, The Indian Musalmans (1871) — colonial perspective",
        "source_type": "scholarly_western", "era": "resistance_colonial",
        "language": "english", "category": "resistance",
    },
]


# ═══════════════════════════════════════════════════════════════════
# API-BASED SOURCES (not from Archive.org)
# ═══════════════════════════════════════════════════════════════════

HADITH_COLLECTIONS = {
    "eng-bukhari":        "Sahih al-Bukhari",
    "eng-muslim":         "Sahih Muslim",
    "eng-abudawud":       "Sunan Abu Dawud",
    "eng-ibnmajah":       "Sunan Ibn Majah",
    "eng-tirmidhi":       "Jami at-Tirmidhi",
    "eng-nasai":          "Sunan an-Nasai",
    "eng-nawawi40":       "Nawawi 40 Hadith",
    "eng-riyadussalihin": "Riyad as-Salihin",
    "eng-bulughalmaram":  "Bulugh al-Maram",
}

FAWAZ_BASE = "https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions"

QURAN_TRANSLATIONS = {
    "en.pickthall": "Holy Quran (Pickthall translation)",
    "en.yusufali":  "Holy Quran (Yusuf Ali translation)",
}


# ═══════════════════════════════════════════════════════════════════
# MASTER GETTER
# ═══════════════════════════════════════════════════════════════════

ALL_CATEGORIES = {
    "tabari": get_tabari_sources,
    "classical": lambda: CLASSICAL_SOURCES,
    "missing": lambda: MISSING_HISTORIANS,
    "persia": lambda: PERSIA_SOURCES,
    "caucasus": lambda: CAUCASUS_SOURCES,
    "timur": lambda: TIMUR_SOURCES,
    "andalusia": lambda: ANDALUSIA_SOURCES,
    "ottoman": lambda: OTTOMAN_SOURCES,
    "resistance": lambda: RESISTANCE_SOURCES,
}


def get_all_sources(categories=None):
    """
    Get all source entries, optionally filtered by category.

    Args:
        categories: List of category names, or None for all.

    Returns:
        List of source dicts.
    """
    if categories is None:
        categories = list(ALL_CATEGORIES.keys())

    sources = []
    for cat in categories:
        if cat in ALL_CATEGORIES:
            sources.extend(ALL_CATEGORIES[cat]())
        else:
            print(f"  Unknown category: {cat}")
    return sources


def get_source_by_short_name(short_name):
    """Find a single source by its short_name."""
    for src in get_all_sources():
        if src.get("short_name") == short_name:
            return src
    return None


def list_categories():
    """List all available categories and their source counts."""
    for cat, getter in ALL_CATEGORIES.items():
        sources = getter()
        print(f"  {cat:20s} {len(sources):>3} sources")
