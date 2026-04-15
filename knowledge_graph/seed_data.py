"""
Islam Stories — Seed Data for Knowledge Graph
Seeds figures, events, themes, and priority entries for
lineage, relationships, motivations, deaths, quotes, scholarly debates.

Run:  python -m rag.knowledge.seed_data
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

# ═══════════════════════════════════════════════════════════════════════
# THEMES — 20 canonical themes
# ═══════════════════════════════════════════════════════════════════════

THEMES = [
    ("justice_vs_power",          "Justice vs Power",           "The tension between moral authority and political power"),
    ("faith_under_oppression",    "Faith Under Oppression",     "Maintaining belief under persecution or colonial rule"),
    ("knowledge_as_resistance",   "Knowledge as Resistance",    "Scholarship and learning as acts of defiance against tyranny"),
    ("loyalty_and_betrayal",      "Loyalty and Betrayal",       "Bonds of allegiance tested by ambition or circumstance"),
    ("conquest_and_mercy",        "Conquest and Mercy",         "Military expansion tempered — or not — by compassion"),
    ("decline_and_renewal",       "Decline and Renewal",        "Civilizational cycles of decay and revival"),
    ("identity_under_occupation", "Identity Under Occupation",  "Cultural and religious identity preserved under foreign rule"),
    ("scholar_vs_ruler",          "Scholar vs Ruler",           "Intellectual authority challenging political power"),
    ("woman_in_power",            "Woman in Power",             "Female agency in patriarchal political structures"),
    ("sufi_inner_outer",          "Sufi Inner and Outer",       "Mystical experience meeting political reality"),
    ("diaspora_faith",            "Diaspora Faith",             "Islam carried across trade routes and migrations"),
    ("succession_crisis",         "Succession Crisis",          "Contested transfers of power and their consequences"),
    ("martyrdom",                 "Martyrdom",                  "Choosing death over compromise — and its afterlife in memory"),
    ("east_west_encounter",       "East-West Encounter",        "Cross-civilizational contact between Islamic and European worlds"),
    ("slave_who_became_king",     "The Slave Who Became King",  "From bondage to sovereignty — the Mamluk paradigm"),
    ("the_mentor_betrayal",       "The Mentor Betrayal",        "Student surpassing or turning against the teacher"),
    ("empire_building",           "Empire Building",            "The mechanics and morality of state creation"),
    ("intellectual_legacy",       "Intellectual Legacy",         "Ideas that outlived the thinkers who created them"),
    ("the_last_stand",            "The Last Stand",             "Final defenses against overwhelming odds"),
    ("trade_as_civilization",     "Trade as Civilization",       "Commerce as the vehicle for cultural and religious spread"),
]

# ═══════════════════════════════════════════════════════════════════════
# FIGURES — Priority figures for Phase A + full roster
# Format: (name, variants, tier, eras, series, birth_death,
#          dramatic_question, generation, tabaqat_vol, sahabi_cats,
#          bayah_pledges, known_for, hadith_count, death_circumstance)
# ═══════════════════════════════════════════════════════════════════════

FIGURES = [
    # ── Tier S — Never depicted ─────────────────────────────────────
    ("Prophet Muhammad (PBUH)",
     ["Muhammad", "Rasulullah", "Al-Mustafa"],
     "S", ["rashidun"], ["The Sword and the Succession"],
     "570-632 CE",
     "The Messenger whose revelation reshaped the world",
     "sahabi", None, [], [], "The Final Prophet of Islam",
     0, "natural"),

    ("Abu Bakr al-Siddiq",
     ["Abu Bakr", "Al-Siddiq"],
     "S", ["rashidun"], ["The Sword and the Succession"],
     "573-634 CE",
     "What does it mean to be the first to believe — and the first to hold power after the Prophet?",
     "sahabi", 3, ["muhajir"], ["aqaba_2", "badr", "hudaybiyya", "ridwan"],
     "First Caliph, united Arabia during Ridda Wars",
     142, "natural"),

    ("Umar ibn al-Khattab",
     ["Umar", "Al-Farooq", "Umar I"],
     "S", ["rashidun"], ["The Sword and the Succession"],
     "584-644 CE",
     "Can justice survive the exercise of absolute power?",
     "sahabi", 3, ["muhajir", "warrior"], ["badr", "hudaybiyya", "ridwan"],
     "Second Caliph, conquered Persia and Levant, dismissed Khalid",
     537, "assassinated"),

    ("Uthman ibn Affan",
     ["Uthman", "Dhun-Nurayn"],
     "S", ["rashidun"], ["The Sword and the Succession"],
     "576-656 CE",
     "Does generosity protect a ruler from the consequences of nepotism?",
     "sahabi", 3, ["muhajir"], ["badr", "hudaybiyya", "ridwan"],
     "Third Caliph, compiled the Quran, killed by rebels",
     146, "assassinated"),

    ("Ali ibn Abi Talib",
     ["Ali", "Amir al-Mu'minin", "Abu al-Hasan"],
     "S", ["rashidun"], ["The Sword and the Succession"],
     "601-661 CE",
     "What happens when the most qualified man inherits an impossible situation?",
     "sahabi", 3, ["muhajir", "warrior", "scholar"], ["badr", "hudaybiyya", "ridwan"],
     "Fourth Caliph, faced three civil wars, gateway to knowledge",
     536, "assassinated"),

    ("Khadijah bint Khuwaylid",
     ["Khadijah", "Umm al-Mu'minin"],
     "S", ["rashidun"], ["The Sword and the Succession", "Women of Islam"],
     "555-619 CE",
     "The woman who believed first — before revelation had proof",
     "sahabi", None, ["woman"], [],
     "First wife of the Prophet, first Muslim, funded early Islam",
     0, "natural"),

    ("Aisha bint Abi Bakr",
     ["Aisha", "Umm al-Mu'minin"],
     "S", ["rashidun"], ["The Sword and the Succession", "Women of Islam"],
     "613-678 CE",
     "Can a woman be both the keeper of Prophetic knowledge and a political actor?",
     "sahabi", 8, ["woman", "scholar"], [],
     "Greatest female narrator of hadith, led Battle of the Camel",
     2210, "natural"),

    # ── Tier A — No invented dialogue ───────────────────────────────

    ("Husayn ibn Ali",
     ["Husayn", "Al-Husayn", "Imam Husayn"],
     "A", ["rashidun", "umayyad"], ["The Sword and the Succession", "The Umayyad Paradox"],
     "626-680 CE",
     "Is there a point where refusing to submit becomes the only form of faith left?",
     "sahabi", 3, ["martyr"], [],
     "Martyred at Karbala, grandson of the Prophet",
     0, "martyrdom"),

    ("Hassan ibn Ali",
     ["Hassan", "Al-Hasan"],
     "A", ["rashidun", "umayyad"], ["The Sword and the Succession"],
     "624-670 CE",
     "Is abdication cowardice — or the bravest possible act?",
     "sahabi", 3, [], [],
     "Abdicated the caliphate to Muawiyah to prevent bloodshed",
     0, "natural"),

    ("Hamza ibn Abd al-Muttalib",
     ["Hamza", "Asad Allah", "Lion of God"],
     "A", ["rashidun"], ["The Sword and the Succession"],
     "570-625 CE",
     "What does it cost to be the first shield of a new faith?",
     "sahabi", 3, ["warrior", "martyr", "muhajir"], ["badr"],
     "Uncle of the Prophet, martyred at Uhud",
     0, "battle"),

    ("Jafar ibn Abi Talib",
     ["Jafar", "Jafar al-Tayyar"],
     "A", ["rashidun"], ["The Sword and the Succession"],
     "590-629 CE",
     "The diplomat who spoke truth to power — and the warrior who paid for it",
     "sahabi", 3, ["muhajir", "martyr"], [],
     "Led migration to Abyssinia, martyred at Mu'tah",
     0, "battle"),

    ("Sumayya bint Khabbat",
     ["Sumayya"],
     "A", ["rashidun"], ["The Sword and the Succession", "Women of Islam"],
     "d. 615 CE",
     "The first to die for a belief the world hadn't yet heard of",
     "sahabi", None, ["woman", "martyr", "convert"], [],
     "First martyr in Islam",
     0, "martyrdom"),

    ("Zaynab bint Ali",
     ["Zaynab", "Sayyida Zaynab"],
     "A", ["umayyad"], ["The Umayyad Paradox", "Women of Islam"],
     "626-682 CE",
     "When the men are dead, who carries the message?",
     "sahabi", None, ["woman"], [],
     "Witnessed Karbala, spoke truth to Yazid in his own court",
     0, "natural"),

    ("Fatimah bint Muhammad",
     ["Fatimah", "Al-Zahra", "Fatimah al-Zahra"],
     "A", ["rashidun"], ["The Sword and the Succession", "Women of Islam"],
     "605-632 CE",
     "The daughter who inherited a mission, not a throne",
     "sahabi", 8, ["woman"], [],
     "Daughter of the Prophet, mother of Hassan and Husayn",
     0, "natural"),

    # ── Tier B — Full depiction, pilot priority figures ─────────────

    ("Khalid ibn Walid",
     ["Khalid", "Sayf Allah", "The Sword of God"],
     "B", ["rashidun"], ["The Sword and the Succession"],
     "592-642 CE",
     "What happens to a weapon when the hand that wields it lets go?",
     "sahabi", 3, ["warrior", "muhajir", "convert"], ["conquest_mecca"],
     "Undefeated in 100+ battles, dismissed by Umar at peak of power",
     18, "natural"),

    ("Abu Ubayda ibn al-Jarrah",
     ["Abu Ubayda", "Amin al-Ummah"],
     "B", ["rashidun"], ["The Sword and the Succession"],
     "583-639 CE",
     "What does it mean to be trustworthy in a world that rewards ambition?",
     "sahabi", 3, ["muhajir", "warrior"], ["aqaba_2", "badr", "hudaybiyya", "ridwan"],
     "Trusted commander, replaced Khalid, died in Amwas plague",
     14, "plague"),

    ("Muawiyah ibn Abi Sufyan",
     ["Muawiyah", "Muawiyah I"],
     "B", ["rashidun", "umayyad"], ["The Sword and the Succession", "The Umayyad Paradox"],
     "602-680 CE",
     "Can pragmatism build what idealism cannot hold?",
     "sahabi", 3, ["convert"], ["conquest_mecca"],
     "Founded the Umayyad dynasty, master politician",
     163, "natural"),

    ("Amr ibn al-As",
     ["Amr", "Amr ibn al-As"],
     "B", ["rashidun", "umayyad"], ["The Sword and the Succession", "The Umayyad Paradox"],
     "573-664 CE",
     "Is there a line between strategic genius and moral bankruptcy?",
     "sahabi", 3, ["warrior", "convert"], ["conquest_mecca"],
     "Conquered Egypt, raised Qurans at Siffin, shaped Umayyad victory",
     39, "natural"),

    ("Saladin",
     ["Salah ad-Din", "Salah ad-Din al-Ayyubi", "Saladin Ayyubi"],
     "B", ["crusades"], ["Crusades: Islamic Perspective"],
     "1137-1193 CE",
     "Can a conqueror be merciful without being weak?",
     "later", None, [], [],
     "Retook Jerusalem, showed mercy to Crusaders, founded Ayyubid dynasty",
     0, "natural"),

    ("Baybars",
     ["Baybars I", "Al-Zahir Baybars", "Rukn al-Din Baybars"],
     "B", ["mamluk"], ["When Islam Stopped the Mongols"],
     "1223-1277 CE",
     "Can a slave become a king without becoming a tyrant?",
     "later", None, [], [],
     "Slave soldier who defeated Mongols at Ain Jalut, built Mamluk empire",
     0, "natural"),

    ("Nur ad-Din Zengi",
     ["Nur ad-Din", "Nur al-Din"],
     "B", ["crusades"], ["Crusades: Islamic Perspective"],
     "1118-1174 CE",
     "What does it mean to build the stage for someone else's glory?",
     "later", None, [], [],
     "United Syria, paved the way for Saladin, died before seeing Jerusalem freed",
     0, "natural"),

    ("Ibn Khaldun",
     ["Abd al-Rahman ibn Khaldun", "Wali al-Din"],
     "B", ["mamluk"], ["Scholars and Thinkers"],
     "1332-1406 CE",
     "Can one man see the pattern behind the rise and fall of every civilization?",
     "later", None, [], [],
     "Father of sociology, wrote the Muqaddimah",
     0, "natural"),

    ("Al-Ghazali",
     ["Abu Hamid al-Ghazali", "Imam al-Ghazali", "Hujjat al-Islam"],
     "B", ["abbasid"], ["Scholars and Thinkers", "The Abbasid: Glory and Rot"],
     "1058-1111 CE",
     "What happens when the greatest mind of an age walks away from everything?",
     "later", None, [], [],
     "Revived Islamic spirituality, critiqued philosophy, wrote Ihya Ulum al-Din",
     0, "natural"),

    ("Ibn Rushd",
     ["Averroes", "Abu al-Walid ibn Rushd"],
     "B", ["andalusia"], ["The Andalusian Arc", "Scholars and Thinkers"],
     "1126-1198 CE",
     "Can reason and revelation coexist — or must one surrender to the other?",
     "later", None, [], [],
     "Greatest Islamic Aristotelian, influenced both Islamic and European thought",
     0, "natural"),

    ("Tariq ibn Ziyad",
     ["Tariq"],
     "B", ["umayyad", "andalusia"], ["The Andalusian Arc"],
     "670-720 CE",
     "What drives a freed slave to burn his ships and conquer a continent?",
     "tabi_i", None, [], [],
     "Led Muslim conquest of Iberia, burned ships at Gibraltar",
     0, "unknown"),

    ("Abd al-Rahman I",
     ["Abd al-Rahman al-Dakhil", "The Falcon of Quraysh"],
     "B", ["umayyad", "andalusia"], ["The Andalusian Arc"],
     "731-788 CE",
     "Can one man rebuild a dynasty from ashes on a foreign shore?",
     "later", None, [], [],
     "Sole Umayyad survivor, founded Emirate of Cordoba",
     0, "natural"),

    ("Abd al-Rahman III",
     ["Al-Nasir", "Abd al-Rahman III"],
     "B", ["andalusia"], ["The Andalusian Arc"],
     "891-961 CE",
     "What does it cost to build the most glorious city in the world?",
     "later", None, [], [],
     "Declared Caliphate of Cordoba, built Medina Azahara",
     0, "natural"),

    ("Musa ibn Nusayr",
     ["Musa ibn Nusayr"],
     "B", ["umayyad", "andalusia"], ["The Andalusian Arc"],
     "640-716 CE",
     "The governor who sent Tariq — and then tried to claim the glory",
     "tabi_i", None, [], [],
     "Governor of Ifriqiya, launched Iberian conquest, fell from grace",
     0, "natural"),

    ("Said ibn Jubayr",
     ["Said ibn Jubayr", "Sa'id ibn Jubayr"],
     "B", ["umayyad"], ["The Umayyad Paradox", "Scholars and Thinkers"],
     "665-714 CE",
     "When a tyrant demands silence, is speech itself an act of war?",
     "tabi_i", None, [], [],
     "Scholar who refused to betray Ibn Ash'ath, executed by Al-Hajjaj",
     0, "executed"),

    ("Bilal ibn Rabah",
     ["Bilal", "Bilal al-Habashi"],
     "B", ["rashidun"], ["The Sword and the Succession"],
     "580-640 CE",
     "What does freedom sound like?",
     "sahabi", 3, ["convert", "muhajir", "ansar"], ["badr", "hudaybiyya", "ridwan"],
     "First muezzin, freed slave, voice of early Islam",
     44, "natural"),

    ("Abu Dharr al-Ghifari",
     ["Abu Dharr"],
     "B", ["rashidun"], ["The Sword and the Succession"],
     "d. 652 CE",
     "Can radical honesty survive in a world that rewards compromise?",
     "sahabi", 3, ["convert"], [],
     "Champion of the poor, exiled for criticizing wealth accumulation",
     281, "natural"),

    ("Salman al-Farisi",
     ["Salman", "Salman the Persian"],
     "B", ["rashidun"], ["The Sword and the Succession"],
     "568-656 CE",
     "How far must you travel to find what you were always looking for?",
     "sahabi", 3, ["convert", "ansar"], [],
     "Persian seeker who found Islam, suggested the trench at Khandaq",
     60, "natural"),

    ("Khalid ibn Yazid",
     ["Khalid ibn Yazid ibn Muawiyah"],
     "B", ["umayyad"], ["The Umayyad Paradox", "Scholars and Thinkers"],
     "668-704 CE",
     "When power is denied, can knowledge become the greater inheritance?",
     "tabi_i", None, [], [],
     "Umayyad prince who chose alchemy and scholarship over politics",
     0, "natural"),

    ("Umar ibn Abd al-Aziz",
     ["Umar II", "Umar ibn Abd al-Aziz"],
     "B", ["umayyad"], ["The Umayyad Paradox"],
     "682-720 CE",
     "Can one just ruler redeem a dynasty built on conquest?",
     "tabi_i", None, [], [],
     "The fifth righteous caliph, reformed Umayyad excess, possibly poisoned",
     0, "natural"),

    ("Mukhtar al-Thaqafi",
     ["Al-Mukhtar", "Mukhtar"],
     "B", ["umayyad"], ["The Umayyad Paradox"],
     "622-687 CE",
     "Avenger or opportunist — can both be true at once?",
     "sahabi", None, ["warrior"], [],
     "Led revolt to avenge Husayn, killed Husayn's murderers",
     0, "battle"),

    ("Hasan al-Basri",
     ["Hasan al-Basri", "Al-Hasan al-Basri"],
     "B", ["umayyad"], ["The Umayyad Paradox", "Scholars and Thinkers"],
     "642-728 CE",
     "Can a voice of conscience survive in a court of power?",
     "tabi_i", None, [], [],
     "Greatest preacher of early Islam, warned rulers of divine judgment",
     0, "natural"),

    ("Abu Muslim al-Khurasani",
     ["Abu Muslim"],
     "B", ["umayyad", "abbasid"], ["The Umayyad Paradox", "The Abbasid: Glory and Rot"],
     "718-755 CE",
     "What does the revolution owe the revolutionary?",
     "later", None, [], [],
     "Led the Abbasid revolution, then murdered by the dynasty he created",
     0, "assassinated"),

    ("Harun al-Rashid",
     ["Harun", "Al-Rashid"],
     "B", ["abbasid"], ["The Abbasid: Glory and Rot"],
     "763-809 CE",
     "Does the golden age remember the blood that paid for it?",
     "later", None, [], [],
     "Abbasid caliph of the golden age, patron of learning, Thousand and One Nights",
     0, "natural"),

    ("Al-Ma'mun",
     ["Al-Ma'mun", "Abdullah al-Ma'mun"],
     "B", ["abbasid"], ["The Abbasid: Glory and Rot", "Scholars and Thinkers"],
     "786-833 CE",
     "Can a ruler force truth into existence through the power of the state?",
     "later", None, [], [],
     "Founded the House of Wisdom, imposed Mu'tazila doctrine, translation movement",
     0, "natural"),

    ("Imam Ahmad ibn Hanbal",
     ["Ahmad ibn Hanbal", "Ibn Hanbal"],
     "B", ["abbasid"], ["The Abbasid: Glory and Rot", "Scholars and Thinkers"],
     "780-855 CE",
     "What does it cost to say no to the most powerful man on earth?",
     "later", None, [], [],
     "Refused to accept the Quran was created, survived the Mihna persecution",
     0, "natural"),

    ("Qutuz",
     ["Saif al-Din Qutuz", "Al-Muzaffar Qutuz"],
     "B", ["mamluk"], ["When Islam Stopped the Mongols"],
     "d. 1260 CE",
     "Can a slave save a civilization — and survive the salvation?",
     "later", None, [], [],
     "Mamluk sultan who defeated Mongols at Ain Jalut, assassinated by Baybars",
     0, "assassinated"),

    ("Ibn Battuta",
     ["Shams al-Din ibn Battuta", "Abu Abdullah ibn Battuta"],
     "B", ["mamluk"], ["African Islam", "Scholars and Thinkers"],
     "1304-1369 CE",
     "Can one man's journey map an entire civilization?",
     "later", None, [], [],
     "Greatest traveler of the medieval world, 75,000 miles across Islamic world",
     0, "natural"),

    ("Mansa Musa",
     ["Mansa Musa I", "Musa I of Mali"],
     "B", ["africa"], ["African Islam"],
     "1280-1337 CE",
     "What happens when the richest man in history walks through the world?",
     "later", None, [], [],
     "Emperor of Mali, hajj crashed gold markets, built Timbuktu",
     0, "natural"),

    ("Askia Muhammad",
     ["Askia the Great", "Muhammad Ture"],
     "B", ["africa"], ["African Islam"],
     "1443-1538 CE",
     "Can a usurper build a more just empire than the dynasty he overthrew?",
     "later", None, [], [],
     "Expanded Songhai Empire, patron of Timbuktu scholarship",
     0, "natural"),

    ("Rumi",
     ["Jalal al-Din Rumi", "Mawlana", "Mevlana"],
     "B", ["abbasid", "mongol"], ["Scholars and Thinkers"],
     "1207-1273 CE",
     "Can love survive the destruction of everything you know?",
     "later", None, [], [],
     "Greatest Sufi poet, wrote Masnavi, founded Mevlevi order",
     0, "natural"),

    ("Ibn Arabi",
     ["Muhyi al-Din ibn Arabi", "Al-Shaykh al-Akbar"],
     "B", ["andalusia", "crusades"], ["The Andalusian Arc", "Scholars and Thinkers"],
     "1165-1240 CE",
     "Is there a unity behind all faiths that only mystics can see?",
     "later", None, [], [],
     "Greatest Sufi metaphysician, Fusus al-Hikam, unity of being",
     0, "natural"),

    ("Babur",
     ["Zahir al-Din Babur"],
     "B", ["south_asia"], ["South Asia"],
     "1483-1530 CE",
     "Can a man who lost everything five times still build an empire?",
     "later", None, [], [],
     "Founded the Mughal Empire, wrote the Baburnama",
     0, "natural"),

    ("Akbar",
     ["Jalal al-Din Akbar", "Akbar the Great"],
     "B", ["south_asia"], ["South Asia"],
     "1542-1605 CE",
     "Can an empire be built on tolerance — or does tolerance require empire?",
     "later", None, [], [],
     "Greatest Mughal emperor, Din-i Ilahi, universal tolerance policy",
     0, "natural"),

    ("Tipu Sultan",
     ["Tiger of Mysore", "Tipu Sahib"],
     "B", ["south_asia", "resistance_colonial"], ["South Asia", "Resistance and Colonialism"],
     "1750-1799 CE",
     "Is it better to live as a jackal or die as a lion?",
     "later", None, [], [],
     "Last major Indian ruler to resist British, died defending Seringapatam",
     0, "battle"),

    ("Suleiman the Magnificent",
     ["Suleiman I", "Kanuni Sultan Suleiman"],
     "B", ["ottoman"], ["Ottoman Empire"],
     "1494-1566 CE",
     "What does it cost to be magnificent?",
     "later", None, [], [],
     "Longest-reigning Ottoman sultan, lawgiver, patron of arts",
     0, "natural"),

    ("Mehmed II",
     ["Mehmed the Conqueror", "Fatih Sultan Mehmed"],
     "B", ["ottoman"], ["Ottoman Empire"],
     "1432-1481 CE",
     "Can one conquest define a civilization for centuries?",
     "later", None, [], [],
     "Conquered Constantinople in 1453, ended the Byzantine Empire",
     0, "natural"),

    ("Imam Shamil",
     ["Shamil", "Sheikh Shamil"],
     "B", ["resistance_colonial"], ["Resistance and Colonialism"],
     "1797-1871 CE",
     "Can faith hold a mountain against an empire?",
     "later", None, [], [],
     "Led 25-year resistance against Russian Empire in the Caucasus",
     0, "natural"),

    ("Emir Abdelkader",
     ["Abdelkader ibn Muhieddine", "Abdelkader"],
     "B", ["resistance_colonial"], ["Resistance and Colonialism"],
     "1808-1883 CE",
     "Can the man who fights colonialism also save its victims?",
     "later", None, [], [],
     "Resisted French in Algeria, saved Christians in Damascus, Sufi scholar",
     0, "natural"),

    ("Muhammad Ahmad al-Mahdi",
     ["The Mahdi", "Muhammad Ahmad"],
     "B", ["resistance_colonial", "africa"], ["Resistance and Colonialism", "African Islam"],
     "1844-1885 CE",
     "When does resistance become revelation?",
     "later", None, [], [],
     "Declared himself Mahdi, defeated British at Khartoum",
     0, "natural"),

    ("Omar Mukhtar",
     ["Lion of the Desert"],
     "B", ["resistance_colonial"], ["Resistance and Colonialism"],
     "1858-1931 CE",
     "We do not surrender. We win or we die.",
     "later", None, [], [],
     "Led Libyan resistance against Italian colonialism for 20 years",
     0, "executed"),

    ("Usman dan Fodio",
     ["Dan Fodio", "Sheikh Uthman"],
     "B", ["africa"], ["African Islam"],
     "1754-1817 CE",
     "How does a scholar become a revolutionary — and what does he owe to the state he builds?",
     "later", None, [], [],
     "Founded the Sokoto Caliphate through jihad, wrote over 100 treatises",
     0, "natural"),

    ("Muhammad Iqbal",
     ["Iqbal", "Allama Iqbal"],
     "B", ["resistance_colonial"], ["Resistance and Colonialism", "Scholars and Thinkers"],
     "1877-1938 CE",
     "Can philosophy rebuild a civilization that colonialism dismantled?",
     "later", None, [], [],
     "Synthesized Rumi and Nietzsche into Islamic philosophy of selfhood",
     0, "natural"),

    ("Ibn Taymiyyah",
     ["Taqi al-Din ibn Taymiyyah"],
     "B", ["mamluk"], ["Scholars and Thinkers", "When Islam Stopped the Mongols"],
     "1263-1328 CE",
     "Can a scholar's pen be more dangerous than an army's sword?",
     "later", None, [], [],
     "Issued fatwa against Mongol converts, imprisoned for his views, shaped reform movements",
     0, "natural"),

    ("Al-Khwarizmi",
     ["Muhammad ibn Musa al-Khwarizmi"],
     "B", ["abbasid"], ["The Abbasid: Glory and Rot", "Scholars and Thinkers"],
     "780-850 CE",
     "Can numbers change the world more than swords?",
     "later", None, [], [],
     "Father of algebra, gave us 'algorithm', revolutionized mathematics",
     0, "natural"),

    ("Ibn Sina",
     ["Avicenna", "Abu Ali ibn Sina"],
     "B", ["abbasid"], ["Scholars and Thinkers"],
     "980-1037 CE",
     "Can one mind master both the body and the soul?",
     "later", None, [], [],
     "Canon of Medicine used for 600 years, philosopher-physician",
     0, "natural"),

    ("Richard I",
     ["Richard the Lionheart", "Richard Coeur de Lion"],
     "B", ["crusades"], ["Crusades: Islamic Perspective"],
     "1157-1199 CE",
     "The enemy Saladin respected — what does that say about both men?",
     "later", None, [], [],
     "English king, Third Crusade, negotiated with Saladin",
     0, "battle"),

    ("Shajarat al-Durr",
     ["Shajar al-Durr"],
     "B", ["mamluk"], ["When Islam Stopped the Mongols", "Women of Islam"],
     "d. 1257 CE",
     "Can a woman rule the Islamic world — and what happens when she tries?",
     "later", None, [], [],
     "Only Muslim queen of Egypt, defeated Crusaders, bridge to Mamluk era",
     0, "assassinated"),

    ("Rabia al-Adawiyya",
     ["Rabia", "Rabia of Basra"],
     "B", ["umayyad", "abbasid"], ["Scholars and Thinkers", "Women of Islam"],
     "717-801 CE",
     "Can love of God be so pure it needs neither fear of hell nor hope of paradise?",
     "later", None, [], [],
     "Founder of the love tradition in Sufism, freed slave who became spiritual master",
     0, "natural"),

    ("Nana Asma'u",
     ["Asma'u bint Usman dan Fodio"],
     "B", ["africa"], ["African Islam", "Women of Islam"],
     "1793-1864 CE",
     "Can a woman's pen unite an empire that her father's sword created?",
     "later", None, [], [],
     "Poet, educator, built women's education network in Sokoto Caliphate",
     0, "natural"),

    ("Zubayda bint Jafar",
     ["Zubayda"],
     "B", ["abbasid"], ["The Abbasid: Glory and Rot", "Women of Islam"],
     "762-831 CE",
     "What does a queen owe to the pilgrims who will never know her name?",
     "later", None, [], [],
     "Abbasid queen, built water infrastructure along hajj route",
     0, "natural"),

    # ── Tier C — Document actions, never simple villain ────────────

    ("Yazid ibn Muawiyah",
     ["Yazid I", "Yazid"],
     "C", ["umayyad"], ["The Umayyad Paradox"],
     "647-683 CE",
     "History's villain — but was there a logic to his choices?",
     "tabi_i", None, [], [],
     "Second Umayyad caliph, ordered actions leading to Karbala",
     0, "natural"),

    ("Al-Hajjaj ibn Yusuf",
     ["Al-Hajjaj", "Hajjaj al-Thaqafi"],
     "C", ["umayyad"], ["The Umayyad Paradox"],
     "661-714 CE",
     "Can terror build order — and at what cost?",
     "tabi_i", None, [], [],
     "Umayyad governor, brutal enforcer, standardized Arabic script",
     0, "natural"),

    ("Aurangzeb",
     ["Alamgir", "Muhi al-Din Aurangzeb"],
     "C", ["south_asia"], ["South Asia"],
     "1618-1707 CE",
     "Did the most pious Mughal destroy the empire his ancestors built?",
     "later", None, [], [],
     "Last great Mughal emperor, expanded then destabilized the empire",
     0, "natural"),

    ("Nadir Shah",
     ["Nadir Shah Afshar"],
     "C", ["south_asia", "persia"], ["South Asia"],
     "1688-1747 CE",
     "What happens when a conqueror has nothing left to conquer?",
     "later", None, [], [],
     "Sacked Delhi, took Peacock Throne, collapsed into madness",
     0, "assassinated"),

    ("Hulagu Khan",
     ["Hulagu"],
     "C", ["mongol"], ["When Islam Stopped the Mongols"],
     "1217-1265 CE",
     "The man who drowned Baghdad in ink and blood",
     "later", None, [], [],
     "Destroyed Baghdad and the Abbasid caliphate in 1258",
     0, "natural"),

    ("Ibn al-Alqami",
     ["Muhammad ibn al-Alqami"],
     "C", ["abbasid", "mongol"], ["The Abbasid: Glory and Rot", "When Islam Stopped the Mongols"],
     "d. 1258 CE",
     "History's most famous traitor — or a man with no options?",
     "later", None, [], [],
     "Last Abbasid vizier, accused of opening Baghdad to Mongols",
     0, "natural"),
]

# ═══════════════════════════════════════════════════════════════════════
# EVENTS — 32+ key events
# ═══════════════════════════════════════════════════════════════════════

EVENTS = [
    # Rashidun era
    ("Battle of Badr", ["Ghazwat Badr"], "624", "2 AH", "Badr, Hejaz", "rashidun",
     "First major Muslim military victory — 313 vs 1000. Defined the community."),
    ("Battle of Uhud", ["Ghazwat Uhud"], "625", "3 AH", "Uhud, near Medina", "rashidun",
     "Near-defeat that tested faith. Hamza martyred. Archers disobeyed."),
    ("Battle of the Trench", ["Ghazwat al-Khandaq", "Battle of Khandaq"], "627", "5 AH", "Medina", "rashidun",
     "Siege of Medina. Salman's trench strategy. Coalition collapsed."),
    ("Treaty of Hudaybiyya", [], "628", "6 AH", "Hudaybiyya, near Mecca", "rashidun",
     "Diplomacy that looked like defeat but opened the conquest."),
    ("Conquest of Mecca", ["Fath Makkah"], "630", "8 AH", "Mecca", "rashidun",
     "Bloodless conquest. General amnesty. Khalid's conversion beforehand."),
    ("Battle of Mu'tah", ["Ghazwat Mu'tah"], "629", "8 AH", "Mu'tah, Jordan", "rashidun",
     "First Muslim-Byzantine clash. Jafar martyred. Khalid's tactical retreat saved the army."),
    ("Death of the Prophet", [], "632", "11 AH", "Medina", "rashidun",
     "The event that created the succession crisis."),
    ("Ridda Wars", ["Wars of Apostasy"], "632-633", "11-12 AH", "Arabia", "rashidun",
     "Abu Bakr reunited Arabia. Khalid commanded. Musaylimah killed."),
    ("Battle of Yarmouk", ["Yarmuk"], "636", "15 AH", "Yarmouk River, Syria", "rashidun",
     "Decisive defeat of Byzantium. Khalid's tactical masterpiece. 6-day battle."),
    ("Battle of Qadisiyyah", ["Qadisiyya"], "636", "15 AH", "Qadisiyyah, Iraq", "rashidun",
     "Destruction of Sassanid field army. Opened Persia to Islam."),
    ("Conquest of Jerusalem", ["Fath al-Quds"], "637", "16 AH", "Jerusalem", "rashidun",
     "Umar received keys personally. Refused to pray in Church of the Holy Sepulchre."),
    ("Dismissal of Khalid", [], "638", "17 AH", "Syria", "rashidun",
     "Umar dismissed Khalid at peak of power. Khalid obeyed without revolt."),
    ("Battle of the Camel", ["Jamal"], "656", "36 AH", "Basra, Iraq", "rashidun",
     "First Muslim civil war battle. Aisha vs Ali. Companions killing companions."),
    ("Battle of Siffin", [], "657", "37 AH", "Siffin, Syria", "rashidun",
     "Ali vs Muawiyah. Qurans raised on spears. Arbitration debacle."),
    ("Assassination of Ali", [], "661", "40 AH", "Kufa, Iraq", "rashidun",
     "Kharijite assassination. End of Rashidun era. Islam's first dynasty begins."),

    # Umayyad era
    ("Battle of Karbala", ["Waq'at Karbala"], "680", "61 AH", "Karbala, Iraq", "umayyad",
     "Husayn martyred with 72 companions. Watershed moment. Sunni-Shia divide crystallized."),
    ("Siege of Mecca (683)", ["Siege by Yazid"], "683", "64 AH", "Mecca", "umayyad",
     "Umayyad army besieged Mecca, Ka'ba damaged by fire."),
    ("Mukhtar's Revolt", [], "685-687", "66-67 AH", "Kufa, Iraq", "umayyad",
     "Mukhtar al-Thaqafi avenged Husayn by killing his murderers."),
    ("Conquest of Iberia", ["Fath al-Andalus"], "711", "92 AH", "Iberian Peninsula", "umayyad",
     "Tariq ibn Ziyad crossed Gibraltar. Visigothic kingdom fell in 2 years."),

    # Abbasid era
    ("Abbasid Revolution", [], "750", "132 AH", "Khorasan to Damascus", "abbasid",
     "Abu Muslim's revolution destroyed the Umayyads. Blood-soaked transition."),
    ("Founding of Baghdad", ["Madinat al-Salam"], "762", "145 AH", "Baghdad, Iraq", "abbasid",
     "Al-Mansur built the Round City. Center of the world for 500 years."),
    ("Fall of Baghdad to Mongols", [], "1258", "656 AH", "Baghdad, Iraq", "abbasid",
     "Hulagu destroyed the Abbasid caliphate. Libraries in the Tigris. Civilization-ending event."),

    # Crusades era
    ("Battle of Hattin", [], "1187", "583 AH", "Horns of Hattin, Palestine", "crusades",
     "Saladin destroyed Crusader army. Jerusalem retaken. True Cross captured."),
    ("Reconquest of Jerusalem", ["Fath al-Quds by Saladin"], "1187", "583 AH", "Jerusalem", "crusades",
     "Saladin showed mercy where Crusaders had massacred. No revenge killings."),

    # Mongol / Mamluk era
    ("Battle of Ain Jalut", ["Ayn Jalut"], "1260", "658 AH", "Ain Jalut, Palestine", "mamluk",
     "First major Mongol defeat. Qutuz and Baybars saved Islamic civilization."),
    ("Assassination of Qutuz", [], "1260", "658 AH", "Sinai, Egypt", "mamluk",
     "Baybars killed Qutuz on return from Ain Jalut. Took the sultanate."),

    # Ottoman era
    ("Fall of Constantinople", ["Fath al-Qustantiniyya"], "1453", "857 AH", "Constantinople", "ottoman",
     "Mehmed II conquered the unconquerable city. End of Byzantine Empire. New era begins."),

    # South Asia
    ("First Battle of Panipat", [], "1526", "932 AH", "Panipat, India", "south_asia",
     "Babur defeated Ibrahim Lodi. Founded the Mughal Empire with 12,000 troops vs 100,000."),
    ("Battle of Seringapatam", [], "1799", "", "Seringapatam, India", "south_asia",
     "Tipu Sultan's last stand. British conquest of southern India secured."),

    # Africa
    ("Mansa Musa's Hajj", [], "1324", "", "Mali to Mecca", "africa",
     "So much gold distributed it crashed markets in Cairo. Put Mali on world map."),
    ("Fall of Timbuktu to Moroccans", [], "1591", "", "Timbuktu", "africa",
     "Moroccan army destroyed Songhai Empire. Scholars scattered. Libraries lost."),

    # Resistance
    ("Battle of Khartoum", [], "1885", "", "Khartoum, Sudan", "resistance_colonial",
     "The Mahdi's forces took Khartoum. Gordon killed. British humiliated."),
]

# ═══════════════════════════════════════════════════════════════════════
# FIGURE-THEME ASSIGNMENTS — Priority figures
# (figure_name, [theme_slugs])
# ═══════════════════════════════════════════════════════════════════════

FIGURE_THEME_MAP = {
    "Khalid ibn Walid": ["loyalty_and_betrayal", "justice_vs_power", "conquest_and_mercy"],
    "Husayn ibn Ali": ["martyrdom", "faith_under_oppression", "justice_vs_power", "loyalty_and_betrayal"],
    "Ali ibn Abi Talib": ["justice_vs_power", "succession_crisis", "loyalty_and_betrayal"],
    "Umar ibn al-Khattab": ["justice_vs_power", "empire_building", "conquest_and_mercy"],
    "Abu Bakr al-Siddiq": ["loyalty_and_betrayal", "succession_crisis", "empire_building"],
    "Muawiyah ibn Abi Sufyan": ["succession_crisis", "empire_building", "loyalty_and_betrayal"],
    "Amr ibn al-As": ["loyalty_and_betrayal", "conquest_and_mercy", "empire_building"],
    "Saladin": ["conquest_and_mercy", "east_west_encounter", "empire_building"],
    "Baybars": ["slave_who_became_king", "the_mentor_betrayal", "empire_building"],
    "Said ibn Jubayr": ["scholar_vs_ruler", "faith_under_oppression", "martyrdom"],
    "Al-Ghazali": ["scholar_vs_ruler", "sufi_inner_outer", "intellectual_legacy"],
    "Ibn Rushd": ["scholar_vs_ruler", "east_west_encounter", "intellectual_legacy"],
    "Ibn Khaldun": ["decline_and_renewal", "intellectual_legacy", "trade_as_civilization"],
    "Bilal ibn Rabah": ["faith_under_oppression", "slave_who_became_king"],
    "Omar Mukhtar": ["the_last_stand", "faith_under_oppression", "identity_under_occupation"],
    "Rumi": ["sufi_inner_outer", "intellectual_legacy"],
    "Mansa Musa": ["trade_as_civilization", "empire_building", "diaspora_faith"],
    "Shajarat al-Durr": ["woman_in_power", "the_last_stand"],
    "Rabia al-Adawiyya": ["sufi_inner_outer", "woman_in_power"],
    "Nana Asma'u": ["woman_in_power", "knowledge_as_resistance"],
    "Aisha bint Abi Bakr": ["woman_in_power", "succession_crisis", "intellectual_legacy"],
    "Abu Ubayda ibn al-Jarrah": ["loyalty_and_betrayal", "conquest_and_mercy"],
    "Tariq ibn Ziyad": ["conquest_and_mercy", "slave_who_became_king"],
    "Nur ad-Din Zengi": ["the_mentor_betrayal", "empire_building"],
    "Imam Shamil": ["the_last_stand", "faith_under_oppression", "identity_under_occupation"],
    "Emir Abdelkader": ["faith_under_oppression", "identity_under_occupation", "east_west_encounter"],
    "Qutuz": ["slave_who_became_king", "the_last_stand"],
    "Umar ibn Abd al-Aziz": ["justice_vs_power", "decline_and_renewal"],
    "Abu Muslim al-Khurasani": ["the_mentor_betrayal", "loyalty_and_betrayal"],
    "Yazid ibn Muawiyah": ["succession_crisis", "justice_vs_power"],
    "Al-Hajjaj ibn Yusuf": ["justice_vs_power", "scholar_vs_ruler"],
}

# ═══════════════════════════════════════════════════════════════════════
# FIGURE MOTIVATIONS — Priority entries
# (figure_name, motivation, is_primary, conflicts_with, evidence)
# ═══════════════════════════════════════════════════════════════════════

MOTIVATIONS = [
    ("Khalid ibn Walid", "LOYALTY", True, "JUSTICE",
     "Obeyed Umar's dismissal without revolt despite being history's greatest general"),
    ("Khalid ibn Walid", "JUSTICE", False, "LOYALTY",
     "Blood money incident — Khalid executed Malik ibn Nuwayra, Abu Bakr rebuked him"),
    ("Husayn ibn Ali", "FAITH", True, "SURVIVAL",
     "Chose martyrdom at Karbala over giving bayah to Yazid"),
    ("Husayn ibn Ali", "JUSTICE", True, None,
     "Refused to legitimize what he saw as illegitimate rule"),
    ("Amr ibn al-As", "PRAGMATISM", True, "LOYALTY",
     "Switched sides at Siffin with the Quran-on-spears tactic, securing Egypt for Muawiyah"),
    ("Amr ibn al-As", "POWER", False, None,
     "Demanded governorship of Egypt as price for supporting Muawiyah"),
    ("Baybars", "POWER", True, "IDEOLOGY",
     "Assassinated Qutuz to seize the sultanate immediately after Ain Jalut"),
    ("Baybars", "FAITH", False, None,
     "Genuinely defended Islam against Mongols and Crusaders, built mosques and madrasas"),
    ("Said ibn Jubayr", "JUSTICE", True, "SURVIVAL",
     "Refused to betray Ibn Ash'ath to Al-Hajjaj, knowing it meant death"),
    ("Muawiyah ibn Abi Sufyan", "PRAGMATISM", True, "LOYALTY",
     "Refused bayah to Ali, claiming Uthman's blood unpunished"),
    ("Muawiyah ibn Abi Sufyan", "POWER", True, None,
     "Built dynastic succession — first to break the elective caliphate model"),
    ("Ali ibn Abi Talib", "JUSTICE", True, "PRAGMATISM",
     "Refused to keep Muawiyah as governor despite political cost"),
    ("Abu Ubayda ibn al-Jarrah", "LOYALTY", True, None,
     "Accepted command over Khalid without ego, maintained unity"),
    ("Abu Bakr al-Siddiq", "FAITH", True, None,
     "Launched Ridda Wars to preserve the community's unity after Prophet's death"),
    ("Umar ibn al-Khattab", "JUSTICE", True, "POWER",
     "Dismissed Khalid to prove no one is above accountability"),
    ("Saladin", "FAITH", True, None,
     "Died with almost no personal wealth despite ruling an empire"),
    ("Saladin", "LEGACY", False, None,
     "Showed mercy at Jerusalem specifically contrasting Crusader massacre"),
    ("Omar Mukhtar", "FAITH", True, "SURVIVAL",
     "Fought until captured at 73, refused to negotiate surrender"),
    ("Al-Ghazali", "KNOWLEDGE", True, "POWER",
     "Abandoned the most prestigious academic position in the Islamic world for spiritual search"),
]

# ═══════════════════════════════════════════════════════════════════════
# FIGURE DEATHS — Priority entries
# (figure_name, circumstance, last_words, last_words_source, witnesses, location, date_ce, source)
# ═══════════════════════════════════════════════════════════════════════

DEATHS = [
    ("Khalid ibn Walid",
     "Died in bed in Homs after being dismissed from command. Reportedly bitter that he died of natural causes.",
     "I have fought in so many battles seeking martyrdom that there is no spot on my body left without a scar or a wound made by a spear or sword. And yet here I am, dying on my bed like an old camel. May the eyes of the cowards never rest.",
     "Various — reliability disputed. Attributed in Al-Tabari and later sources.",
     ["Family members in Homs"], "Homs, Syria", "642 CE", "Al-Tabari, Ibn Sa'd"),

    ("Husayn ibn Ali",
     "Martyred at Karbala with 72 companions against thousands of Umayyad troops under Ibn Ziyad's command. Denied water for days.",
     "If the religion of Muhammad cannot survive except by my death, then O swords, take me.",
     "Various Shia and Sunni sources — wording varies significantly between traditions.",
     ["Zaynab bint Ali", "Ali ibn Husayn (Zayn al-Abidin)"], "Karbala, Iraq", "680 CE",
     "Al-Tabari, Abu Mikhnaf"),

    ("Umar ibn al-Khattab",
     "Assassinated during Fajr prayer by Abu Lu'lu'a, a Persian slave with a grievance.",
     "O young man, your face is the face of one who does not lie. Go to Aisha and ask if Umar may be buried beside the Prophet.",
     "Al-Tabari, Ibn Sa'd — well-attested in multiple sources.",
     ["Companions in the mosque"], "Medina", "644 CE", "Al-Tabari, Ibn Sa'd"),

    ("Uthman ibn Affan",
     "Killed by rebels in his own home while reading the Quran. Refused to allow his guards to fight.",
     "O God, You are sufficient for me against them.",
     "Al-Tabari, Al-Baladhuri — multiple chains.",
     ["His wife Na'ila who lost fingers defending him"], "Medina", "656 CE", "Al-Tabari"),

    ("Ali ibn Abi Talib",
     "Struck by Ibn Muljam's poisoned sword during Fajr prayer in Kufa mosque.",
     "By the Lord of the Ka'ba, I have succeeded.",
     "Multiple sources — widely attested. Consistent across Sunni and Shia traditions.",
     ["His sons Hassan and Husayn"], "Kufa, Iraq", "661 CE", "Al-Tabari, Ibn Sa'd"),

    ("Said ibn Jubayr",
     "Executed by Al-Hajjaj ibn Yusuf for supporting Ibn al-Ash'ath's revolt. Refused to recant.",
     "O God, do not let him kill anyone after me.",
     "Al-Tabari — Al-Hajjaj reportedly died shortly after, and sources note the connection.",
     ["Court of Al-Hajjaj"], "Wasit, Iraq", "714 CE", "Al-Tabari"),

    ("Saladin",
     "Died of fever in Damascus. Had given away nearly all his wealth. Not enough money for his burial.",
     "There is no God but God. He had no specific last words recorded, but his minister Al-Fadil was present.",
     "Baha al-Din ibn Shaddad (his biographer, eyewitness).",
     ["Baha al-Din", "Al-Fadil"], "Damascus, Syria", "1193 CE", "Baha al-Din, Ibn al-Athir"),

    ("Baybars",
     "Reportedly died from drinking poisoned kumiss intended for someone else. Some sources dispute this.",
     "No recorded last words.",
     "Al-Maqrizi — circumstances disputed across sources.",
     [], "Damascus, Syria", "1277 CE", "Al-Maqrizi"),

    ("Omar Mukhtar",
     "Captured by Italian forces, tried in a show trial, hanged before 20,000 forced Libyan spectators.",
     "To God we belong and to God we shall return.",
     "Italian colonial records and oral tradition.",
     ["20,000 forced spectators at Suluq"], "Suluq, Libya", "1931 CE",
     "Italian military records"),

    ("Qutuz",
     "Assassinated by Baybars and fellow Mamluk officers while returning from Ain Jalut victory.",
     "No recorded last words.",
     "Al-Maqrizi, Ibn Taghribirdi.",
     ["Baybars and conspirators"], "Sinai", "1260 CE", "Al-Maqrizi"),

    ("Abu Ubayda ibn al-Jarrah",
     "Died in the Amwas plague. Refused to flee the plague citing the Prophet's instructions.",
     "O people, this plague is a mercy from your Lord, the prayer of your Prophet, and the death of the righteous before you.",
     "Al-Tabari, Ibn Sa'd — well-attested.",
     ["Mu'adh ibn Jabal (who died the same plague)"], "Amwas, Palestine", "639 CE", "Al-Tabari"),

    ("Hamza ibn Abd al-Muttalib",
     "Martyred at the Battle of Uhud. Killed by Wahshi ibn Harb with a javelin. Body mutilated by Hind bint Utba.",
     "No recorded last words — died in battle.",
     "Ibn Hisham, Al-Tabari.",
     ["Companions at Uhud"], "Uhud, near Medina", "625 CE", "Ibn Hisham"),

    ("Tipu Sultan",
     "Killed fighting at the breach of Seringapatam. Refused to retreat or negotiate.",
     "To live like a lion for a day is far better than to live like a jackal for a hundred years.",
     "Attributed — exact circumstances and wording disputed. British and Indian sources vary.",
     ["British soldiers, Tipu's courtiers"], "Seringapatam, India", "1799 CE",
     "British military records, Indian oral tradition"),
]

# ═══════════════════════════════════════════════════════════════════════
# FIGURE QUOTES — Priority pilot entries
# (figure_name, quote, context, chain_strength, source, use_in_script)
# ═══════════════════════════════════════════════════════════════════════

QUOTES = [
    ("Khalid ibn Walid",
     "I have fought in so many battles seeking martyrdom that there is no spot on my body left without a scar.",
     "Reportedly said on his deathbed in Homs",
     "daif", "Various later compilations",
     "PILOT — deathbed scene, episode cold close"),

    ("Khalid ibn Walid",
     "I am the Sword of God, drawn against the unbelievers.",
     "Battle cry attributed to Khalid",
     "hasan", "Al-Tabari, Al-Waqidi",
     "PILOT — battle establishing shot"),

    ("Umar ibn al-Khattab",
     "If a lamb were to die on the banks of the Euphrates, I would fear that God would ask me about it.",
     "Statement about responsibility of the caliph",
     "hasan", "Various hadith compilations",
     "Justice theme — used when discussing Umar's governance philosophy"),

    ("Umar ibn al-Khattab",
     "When did you enslave people when their mothers bore them free?",
     "Rebuking Amr ibn al-As's son for beating a Coptic man",
     "hasan", "Various — widely cited but exact chain debated",
     "Key moment for justice_vs_power theme"),

    ("Husayn ibn Ali",
     "Death with dignity is better than a life of humiliation.",
     "Attributed before or during Karbala",
     "unknown", "Various Shia and Sunni sources — wording varies",
     "Karbala episode — establishes refusal to submit"),

    ("Ali ibn Abi Talib",
     "People are of two types: they are either your brothers in faith or your equals in humanity.",
     "From letter to Malik al-Ashtar, governor of Egypt",
     "scholarly", "Nahj al-Balagha — authenticity debated but widely accepted",
     "Governance theme — interfaith relations"),

    ("Saladin",
     "I warn you against shedding blood, indulging in it and making a habit of it, for blood never sleeps.",
     "Advice to his son, reported by Baha al-Din",
     "scholarly", "Baha al-Din ibn Shaddad",
     "Conquest and mercy theme — restraint after victory"),

    ("Said ibn Jubayr",
     "I only helped him because he was fighting injustice.",
     "Explaining his support for Ibn al-Ash'ath's revolt to Al-Hajjaj",
     "hasan", "Al-Tabari",
     "Scholar vs ruler episode — the moment of defiance"),

    ("Omar Mukhtar",
     "We do not surrender. We win or we die.",
     "Reportedly said during Italian interrogation",
     "unknown", "Oral tradition, Italian records reference defiance",
     "Last stand episode — closing line"),

    ("Abu Bakr al-Siddiq",
     "If Muhammad is worshipped, then Muhammad is dead. But if it is God who is worshipped, then God is alive and never dies.",
     "Calming the companions after the Prophet's death",
     "sahih", "Al-Bukhari, Al-Tabari",
     "Succession crisis — the moment leadership transfers"),

    ("Baybars",
     "No attributed direct quotes with reliable chains survive.",
     "Actions documented extensively but personal speech rarely preserved",
     "unknown", "Al-Maqrizi notes his deeds but few direct quotations",
     "NOTE: Use narration over dialogue for Baybars episodes"),
]

# ═══════════════════════════════════════════════════════════════════════
# FIGURE LINEAGE — Priority entries
# (figure_name, related_name, lineage_type, direction, divergence, notes)
# ═══════════════════════════════════════════════════════════════════════

LINEAGE = [
    ("Khalid ibn Walid", "Prophet Muhammad (PBUH)", "MILITARY_PATRON", "ancestor", None,
     "The Prophet named him 'Sword of God' after Mu'tah"),
    ("Khalid ibn Walid", "Abu Bakr al-Siddiq", "MILITARY_PATRON", "ancestor", None,
     "Abu Bakr gave Khalid supreme command in Ridda Wars and Syria"),
    ("Khalid ibn Walid", "Baybars", "MILITARY_PATRON", "ancestor", "SURPASSED",
     "Parallel: both undefeated commanders, both faced political tension with civilian authority"),
    ("Baybars", "Qutuz", "MILITARY_PATRON", "ancestor", "BETRAYED",
     "Qutuz was sultan, Baybars his general — then Baybars killed him"),
    ("Saladin", "Nur ad-Din Zengi", "MILITARY_PATRON", "ancestor", "SURPASSED",
     "Nur ad-Din sent Saladin to Egypt — Saladin became greater than his master"),
    ("Husayn ibn Ali", "Ali ibn Abi Talib", "BIOLOGICAL", "ancestor", "MARTYRED",
     "Son of Ali, grandson of the Prophet"),
    ("Hassan ibn Ali", "Ali ibn Abi Talib", "BIOLOGICAL", "ancestor", "COMPLETED",
     "Chose peace where his father chose war — completed the cycle differently"),
    ("Al-Ghazali", "Ibn Rushd", "INTELLECTUAL", "ancestor", "BETRAYED",
     "Ghazali's Tahafut al-Falasifa challenged philosophy; Ibn Rushd's Tahafut al-Tahafut responded a century later"),
    ("Umar ibn Abd al-Aziz", "Umar ibn al-Khattab", "POLITICAL_HEIR", "ancestor", "COMPLETED",
     "Called the fifth righteous caliph — tried to return Umayyad rule to Rashidun ideals"),
    ("Abu Muslim al-Khurasani", "Abbasid caliphs", "POLITICAL_HEIR", "ancestor", "BETRAYED",
     "Made the revolution possible, then was murdered by the dynasty he created"),
    ("Fatimah bint Muhammad", "Prophet Muhammad (PBUH)", "BIOLOGICAL", "ancestor", None,
     "Daughter of the Prophet, mother of the Alid line"),
    ("Zaynab bint Ali", "Ali ibn Abi Talib", "BIOLOGICAL", "ancestor", None,
     "Daughter of Ali, witnessed Karbala, spoke to power"),
    ("Nana Asma'u", "Usman dan Fodio", "BIOLOGICAL", "ancestor", "COMPLETED",
     "Continued her father's educational mission through women's networks"),
    ("Rumi", "Shams-i Tabrizi", "SUFI_SILSILA", "ancestor", None,
     "Shams transformed Rumi from scholar to mystic poet"),
    ("Emir Abdelkader", "Ibn Arabi", "SUFI_SILSILA", "ancestor", None,
     "Abdelkader was a devoted student of Ibn Arabi's works"),
]

# ═══════════════════════════════════════════════════════════════════════
# FIGURE RELATIONSHIPS — Priority entries
# (figure_a, figure_b, relationship, description, resolution)
# ═══════════════════════════════════════════════════════════════════════

RELATIONSHIPS = [
    ("Khalid ibn Walid", "Abu Ubayda ibn al-Jarrah", "ALLY",
     "Khalid commanded, Abu Ubayda replaced him — evolved into quiet command hierarchy tension. Abu Ubayda's humility vs Khalid's glory.",
     "DEATH_ENDED_IT"),
    ("Khalid ibn Walid", "Umar ibn al-Khattab", "POLITICAL_OPPONENT",
     "Umar dismissed Khalid mid-campaign. Not personal hatred — Umar feared hero worship would corrupt the community.",
     "VICTORY_A"),
    ("Husayn ibn Ali", "Yazid ibn Muawiyah", "ANTAGONIST",
     "Direct opposition. Husayn refused bayah. Yazid's forces killed him at Karbala.",
     "DEATH_ENDED_IT"),
    ("Hassan ibn Ali", "Muawiyah ibn Abi Sufyan", "POLITICAL_OPPONENT",
     "Hassan abdicated rather than fight — pragmatic peace over principled war.",
     "RECONCILED"),
    ("Ali ibn Abi Talib", "Muawiyah ibn Abi Sufyan", "POLITICAL_OPPONENT",
     "First fitna. Ali the legitimate caliph, Muawiyah the rebel governor. Siffin arbitration failed.",
     "UNRESOLVED"),
    ("Saladin", "Richard I", "MUTUAL_RESPECT",
     "Enemies who admired each other. Saladin sent ice and a horse when Richard was sick.",
     "RECONCILED"),
    ("Saladin", "Nur ad-Din Zengi", "RIVAL",
     "Protégé who outgrew the master. Nur ad-Din grew suspicious. Death resolved it before open conflict.",
     "DEATH_ENDED_IT"),
    ("Baybars", "Qutuz", "RIVAL",
     "Allied to defeat Mongols at Ain Jalut. Baybars killed Qutuz on the return march for the sultanate.",
     "VICTORY_A"),
    ("Al-Ghazali", "Ibn Rushd", "IDEOLOGICAL_OPPONENT",
     "Ghazali attacked philosophy in Tahafut al-Falasifa. Ibn Rushd defended it in Tahafut al-Tahafut. Across a century.",
     "UNRESOLVED"),
    ("Khalid ibn Walid", "Baybars", "PARALLEL",
     "Same dilemma, 600 years apart: supreme military genius under political authority that feared their power.",
     "TRANSCENDED"),
    ("Said ibn Jubayr", "Al-Hajjaj ibn Yusuf", "ANTAGONIST",
     "Scholar vs tyrant. Said refused to submit. Al-Hajjaj executed him — then reportedly died soon after.",
     "DEATH_ENDED_IT"),
    ("Abu Bakr al-Siddiq", "Ali ibn Abi Talib", "POLITICAL_OPPONENT",
     "Succession dispute after Prophet's death. Fatimah's anger at Abu Bakr over Fadak. Reconciled later — sources disagree on timing.",
     "RECONCILED"),
    ("Umar ibn al-Khattab", "Amr ibn al-As", "ALLY",
     "Umar appointed Amr governor of Egypt but kept him on a tight leash. Tension over wealth accumulation.",
     "DEATH_ENDED_IT"),
    ("Imam Ahmad ibn Hanbal", "Al-Ma'mun", "ANTAGONIST",
     "Al-Ma'mun imposed Mu'tazila creed. Ahmad refused to accept the Quran was created. The Mihna persecution.",
     "VICTORY_A"),
    ("Imam Shamil", "Omar Mukhtar", "PARALLEL",
     "Both fought colonial empires from mountain/desert strongholds. Both became legends of resistance.",
     "TRANSCENDED"),
]

# ═══════════════════════════════════════════════════════════════════════
# SCHOLARLY DEBATES — Pilot-relevant entries
# (topic, event_name, figure_name, position_a, position_b, key_scholars, script_instruction)
# ═══════════════════════════════════════════════════════════════════════

SCHOLARLY_DEBATES = [
    ("Reason for Khalid's dismissal by Umar",
     "Dismissal of Khalid", "Khalid ibn Walid",
     "Umar dismissed Khalid over the blood money incident and unauthorized distribution of wealth",
     "Umar feared the army worshipped Khalid rather than God — removal was about principle, not personal grudge",
     ["Al-Tabari", "Al-Baladhuri", "Ibn Sa'd", "Al-Waqidi"],
     "PRESENT BOTH. Do NOT resolve. Let the audience sit with the ambiguity."),

    ("Did Khalid murder Malik ibn Nuwayra?",
     "Ridda Wars", "Khalid ibn Walid",
     "Khalid executed Malik as an apostate and married his wife — improper and potentially criminal",
     "Malik had withheld zakat and resisted; execution was within wartime authority. Marriage was after waiting period.",
     ["Al-Tabari", "Ibn Sa'd"],
     "PRESENT BOTH accounts. Abu Bakr's rebuke is well-attested. Umar's fury is documented. Let the facts speak."),

    ("Husayn's decision to go to Karbala",
     "Battle of Karbala", "Husayn ibn Ali",
     "Husayn was invited by Kufa and had legitimate expectations of support that were betrayed",
     "Husayn knew the odds and chose martyrdom deliberately as a statement against injustice",
     ["Al-Tabari", "Abu Mikhnaf"],
     "PRESENT BOTH Sunni and Shia accounts. Never resolve. This is the most sensitive topic in the series."),

    ("Ali's right to the caliphate after the Prophet",
     "Death of the Prophet", "Ali ibn Abi Talib",
     "Ali was designated by the Prophet at Ghadir Khumm and should have been first caliph (Shia view)",
     "The companions chose Abu Bakr through shura/consultation, which was legitimate (Sunni view)",
     ["Al-Tabari", "Ibn Hisham", "Ibn Sa'd"],
     "PRESENT BOTH with equal weight. Acknowledge this as the foundational Sunni-Shia divergence. Do not take sides."),

    ("Was Muawiyah a legitimate ruler or a rebel?",
     "Battle of Siffin", "Muawiyah ibn Abi Sufyan",
     "Muawiyah was a rebel (baghi) against the legitimate caliph Ali",
     "Muawiyah was exercising ijtihad in demanding justice for Uthman's blood before giving bayah",
     ["Al-Tabari", "Al-Baladhuri", "Ibn Khaldun"],
     "PRESENT BOTH. Note that even scholars who call Muawiyah's position wrong acknowledge his political genius."),

    ("Saladin's treatment of Crusader prisoners",
     "Reconquest of Jerusalem", "Saladin",
     "Saladin showed unprecedented mercy, fulfilling Islamic principles of war",
     "Saladin's mercy was also strategic — ransom revenue and propaganda value against Crusader brutality",
     ["Baha al-Din", "Ibn al-Athir", "William of Tyre"],
     "Both can be true simultaneously. Present mercy as genuine AND strategic."),

    ("Was Baybars justified in killing Qutuz?",
     "Assassination of Qutuz", "Baybars",
     "Baybars was promised Aleppo as reward for Ain Jalut; Qutuz reneged; betrayal justified response",
     "Nothing justifies assassination of a sultan who just saved Islamic civilization from Mongols",
     ["Al-Maqrizi", "Ibn Taghribirdi"],
     "PRESENT BOTH. The audience should feel the moral weight. This is the slave_who_became_king theme at its darkest."),
]


# ═══════════════════════════════════════════════════════════════════════
# SEEDER FUNCTION
# ═══════════════════════════════════════════════════════════════════════

def seed_all(db_url: str = None):
    url = db_url or os.getenv("ISLAM_STORIES_DB_URL")
    if not url:
        print("ERROR: ISLAM_STORIES_DB_URL not set")
        sys.exit(1)

    conn = psycopg2.connect(url)
    conn.autocommit = True
    cur = conn.cursor()

    # ── Themes ──────────────────────────────────────────────────────
    print("Seeding themes...")
    for slug, name, desc in THEMES:
        cur.execute("""
            INSERT INTO themes (slug, name, description)
            VALUES (%s, %s, %s)
            ON CONFLICT (slug) DO NOTHING
        """, (slug, name, desc))
    print(f"  ✓ {len(THEMES)} themes")

    # ── Figures ─────────────────────────────────────────────────────
    print("Seeding figures...")
    for f in FIGURES:
        (name, variants, tier, eras, series, birth_death,
         dramatic_q, gen, tabaqat, sahabi_cats,
         bayah, known_for, hadith_count, death_circ) = f
        cur.execute("""
            INSERT INTO figures (
                name, name_variants, sensitivity_tier, era, series,
                birth_death, dramatic_question, generation, tabaqat_volume,
                sahabi_categories, bayah_pledges, known_for,
                primary_hadith_count, death_circumstance
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (name) DO NOTHING
        """, (name, variants, tier, eras, series, birth_death,
              dramatic_q, gen, tabaqat, sahabi_cats,
              bayah, known_for, hadith_count, death_circ))
    print(f"  ✓ {len(FIGURES)} figures")

    # ── Events ──────────────────────────────────────────────────────
    print("Seeding events...")
    for e in EVENTS:
        name, variants, date_ce, date_ah, location, era, significance = e
        cur.execute("""
            INSERT INTO events (name, name_variants, date_ce, date_ah, location, era, significance)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (name) DO NOTHING
        """, (name, variants, date_ce, date_ah, location, era, significance))
    print(f"  ✓ {len(EVENTS)} events")

    # Build lookup maps
    cur.execute("SELECT id, name FROM figures")
    fig_map = {name: fid for fid, name in cur.fetchall()}

    cur.execute("SELECT id, slug FROM themes")
    theme_map = {slug: tid for tid, slug in cur.fetchall()}

    cur.execute("SELECT id, name FROM events")
    event_map = {name: eid for eid, name in cur.fetchall()}

    # ── Link events to figure_ids ───────────────────────────────────
    print("Linking events to figures...")
    event_figure_map = {
        "Battle of Badr": ["Khalid ibn Walid", "Hamza ibn Abd al-Muttalib", "Bilal ibn Rabah"],
        "Battle of Uhud": ["Hamza ibn Abd al-Muttalib"],
        "Battle of Mu'tah": ["Khalid ibn Walid", "Jafar ibn Abi Talib"],
        "Conquest of Mecca": ["Khalid ibn Walid"],
        "Ridda Wars": ["Khalid ibn Walid", "Abu Bakr al-Siddiq"],
        "Battle of Yarmouk": ["Khalid ibn Walid", "Abu Ubayda ibn al-Jarrah"],
        "Battle of Qadisiyyah": [],
        "Conquest of Jerusalem": ["Umar ibn al-Khattab"],
        "Dismissal of Khalid": ["Khalid ibn Walid", "Umar ibn al-Khattab", "Abu Ubayda ibn al-Jarrah"],
        "Battle of the Camel": ["Ali ibn Abi Talib", "Aisha bint Abi Bakr"],
        "Battle of Siffin": ["Ali ibn Abi Talib", "Muawiyah ibn Abi Sufyan", "Amr ibn al-As"],
        "Assassination of Ali": ["Ali ibn Abi Talib"],
        "Battle of Karbala": ["Husayn ibn Ali", "Zaynab bint Ali", "Yazid ibn Muawiyah"],
        "Mukhtar's Revolt": ["Mukhtar al-Thaqafi"],
        "Conquest of Iberia": ["Tariq ibn Ziyad", "Musa ibn Nusayr"],
        "Battle of Hattin": ["Saladin"],
        "Reconquest of Jerusalem": ["Saladin"],
        "Battle of Ain Jalut": ["Baybars", "Qutuz"],
        "Assassination of Qutuz": ["Baybars", "Qutuz"],
        "Fall of Constantinople": ["Mehmed II"],
        "First Battle of Panipat": ["Babur"],
        "Battle of Seringapatam": ["Tipu Sultan"],
        "Mansa Musa's Hajj": ["Mansa Musa"],
        "Battle of Khartoum": ["Muhammad Ahmad al-Mahdi"],
    }
    for event_name, figure_names in event_figure_map.items():
        if event_name in event_map:
            fids = [fig_map[fn] for fn in figure_names if fn in fig_map]
            if fids:
                cur.execute("UPDATE events SET figure_ids = %s WHERE id = %s",
                            (fids, event_map[event_name]))
    print("  ✓ event-figure links")

    # ── Figure-Theme assignments ────────────────────────────────────
    print("Seeding figure-theme assignments...")
    ft_count = 0
    for fig_name, theme_slugs in FIGURE_THEME_MAP.items():
        if fig_name not in fig_map:
            continue
        for slug in theme_slugs:
            if slug not in theme_map:
                continue
            cur.execute("""
                INSERT INTO figure_themes (figure_id, theme_id)
                VALUES (%s, %s)
                ON CONFLICT (figure_id, theme_id) DO NOTHING
            """, (fig_map[fig_name], theme_map[slug]))
            ft_count += 1
    print(f"  ✓ {ft_count} figure-theme links")

    # ── Motivations ─────────────────────────────────────────────────
    print("Seeding motivations...")
    mot_count = 0
    for fig_name, motivation, is_primary, conflicts, evidence in MOTIVATIONS:
        if fig_name not in fig_map:
            continue
        cur.execute("""
            INSERT INTO figure_motivations (figure_id, motivation, is_primary, conflicts_with, evidence)
            VALUES (%s, %s, %s, %s, %s)
        """, (fig_map[fig_name], motivation, is_primary, conflicts, evidence))
        mot_count += 1
    print(f"  ✓ {mot_count} motivations")

    # ── Deaths ──────────────────────────────────────────────────────
    print("Seeding deaths...")
    death_count = 0
    for d in DEATHS:
        fig_name, circ, last_words, lw_source, witnesses, location, date_ce, source = d
        if fig_name not in fig_map:
            continue
        cur.execute("""
            INSERT INTO figure_deaths (figure_id, circumstance, last_words, last_words_source,
                                       witnesses, location, date_ce, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (figure_id) DO NOTHING
        """, (fig_map[fig_name], circ, last_words, lw_source, witnesses, location, date_ce, source))
        death_count += 1
    print(f"  ✓ {death_count} deaths")

    # ── Quotes ──────────────────────────────────────────────────────
    print("Seeding quotes...")
    q_count = 0
    for fig_name, quote, context, strength, source, use in QUOTES:
        if fig_name not in fig_map:
            continue
        cur.execute("""
            INSERT INTO figure_quotes (figure_id, quote, context, chain_strength, source, use_in_script)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (fig_map[fig_name], quote, context, strength, source, use))
        q_count += 1
    print(f"  ✓ {q_count} quotes")

    # ── Lineage ─────────────────────────────────────────────────────
    print("Seeding lineage...")
    lin_count = 0
    for fig_name, rel_name, ltype, direction, divergence, notes in LINEAGE:
        fig_id = fig_map.get(fig_name)
        rel_id = fig_map.get(rel_name)
        if not fig_id:
            continue
        cur.execute("""
            INSERT INTO figure_lineage (figure_id, related_id, related_name, lineage_type,
                                        direction, divergence, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (fig_id, rel_id, rel_name, ltype, direction, divergence, notes))
        lin_count += 1
    print(f"  ✓ {lin_count} lineage entries")

    # ── Relationships ───────────────────────────────────────────────
    print("Seeding relationships...")
    rel_count = 0
    for fig_a, fig_b, rtype, desc, resolution in RELATIONSHIPS:
        a_id = fig_map.get(fig_a)
        b_id = fig_map.get(fig_b)
        if not a_id or not b_id:
            continue
        cur.execute("""
            INSERT INTO figure_relationships (figure_a_id, figure_b_id, relationship, description, resolution)
            VALUES (%s, %s, %s, %s, %s)
        """, (a_id, b_id, rtype, desc, resolution))
        rel_count += 1
    print(f"  ✓ {rel_count} relationships")

    # ── Scholarly debates ───────────────────────────────────────────
    print("Seeding scholarly debates...")
    sd_count = 0
    for topic, event_name, fig_name, pos_a, pos_b, scholars, instruction in SCHOLARLY_DEBATES:
        eid = event_map.get(event_name)
        fid = fig_map.get(fig_name)
        cur.execute("""
            INSERT INTO scholarly_debates (topic, event_id, figure_id, position_a, position_b,
                                           key_scholars, script_instruction)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (topic, eid, fid, pos_a, pos_b, scholars, instruction))
        sd_count += 1
    print(f"  ✓ {sd_count} scholarly debates")

    cur.close()
    conn.close()
    print("\nSeed complete.")


if __name__ == "__main__":
    seed_all()
