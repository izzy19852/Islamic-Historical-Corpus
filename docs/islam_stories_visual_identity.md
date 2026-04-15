# Islam Stories — Visual Identity
**The locked visual grammar for all production**
**READ BY:** visual_brief.py + every Kling prompt generator

---

## The Single Style Rule

One painterly dark expressionist register across all scene types.
No switching. No mixing with photorealistic footage.
No clean CGI. No modern lighting.

**Aesthetic DNA:**
- **Persian miniature:** jewel tones, gold accents, flat depth
- **Arcane (Fortiche):** painterly, emotionally expressive, chiaroscuro
- **Islamic manuscript illumination:** parchment, calligraphy, ink bleed
- **Fall of Civilizations:** atmospheric, slow, civilizational weight

---

## THE SIX SCENE TYPES

### Type 1: ESTABLISHING (kling_establishing)
**Purpose:** Geography, city, landscape, era opening
**Register:** Fall of Civilizations — atmospheric, no figures
**Duration:** 30–90 seconds
**Camera:** Slow pan, aerial, wide

**Kling prompt template:**
> "Painterly aerial establishing shot. [LOCATION] in [ERA] CE.
> [PALETTE] warm light. No modern infrastructure. No figures.
> Atmospheric haze. Persian miniature color palette.
> Brushstroke texture visible. Meditative pace."

### Type 2: TACTICAL MAP (ai_map)
**Purpose:** Battle overview, troop movements, terrain
**Register:** Kings and Generals — parchment aesthetic
**Duration:** 60–180 seconds (animated)

**Kling prompt template:**
> "Aged parchment map. Hand-drawn cartography style.
> [BATTLE NAME] terrain. [ARMY A] in [COLOR1].
> [ARMY B] in [COLOR2]. Animated arrows showing movement.
> No digital precision — organic, hand-drawn quality.
> Warm candlelight on parchment surface."

### Type 3: CHARACTER / DRAMA (kling_narrative)
**Purpose:** Confrontations, decisions, character moments
**Register:** Arcane — single light source, expressionist
**Duration:** 30–120 seconds

**Kling prompt template:**
> "Cinematic historical drama. [REGION_PALETTE] lighting.
> Stylized painterly figures, elongated proportions.
> Single [LIGHT_SOURCE] light source. Deep shadow.
> [CHARACTER DESCRIPTION] in [SETTING].
> [ACTION/EMOTION]. Arcane visual register.
> Hand-painted texture. No photorealism."

**TIER RULES FOR CHARACTER SCENES:**
- **Tier S:** NEVER show a figure. Show their effect on others.
  Empty seat. Turned back. Reactions of those around them.
- **Tier A:** Show figure from behind, in shadow, or hands/silhouette.
  Never full face. Never direct gaze at camera.
- **Tier B/C:** Full depiction. Stylized, not photorealistic.

### Type 4: BATTLE / ACTION (kling_battle)
**Purpose:** Combat sequences, chaos, physical conflict
**Register:** Atmospheric cinematic — implied violence, no gore
**Duration:** 20–90 seconds

**KLING CONTENT POLICY NOTE:**
Direct depictions of combat, gore, or graphic violence trigger
Kling's content filters. All battle scenes use the cinematic
workaround: wide establishing shots, aftermath, silhouettes,
dust and smoke, weapons and armor without bodies in contact.
The violence is implied, not shown. The scale is felt, not literal.

**Kling prompt template:**
> "Cinematic historical battle. [ERA] CE. [TERRAIN].
> Wide shot — armies in formation, dust rising, banners
> in the wind. Weapons raised, shields locked. God-rays
> through battle smoke. Deep amber and ochre palette.
> Slow motion dust and debris. The scale of the moment —
> not the gore. Painterly atmospheric register.
> Persian miniature color palette. No blood. No gore."

### Type 5: MANUSCRIPT / PRIMARY SOURCE (manuscript_quote)
**Purpose:** When narration cites a primary historical source
**Register:** Islamic illuminated manuscript — authentic
**Duration:** 10–30 seconds

**Kling prompt template:**
> "Aged Islamic manuscript parchment texture.
> Arabic calligraphy emerging as ink bleeds into paper.
> Candlelight or natural light. Deep amber tones.
> Brushstrokes of ink on aged paper. Meditative pace.
> Ottoman/Mamluk/Abbasid manuscript aesthetic."

### Type 6: SCHOLAR CITATION (manuscript_scholar)
**Purpose:** When narration cites a named Islamic scholarly work
**Register:** Classical scholar's study — lamp-lit, intimate
**Duration:** 20–40 seconds

THIS IS THE NEW SCENE TYPE added for the Scholarly Wisdom Standard.
Use specifically when the narrator quotes a named scholar
(Ibn al-Qayyim, Al-Ghazali, Ibn Kathir, etc.) by name.

**Required script fields:**
- `[SCENE_TYPE]` manuscript_scholar
- `[SCHOLAR]` Full name of scholar (correct transliteration)
- `[WORK]` Arabic title transliterated + English translation
- `[CITATION]` Location in work (chapter/volume if known)
- `[CONFIDENCE]` SAHIH / HASAN / DA'IF / VERIFY

**Kling prompt template:**
> "Lamp-lit Islamic scholar's study. Stacked books in Arabic
> script. Open manuscript on a low desk. Ink and parchment.
> Warm amber candlelight, single source, deep shadow.
> [REGION_PALETTE] tones. The scholar's hands turning pages
> or writing. Classical Islamic scholarship aesthetic.
> No figures' faces. No modern elements. Meditative pace."

---

## PALETTES BY REGION

| Region | Palette |
|---|---|
| Arabia / Levant (Rashidun) | warm amber, deep ochre, desert gold |
| Persia / Safavid | grey-green, deep lapis, copper |
| Andalusia | blue-teal, cool white, terracotta |
| Africa / West Africa | deep ochre, earth red, forest green |
| Ottoman | deep blue, gold leaf, burgundy |
| South Asia / Mughal | saffron, deep red, ivory |
| Egypt / Mamluk | golden sand, deep amber, Nile blue |
| Caucasus | grey stone, forest green, snow white |
| Central Asia | sky blue, terracotta, gold |

---

## LIGHTING BY SCENE TYPE

| Scene Type | Lighting |
|---|---|
| Establishing | golden hour OR early morning mist |
| Character/drama | single lamp or torch — one side lit, one dark |
| Battle | harsh midday OR smoke-diffused light |
| Manuscript | candlelight — warm, flickering, close |
| Manuscript_scholar | single oil lamp — intimate, warm, still |

---

## WHAT NEVER APPEARS

- Photorealistic human faces
- Modern lighting rigs or lens flares
- Clean CGI environments
- Contemporary color grading (desaturated, teal-orange)
- Tier S figures' faces or voices (unless authenticated quote)
- Anything that breaks the painterly texture
