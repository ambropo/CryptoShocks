<!-- CSV_COLUMNS: Event_Number, Date_GMT, Time_T0_GMT, Category, Certainty, Return_2h, Return_4h, Window, News_Headline, News_Source, Timing_Assessment, Confounds_Present, Narrative_Fit_Summary, Sources_Searched -->

# Crypto News Event Analysis Instructions

## Overview

This project runs a narrative analysis on a list of "events" that affect crypto currency markets. For each event, the analysis determines whether the news headline is plausibly linked to the observed BTC price move at the time of the event, and assigns a category and certainty score. Results are recorded in a single markdown report and a cumulative CSV dataset.

---

## Input

For each event, a PNG is provided containing:
- A news headline regarding the crypto market
- The date of the news
- The time when the news was released (T₀, GMT 24h)
- A chart of BTC intraday moves (5-minute frequency, ±12h around T₀)
- The BTC log-return in a 2h window (±1h around T₀) and a 4h window (±2h around T₀)

---

## Goal

Determine whether the news is plausibly linked to the BTC price move, and assess the degree of certainty of that assessment. The **2-hour window** (±1h around T₀) is the primary frame for all directional judgments — it determines whether a move occurred and in which direction. The **4-hour window** (±2h around T₀) is used conditionally: to validate whether a move was sustained, to modulate the certainty score, or to cross-check individual bullets in the narrative. The 4h return never overrides the 2h return as the directional signal.

---

## Step-by-Step Analysis Process

### Step 1 — Parse PNG Information

Extract complete information from the PNG: event number; news headline; date and time (T₀); 2h and 4h BTC log-returns (positive log-returns indicate price increases, negative indicate decreases). This is a crucial step — information from the parsed PNG is the basis for all analysis that follows. If a return window is not reported in the PNG, record it as unavailable and treat it as absent in all subsequent steps.

### Step 2 — Systematic News Search

Based on the news headline and date from the PNG, conduct a comprehensive news search covering:
- Crypto-related news from 12 hours before to 12 hours after T₀
- Major financial and macro news (data releases, policy actions, Fed announcements, etc.) for the same calendar day

Search a minimum of 3 crypto news sources (e.g. CoinDesk, CoinTelegraph, The Block) and 3 financial sources (e.g. Reuters, Bloomberg, WSJ). The purpose is to identify confounding events and check for explicit BTC price reporting that might contradict the actual returns in the 2h and 4h windows. The search window is ±12h for completeness, but only events that fall strictly within ±2h of T₀ qualify as confounds in Step 3c — prior-day context or background conditions found in the wider search window inform certainty only.

Only list or cite sources that were actually retrieved and used for the event under review. Do not recycle a fixed outlet list across events, do not pad the source list with outlets that were not opened, and do not use placeholder labels such as "WSJ search" or "broader macro news search". If contemporaneous coverage is thinner than ideal, state that plainly instead of inventing additional source coverage.

### Step 3 — Apply Decision Tree Classification

Apply the checks below in order. Stop at the first check that triggers.

**Step 3a: Check for No Movement**
- If the absolute value of the 2h return < 0.1% → **No Move**
- If the 2h return is unavailable and the absolute value of the 4h return < 0.1% → **No Move**
- A large 2h return always prevents a No Move classification, even if the 4h return is small (e.g. due to a price reversal after T₀+1h)

**Step 3b: Check for Mismatch**
- Determine the expected sign from the substance of the underlying news and contemporaneous reporting, not from the headline wording alone; the headline is only a first-pass cue
- If the 2h return direction is inconsistent with the substantive meaning of the news (e.g. news is clearly positive but 2h return is negative, or vice versa) → **Mismatch**
- This includes cases where systematic search finds articles explicitly stating "BTC rises/increased/up" but the 2h return is negative (< −0.1%), or articles state "BTC falls/decreased/down" but the 2h return is positive (> +0.1%)
- Do not require explicit price-contradiction articles for this check to trigger, but do require that the article substance points clearly in the opposite direction of the 2h move
- If the headline is neutral, procedural, or genuinely ambiguous in polarity, do not assign **Mismatch** on sign alone; read through the underlying news and contemporaneous coverage first
- If the news is cautionary, restrictive, or otherwise negative in substance even though the headline wording is superficially mild, treat it as negative rather than as ambiguous or positive

**Step 3c: Check for Confounding Events**
- If the systematic search reveals alternative events (crypto-specific or macro) that occurred strictly within ±2h of T₀ and are explicitly linked to BTC price moves of similar or greater magnitude in contemporaneous reporting → **Polluted**
- If assigning **Polluted**, identify at least one confounding event by name, approximate time, and source; do not write "None identified" in the confounds field for a Polluted event
- Background conditions from earlier the same day or prior days may lower certainty, but they are not confounds unless a distinct alternative event falls inside the strict ±2h event window
- A pre-existing drift or trend into T₀ is not, by itself, enough for **Polluted**

**Step 3d: Confirm Events**
- Everything else → **Matched**
- A move can still be **Matched** if it was already underway before T₀ but the event plausibly intensified, extended, or confirmed that move within the 2h window, provided no stronger alternative event is identified strictly within ±2h of T₀
- In such cases, lower the certainty score rather than automatically reclassifying the event as **Mismatch** or **Polluted**

---

## Category Examples

- **No Move:** 2h: +0.05%, 4h: −1.08%
- **Mismatch:** News says "BTC surges on adoption news" but 2h: −2.1%, 4h: −1.8%; or news is substantively positive (from underlying coverage) but 2h: −1.5% even without explicit price-contradiction articles
- **Polluted:** Event about exchange hack, but Fed announces rate cut 30 minutes before T₀ with explicit BTC price attribution
- **Matched (lower certainty):** News is substantively negative, BTC was already drifting down before T₀, and the selloff accelerates after the event with no stronger competing catalyst inside ±2h
- **Matched (Certainty 3):** "Major exchange adds BTC" at 14:00, 2h: +3.2%, 4h: +2.8%, move starts 14:05

---

## Certainty Scale

Applied only to Matched events:

| Score | Label | Meaning |
|-------|-------|---------|
| 3 | Unambiguous | Price move clearly consistent with news interpretation; timing aligns closely with T₀; no credible alternative explanation |
| 2 | Confident | Price move consistent with news interpretation; some background volatility but no credible alternative explanation |
| 1 | Moderate | News is neutral or ambiguous; link between news and price move is uncertain; timing ambiguity or high background volatility |
| NA | — | Used for Polluted, Mismatch, and No Move categories |

When assigning certainty, use the 4h return as a secondary input: if the 4h return confirms the 2h move (same sign, similar magnitude), this supports a higher certainty score; if the 4h return reverses the 2h move, consider whether the reversal undermines confidence and lower the score accordingly.

If the move starts before T₀, or if broader background pressure was already present, this should usually reduce certainty rather than force a non-match classification. Reserve non-match classifications for true sign contradictions or for distinct same-window confounds.

---

## Window Field

The `Window` field records whether the 4-hour return **agrees** with the 2-hour return in terms of direction. This is a validation flag, not a classification criterion.

- **Agree** — 4h return has the same sign as the 2h return (both positive or both negative)
- **Disagree** — 4h return has the opposite sign to the 2h return (price reversed after the initial 2h window)
- **NA** — for all non-Matched categories, or when the 4h return is unavailable

---

## Output

Two outputs are produced per event and appended to the output files automatically by the pipeline.

### (1) Markdown Report

All events are written to a single markdown file. The file begins with a fixed header followed by one entry per event. Each entry follows this exact structure:

```markdown
## Event #[N]: [Short descriptive title — 3 to 8 words]
**[Category] | Certainty: [1/2/3/NA]**
<img src="[PNG file path]" alt="Event #[N]" style="max-width: 840px; width: 100%; height: auto;" />

- **News**: One sentence — factual description of the headline and primary source, ending with a markdown link to the single most relevant source for this bullet
- **Market impact**: One sentence — exact 2h and 4h returns from the PNG with brief directional interpretation, ending with a markdown link to the most relevant source for this bullet
- **Timing**: One sentence — when the move starts relative to T₀ based on the 5-minute chart, ending with a markdown link to the most relevant source for this bullet
- **Confounds**: One sentence — note any other events of similar importance present in the ±2h window, or "None identified", ending with a markdown link to the most relevant source for this bullet
- **Narrative fit**: One sentence — judgment linking the news to the price move using the assigned category and certainty, ending with a markdown link to the most relevant source for this bullet
- **Sources**: Comma-separated list of all sources searched during Step 2, ending with a markdown link to the single most relevant source overall
```

**Formatting rules:**
- The file begins with the fixed document header — do not add a second title or preamble
- The pipeline inserts a `---` separator after the document header and then uses `---` only between events
- Each event heading (`## Event #N: Title`) is immediately followed by the bold metadata line with no blank line between them
- The subtitle contains only the category and certainty: `**[Category] | Certainty: [1/2/3/NA]**`
- Immediately after the subtitle, load the PNG file on its own line using the exact HTML `<img ... />` format shown above
- The PNG image appears before the bullet list
- Insert exactly one blank line after the image before the first bullet
- Do not place `---` horizontal rules inside an event entry
- Each bullet uses `-` (standard markdown dash)
- Each bullet title is bold: `- **News**: ...`, `- **Market impact**: ...`, etc.
- Each bullet is a single sentence of approximately 25–45 words — no shorter and no longer than comparable entries
- Every bullet must end with exactly one markdown link to the single most relevant source for that bullet
- Each markdown link must point to a specific article or page actually used for that event; do not use generic homepages
- Use informative link text such as the outlet or source name; do not use generic link text like `Read more`
- The date is recorded in the CSV output only; it does not appear in the markdown subtitle
- Do **not** print a CSV row inside the markdown report
- Before finalising each entry, check it against the other entries already in the output file — length, level of detail and structure must be visually consistent across all events

### (2) CSV Dataset

A row is appended to the cumulative dataset after each event, with columns defined by the `CSV_COLUMNS` header at the top of this file.

---

## Validation & Consistency Rules

### Report consistency

The markdown entries must be **uniform in length, structure and level of detail across all events**. This is critical for the final document to read as a single coherent output rather than a patchwork of reports produced at different times.

- Every entry must contain all six bullets: News, Market impact, Timing, Confounds, Narrative fit, Sources
- Each bullet must be a single sentence — no shorter and no longer than comparable entries already in the output file
- Every bullet must end with exactly one markdown link
- Do not add new bullets or omit existing ones

### End-of-run checklist

Before the run completes, verify:

- [ ] Each entry contains all six bullets
- [ ] No event number in this run duplicates one already in the output file

### CSV consistency rules

- **`Date_GMT`** must always be formatted as `DDmonYYYY` (e.g. `06jan2017`)
- **`Time_T0_GMT`** must always be formatted as `HH:MM`
- **`Category`** must always be exactly one of: `Matched`, `Polluted`, `Mismatch`, `No Move`
- **`Certainty`** must always be an integer (1–3) or exactly `NA` — no other variants
- **`Certainty` must be `NA` if and only if `Category` ≠ `Matched`**
- **`Return_2h` and `Return_4h`** must always be expressed as plain numeric percentages (e.g. `−2.1`, not `−2.1%`); use empty string if the return is not available from the PNG
- **`Window`** must be exactly one of: `Agree`, `Disagree`, `NA`
- **`Window` must be `NA` if `Category` ≠ `Matched` or if the 4h return is unavailable**
- **`News_Source`** must identify the specific outlet or URL most relevant to the headline; it must not be empty
- **`Sources_Searched`** must list only actually retrieved sources separated by commas; do not include placeholders such as `Reuters search`, `WSJ search`, or `broader macro news search`
- **`Confounds_Present`** must name at least one confound for `Polluted` events; it cannot say `None identified` when `Category = Polluted`
- **Column order** must always match the schema exactly
- The header row appears only once, at the top of the cumulative CSV — never re-emitted mid-dataset
- At the end of each run, confirm no event number is duplicated across the output file

---

## Practical Notes

- **Each event is treated independently.** Run fresh, independent news searches for each event's specific date and time. No carryover bias between events.
- **Stop searching when the classification is stable.** Once additional sources are no longer shifting the category or certainty, further retrieval adds no value.
- **Always write in the third person.**
