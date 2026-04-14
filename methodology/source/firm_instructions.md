<!-- CSV_COLUMNS: id_str, firm_name, holders, miners, finserv, bizserv, infrastr, unclass, certainty, invbase, RevShare, AssetShare -->

# Crypto-Firms Classification Instructions

## Overview

This project identifies the channels through which listed companies are exposed to cryptocurrencies. The objective is to classify each firm into one or more crypto-exposure categories reflecting the mechanism through which crypto-related developments may affect its equity price. Classification is based entirely on information retrieved via web search and web scraping.

---

## Input

The input is a CS file containing a list of firms identifiers and the sector they belong to. 

For each firm, the following will be provided:
- All fields from the input CSV row, including firm identifiers and metadata (e.g., `id_str`, `firm_name`, ticker aliases, sector labels, rank fields)
- `expo_firm` is provided for bookkeeping but should **not** be used as a search/disambiguation key

No documents are uploaded. All evidence must be retrieved through targeted web searches and direct scraping of source pages.

Before running web searches, read all input fields **except `expo_firm`** to ensure the correct firm entity is selected (especially for ambiguous names).

---

## Goal

For each firm, assign fractional weights across the following six **mechanism** categories. **These six mechanism weights must sum to 1 and be expressed in increments of 0.1.**

1. `holders`
2. `miners`
3. `finserv`
4. `bizserv`
5. `infrastr`
6. `unclass`

In addition, assign a separate **amplifier dummy**:

7. `invbase` (0/1 only; not part of the weight sum)

---

## Step-by-Step Analysis Process

### Step 1 — Retrieve Earnings Call Transcripts

For each firm, search for and scrape earnings call transcripts covering **January 2017 – December 2024**. Primary sources to target:

- **Seeking Alpha** (seekingalpha.com/symbol/[TICKER]/earnings/transcripts)
- **Motley Fool** (fool.com)
- **Company investor relations pages**
- **S&P Global / Refinitiv** where accessible

**Transcript target — tiered by firm size and coverage:**

- **Large or well-covered firms** (major US-listed names with broad analyst coverage): aim for **5–10 transcripts** spanning the sample period, to capture how crypto exposure evolved over time
- **Smaller or non-US firms** with limited transcript availability: aim for a **minimum of 2–3 transcripts**, supplemented more heavily by 10-K filings and financial news
- In all cases, **stop retrieving additional transcripts once the classification is stable** — i.e. once further transcripts are no longer changing the weight assignment or certainty score
- Use as many distinct years as available within **2017–2024** when selecting transcripts, unless the classification is clearly stable earlier.

Extract all crypto-related passages from each transcript and record short quotes as evidence. Note the earnings call date and quarter for each.

**At least one earnings-call transcript quote must be identified** to anchor the classification. If no transcript is retrievable despite search efforts, this must be flagged and the certainty score is capped at 2.

### Step 2 — Retrieve Supporting Evidence

Run additional targeted searches for each firm to corroborate or enrich the transcript evidence. Two further source types should always be consulted:

- **10-K / annual report filings** via SEC EDGAR (sec.gov) or equivalent non-US regulatory filings — best for precise figures, revenue line disclosures, and balance sheet values that calibrate whether crypto is core or peripheral
- **Financial news** (Bloomberg, Reuters, FT, CNBC, CoinDesk, Cointelegraph, etc.) — best for corroboration and investor base classification, capturing how the market characterises the firm's crypto exposure

All searches cover **January 2017 – December 2024** and are firm-specific. No carryover bias between firms.

### Step 3 — Decision Tree Classification

Assign categories strictly based on what is **explicitly stated or clearly evidenced** across the collected sources. A passing or incidental mention of Bitcoin is insufficient for a non-zero weight; there must be substantive discussion or clear evidence of material exposure.

---

**A. `holders` — Crypto Holders**
Assign weight if:
- The firm holds crypto on its balance sheet as a deliberate treasury investment decision
- It reports mark-to-market gains/losses on crypto holdings as an investment asset
- It purchased crypto separately from any operational need (e.g. not to back a product)
- It references an explicit HODL or treasury accumulation policy

> **Exclusion rule:** Mined BTC held temporarily pending sale is **`miners`** exposure only, not `holders`. Holdings that exist to back or collateralise a financial product (ETP, fund, structured product) are **`finserv`** exposure, not `holders`.

---

**B. `miners` — Crypto Miners**
Assign weight if the firm:
- Operates mining rigs
- Mines BTC/ETH or other proof-of-work assets
- Reports mining revenue, energy costs, or hashrate capacity

> **Exclusion rule:** A miner that also retains mined BTC as a strategic treasury reserve triggers a **`holders`** weight only if the accumulation is deliberate and goes materially beyond operational cash management.

---

**C. `finserv` — Crypto Financial Services**
Assign weight if the firm:
- Runs a crypto exchange or brokerage
- Provides custody, trading, crypto payments, or crypto investment products
- Issues or manages crypto-backed ETPs, funds or structured products
- References transaction volumes or crypto-related fee income
- Holds crypto operationally to back or collateralise a financial product

> **Exclusion rule:** The firm must directly handle, custody, issue, trade or invest in digital assets. Firms that provide support services *around* the ecosystem without touching the asset itself belong in **`bizserv`**.

---

**D. `bizserv` — Crypto Non-Financial Services**
Assign weight if the firm:
- Provides services to the crypto ecosystem that are not financial in nature
- Never takes custody of or transacts in digital assets itself
- Examples include: consulting, advertising, cybersecurity, legal services, auditing, software integration, logistics, events

> **Exclusion rule:** If the service involves hardware, compute, energy or datacentre capacity purpose-built for or primarily directed at crypto/mining workloads, it belongs in **`infrastr`**, not here.

---

**E. `invbase` — Crypto Investor Base Amplifier (Dummy)**
Assign `invbase = 1` if the firm:
- Has management that explicitly comments on crypto-focused shareholders or retail sentiment spillovers
- Is documented in financial media as a primary crypto proxy
- Has retail flows into its stock demonstrably tied to crypto market sentiment

Set `invbase = 0` otherwise.

> **Exclusion rule:** A firm that simply operates in a crypto-adjacent sector does not qualify on this basis alone. Evidence of investor base exposure must be explicit and direct, not inferred from the firm's business model.

> **Important:** `invbase` is an **orthogonal amplifier**, not a mechanism category. It can co-exist with any mechanism-weight mix and does not compete for the 1.0 weight mass.

---

**F. `infrastr` — Crypto Infrastructure Providers**
Assign weight if the firm supplies:
- GPUs, ASICs or other mining hardware
- Cloud compute used for mining or blockchain workloads
- Datacentre or colocation hosting for crypto clients
- Energy infrastructure targeted at miners

> **Exclusion rule:** General-purpose cloud or datacentre providers qualify only if there is explicit evidence of material crypto/mining-directed workloads. Generic tech infrastructure without documented crypto demand belongs in **`unclass`** or **`bizserv`**.

---

**G. `unclass` — Unclassified**
- No meaningful or credible crypto references found across any source
- All category weights = 0.0; `unclass` = 1.0

---

## Weighting Rules

- Mechanism weights (`holders, miners, finserv, bizserv, infrastr, unclass`) must sum to 1.0 and be expressed in **increments of 0.1**
- If only one category is clearly supported → that category = 1.0
- If multiple categories appear → distribute weights according to:
  - Frequency of mentions across sources
  - Economic materiality implied in calls/filings
  - Emphasis by management
- If evidence across all sources is weak or indirect → assign `unclass` = 1.0
- `invbase` is coded separately as **0/1** and does not affect the mechanism-weight sum.

**Threshold principle:** Apply judgment to distinguish substantive from incidental crypto references. Borderline cases must be flagged explicitly in the markdown report under Ambiguities/Limitations.

---

## Certainty Score

Assign a certainty score from **1 to 4** to every classified firm (i.e. any firm not assigned `unclass` = 1.0):

| Score | Label | Meaning |
|-------|-------|---------|
| 4 | Unambiguous | Multiple strong sources, explicit management statements, material crypto activity clearly documented across transcripts and filings |
| 3 | Confident | Clear evidence from at least one scraped transcript quote plus corroborating source |
| 2 | Moderate | Some evidence but indirect or limited to a single weak source; or no transcript quote retrievable despite search efforts |
| 1 | Weak | Classification based on inference or peripheral mentions only; high uncertainty |
| NA | — | Unclassified firms (`unclass` = 1.0) |

---

## Materiality Scales

In addition to mechanism weights and certainty scores, capture two ordinal measures of crypto materiality for use in matching-based econometric exercises (PSM, CEM, etc.). These are extracted incidentally from transcripts and 10-K filings during evidence collection — no additional searches are required.

### Revenue Share (`RevShare`)

Ordinal code reflecting crypto-attributable revenue as a share of total firm revenue:

| Code | Range | Meaning |
|------|-------|---------|
| 0 | — | Negligible / no crypto revenue evident |
| 1 | < ~5% | Minor — crypto mentioned but clearly peripheral |
| 2 | ~5–15% | Moderate — meaningful but not dominant revenue line |
| 3 | ~15–45% | Significant — crypto is a material segment |
| 4 | > ~45% | Major — crypto dominates revenue |
| 5 | ~90%+ | Dominant / near-total — firm is essentially a crypto business |
| NA | — | Not determinable from available sources |

### Asset Share (`AssetShare`)

Ordinal code reflecting crypto assets (holdings, mined coin, collateral, etc.) as a share of total firm assets:

| Code | Range | Meaning |
|------|-------|---------|
| 0 | — | Negligible / no crypto assets on balance sheet |
| 1 | < ~5% | Minor — crypto assets present but peripheral |
| 2 | ~5–15% | Moderate — meaningful balance sheet presence |
| 3 | ~15–45% | Significant — crypto is a material asset class for the firm |
| 4 | > ~45% | Major — crypto dominates the balance sheet |
| 5 | ~90%+ | Dominant / near-total — balance sheet is essentially crypto |
| NA | — | Not determinable from available sources |

**Assignment guidance:**
- Assign based on the **most recent clearly evidenced figure** within the 2017–2024 sample period. If materiality shifted substantially over time, note this under Ambiguities / Limitations.
- For `miners`: RevShare reflects mining revenue; AssetShare reflects mined coin held on balance sheet, not mining equipment.
- For `infrastr` and `bizserv` firms: both scores will typically be 0 or NA unless explicit crypto revenue segment disclosures are available.
- For `unclass` firms: both scores are NA.
- The distribution is expected to be heavily skewed toward 0 and 1; codes 4 and 5 should be reserved for clear pure-play firms only.

---

## Output

Two outputs are produced per firm and appended to the session files.

### (1) Markdown Report

Each report is approximately half a page and follows this structure:

```markdown
## Firm Name

* Rank: ... | Expo: ... | id_str: ...
* Weight summary: holders x.x, miners x.x, finserv x.x, bizserv x.x, infrastr x.x, unclass x.x
* Investor base amplifier (invbase): 0 or 1
* Certainty: X
* RevShare: X | AssetShare: X
* Primary sources: [list the source types that carried most evidential weight, e.g. "10-K filings (2021–2023), Q2 2022 earnings call"]

### Firm Overview
[1 to 2 sentences with a general description of the firm (main business, geography/listing context, and where crypto activities fit), based on information gathered online. Do not include hyperlinks in this section.]

### Exposure Channels Identified
[Which categories apply and why — 2 to 4 sentences]

### Weight Assignment
[One short paragraph explaining the weighting]

### Ambiguities / Limitations
[Weak evidence, borderline calls, exclusion rules applied, transcript not retrievable, etc.]

### Evidence

<small>

**Earnings calls:**
- Date, source: *"short quote"* — [Source name](URL)

**Filings (10-K / press releases):**
- Date, filing type: key figure or disclosure — [Source name](URL)

**Financial news / other:**
- Date, outlet: one-line finding — [Source name](URL)

</small>

```

**Evidence style:** entries should be precise and concise — one line per source with a named hyperlink `[Source name](URL)`. Favour more entries over longer descriptions. The link is mandatory for every entry.

A report is appended to the single output markdown file for each firm processed by the pipeline. To ensure consistency across the full dataset:
- The pipeline inserts a fixed document-level header at the very top of the markdown file before the first firm's `##` heading
- Each firm's report is separated from the next by a single `---` horizontal rule
- Do not place `---` horizontal rules inside a firm entry; `---` is used only after the document preamble and between firms
- Heading levels are fixed: `##` for firm name, `###` for subsections — never altered
- The `##` heading must be the **firm name only** (no suffixes such as "Crypto Exposure Classification")
- The subtitle block must include both `rank` and `expo` from the input CSV
- The subtitle block must follow the exact line order and separators shown above, with each line prefixed by `* `
- Never replace the weight summary with alternate styles (e.g., slashes, reordered categories, partial category lists)
- `Expo` must always be the value from input field `expo_firm` formatted to exactly **two decimals**
- The subtitle block must include `Investor base amplifier (invbase): 0 or 1`
- Do **not** print a CSV line/block inside the markdown report (CSV output is written only to the CSV file)

### (2) CSV Dataset
A row is appended to the cumulative dataset for each firm processed, with the following columns:

``` 
id_str, firm_name, holders, miners, finserv, bizserv, infrastr, unclass, certainty, invbase, RevShare, AssetShare
```

---

## Validation & Consistency Rules

### Report consistency

The markdown reports must be **uniform in length, structure and level of detail across all firms**. This is critical for the final appendix to read as a single coherent document rather than a patchwork of outputs produced at different times.

- Every report must contain all five subsections: Firm Overview, Exposure Channels Identified, Weight Assignment, Ambiguities / Limitations, Evidence
- **Firm Overview** should always be 1–2 sentences and should briefly describe the company based on retrieved sources (not inferred from the ticker/name alone)
- Firm Overview must be plain text only (no markdown links or raw URLs)
- **Exposure Channels Identified** should always be 2–4 sentences — no shorter, no longer
- **Weight Assignment** should always be one short paragraph
- **Ambiguities / Limitations** should always be present; if there are genuinely none, write "None identified"
- The Evidence section should contain a comparable number of entries across firms of similar coverage — do not pad or abbreviate relative to other sessions
- Do not add new subsections or omit existing ones
- Before finalising each report, check it against the other reports already in the output file — length, level of detail and structure should be visually consistent across all firms

### End-of-run checklist

Before the run completes, verify:

- [ ] File begins with the fixed document-level header inserted by the pipeline, followed by the first firm's `##` heading
- [ ] Each firm entry ends with a `---` separator, including the last firm in the file
- [ ] All `<small>` tags are closed (`</small>` present for every `<small>`)
- [ ] Firm count in the file matches the number of firms processed
- [ ] No firm `id_str` is duplicated across the output file

### CSV consistency rules

- **`certainty`** must always be an integer (1–4) or exactly `NA` — no other variants
- **Mechanism weights** (`holders, miners, finserv, bizserv, infrastr, unclass`) must always be expressed to one decimal place and must sum to exactly 1.0 — verify before writing each row
- `invbase` must always be exactly `0` or `1` (no decimals, no NA)
- **Column order** must always match the schema exactly: `id_str, firm_name, holders, miners, finserv, bizserv, infrastr, unclass, certainty, invbase, RevShare, AssetShare`
- `RevShare` and `AssetShare` must always be an integer 0–5 or exactly `NA` — no other variants
- For `unclass = 1.0` firms, both `RevShare` and `AssetShare` must be `NA`
- The header row appears only once, at the top of the cumulative CSV — never re-emitted mid-dataset
- At the end of each run, confirm no `id_str` is duplicated across the output file

---

## Practical Notes

- **Sample period for all searches:** January 2017 – December 2024.
- **Each firm is treated independently.** No information from one firm's classification carries over to the next.
- **At least one earnings-call transcript quote** must anchor every non-unclassified assignment. If none is retrievable, certainty is capped at 2 and the limitation is flagged.
- **Stop searching when classification is stable.** Once additional transcripts or sources are no longer shifting the weight assignment, further retrieval adds no value.
- **Always write in the third person.**
