#!/usr/bin/env python3
"""Generic firm classification loop using OpenAI API + web search.

This version uses the shared firm-classification workflow so that:
- prompts, validation, retries, output files, and logging are consistent
- raw JSONL records share the same top-level schema
- only the provider-specific API call path differs materially
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import html
import json
import os
import re
import ssl
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib import error, request

try:
    import certifi  # type: ignore
except Exception:  # noqa: BLE001
    certifi = None


DEFAULT_MAX_TOKENS = 8096
MANIFEST_FILENAME = "run_manifest.json"
INPUT_SNAPSHOT_FILENAME = "input_firms_snapshot.csv"

def format_model_label(model: str) -> str:
    if model.startswith("gpt-"):
        return "GPT-" + model[len("gpt-") :]
    return model


def build_md_file_header(executed_at_utc: dt.datetime, model: str) -> str:
    executed_str = executed_at_utc.strftime("%Y-%m-%d %H:%M UTC")
    model_label = format_model_label(model)
    return (
        "# Narrative Classification of Crypto-Exposed Firms\n\n"
        "This document reports the results of a systematic narrative classification of crypto-exposed firms. "
        "Each firm is assigned fractional weights across six transmission mechanism categories -- crypto holders, "
        "miners, financial services, non-financial services, infrastructure providers, and unclassified -- based "
        "on evidence retrieved from earnings call transcripts, SEC filings, and financial news covering January 2017 "
        "to December 2024. Classification is performed by a large language model (GPT-5) instructed via a structured "
        "prompt (`firm_instructions.md`) and executed through the OpenAI API using the pipeline script `GO_firm-GPT.py`. "
        "The full classification methodology, category definitions, weighting rules, and output schema are documented "
        "in the accompanying instructions file.\n\n"
        f"Executed: {executed_str} | Model: {model_label}\n\n"
        "---\n\n"
    )


def strip_md_file_header(text: str) -> str:
    if not text.startswith("# Narrative Classification of Crypto-Exposed Firms\n"):
        return text
    marker = "\n---\n\n"
    idx = text.find(marker)
    if idx == -1:
        return text
    return text[idx + len(marker) :]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Classify firms using OpenAI API.")
    p.add_argument("--input-csv", default="LLMs/firm-raw/CRYPTO_FirmListTop.csv")
    p.add_argument("--instructions-md", default="LLMs/firm_instructions.md")
    p.add_argument("--output-csv", default="LLMs/firm-GPT-classification/firm_report.csv")
    p.add_argument("--output-md", default="LLMs/firm-GPT-classification/firm_report.md")
    p.add_argument("--output-html", default="", help="Optional HTML export path (default: output-md with .html)")
    p.add_argument("--raw-jsonl", default="LLMs/firm-GPT-classification/firm_raw.jsonl")
    p.add_argument("--model", default="gpt-5")
    p.add_argument("--start", type=int, default=1, help="1-based start row index")
    p.add_argument("--limit", type=int, default=0, help="Max firms to process (0 = all)")
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--max-continuations", type=int, default=5)
    p.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    p.add_argument("--sleep-seconds", type=float, default=1.0)
    p.add_argument("--overwrite", action="store_true", help="Reclassify already-processed firms")
    return p.parse_args()


def parse_csv_columns(instructions_text: str) -> List[str]:
    m = re.search(r"<!--\s*CSV_COLUMNS:\s*(.+?)\s*-->", instructions_text)
    if not m:
        raise ValueError("No <!-- CSV_COLUMNS: ... --> marker found in instructions.")
    return [c.strip() for c in m.group(1).split(",") if c.strip()]


def load_firms(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        if not {"id_str", "firm_name"}.issubset(set(fieldnames)):
            raise ValueError(f"Input CSV must have id_str and firm_name columns: {path}")
        firms: List[Dict[str, str]] = []
        for row in reader:
            normalized = {k: (row.get(k) or "").strip() for k in fieldnames}
            if normalized.get("id_str") and normalized.get("firm_name"):
                firms.append(normalized)
    return fieldnames, firms


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def manifest_paths(output_dir: Path) -> Tuple[Path, Path]:
    return output_dir / MANIFEST_FILENAME, output_dir / INPUT_SNAPSHOT_FILENAME


def duplicate_ids(rows: List[Dict[str, str]]) -> List[str]:
    seen: set[str] = set()
    dupes: List[str] = []
    for row in rows:
        id_str = (row.get("id_str") or "").strip()
        if not id_str:
            continue
        if id_str in seen and id_str not in dupes:
            dupes.append(id_str)
        seen.add(id_str)
    return dupes


def first_manifest_difference(expected: List[Dict[str, str]], actual: List[Dict[str, str]]) -> str:
    if len(expected) != len(actual):
        return f"row count differs: manifest has {len(expected)} firms, current input has {len(actual)} firms"
    for idx, (exp_row, act_row) in enumerate(zip(expected, actual), start=1):
        if exp_row != act_row:
            exp_id = exp_row.get("id_str", "")
            exp_name = exp_row.get("firm_name", "")
            act_id = act_row.get("id_str", "")
            act_name = act_row.get("firm_name", "")
            return (
                f"first difference at row {idx}: manifest has {exp_id} / {exp_name}; "
                f"current input has {act_id} / {act_name}"
            )
    return "input rows differ from the saved manifest"


def read_manifest(manifest_path: Path) -> Dict[str, Any]:
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Could not read manifest file {manifest_path}: {e}") from e


def build_manifest(
    *,
    input_csv: Path,
    input_bytes: bytes,
    fieldnames: List[str],
    firms: List[Dict[str, str]],
    executed_at_utc: dt.datetime,
) -> Dict[str, Any]:
    return {
        "created_at_utc": executed_at_utc.isoformat(timespec="seconds") + "Z",
        "input_csv": str(input_csv),
        "input_csv_sha256": sha256_bytes(input_bytes),
        "fieldnames": fieldnames,
        "row_count": len(firms),
        "firms": firms,
    }


def validate_output_parent_layout(output_csv: Path, output_md: Path, raw_jsonl: Path) -> Path:
    parents = {output_csv.parent, output_md.parent, raw_jsonl.parent}
    if len(parents) != 1:
        raise RuntimeError(
            "Output files must live in the same folder so the run manifest is unambiguous. "
            f"Got: csv={output_csv.parent}, md={output_md.parent}, raw={raw_jsonl.parent}"
        )
    output_dir = output_csv.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_manifest_and_snapshot(manifest_path: Path, snapshot_path: Path, manifest: Dict[str, Any], input_bytes: bytes) -> None:
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    snapshot_path.write_bytes(input_bytes)


def validate_or_create_manifest(
    *,
    output_dir: Path,
    input_csv: Path,
    input_bytes: bytes,
    fieldnames: List[str],
    firms: List[Dict[str, str]],
    executed_at_utc: dt.datetime,
    output_paths: List[Path],
) -> Dict[str, Any]:
    # The manifest locks the input universe to a specific ordered CSV snapshot.
    # That prevents a resumed run from silently mixing classifications produced
    # against different firm lists in the same output folder.
    manifest_path, snapshot_path = manifest_paths(output_dir)
    any_output_exists = any(path.exists() for path in output_paths)

    if any_output_exists and not manifest_path.exists():
        raise RuntimeError(
            f"Output folder {output_dir} already contains classification files but is missing {MANIFEST_FILENAME}. "
            "Refusing to continue because the existing output universe cannot be verified."
        )

    if manifest_path.exists() and not snapshot_path.exists():
        raise RuntimeError(
            f"Output folder {output_dir} has {MANIFEST_FILENAME} but is missing {INPUT_SNAPSHOT_FILENAME}. "
            "Refusing to continue because the saved input snapshot is incomplete."
        )

    if manifest_path.exists():
        manifest = read_manifest(manifest_path)
        manifest_firms = manifest.get("firms")
        manifest_fieldnames = manifest.get("fieldnames")
        if not isinstance(manifest_firms, list) or not all(isinstance(row, dict) for row in manifest_firms):
            raise RuntimeError(f"Manifest file {manifest_path} does not contain a valid firm list.")
        if manifest_fieldnames != fieldnames:
            raise RuntimeError(
                f"Current input columns do not match {manifest_path}. "
                f"Manifest columns: {manifest_fieldnames}; current columns: {fieldnames}"
            )
        manifest_hash = str(manifest.get("input_csv_sha256", "")).strip()
        current_hash = sha256_bytes(input_bytes)
        if manifest_hash != current_hash or manifest_firms != firms:
            raise RuntimeError(
                "Current input CSV does not match the saved manifest for this output folder. "
                + first_manifest_difference(manifest_firms, firms)
            )
        return manifest

    manifest = build_manifest(
        input_csv=input_csv,
        input_bytes=input_bytes,
        fieldnames=fieldnames,
        firms=firms,
        executed_at_utc=executed_at_utc,
    )
    write_manifest_and_snapshot(manifest_path, snapshot_path, manifest, input_bytes)
    return manifest


def read_output_csv_rows(output_csv: Path) -> List[Dict[str, str]]:
    if not output_csv.exists():
        return []
    with output_csv.open("r", encoding="utf-8-sig", newline="") as f:
        return [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(f)]


def read_markdown_ids(output_md: Path) -> List[str]:
    if not output_md.exists():
        return []
    text = strip_md_file_header(output_md.read_text(encoding="utf-8"))
    return re.findall(r"id_str:\s*([^\s]+)", text)


def read_raw_ids(raw_jsonl: Path) -> List[str]:
    ids: List[str] = []
    if not raw_jsonl.exists():
        return ids
    with raw_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            firm = obj.get("firm")
            if isinstance(firm, dict):
                id_str = str(firm.get("id_str", "")).strip()
                if id_str:
                    ids.append(id_str)
    return ids


def validate_existing_outputs(
    *,
    expected_firms: List[Dict[str, str]],
    output_csv: Path,
    output_md: Path,
    raw_jsonl: Path,
) -> Tuple[set[str], set[str], set[str]]:
    # Before resuming, confirm that all partially written outputs still refer to
    # the same firm universe recorded in the manifest and do not already contain
    # duplicate or out-of-scope IDs.
    expected_ids = [(row.get("id_str") or "").strip() for row in expected_firms]
    expected_id_set = set(expected_ids)

    csv_rows = read_output_csv_rows(output_csv)
    csv_ids = [(row.get("id_str") or "").strip() for row in csv_rows if (row.get("id_str") or "").strip()]
    md_ids = read_markdown_ids(output_md)
    raw_ids = read_raw_ids(raw_jsonl)

    for label, ids in [("CSV", csv_ids), ("markdown", md_ids), ("raw JSONL", raw_ids)]:
        dupes = sorted({id_str for id_str in ids if ids.count(id_str) > 1})
        if dupes:
            raise RuntimeError(f"Existing {label} output contains duplicate firm IDs: {', '.join(dupes[:10])}")
        extras = sorted(set(ids) - expected_id_set)
        if extras:
            raise RuntimeError(
                f"Existing {label} output contains firm IDs that are not in the saved manifest: {', '.join(extras[:10])}"
            )

    return set(csv_ids), set(md_ids), set(raw_ids)


def validate_final_outputs(output_csv: Path, expected_firms: List[Dict[str, str]]) -> None:
    csv_rows = read_output_csv_rows(output_csv)
    expected_ids = [(row.get("id_str") or "").strip() for row in expected_firms]
    actual_ids = [(row.get("id_str") or "").strip() for row in csv_rows]

    if len(csv_rows) != len(expected_firms):
        raise RuntimeError(
            f"Final output CSV row count mismatch: expected {len(expected_firms)} firms, found {len(csv_rows)}."
        )

    dupes = duplicate_ids(csv_rows)
    if dupes:
        raise RuntimeError(f"Final output CSV contains duplicate firm IDs: {', '.join(dupes[:10])}")

    if actual_ids != expected_ids:
        for idx, (expected_id, actual_id) in enumerate(zip(expected_ids, actual_ids), start=1):
            if expected_id != actual_id:
                exp_name = expected_firms[idx - 1].get("firm_name", "")
                act_name = csv_rows[idx - 1].get("firm_name", "") if idx - 1 < len(csv_rows) else ""
                raise RuntimeError(
                    "Final output CSV firm order does not match the saved input manifest. "
                    f"First difference at row {idx}: expected {expected_id} / {exp_name}; "
                    f"found {actual_id} / {act_name}."
                )
        raise RuntimeError("Final output CSV firm order does not match the saved input manifest.")


def existing_csv_ids(output_csv: Path) -> set[str]:
    done: set[str] = set()
    if not output_csv.exists():
        return done
    with output_csv.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            id_str = (row.get("id_str") or "").strip()
            if id_str:
                done.add(id_str)
    return done


def existing_markdown_ids(output_md: Path) -> set[str]:
    if not output_md.exists():
        return set()
    text = strip_md_file_header(output_md.read_text(encoding="utf-8"))
    return set(re.findall(r"id_str:\s*([^\s]+)", text))


def existing_raw_ids(raw_jsonl: Path) -> set[str]:
    done: set[str] = set()
    if not raw_jsonl.exists():
        return done
    with raw_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            firm = obj.get("firm")
            if isinstance(firm, dict):
                id_str = str(firm.get("id_str", "")).strip()
                if id_str:
                    done.add(id_str)
    return done


def ensure_output_files(
    output_csv: Path, output_md: Path, raw_jsonl: Path, csv_columns: List[str], md_file_header: str
) -> None:
    for p in [output_csv, output_md, raw_jsonl]:
        p.parent.mkdir(parents=True, exist_ok=True)
    if not output_csv.exists():
        with output_csv.open("w", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=csv_columns).writeheader()
    if not output_md.exists():
        output_md.write_text(md_file_header, encoding="utf-8")
    if not raw_jsonl.exists():
        raw_jsonl.write_text("", encoding="utf-8")


def build_output_contract(csv_columns: List[str]) -> str:
    return (
        "\n\nAfter completing the markdown report for a firm, output a single ```json ... ``` block "
        "containing one flat JSON object with exactly these keys in any order: "
        f"{', '.join(csv_columns)}. "
        "Use empty strings for unknown values rather than omitting keys."
    )


def read_recent_markdown_excerpt(output_md: Path, max_chars: int = 6000) -> str:
    if not output_md.exists():
        return ""
    text = strip_md_file_header(output_md.read_text(encoding="utf-8"))
    text = text.strip()
    if not text:
        return ""
    if len(text) > max_chars:
        text = "...\n" + text[-max_chars:]
    return text



def build_user_prompt(
    firm: Dict[str, str],
    recent_markdown_excerpt: str = "",
    validation_error: Optional[str] = None,
) -> str:
    lines = [
        "Classify this single firm using the provided instructions and web search.",
        f"id_str: {firm['id_str']}",
        f"firm_name: {firm['firm_name']}",
        "Before searching, read all provided input fields except `expo_firm` to disambiguate the correct company entity.",
        "Use these fields only for firm identification/context, not as direct evidence of crypto exposure.",
        "Sample period: January 2017 to December 2024.",
        "Do not carry over information from other firms.",
    ]
    extras = [f"- {k}: {v}" for k, v in firm.items() if k not in {"id_str", "firm_name", "expo_firm"} and v]
    if extras:
        lines.append("Additional input fields:")
        lines.extend(extras)
    if recent_markdown_excerpt:
        lines.extend(
            [
                "Existing output excerpt for consistency reference:",
                "```markdown",
                recent_markdown_excerpt,
                "```",
            ]
        )
    if validation_error:
        lines.append(
            "Your previous output could not be parsed mechanically. Return the full markdown report "
            f"followed by a corrected JSON block. Parse issue: {validation_error}"
        )
    return "\n".join(lines)



def extract_markdown_and_json(text: str) -> Tuple[str, Dict[str, Any]]:
    m = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL | re.IGNORECASE)
    if m:
        return text[:m.start()].rstrip(), json.loads(m.group(1))

    decoder = json.JSONDecoder()
    for i in range(len(text) - 1, -1, -1):
        if text[i] != "}":
            continue
        for j in range(i, -1, -1):
            if text[j] != "{":
                continue
            try:
                obj, _ = decoder.raw_decode(text[j : i + 1])
                if isinstance(obj, dict):
                    return text[:j].rstrip(), obj
            except json.JSONDecodeError:
                continue
        break
    raise ValueError("Could not find a JSON object in the response.")



def normalize_csv_row(json_obj: Dict[str, Any], csv_columns: List[str], firm: Dict[str, str]) -> Dict[str, str]:
    return {col: str(json_obj.get(col, "")).strip() for col in csv_columns}


def _fmt_weight(val: str) -> str:
    try:
        return f"{float(val):.1f}"
    except Exception:  # noqa: BLE001
        return val


def _fmt_expo_2dp(val: str) -> str:
    try:
        return f"{float(val):.2f}"
    except Exception:  # noqa: BLE001
        return val


def _extract_primary_sources(body: str) -> Tuple[str, str]:
    """
    Extract a top-of-body primary sources line (if present), and return:
    (primary_sources_text, body_without_that_line).
    """
    lines = body.splitlines()
    primary_sources = "None reported"
    out_lines: List[str] = []
    removed = False

    for i, line in enumerate(lines):
        if removed:
            out_lines.append(line)
            continue
        m = re.match(r"^\s*(?:[-*]\s+)?\*{0,2}\s*Primary sources\*{0,2}\s*:\s*(.*)\s*$", line, flags=re.IGNORECASE)
        if m:
            primary_sources = m.group(1).strip() or "None reported"
            removed = True
            # Drop one immediately following blank line, if present.
            if i + 1 < len(lines) and not lines[i + 1].strip():
                lines[i + 1] = "__DROPPED_BLANK__"
            continue
        out_lines.append(line)

    out_lines = [ln for ln in out_lines if ln != "__DROPPED_BLANK__"]
    return primary_sources, "\n".join(out_lines).strip()


def build_fixed_subtitle(firm: Dict[str, str], csv_row: Dict[str, str], primary_sources: str) -> str:
    rank = (firm.get("SortExpo") or firm.get("rank") or "").strip()
    expo = _fmt_expo_2dp((firm.get("expo_firm") or "").strip())
    weight_keys = ["holders", "miners", "finserv", "bizserv", "infrastr", "unclass"]
    weight_summary = ", ".join(f"{k} {_fmt_weight(csv_row.get(k, ''))}" for k in weight_keys)
    invbase_raw = (csv_row.get("invbase") or "").strip()
    try:
        invbase = str(int(float(invbase_raw)))
        if invbase not in {"0", "1"}:
            invbase = invbase_raw or "0"
    except Exception:  # noqa: BLE001
        invbase = invbase_raw or "0"
    certainty = (csv_row.get("certainty") or "").strip()
    revshare = (csv_row.get("RevShare") or "").strip()
    assetshare = (csv_row.get("AssetShare") or "").strip()
    return "\n".join(
        [
            f"* Rank: {rank} | Expo: {expo} | id_str: {firm.get('id_str', '')}",
            f"* Weight summary: {weight_summary}",
            f"* Investor base amplifier (invbase): {invbase}",
            f"* Certainty: {certainty}",
            f"* RevShare: {revshare} | AssetShare: {assetshare}",
            f"* Primary sources: {primary_sources}",
        ]
    )


def canonicalize_firm_markdown(markdown: str, firm: Dict[str, str], csv_row: Dict[str, str]) -> str:
    # The model is free to structure its prose body, but the title/subtitle block
    # must be deterministic so the combined markdown file stays machine-readable
    # and visually consistent across many firms and resumed runs.
    lines = markdown.strip().splitlines()
    body_start = 0
    for i, line in enumerate(lines):
        if line.strip().startswith("### "):
            body_start = i
            break

    prefix = "\n".join(lines[:body_start]).strip() if body_start else ""
    body = "\n".join(lines[body_start:]).strip() if body_start else markdown.strip()

    primary_sources = "None reported"
    if prefix:
        for line in prefix.splitlines():
            m = re.match(r"^\s*(?:[-*]\s+)?\*{0,2}\s*Primary sources\*{0,2}\s*:\s*(.*)\s*$", line, flags=re.IGNORECASE)
            if m:
                primary_sources = m.group(1).strip() or primary_sources
                break

    if primary_sources == "None reported":
        primary_sources, body = _extract_primary_sources(body)

    title = f"## {firm.get('firm_name', '')}"
    subtitle = build_fixed_subtitle(firm, csv_row, primary_sources)
    if body:
        body_lines = [line for line in body.splitlines() if line.strip() != "---"]
        body = "\n".join(body_lines).strip()
        body = body + "\n\n---"

    parts = [title, "", subtitle]
    if body:
        parts.extend(["", body])
    return "\n".join(parts).strip()


def markdown_to_html(markdown_text: str) -> str:
    """
    Convert markdown to HTML.
    Prefer python-markdown if installed; otherwise fall back to a safe <pre> rendering.
    """
    try:
        import markdown as md  # type: ignore

        return md.markdown(
            markdown_text,
            extensions=["extra", "sane_lists", "tables", "nl2br"],
        )
    except Exception:  # noqa: BLE001
        return f"<pre>{html.escape(markdown_text)}</pre>"


def sanitize_markdown_for_html_export(markdown_text: str) -> str:
    # Some firm writeups include raw <small> wrappers around Evidence sections.
    # When those tags are unbalanced, the browser keeps shrinking later content.
    # Strip them at export time so the HTML stays visually stable without
    # changing the underlying markdown report.
    return re.sub(r"</?small>", "", markdown_text, flags=re.IGNORECASE)


def export_markdown_file_to_html(output_md: Path, output_html: Path) -> None:
    output_html.parent.mkdir(parents=True, exist_ok=True)
    md_text = output_md.read_text(encoding="utf-8") if output_md.exists() else ""
    md_text = sanitize_markdown_for_html_export(md_text)
    body = markdown_to_html(md_text)
    doc = (
        "<!doctype html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        f"  <title>{html.escape(output_md.stem)}</title>\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        "  <style>\n"
        "    body { max-width: 900px; margin: 2rem auto; padding: 0 1rem; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 16px; line-height: 1.5; color: #1f2937; }\n"
        "    h1, h2, h3, h4, h5, h6, p, li { font-size: inherit; }\n"
        "    h1 { font-size: 1.75rem; margin: 0 0 1rem; font-weight: 700; }\n"
        "    h2 { font-size: 1.25rem; margin: 2rem 0 0.75rem; font-weight: 700; }\n"
        "    h3 { font-size: 1rem; margin: 1.25rem 0 0.5rem; font-weight: 700; }\n"
        "    p, ul, ol { margin: 0.6rem 0; }\n"
        "    ul, ol { padding-left: 1.25rem; }\n"
        "    li + li { margin-top: 0.25rem; }\n"
        "    small { font-size: 1em; }\n"
        "    pre { white-space: pre-wrap; word-wrap: break-word; background: #f7f7f7; padding: 1rem; border-radius: 6px; }\n"
        "    code { background: #f7f7f7; padding: 0.1rem 0.25rem; border-radius: 4px; }\n"
        "    hr { border: 0; border-top: 1px solid #ddd; margin: 1.5rem 0; }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        f"{body}\n"
        "</body>\n"
        "</html>\n"
    )
    output_html.write_text(doc, encoding="utf-8")


def extract_output_text(resp: Dict[str, Any]) -> str:
    out_text = resp.get("output_text")
    if isinstance(out_text, str) and out_text.strip():
        return out_text.strip()

    chunks: List[str] = []
    for item in resp.get("output", []):
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            ctype = content.get("type")
            if ctype in {"output_text", "text"} and isinstance(content.get("text"), str):
                chunks.append(content["text"])
    return "\n".join(chunks).strip()



def post_responses(payload: Dict[str, Any], api_key: str, ssl_context: Optional[ssl.SSLContext]) -> Dict[str, Any]:
    req = request.Request(
        url="https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=240, context=ssl_context) as resp:
        return json.loads(resp.read().decode("utf-8"))



def call_once_with_tool_variants(
    api_key: str,
    model: str,
    system: str,
    input_text: str,
    previous_response_id: Optional[str],
    ssl_context: Optional[ssl.SSLContext],
    max_tokens: int,
) -> Dict[str, Any]:
    last_error: Optional[Exception] = None

    for tool_type in ["web_search", "web_search_preview"]:
        # The Responses API has used both tool names across versions. Try both so
        # older environments fail over cleanly instead of aborting the run.
        payload: Dict[str, Any] = {
            "model": model,
            "instructions": system,
            "input": input_text,
            "max_output_tokens": max_tokens,
            "tool_choice": "auto",
            "tools": [{"type": tool_type}],
        }
        if previous_response_id:
            payload["previous_response_id"] = previous_response_id

        try:
            return post_responses(payload, api_key=api_key, ssl_context=ssl_context)
        except error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            lower = body.lower()
            if any(
                token in lower
                for token in [
                    "unknown parameter",
                    "invalid type",
                    "unsupported",
                    "unrecognized",
                    "invalid value",
                    "response_format",
                    "cannot be used with json mode",
                    "model not found",
                    "does not exist",
                ]
            ):
                last_error = RuntimeError(f"HTTP {e.code}: {body}")
                continue
            raise RuntimeError(f"HTTP {e.code}: {body}") from e
        except Exception as e:  # noqa: BLE001
            last_error = e
            continue

    if last_error is not None:
        raise RuntimeError(str(last_error))
    raise RuntimeError("Failed OpenAI call with all web search tool variants.")



def call_gpt(
    api_key: str,
    model: str,
    system: str,
    user_prompt: str,
    max_continuations: int,
    max_tokens: int,
) -> Tuple[str, Dict[str, Any], List[Dict[str, Any]]]:
    ssl_context: Optional[ssl.SSLContext] = None
    if certifi is not None:
        ssl_context = ssl.create_default_context(cafile=certifi.where())

    all_text: List[str] = []
    raw_items: List[Dict[str, Any]] = []
    metadata: Dict[str, Any] = {}

    current_input = user_prompt
    previous_response_id: Optional[str] = None
    response_ids: List[str] = []

    for continuation in range(max_continuations):
        # When the model hits the output-token cap, continue on the same response
        # thread so long firm writeups can be recovered without starting over.
        response = call_once_with_tool_variants(
            api_key=api_key,
            model=model,
            system=system,
            input_text=current_input,
            previous_response_id=previous_response_id,
            ssl_context=ssl_context,
            max_tokens=max_tokens,
        )
        raw_items.append(response)

        text = extract_output_text(response)
        if text:
            all_text.append(text)

        if response.get("id"):
            response_ids.append(str(response["id"]))

        incomplete = response.get("incomplete_details") or {}
        reason = (incomplete.get("reason") if isinstance(incomplete, dict) else None) or ""
        metadata = {
            "provider": "openai",
            "model": response.get("model", model),
            "status": response.get("status"),
            "termination_reason": reason or response.get("status"),
            "usage": response.get("usage"),
            "continuation_count": continuation,
            "response_ids": response_ids[:],
        }

        if reason == "max_output_tokens" and response.get("id"):
            previous_response_id = str(response["id"])
            current_input = "Continue exactly where you left off. Do not restart."
            continue

        break

    return "\n".join(all_text).strip(), metadata, raw_items



def classify_firm(
    api_key: str,
    model: str,
    system: str,
    firm: Dict[str, str],
    csv_columns: List[str],
    recent_markdown_excerpt: str,
    max_retries: int,
    max_continuations: int,
    max_tokens: int,
) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
    last_error: Optional[str] = None
    last_metadata: Dict[str, Any] = {}

    for attempt in range(1, max_retries + 1):
        prompt = build_user_prompt(
            firm,
            recent_markdown_excerpt=recent_markdown_excerpt,
            validation_error=last_error,
        )
        try:
            # Feed parse/validation failures back into the next attempt so the
            # model can repair structure while keeping the same firm context.
            text, metadata, raw_items = call_gpt(
                api_key=api_key,
                model=model,
                system=system,
                user_prompt=prompt,
                max_continuations=max_continuations,
                max_tokens=max_tokens,
            )
            last_metadata = metadata
            markdown, json_obj = extract_markdown_and_json(text)
            csv_row = normalize_csv_row(json_obj, csv_columns, firm)
            markdown = canonicalize_firm_markdown(markdown, firm, csv_row)
            return markdown, csv_row, {**metadata, "raw_items": raw_items}
        except Exception as e:  # noqa: BLE001
            last_error = str(e)
            print(f"  [attempt {attempt}/{max_retries}] Error: {last_error}", file=sys.stderr)
            if attempt == max_retries:
                break

    raise RuntimeError(
        f"Failed to classify {firm['id_str']} - {firm['firm_name']} after {max_retries} attempts. "
        f"Last error: {last_error}. Model: {last_metadata.get('model', 'NA')}"
    )



def main() -> int:
    args = parse_args()
    executed_at_utc = dt.datetime.utcnow()

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("ERROR: OPENAI_API_KEY is not set.", file=sys.stderr)
        return 2

    input_csv = Path(args.input_csv)
    instructions_md = Path(args.instructions_md)
    output_csv = Path(args.output_csv)
    output_md = Path(args.output_md)
    output_html = Path(args.output_html) if args.output_html else output_md.with_suffix(".html")
    raw_jsonl = Path(args.raw_jsonl)
    try:
        output_dir = validate_output_parent_layout(output_csv, output_md, raw_jsonl)
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    if not input_csv.exists():
        print(f"ERROR: input CSV not found: {input_csv}", file=sys.stderr)
        return 2
    if not instructions_md.exists():
        print(f"ERROR: instruction file not found: {instructions_md}", file=sys.stderr)
        return 2

    input_bytes = input_csv.read_bytes()
    instructions_text = instructions_md.read_text(encoding="utf-8")
    csv_columns = parse_csv_columns(instructions_text)
    md_file_header = build_md_file_header(executed_at_utc, args.model)
    fieldnames, firms = load_firms(input_csv)
    dupes = duplicate_ids(firms)
    if dupes:
        print(f"ERROR: input CSV contains duplicate id_str values: {', '.join(dupes[:10])}", file=sys.stderr)
        return 2
    try:
        manifest = validate_or_create_manifest(
            output_dir=output_dir,
            input_csv=input_csv,
            input_bytes=input_bytes,
            fieldnames=fieldnames,
            firms=firms,
            executed_at_utc=executed_at_utc,
            output_paths=[output_csv, output_md, raw_jsonl],
        )
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    ensure_output_files(output_csv, output_md, raw_jsonl, csv_columns, md_file_header)
    if args.start < 1 or args.start > len(firms):
        print(f"ERROR: --start must be between 1 and {len(firms)}", file=sys.stderr)
        return 2

    selected = firms[args.start - 1 :]
    if args.limit > 0:
        selected = selected[: args.limit]

    try:
        csv_done, md_done, raw_done = validate_existing_outputs(
            expected_firms=manifest["firms"],
            output_csv=output_csv,
            output_md=output_md,
            raw_jsonl=raw_jsonl,
        )
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    done = set() if args.overwrite else (csv_done & md_done & raw_done)
    system = instructions_text.strip() + build_output_contract(csv_columns)

    print(f"Provider: openai")
    print(f"Model: {args.model}")
    print(f"CSV columns: {csv_columns}")
    print(f"Loaded {len(firms)} firms; selected {len(selected)} from row {args.start}.")
    print(f"Output folder: {output_dir}")
    print(f"Already processed: {len(done)}")
    print(f"Existing output state: csv={len(csv_done)} md={len(md_done)} raw={len(raw_done)}")

    processed_now = 0
    skipped = 0

    for idx, firm in enumerate(selected, start=args.start):
        id_str = firm["id_str"]
        has_csv = id_str in csv_done
        has_md = id_str in md_done
        has_raw = id_str in raw_done
        if not args.overwrite and has_csv and has_md and has_raw:
            skipped += 1
            print(f"[{idx}] SKIP {id_str} {firm['firm_name']}")
            continue

        if not args.overwrite and (has_csv or has_md or has_raw):
            print(f"[{idx}] REPAIR {id_str} {firm['firm_name']} csv={int(has_csv)} md={int(has_md)} raw={int(has_raw)}")
        else:
            print(f"[{idx}] RUN  {id_str} {firm['firm_name']}")
        started = time.time()
        recent_markdown_excerpt = read_recent_markdown_excerpt(output_md)

        try:
            markdown, csv_row, raw_resp = classify_firm(
                api_key=api_key,
                model=args.model,
                system=system,
                firm=firm,
                csv_columns=csv_columns,
                recent_markdown_excerpt=recent_markdown_excerpt,
                max_retries=args.max_retries,
                max_continuations=args.max_continuations,
                max_tokens=args.max_tokens,
            )
        except RuntimeError as e:
            print(f"[{idx}] FAIL {id_str}: {e}", file=sys.stderr)
            continue

        if args.overwrite or not has_md:
            with output_md.open("a", encoding="utf-8") as f:
                f.write(markdown.strip() + "\n\n")
            md_done.add(id_str)
        if args.overwrite or not has_raw:
            with raw_jsonl.open("a", encoding="utf-8") as f:
                f.write(json.dumps({
                    "timestamp_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    "provider": "openai",
                    "firm": firm,
                    **raw_resp,
                }, ensure_ascii=False) + "\n")
            raw_done.add(id_str)
        if args.overwrite or not has_csv:
            with output_csv.open("a", encoding="utf-8", newline="") as f:
                csv.DictWriter(f, fieldnames=csv_columns).writerow(csv_row)
            csv_done.add(id_str)

        elapsed = time.time() - started
        processed_now += 1
        done.add(id_str)
        csv_summary = " | ".join(f"{k}={v}" for k, v in csv_row.items() if k not in {"id_str", "firm_name"})
        print(f"[{idx}] OK   {id_str} {csv_summary} in {elapsed:.1f}s")

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    print(
        f"\nCompleted. processed_now={processed_now}, skipped={skipped}, total_selected={len(selected)}\n"
        f"Outputs:\n- {output_csv}\n- {output_md}\n- {raw_jsonl}"
    )
    try:
        validate_final_outputs(output_csv, manifest["firms"])
        print(f"- validated exact firm match against {output_dir / MANIFEST_FILENAME}")
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: final output validation failed: {e}", file=sys.stderr)
        return 1
    try:
        export_markdown_file_to_html(output_md, output_html)
        print(f"- {output_html}")
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: HTML export failed: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
