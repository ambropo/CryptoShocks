#!/usr/bin/env python3
"""Crypto news event analysis loop using OpenAI API + web search.

Mirrors the structure of GO_firm-GPT.py; implements event-instructions.md.
- All analysis instructions live in the instructions markdown file.
- PNGs are sent inline as base64 images (no Files API upload required).
- One markdown entry and one CSV row are appended per event.
- raw JSONL records share the same top-level schema as the firm companion script.
"""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
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
EVENT_IMAGE_MAX_WIDTH_PX = 840
EVENT_BULLET_ORDER = [
    "News",
    "Market impact",
    "Timing",
    "Confounds",
    "Narrative fit",
    "Sources",
]
ALLOWED_CATEGORIES = {"Matched", "Polluted", "Mismatch", "No Move"}
ALLOWED_CERTAINTY = {"1", "2", "3", "NA"}
ALLOWED_WINDOW = {"Agree", "Disagree", "NA"}


def format_model_label(model: str) -> str:
    if model.startswith("gpt-"):
        return "GPT-" + model[len("gpt-") :]
    return model


def build_md_file_header(executed_at_utc: dt.datetime, model: str) -> str:
    executed_str = executed_at_utc.strftime("%Y-%m-%d %H:%M UTC")
    model_label = format_model_label(model)
    return (
        "# Narrative Classification of Crypto News Events\n\n"
        "This document reports the results of a systematic narrative classification of cryptocurrency news events. "
        "For each event, the analysis determines whether a news headline is plausibly linked to the observed Bitcoin "
        "price move in the 2-hour window around the event time (T0, primary), with the 4-hour window used "
        "conditionally for validation, and assigns a category -- "
        "Matched, No Move, Mismatch, or Polluted -- together with a certainty score. Classification is performed by "
        f"a large language model ({model_label}) instructed via a structured prompt (`event_instructions.md`) and "
        "executed through the OpenAI API using the pipeline script `GO_event-GPT.py`. "
        "The full classification methodology, decision tree, certainty scale, and output schema are documented in "
        "the accompanying instructions file.\n\n"
        f"Executed: {executed_str} | Model: {model_label}\n\n"
        "---\n\n"
    )


def strip_md_file_header(text: str) -> str:
    if not text.startswith("# Narrative Classification of Crypto News Events\n"):
        return text
    marker = "\n---\n\n"
    idx = text.find(marker)
    if idx == -1:
        return text
    return text[idx + len(marker) :]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyse crypto news events using OpenAI API.")
    p.add_argument("--input-dir", default="LLMs/event-raw", help="Directory containing event PNG files")
    p.add_argument("--instructions-md", default="LLMs/event-instructions.md")
    p.add_argument("--output-csv", default="LLMs/event-GPT/event_dataset.csv")
    p.add_argument("--output-md", default="LLMs/event-GPT/event_report.md")
    p.add_argument("--output-html", default="", help="HTML export path (default: output-md with .html)")
    p.add_argument("--raw-jsonl", default="LLMs/event-GPT/event_raw.jsonl")
    p.add_argument("--model", default="gpt-5")
    p.add_argument("--start", type=int, default=1, help="1-based start index into sorted PNG list")
    p.add_argument("--limit", type=int, default=0, help="Max events to process (0 = all)")
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--max-continuations", type=int, default=5)
    p.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    p.add_argument("--sleep-seconds", type=float, default=1.0)
    p.add_argument("--overwrite", action="store_true", help="Re-analyse already-processed events")
    return p.parse_args()


def parse_csv_columns(instructions_text: str) -> List[str]:
    m = re.search(r"<!--\s*CSV_COLUMNS:\s*(.+?)\s*-->", instructions_text)
    if not m:
        raise ValueError("No <!-- CSV_COLUMNS: ... --> marker found in instructions.")
    return [c.strip() for c in m.group(1).split(",") if c.strip()]


def load_event_pngs(input_dir: Path) -> List[Path]:
    def sort_key(path: Path) -> tuple[int, int, str]:
        num_str = extract_event_number_from_filename(path)
        if num_str:
            return (0, int(num_str), path.name)
        return (1, 0, path.name)

    pngs = sorted(input_dir.glob("*.png"), key=sort_key)
    if not pngs:
        raise ValueError(f"No PNG files found in {input_dir}")
    return pngs


def existing_csv_numbers(output_csv: Path) -> set[str]:
    done: set[str] = set()
    if not output_csv.exists():
        return done
    with output_csv.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            num = (row.get("Event_Number") or "").strip()
            if num:
                done.add(num)
    return done


def existing_markdown_numbers(output_md: Path) -> set[str]:
    if not output_md.exists():
        return set()
    text = strip_md_file_header(output_md.read_text(encoding="utf-8"))
    return set(re.findall(r"^## Event #(\d+)", text, flags=re.MULTILINE))


def existing_raw_numbers(raw_jsonl: Path) -> set[str]:
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
            png_file = str(obj.get("png_file", "")).strip()
            if not png_file:
                continue
            num = extract_event_number_from_filename(Path(png_file))
            if num:
                done.add(num)
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
        "\n\nAfter completing the markdown report for an event, output a single ```json ... ``` block "
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
    png_name: str,
    png_markdown_path: str,
    recent_markdown_excerpt: str = "",
    validation_error: Optional[str] = None,
) -> str:
    lines = [
        f"PNG file: {png_name}",
        f"PNG markdown path: {png_markdown_path}",
        "Analyse this event following the instructions.",
    ]
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
            "Your previous output could not be parsed mechanically. Return the full "
            f"markdown report followed by a corrected JSON block. Parse issue: {validation_error}"
        )
    return "\n".join(lines)


def extract_markdown_and_json(text: str) -> Tuple[str, Dict[str, Any]]:
    m = re.search(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL | re.IGNORECASE)
    if m:
        return text[: m.start()].rstrip(), json.loads(m.group(1))

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


def normalize_csv_row(
    json_obj: Dict[str, Any], csv_columns: List[str], event_number_hint: str
) -> Dict[str, str]:
    return {col: str(json_obj.get(col, "")).strip() for col in csv_columns}


def parse_optional_return(raw_value: str, field_name: str) -> Optional[float]:
    value = raw_value.strip()
    if not value:
        return None
    if value.endswith("%"):
        raise ValueError(f"{field_name} must not include a percent sign.")
    normalized = value.replace("−", "-").replace("–", "-")
    try:
        return float(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a plain numeric value or blank.") from exc


def extract_event_number_from_filename(png_path: Path) -> str:
    m = re.search(r"(\d+)", png_path.stem)
    if not m:
        return ""
    raw = m.group(1)
    stripped = raw.lstrip("0")
    return stripped if stripped else raw


def extract_event_image_path(markdown: str) -> str:
    md_match = re.search(r"!\[[^\]]*\]\(([^)]+)\)", markdown)
    if md_match:
        return md_match.group(1).strip()
    html_match = re.search(r'<img\b[^>]*\bsrc="([^"]+)"', markdown, flags=re.IGNORECASE)
    if html_match:
        return html_match.group(1).strip()
    return ""


def normalize_event_bullets(lines: List[str]) -> List[str]:
    # The model sometimes wraps bullet text onto follow-on lines or reorders
    # sections. Collapse each bullet to one line and restore the canonical order
    # before appending to the cumulative markdown report.
    bullets: List[Tuple[str, str]] = []
    current_label = ""
    current_parts: List[str] = []

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line == "---":
            continue
        match = re.match(r"- \*\*(.+?)\*\*:\s*(.*)$", line)
        if match:
            if current_label:
                bullets.append((current_label, " ".join(current_parts).strip()))
            current_label = match.group(1).strip()
            current_parts = [match.group(2).strip()]
            continue
        if current_label:
            current_parts.append(line)

    if current_label:
        bullets.append((current_label, " ".join(current_parts).strip()))

    ordered_labels = [label for label in EVENT_BULLET_ORDER if any(label == seen for seen, _ in bullets)]
    ordered_labels.extend(label for label, _ in bullets if label not in EVENT_BULLET_ORDER)

    bullet_map = {label: content for label, content in bullets}
    normalized: List[str] = []
    for label in ordered_labels:
        content = re.sub(r"\s+", " ", bullet_map[label]).strip()
        normalized.append(f"- **{label}**: {content}")
    return normalized


def canonicalize_event_markdown(markdown: str) -> str:
    # Keep the persisted markdown mechanically uniform even when the model is
    # slightly inconsistent about spacing, separators, or image placement.
    lines = [
        line.rstrip()
        for line in markdown.strip().splitlines()
        if line.strip() not in {"---", "```", "```json"}
    ]
    if not lines:
        return ""

    heading = next((line.strip() for line in lines if line.strip().startswith("## Event #")), "")
    subtitle = next((line.strip() for line in lines if line.strip().startswith("**") and "Certainty:" in line), "")
    image_path = extract_event_image_path(markdown)
    image_number = ""
    if heading:
        match = re.search(r"Event #(\d+)", heading)
        if match:
            image_number = match.group(1)

    bullet_lines = normalize_event_bullets(lines)

    normalized_parts: List[str] = []
    if heading:
        normalized_parts.append(heading)
    if subtitle:
        normalized_parts.append(subtitle)
        normalized_parts.append("")
    if image_path:
        alt = f"Event #{image_number}" if image_number else "Event image"
        normalized_parts.append(
            f'<img src="{image_path}" alt="{alt}" style="max-width: {EVENT_IMAGE_MAX_WIDTH_PX}px; width: 100%; height: auto;" />'
        )
        normalized_parts.append("")
    if bullet_lines:
        normalized_parts.extend(bullet_lines)

    body = "\n".join(normalized_parts).strip()
    body = re.sub(
        r'(?m)^(\*\*.*?Certainty:.*?\*\*)\n+(<img src="[^"]+" alt="[^"]+" style="[^"]+" />)',
        r"\1\n\n\2",
        body,
    )
    body = re.sub(
        r'(?m)^(<img src="[^"]+" alt="[^"]+" style="[^"]+" />)\n+(?=- \*\*)',
        r"\1\n\n",
        body,
    )
    if body:
        body = body + "\n\n---"
    return body


def validate_markdown_entry(markdown: str, csv_row: Dict[str, str], event_number_hint: str) -> None:
    lines = [line.rstrip() for line in markdown.strip().splitlines() if line.strip() and line.strip() != "---"]
    if not lines:
        raise ValueError("Markdown report is empty.")

    heading = next((line for line in lines if line.startswith("## Event #")), "")
    heading_match = re.fullmatch(r"## Event #(\d+): .+", heading)
    if not heading_match:
        raise ValueError("Markdown heading must follow '## Event #N: Title'.")
    event_number = heading_match.group(1)
    if event_number_hint and event_number != event_number_hint:
        raise ValueError(f"Markdown event number {event_number} does not match expected {event_number_hint}.")


def validate_csv_row(csv_row: Dict[str, str], event_number_hint: str) -> None:
    # This validator only enforces rules that matter for downstream use of the
    # event dataset. It intentionally stays lighter-touch on prose formatting so
    # the run does not fail on cosmetic markdown variation.
    event_number = csv_row.get("Event_Number", "").strip()
    if not re.fullmatch(r"\d+", event_number):
        raise ValueError("Event_Number must be a non-empty integer.")
    if event_number_hint and event_number != event_number_hint:
        raise ValueError(f"Event_Number {event_number} does not match expected {event_number_hint}.")

    if not re.fullmatch(r"\d{2}[a-z]{3}\d{4}", csv_row.get("Date_GMT", "")):
        raise ValueError("Date_GMT must be formatted as DDmonYYYY.")
    if not re.fullmatch(r"\d{2}:\d{2}", csv_row.get("Time_T0_GMT", "")):
        raise ValueError("Time_T0_GMT must be formatted as HH:MM.")

    category = csv_row.get("Category", "")
    certainty = csv_row.get("Certainty", "")
    window = csv_row.get("Window", "")
    if category not in ALLOWED_CATEGORIES:
        raise ValueError(f"Category must be one of {sorted(ALLOWED_CATEGORIES)}.")
    if certainty not in ALLOWED_CERTAINTY:
        raise ValueError(f"Certainty must be one of {sorted(ALLOWED_CERTAINTY)}.")
    if window not in ALLOWED_WINDOW:
        raise ValueError(f"Window must be one of {sorted(ALLOWED_WINDOW)}.")

    return_2h = parse_optional_return(csv_row.get("Return_2h", ""), "Return_2h")
    return_4h = parse_optional_return(csv_row.get("Return_4h", ""), "Return_4h")

    if category == "Matched" and certainty == "NA":
        raise ValueError("Matched rows must have certainty 1, 2, or 3.")
    if category != "Matched" and certainty != "NA":
        raise ValueError("Non-Matched rows must have certainty NA.")

    if category != "Matched" and window != "NA":
        raise ValueError("Non-Matched rows must have Window = NA.")
    if category == "Matched" and return_4h is None and window != "NA":
        raise ValueError("Matched rows with missing Return_4h must have Window = NA.")
    if category == "Matched" and return_2h is not None and return_4h is not None:
        expected_window = "Agree" if return_2h * return_4h >= 0 else "Disagree"
        if window != expected_window:
            raise ValueError(f"Window must be {expected_window} given Return_2h and Return_4h.")

    if category == "No Move":
        if return_2h is not None and abs(return_2h) >= 0.1:
            raise ValueError("No Move requires |Return_2h| < 0.1 when Return_2h is available.")
        if return_2h is None and return_4h is not None and abs(return_4h) >= 0.1:
            raise ValueError("No Move requires |Return_4h| < 0.1 when Return_2h is unavailable.")

def validate_event_output(markdown: str, csv_row: Dict[str, str], event_number_hint: str) -> None:
    validate_csv_row(csv_row, event_number_hint)
    validate_markdown_entry(markdown, csv_row, event_number_hint)


def format_markdown_entry(markdown: str) -> str:
    """Ensure an event entry ends with exactly one separator block."""
    return markdown.strip() + "\n\n"


# ── HTML Export ───────────────────────────────────────────────────────────────

def markdown_to_html(markdown_text: str) -> str:
    """Convert markdown to HTML.
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


def export_markdown_file_to_html(output_md: Path, output_html: Path) -> None:
    output_html.parent.mkdir(parents=True, exist_ok=True)
    md_text = output_md.read_text(encoding="utf-8") if output_md.exists() else ""
    md_text = re.sub(
        r'(?m)^(<img src="[^"]+" alt="[^"]+" style="[^"]+" />)\n(?=- \*\*)',
        r"\1\n\n",
        md_text,
    )
    body = markdown_to_html(md_text)
    doc = (
        "<!doctype html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        f"  <title>{html.escape(output_md.stem)}</title>\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        "  <style>\n"
        "    body { max-width: 900px; margin: 2rem auto; padding: 0 1rem; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.5; }\n"
        f"    img {{ width: 100%; max-width: {EVENT_IMAGE_MAX_WIDTH_PX}px; height: auto; display: block; margin: 0.5rem 0 1rem; }}\n"
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


# ── API Call ──────────────────────────────────────────────────────────────────

def encode_png_b64(png_path: Path) -> str:
    """Return the PNG file as a base64-encoded string."""
    return base64.b64encode(png_path.read_bytes()).decode("ascii")


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
    image_b64: str,
    user_text: str,
    previous_response_id: Optional[str],
    ssl_context: Optional[ssl.SSLContext],
    max_tokens: int,
) -> Dict[str, Any]:
    last_error: Optional[Exception] = None

    for tool_type in ["web_search", "web_search_preview"]:
        input_content: List[Dict[str, Any]] = [
            {"type": "input_image", "image_url": f"data:image/png;base64,{image_b64}"},
            {"type": "input_text", "text": user_text},
        ]
        payload: Dict[str, Any] = {
            "model": model,
            "instructions": system,
            "input": [{"role": "user", "content": input_content}],
            "max_output_tokens": max_tokens,
            "tool_choice": "auto",
            "tools": [
                {
                    "type": tool_type,
                    "user_location": {
                        "type": "approximate",
                        "country": "US",
                        "city": "New York",
                        "region": "New York",
                        "timezone": "America/New_York",
                    },
                }
            ],
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
    image_b64: str,
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

    current_text = user_prompt
    previous_response_id: Optional[str] = None
    response_ids: List[str] = []

    for continuation in range(max_continuations):
        response = call_once_with_tool_variants(
            api_key=api_key,
            model=model,
            system=system,
            image_b64=image_b64,
            user_text=current_text,
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
            current_text = "Continue exactly where you left off. Do not restart."
            continue

        break

    return "\n".join(all_text).strip(), metadata, raw_items


def analyze_event(
    api_key: str,
    model: str,
    system: str,
    png_path: Path,
    png_markdown_path: str,
    csv_columns: List[str],
    event_number_hint: str,
    recent_markdown_excerpt: str,
    max_retries: int,
    max_continuations: int,
    max_tokens: int,
) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
    image_b64 = encode_png_b64(png_path)

    last_error: Optional[str] = None
    last_metadata: Dict[str, Any] = {}

    for attempt in range(1, max_retries + 1):
        prompt = build_user_prompt(
            png_path.name,
            png_markdown_path=png_markdown_path,
            recent_markdown_excerpt=recent_markdown_excerpt,
            validation_error=last_error,
        )
        try:
            # Retry with the previous validation error in the prompt so the model
            # can repair a malformed response without losing the original event
            # context or rerunning the whole batch manually.
            text, metadata, raw_items = call_gpt(
                api_key=api_key,
                model=model,
                system=system,
                image_b64=image_b64,
                user_prompt=prompt,
                max_continuations=max_continuations,
                max_tokens=max_tokens,
            )
            last_metadata = metadata
            markdown, json_obj = extract_markdown_and_json(text)
            csv_row = normalize_csv_row(json_obj, csv_columns, event_number_hint)
            markdown = canonicalize_event_markdown(markdown)
            validate_event_output(markdown, csv_row, event_number_hint)
            return markdown, csv_row, {**metadata, "raw_items": raw_items}
        except Exception as e:  # noqa: BLE001
            last_error = str(e)
            print(f"  [attempt {attempt}/{max_retries}] Error: {last_error}", file=sys.stderr)
            if attempt == max_retries:
                break

    raise RuntimeError(
        f"Failed to analyse event from {png_path.name} after {max_retries} attempts. "
        f"Last error: {last_error}. Model: {last_metadata.get('model', 'NA')}"
    )


def main() -> int:
    args = parse_args()
    executed_at_utc = dt.datetime.utcnow()

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("ERROR: OPENAI_API_KEY is not set.", file=sys.stderr)
        return 2

    input_dir = Path(args.input_dir)
    instructions_md = Path(args.instructions_md)
    output_csv = Path(args.output_csv)
    output_md = Path(args.output_md)
    output_html = Path(args.output_html) if args.output_html else output_md.with_suffix(".html")
    raw_jsonl = Path(args.raw_jsonl)

    if not input_dir.is_dir():
        print(f"ERROR: input directory not found: {input_dir}", file=sys.stderr)
        return 2
    if not instructions_md.exists():
        print(f"ERROR: instruction file not found: {instructions_md}", file=sys.stderr)
        return 2

    instructions_text = instructions_md.read_text(encoding="utf-8")
    csv_columns = parse_csv_columns(instructions_text)
    system = instructions_text.strip() + build_output_contract(csv_columns)
    md_file_header = build_md_file_header(executed_at_utc, args.model)
    ensure_output_files(output_csv, output_md, raw_jsonl, csv_columns, md_file_header)

    try:
        png_paths = load_event_pngs(input_dir)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    if args.start < 1 or args.start > len(png_paths):
        print(f"ERROR: --start must be between 1 and {len(png_paths)}", file=sys.stderr)
        return 2

    selected = png_paths[args.start - 1 :]
    if args.limit > 0:
        selected = selected[: args.limit]

    csv_done = existing_csv_numbers(output_csv)
    md_done = existing_markdown_numbers(output_md)
    raw_done = existing_raw_numbers(raw_jsonl)
    done = set() if args.overwrite else (csv_done & md_done & raw_done)

    print(f"Provider: openai")
    print(f"Model: {args.model}")
    print(f"CSV columns: {csv_columns}")
    print(f"Found {len(png_paths)} PNGs; selected {len(selected)} from index {args.start}.")
    print(f"Already processed: {len(done)}")
    print(f"Existing output state: csv={len(csv_done)} md={len(md_done)} raw={len(raw_done)}")

    processed_now = 0
    skipped = 0

    for idx, png_path in enumerate(selected, start=args.start):
        event_number_hint = extract_event_number_from_filename(png_path)
        has_csv = bool(event_number_hint and event_number_hint in csv_done)
        has_md = bool(event_number_hint and event_number_hint in md_done)
        has_raw = bool(event_number_hint and event_number_hint in raw_done)
        if event_number_hint and not args.overwrite and has_csv and has_md and has_raw:
            skipped += 1
            print(f"[{idx}] SKIP {png_path.name} (Event_Number={event_number_hint})")
            continue

        if event_number_hint and not args.overwrite and (has_csv or has_md or has_raw):
            print(f"[{idx}] REPAIR {png_path.name} csv={int(has_csv)} md={int(has_md)} raw={int(has_raw)}")
        else:
            print(f"[{idx}] RUN  {png_path.name}")
        started = time.time()
        recent_markdown_excerpt = read_recent_markdown_excerpt(output_md)
        png_markdown_path = os.path.relpath(png_path, output_md.parent)

        try:
            markdown, csv_row, raw_resp = analyze_event(
                api_key=api_key,
                model=args.model,
                system=system,
                png_path=png_path,
                png_markdown_path=png_markdown_path,
                csv_columns=csv_columns,
                event_number_hint=event_number_hint,
                recent_markdown_excerpt=recent_markdown_excerpt,
                max_retries=args.max_retries,
                max_continuations=args.max_continuations,
                max_tokens=args.max_tokens,
            )
        except RuntimeError as e:
            print(f"[{idx}] FAIL {png_path.name}: {e}", file=sys.stderr)
            continue

        if args.overwrite or not has_md:
            with output_md.open("a", encoding="utf-8") as f:
                f.write(format_markdown_entry(markdown))
            if event_number_hint:
                md_done.add(event_number_hint)
        if args.overwrite or not has_raw:
            with raw_jsonl.open("a", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "timestamp_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                            "provider": "openai",
                            "png_file": png_path.name,
                            **raw_resp,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            if event_number_hint:
                raw_done.add(event_number_hint)
        if args.overwrite or not has_csv:
            with output_csv.open("a", encoding="utf-8", newline="") as f:
                csv.DictWriter(f, fieldnames=csv_columns).writerow(csv_row)
            if event_number_hint:
                csv_done.add(event_number_hint)

        elapsed = time.time() - started
        processed_now += 1
        event_number = csv_row.get("Event_Number", "?")
        done.add(event_number)
        csv_summary = " | ".join(
            f"{k}={v}"
            for k, v in csv_row.items()
            if k not in {"News_Headline", "Narrative_Fit_Summary", "Sources_Searched"}
        )
        print(f"[{idx}] OK   {png_path.name} {csv_summary} in {elapsed:.1f}s")

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    print(
        f"\nCompleted. processed_now={processed_now}, skipped={skipped}, total_selected={len(selected)}\n"
        f"Outputs:\n- {output_csv}\n- {output_md}\n- {raw_jsonl}"
    )
    try:
        export_markdown_file_to_html(output_md, output_html)
        print(f"- {output_html}")
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: HTML export failed: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
