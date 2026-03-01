"""Publish step for SOU forarbeten: norm -> published."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date
from logging import getLogger
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - exercised only when PyYAML is missing
    yaml = None

try:
    import jsonschema  # type: ignore
except Exception:  # pragma: no cover - exercised only when jsonschema is missing
    jsonschema = None


logger = getLogger("paragrafenai.noop")

DEFAULT_CONFIG_PATH = "config/pipeline_config.yaml"
DEFAULT_CONFIG = {
    "norm_dir": "data/norm/forarbete/sou",
    "published_dir": "data/published/forarbete/sou",
    "schema_path": "schemas/chunk_metadata_schema.json",
    "idempotency_strategy": "sha256",
    "log_level": "noop",
    "batch_size": 100,
}
PARAGRAFEN_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
FILENAME_SOU_RE = re.compile(
    r"[Ss][Oo][Uu][\s_-]?(\d{4})[\s:_-](\d+[a-zA-Z]?)", re.IGNORECASE
)
SOU_RE = re.compile(
    r"(?i)\b(?:SOU[\s_-]*)?(\d{4})[\s:_-]*(\d+)([a-zA-Z]?)(?:\s+(.*))?$"
)


class PublishError(Exception):
    """Base class for publish exceptions."""


class ExtractionError(PublishError):
    """Raised when required metadata extraction fails."""


class ValidationError(PublishError):
    """Raised when front_matter validation fails."""


class PublishPartialFailureError(PublishError):
    """Raised when processing completes with one or more failures."""

    def __init__(self, message: str, results: dict[str, int]) -> None:
        super().__init__(message)
        self.results = results


class PublishAbortError(PublishError):
    """Raised when failure threshold is exceeded."""

    def __init__(self, message: str, results: dict[str, int]) -> None:
        super().__init__(message)
        self.results = results


@dataclass
class Config:
    norm_dir: Path
    published_dir: Path
    schema_path: Path
    batch_size: int = 100
    idempotency_strategy: str = "sha256"
    log_level: str = "noop"


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    """Minimal YAML parser for simple nested key-value config files."""
    data: dict[str, Any] = {}
    current_section: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if not line.startswith(" "):
            if not line.endswith(":"):
                continue
            current_section = line[:-1].strip()
            data[current_section] = {}
            continue
        if current_section is None:
            continue
        stripped = line.strip()
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if value.isdigit():
            parsed: Any = int(value)
        elif value.lower() in {"true", "false"}:
            parsed = value.lower() == "true"
        else:
            parsed = value
        data[current_section][key] = parsed
    return data


def _default_config_yaml() -> str:
    return (
        "forarbete_publish:\n"
        '  norm_dir: "data/norm/forarbete/sou"\n'
        '  published_dir: "data/published/forarbete/sou"\n'
        '  schema_path: "schemas/chunk_metadata_schema.json"\n'
        '  idempotency_strategy: "sha256"\n'
        '  log_level: "noop"\n'
        "  batch_size: 100\n"
    )


def ensure_default_config(config_path: Path) -> None:
    if config_path.exists():
        return
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(_default_config_yaml(), encoding="utf-8")
    logger.info("Created default config at %s", config_path)


def _resolve_path(path_str: str, base_dir: Path) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def load_config(config_path_arg: str | None = None) -> tuple[Config, Path]:
    raw_path = config_path_arg or os.environ.get("PARAGRAFEN_CONFIG", DEFAULT_CONFIG_PATH)
    config_path = Path(raw_path).resolve()
    ensure_default_config(config_path)

    text = config_path.read_text(encoding="utf-8")
    if yaml is not None:
        loaded = yaml.safe_load(text) or {}
    else:
        loaded = _parse_simple_yaml(text)

    section = loaded.get("forarbete_publish", {})
    merged = dict(DEFAULT_CONFIG)
    merged.update(section)

    config_dir = config_path.parent
    cfg = Config(
        norm_dir=_resolve_path(str(merged["norm_dir"]), config_dir),
        published_dir=_resolve_path(str(merged["published_dir"]), config_dir),
        schema_path=_resolve_path(str(merged["schema_path"]), config_dir),
        batch_size=int(merged.get("batch_size", 100)),
        idempotency_strategy=str(merged.get("idempotency_strategy", "sha256")),
        log_level=str(merged.get("log_level", "noop")),
    )
    return cfg, config_path


def canonicalize_sou_number(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        raise ExtractionError("Empty SOU number")

    cleaned = raw.replace("_", " ").replace(":", " ").replace("-", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    match = SOU_RE.search(cleaned)
    if not match:
        raise ExtractionError(f"Could not canonicalize SOU number: {value!r}")

    year = match.group(1)
    seq_digits = match.group(2)
    suffix = match.group(3) or ""
    tail = (match.group(4) or "").strip()

    seq_normalized = str(int(seq_digits))
    canonical = f"SOU {year}:{seq_normalized}{suffix}"
    if tail:
        canonical = f"{canonical} {tail}"
    return canonical


def extract_sou_number(doc: dict[str, Any], filename: str) -> str:
    metadata = doc.get("metadata", {}) if isinstance(doc, dict) else {}
    for key in ("sou_number", "beteckning"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return canonicalize_sou_number(value)

    file_match = FILENAME_SOU_RE.search(Path(filename).name)
    if file_match:
        year = file_match.group(1)
        seq_with_suffix = file_match.group(2)
        seq_match = re.match(r"(\d+)([a-zA-Z]?)", seq_with_suffix)
        if not seq_match:
            raise ExtractionError(f"Invalid SOU sequence in filename: {filename}")
        seq_num = str(int(seq_match.group(1)))
        seq_suffix = seq_match.group(2)
        return f"SOU {year}:{seq_num}{seq_suffix}"

    raise ExtractionError(f"Could not extract SOU number from metadata or filename: {filename}")


def extract_title(doc: dict[str, Any], sou_number: str) -> str:
    metadata = doc.get("metadata", {}) if isinstance(doc, dict) else {}
    for key in ("title", "titel"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    front_page_text = doc.get("front_page_text")
    if isinstance(front_page_text, str) and front_page_text.strip():
        first_line = front_page_text.split("\n", 1)[0].strip()
        if first_line:
            return first_line[:500]

    fallback = f"[Titel saknas — {sou_number}]"
    logger.warning("Missing title for %s, using fallback", sou_number)
    return fallback


def generate_source_id(sou_number: str) -> str:
    return str(uuid.uuid5(PARAGRAFEN_NAMESPACE, f"forarbete::sou::{sou_number}"))


def sha256_of_norm(doc: dict[str, Any]) -> str:
    canonical = json.dumps(doc, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_front_matter(doc: dict[str, Any], filename: str, published_at: str | None = None) -> dict[str, Any]:
    sou_number = extract_sou_number(doc, filename)
    year_match = re.search(r"\b(\d{4})\b", sou_number)
    if not year_match:
        raise ExtractionError(f"Could not derive year from SOU number: {sou_number}")
    year = int(year_match.group(1))
    title = extract_title(doc, sou_number)
    published_date = published_at or date.today().isoformat()

    return {
        "source_id": generate_source_id(sou_number),
        "source_type": "forarbete",
        "document_subtype": "sou",
        "sou_number": sou_number,
        "title": title,
        "year": year,
        "authority_level": "preparatory",
        "published_at": published_date,
        "publish_version": "1.0",
        "norm_sha256": sha256_of_norm(doc),
    }


def _validate_front_matter_fallback(front_matter: dict[str, Any], schema: dict[str, Any]) -> None:
    required = schema.get("required", [])
    for key in required:
        if key not in front_matter:
            raise ValidationError(f"Missing required field: {key}")

    properties = schema.get("properties", {})
    type_map = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "object": dict,
        "array": list,
    }
    for key, rules in properties.items():
        if key not in front_matter:
            continue
        value = front_matter[key]
        expected_type = rules.get("type")
        if expected_type in type_map and not isinstance(value, type_map[expected_type]):
            raise ValidationError(f"Field {key!r} expected type {expected_type}, got {type(value).__name__}")
        enum = rules.get("enum")
        if enum and value not in enum:
            raise ValidationError(f"Field {key!r} has invalid value {value!r}")


def validate_front_matter(front_matter: dict[str, Any], schema: dict[str, Any]) -> None:
    try:
        if jsonschema is not None:
            jsonschema.validate(instance=front_matter, schema=schema)
        else:
            _validate_front_matter_fallback(front_matter, schema)
    except Exception as exc:
        raise ValidationError(str(exc)) from exc


def load_schema(schema_path: Path) -> dict[str, Any]:
    return json.loads(schema_path.read_text(encoding="utf-8"))


def _chunked(items: list[Path], size: int) -> list[list[Path]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _load_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _should_skip_by_idempotency(target_path: Path, norm_sha: str) -> bool:
    if not target_path.exists():
        return False
    try:
        existing = _load_json_file(target_path)
        existing_sha = (
            existing.get("front_matter", {}).get("norm_sha256")
            if isinstance(existing, dict)
            else None
        )
        return existing_sha == norm_sha
    except Exception:
        return False


def process_document(
    source_path: Path,
    target_path: Path,
    schema: dict[str, Any],
    dry_run: bool = False,
    force: bool = False,
) -> str:
    doc = _load_json_file(source_path)
    front_matter = build_front_matter(doc, source_path.name)
    validate_front_matter(front_matter, schema)
    output = {"front_matter": front_matter, "document": doc}

    if not force and _should_skip_by_idempotency(target_path, front_matter["norm_sha256"]):
        logger.debug("Skipped unchanged: %s", source_path.name)
        return "skipped"

    action = "created" if not target_path.exists() else "updated"
    if dry_run:
        logger.info("Dry-run %s: %s", action, target_path)
        return action

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("%s: %s", action, target_path.name)
    return action


def publish_forarbete(
    config_path: str | None = None,
    dry_run: bool = False,
    sample: int | None = None,
    force: bool = False,
) -> dict[str, int]:
    cfg, _ = load_config(config_path)
    schema = load_schema(cfg.schema_path)

    if not cfg.norm_dir.exists():
        raise FileNotFoundError(f"norm_dir does not exist: {cfg.norm_dir}")

    norm_files = sorted(cfg.norm_dir.rglob("*.json"))
    if sample is not None:
        if sample < 0:
            raise ValueError("sample must be >= 0")
        if sample < len(norm_files):
            norm_files = random.sample(norm_files, sample)
            norm_files = sorted(norm_files)

    if not dry_run:
        cfg.published_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "total": len(norm_files),
        "created": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
    }

    batches = _chunked(norm_files, max(cfg.batch_size, 1))
    for batch in batches:
        for source_path in batch:
            try:
                relative = source_path.relative_to(cfg.norm_dir)
                target_path = cfg.published_dir / relative
                status = process_document(
                    source_path=source_path,
                    target_path=target_path,
                    schema=schema,
                    dry_run=dry_run,
                    force=force,
                )
                results[status] += 1
            except (ValidationError, ExtractionError, json.JSONDecodeError, FileNotFoundError) as exc:
                results["failed"] += 1
                logger.error("Failed processing %s: %s", source_path, exc)
            except Exception as exc:  # pragma: no cover - safety net
                results["failed"] += 1
                logger.error("Unexpected failure in %s: %s", source_path, exc)

            if results["failed"] > 100:
                logger.critical("Aborting publish run: failure threshold exceeded (%s)", results["failed"])
                raise PublishAbortError("Exceeded failure threshold (>100)", results)

    if results["failed"] > 0:
        raise PublishPartialFailureError("Publish completed with partial failures", results)
    return results


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish SOU documents from norm to published.")
    parser.add_argument("--config", type=str, default=None, help="Path to pipeline_config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing output files")
    parser.add_argument("--sample", type=int, default=None, help="Process a random sample of files")
    parser.add_argument("--force", action="store_true", help="Write files even if idempotency hash matches")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        publish_forarbete(
            config_path=args.config,
            dry_run=args.dry_run,
            sample=args.sample,
            force=args.force,
        )
        return 0
    except PublishPartialFailureError:
        return 1
    except PublishAbortError:
        return 2


if __name__ == "__main__":
    sys.exit(main())

