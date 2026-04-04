from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Config location relative to this script: ../config/bridge_fee_table.json
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR.parent / "config" / "bridge_fee_table.json"
BACKUP_SUFFIX = ".bak"


ALLOWED_FEE_TYPES = {"flat", "percent", "percent_plus_flat"}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Import and validate bridge fee table (CSV or JSON)."
    )
    p.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to input CSV or JSON file containing fee table rows.",
    )
    p.add_argument(
        "--deploy",
        action="store_true",
        help="If set, write the validated table to the canonical config path.",
    )
    p.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Canonical config path to write (default: {DEFAULT_CONFIG_PATH})",
    )
    p.add_argument(
        "--pretty",
        action="store_true",
        help="Write pretty-printed JSON when deploying.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print summary but do not write any files (default).",
    )
    p.add_argument(
        "--backup",
        action="store_true",
        help="When deploying, create a timestamped backup of the old config.",
    )
    return p.parse_args()


def read_input(path: Path) -> List[Dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    lower = path.suffix.lower()
    if lower in {".json"}:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
            if not isinstance(data, list):
                raise ValueError("JSON input must be an array of objects (rows).")
            return data
    elif lower in {".csv", ".txt"}:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            rows = [dict(r) for r in reader]
            return rows
    else:
        # Try to parse as JSON first, then CSV as fallback
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, list):
                return data
        except Exception:
            pass
        # CSV fallback
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            rows = [dict(r) for r in reader]
            return rows


def to_decimal_or_none(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float)):
        return Decimal(str(v))
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return Decimal(s)
        except InvalidOperation:
            raise ValueError(f"Invalid decimal value: {v!r}")
    raise ValueError(f"Unsupported numeric type: {type(v)}")


def normalize_optional_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s.lower() if s != "" else None
    return str(v).strip().lower()


def validate_and_normalize_row(
    raw: Dict[str, Any], index: int
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Validate a raw row and return (normalized_row, warnings).
    Raises ValueError on fatal validation errors.
    """
    warnings: List[str] = []

    # Helper to read keys case-insensitively
    def get(k: str) -> Any:
        # Prefer exact key, else case-insensitive match
        if k in raw:
            return raw[k]
        for key in raw:
            if key.lower() == k.lower():
                return raw[key]
        return None

    src_chain = get("src_chain") or get("source_chain") or ""
    dst_chain = get("dst_chain") or get("dest_chain") or get("dst") or ""
    fee_type = get("fee_type") or get("type") or ""
    protocol = get("protocol_id") or get("protocol") or None
    token = get("token") or None

    src_chain_s = str(src_chain).strip().lower()
    dst_chain_s = str(dst_chain).strip().lower()
    fee_type_s = str(fee_type).strip().lower()
    protocol_s = normalize_optional_str(protocol)
    token_s = normalize_optional_str(token)

    if not src_chain_s:
        raise ValueError(f"row {index}: missing src_chain")
    if not dst_chain_s:
        raise ValueError(f"row {index}: missing dst_chain")
    if fee_type_s not in ALLOWED_FEE_TYPES:
        raise ValueError(
            f"row {index}: invalid fee_type {fee_type!r} (must be one of {ALLOWED_FEE_TYPES})"
        )

    # Parse numeric fields
    percent = to_decimal_or_none(get("percent"))
    flat = to_decimal_or_none(get("flat"))
    min_fee = to_decimal_or_none(get("min_fee"))
    max_fee = to_decimal_or_none(get("max_fee"))

    # Validate according to fee_type
    if fee_type_s == "percent":
        if percent is None:
            raise ValueError(
                f"row {index}: fee_type 'percent' requires 'percent' field"
            )
        if percent < 0 or percent > 1:
            raise ValueError(f"row {index}: percent must be between 0 and 1")
        # flat should be None or zero
        if flat is not None and flat != 0:
            warnings.append(f"row {index}: flat is ignored for fee_type 'percent'")

    elif fee_type_s == "flat":
        if flat is None:
            raise ValueError(f"row {index}: fee_type 'flat' requires 'flat' field")
        if flat < 0:
            raise ValueError(f"row {index}: flat must be >= 0")
        if percent is not None and percent != 0:
            warnings.append(f"row {index}: percent is ignored for fee_type 'flat'")

    elif fee_type_s == "percent_plus_flat":
        if percent is None:
            raise ValueError(
                f"row {index}: fee_type 'percent_plus_flat' requires 'percent' field"
            )
        if percent < 0 or percent > 1:
            raise ValueError(f"row {index}: percent must be between 0 and 1")
        if flat is None:
            # allow flat=0 implicitly
            flat = Decimal("0")
        if flat < 0:
            raise ValueError(f"row {index}: flat must be >= 0")

    # min/max validation
    if min_fee is not None and min_fee < 0:
        raise ValueError(f"row {index}: min_fee must be >= 0")
    if max_fee is not None and max_fee < 0:
        raise ValueError(f"row {index}: max_fee must be >= 0")
    if min_fee is not None and max_fee is not None and min_fee > max_fee:
        raise ValueError(f"row {index}: min_fee > max_fee")

    # last_updated: optional, if absent set to now in ISO format on deploy; keep raw if provided
    last_updated_raw = get("last_updated")
    last_updated_str: Optional[str] = None
    if last_updated_raw is not None and str(last_updated_raw).strip() != "":
        # try parse-ish; accept ISO-like strings
        try:
            # If it's parseable to datetime, reformat as ISO; else keep as string
            dt = datetime.fromisoformat(str(last_updated_raw).replace("Z", "+00:00"))
            last_updated_str = (
                dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            )
        except Exception:
            last_updated_str = str(last_updated_raw)

    # notes
    notes_raw = get("notes")
    notes = None if notes_raw is None else str(notes_raw).strip()

    normalized = {
        "protocol_id": protocol_s,
        "src_chain": src_chain_s,
        "dst_chain": dst_chain_s,
        "token": token_s,
        "fee_type": fee_type_s,
        "percent": float(percent) if percent is not None else None,
        "flat": float(flat) if flat is not None else None,
        "min_fee": float(min_fee) if min_fee is not None else None,
        "max_fee": float(max_fee) if max_fee is not None else None,
        "last_updated": last_updated_str,
        "notes": notes,
    }

    return normalized, warnings


def validate_rows(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    normalized_rows: List[Dict[str, Any]] = []
    warnings: List[str] = []
    for i, raw in enumerate(rows):
        try:
            nr, ws = validate_and_normalize_row(raw, i)
            normalized_rows.append(nr)
            warnings.extend(ws)
        except Exception as exc:
            raise ValueError(f"Validation error at row {i}: {exc}") from exc
    return normalized_rows, warnings


def backup_file(path: Path) -> Optional[Path]:
    if not path.exists():
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bak = path.with_name(path.name + BACKUP_SUFFIX + "." + ts)
    path.replace(bak)
    return bak


def write_config(path: Path, rows: List[Dict[str, Any]], pretty: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if pretty:
        text = json.dumps(rows, indent=2, ensure_ascii=False)
    else:
        text = json.dumps(rows, separators=(",", ":"), ensure_ascii=False)
    path.write_text(text, encoding="utf-8")


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)

    try:
        raw_rows = read_input(input_path)
    except Exception as exc:
        print(f"ERROR: failed to read input file: {exc}", file=sys.stderr)
        return 2

    if not raw_rows:
        print("ERROR: input contained no rows", file=sys.stderr)
        return 3

    try:
        normalized, warnings = validate_rows(raw_rows)
    except Exception as exc:
        print(f"ERROR: validation failed: {exc}", file=sys.stderr)
        return 4

    # Fill last_updated when missing
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    for r in normalized:
        if not r.get("last_updated"):
            r["last_updated"] = now_iso

    # Summarize
    print(f"Validated {len(normalized)} row(s).")
    if warnings:
        print("Warnings:")
        for w in warnings:
            print("  -", w)

    if args.dry_run and not args.deploy:
        print("Dry-run complete. Nothing written.")
        return 0

    if args.deploy:
        cfg_path = Path(args.config)
        try:
            if args.backup and cfg_path.exists():
                bak = backup_file(cfg_path)
                print(f"Backed up existing config to {bak}")
            write_config(cfg_path, normalized, pretty=args.pretty)
            print(f"Wrote {len(normalized)} entries to {cfg_path}")
            return 0
        except Exception as exc:
            print(f"ERROR: failed to write config file: {exc}", file=sys.stderr)
            return 5

    # If not deploy and not dry-run (shouldn't happen due to flags), just exit
    print("No action taken (neither --deploy nor --dry-run set).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
