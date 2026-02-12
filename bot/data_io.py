from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pandas as pd


class ValidationError(ValueError):
    pass


PRIORITY_RE_PRIMARY = re.compile(r"(.+?)\s*[,;–-]\s*([0-9]+(?:[.,][0-9]+)?)\s*(?:ч|час|h)?", re.IGNORECASE)
PRIORITY_RE_FALLBACK = re.compile(r"(.+?)\s+([0-9]+(?:[.,][0-9]+)?)")
PRIORITY_COL_RE = re.compile(r"^\s*\d+\s*[-–]?[йя]?\s*приоритет", re.IGNORECASE)


@dataclass
class ParseIssue:
    row: int
    column: str
    value: str
    problem: str


@dataclass
class ParseResult:
    records: list[dict]
    detected_format: str
    sheet_name: str | None
    preview_rows: list[dict]
    issues: list[ParseIssue]


def _slugify(value: str) -> str:
    norm = unicodedata.normalize("NFKD", value)
    ascii_text = norm.encode("ascii", "ignore").decode("ascii").lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", ascii_text).strip("_")
    return cleaned or "item"


def _unique_slug(base: str, seen: set[str]) -> str:
    if base not in seen:
        seen.add(base)
        return base
    i = 1
    while f"{base}_{i}" in seen:
        i += 1
    candidate = f"{base}_{i}"
    seen.add(candidate)
    return candidate


def _read_table(file_bytes: bytes, filename: str) -> tuple[pd.DataFrame, str | None]:
    lower = filename.lower()
    bio = BytesIO(file_bytes)
    if lower.endswith(".csv"):
        return pd.read_csv(bio), None
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        sheets: dict[str, pd.DataFrame] = pd.read_excel(bio, sheet_name=None)
        if not sheets:
            raise ValidationError("Excel файл не содержит листов")
        chosen = "result" if "result" in {k.lower(): k for k in sheets}.keys() else next(iter(sheets.keys()))
        if chosen != "result":
            for key in sheets:
                if key.lower() == "result":
                    chosen = key
                    break
        return sheets[chosen], chosen
    raise ValidationError("Поддерживаются только CSV/XLSX файлы")


def _normalize_long(df: pd.DataFrame) -> tuple[list[dict], list[ParseIssue], str]:
    expected = ["region_code", "region_name", "warehouse_id", "warehouse_name", "time_hours"]
    if not all(c in df.columns for c in expected):
        raise ValidationError("not_long")
    issues: list[ParseIssue] = []
    out: list[dict] = []
    for i, row in df.iterrows():
        time = row["time_hours"]
        parsed_time: float | None
        if pd.isna(time):
            parsed_time = None
        else:
            try:
                parsed_time = float(str(time).replace(",", "."))
                if parsed_time <= 0:
                    issues.append(ParseIssue(i + 2, "time_hours", str(time), "time_hours <= 0, сохранено как NULL"))
                    parsed_time = None
            except Exception:  # noqa: BLE001
                issues.append(ParseIssue(i + 2, "time_hours", str(time), "не удалось распарсить time_hours, сохранено как NULL"))
                parsed_time = None
        out.append(
            {
                "region_code": str(row["region_code"]).strip(),
                "region_name": str(row["region_name"]).strip(),
                "warehouse_id": str(row["warehouse_id"]).strip(),
                "warehouse_name": str(row["warehouse_name"]).strip(),
                "time_hours": parsed_time,
            }
        )
    return out, issues, "long"


def _parse_priority_cell(value: object) -> tuple[str | None, float | None]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, None
    txt = str(value).strip()
    if not txt:
        return None, None
    m = PRIORITY_RE_PRIMARY.search(txt) or PRIORITY_RE_FALLBACK.search(txt)
    if not m:
        return txt, None
    name = m.group(1).strip()
    hours = float(m.group(2).replace(",", "."))
    if hours <= 0:
        return name, None
    return name, hours


def _normalize_priority_wide(df: pd.DataFrame) -> tuple[list[dict], list[ParseIssue], str]:
    region_col = "region_name" if "region_name" in df.columns else df.columns[0]
    priority_cols = [c for c in df.columns if PRIORITY_COL_RE.search(str(c))]
    if not priority_cols:
        raise ValidationError("not_priority")

    issues: list[ParseIssue] = []
    rows: list[dict] = []
    for i, row in df.iterrows():
        region_name = str(row[region_col]).strip()
        if not region_name:
            continue
        for col in priority_cols:
            wh_name, hours = _parse_priority_cell(row[col])
            if wh_name is None:
                continue
            if hours is None:
                issues.append(ParseIssue(i + 2, str(col), str(row[col]), "не извлечено время"))
            rows.append(
                {
                    "region_code": None,
                    "region_name": region_name,
                    "warehouse_id": None,
                    "warehouse_name": wh_name,
                    "time_hours": hours,
                }
            )
    if not rows:
        raise ValidationError("Не удалось извлечь данные из колонок приоритетов")
    return rows, issues, "priority_wide"


def _normalize_wide_matrix(df: pd.DataFrame) -> tuple[list[dict], list[ParseIssue], str]:
    if len(df.columns) < 2:
        raise ValidationError("not_wide")
    region_col = "region_name" if "region_name" in df.columns else df.columns[0]
    value_cols = [c for c in df.columns if c != region_col]
    melted = df.melt(id_vars=[region_col], value_vars=value_cols, var_name="warehouse_name", value_name="time_hours")

    rows: list[dict] = []
    issues: list[ParseIssue] = []
    for i, row in melted.iterrows():
        region_name = str(row[region_col]).strip()
        warehouse_name = str(row["warehouse_name"]).strip()
        if not region_name or not warehouse_name:
            continue
        raw_time = row["time_hours"]
        hours: float | None
        if pd.isna(raw_time) or str(raw_time).strip() == "":
            hours = None
        else:
            try:
                hours = float(str(raw_time).replace(",", "."))
                if hours <= 0:
                    issues.append(ParseIssue(i + 2, "time_hours", str(raw_time), "time_hours <= 0, сохранено как NULL"))
                    hours = None
            except Exception:  # noqa: BLE001
                issues.append(ParseIssue(i + 2, "time_hours", str(raw_time), "нечисловое время, сохранено как NULL"))
                hours = None
        rows.append(
            {
                "region_code": None,
                "region_name": region_name,
                "warehouse_id": None,
                "warehouse_name": warehouse_name,
                "time_hours": hours,
            }
        )
    if not rows:
        raise ValidationError("Не удалось извлечь данные из wide-матрицы")
    return rows, issues, "wide_matrix"


def _finalize_records(rows: list[dict]) -> list[dict]:
    region_seen: set[str] = set()
    wh_seen: set[str] = set()
    region_map: dict[str, str] = {}
    wh_map: dict[str, str] = {}

    for row in rows:
        rn = row["region_name"]
        wn = row["warehouse_name"]
        if rn not in region_map:
            region_map[rn] = _unique_slug(_slugify(rn), region_seen)
        if wn not in wh_map:
            wh_map[wn] = _unique_slug(_slugify(wn), wh_seen)

    for row in rows:
        if not row.get("region_code"):
            row["region_code"] = region_map[row["region_name"]]
        if not row.get("warehouse_id"):
            row["warehouse_id"] = wh_map[row["warehouse_name"]]
    return rows


def parse_speeds_file(filepath: str | Path) -> ParseResult:
    path = Path(filepath)
    content = path.read_bytes()
    return parse_speeds(content, path.name)


def parse_speeds(file_bytes: bytes, filename: str) -> ParseResult:
    df, sheet_name = _read_table(file_bytes, filename)
    try:
        rows, issues, fmt = _normalize_long(df)
    except ValidationError:
        try:
            rows, issues, fmt = _normalize_priority_wide(df)
        except ValidationError:
            try:
                rows, issues, fmt = _normalize_wide_matrix(df)
            except ValidationError as exc:
                preview = df.head(10).to_dict(orient="records")
                raise ValidationError(
                    "Не удалось определить формат. Ожидаю long, priority-wide или wide-matrix. "
                    f"Первые строки: {preview}"
                ) from exc

    records = _finalize_records(rows)
    preview_rows = records[:10]
    return ParseResult(records=records, detected_format=fmt, sheet_name=sheet_name, preview_rows=preview_rows, issues=issues)


def parse_sales(file_bytes: bytes, filename: str) -> list[dict]:
    df, _ = _read_table(file_bytes, filename)
    expected = ["region_code", "orders"]
    if "region_name" in df.columns and "region_code" not in df.columns:
        df["region_code"] = df["region_name"].map(lambda x: _slugify(str(x)))
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValidationError(f"В файле sales не хватает колонок: {missing}")

    out = []
    for i, row in df.iterrows():
        try:
            orders = int(float(row["orders"]))
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(f"Строка {i + 2}: orders должен быть числом") from exc
        if orders < 0:
            raise ValidationError(f"Строка {i + 2}: orders должен быть >= 0")
        out.append({"region_code": str(row["region_code"]).strip(), "orders": orders})
    if not out:
        raise ValidationError("Файл sales пуст")
    return out
