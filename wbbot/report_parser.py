from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from pathlib import Path
from typing import Any

import pandas as pd


METRIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "sales": ("реализ", "продаж", "выручк"),
    "payout_goods": ("к перечислению за товар", "перечислению за товар", "к перечислению"),
    "fines": ("штраф", "неустой"),
    "storage": ("хранени"),
    "logistics": ("логист", "доставк", "перевоз"),
    "deductions": ("удержан", "прочие выплаты", "прочие удержания", "компенсац"),
    "total_payment": ("итого к оплате", "к оплате", "итого к перечислению", "итого"),
    "tax": ("налог",),
    "cost_price": ("себестоим",),
}

DATE_PATTERN = re.compile(r"(\d{2}\.\d{2}\.\d{4})")


@dataclass
class ParsedReport:
    date_start: datetime | None
    date_end: datetime | None
    metrics: dict[str, float]
    notes: list[str]


def _norm(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip().lower())


def _to_number(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().replace("\xa0", " ").replace(" ", "")
    text = text.replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _extract_period(frames: list[pd.DataFrame]) -> tuple[datetime | None, datetime | None]:
    dates: list[datetime] = []
    for df in frames:
        for col in df.columns:
            col_n = _norm(col)
            if "дата" in col_n or "period" in col_n:
                parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                dates.extend([d.to_pydatetime() for d in parsed.dropna().tolist()])

        top = df.head(10).astype(str)
        for value in top.to_numpy().flatten():
            for match in DATE_PATTERN.findall(value):
                try:
                    dates.append(datetime.strptime(match, "%d.%m.%Y"))
                except ValueError:
                    pass

    if not dates:
        return None, None
    return min(dates), max(dates)


def _extract_metrics(frames: list[pd.DataFrame]) -> dict[str, float]:
    candidates: dict[str, list[float]] = {k: [] for k in METRIC_KEYWORDS}

    for df in frames:
        clean = df.copy()
        clean.columns = [_norm(c) for c in clean.columns]
        clean = clean.dropna(how="all")

        # Column-based extraction (best for detailed reports)
        for col in clean.columns:
            if col.startswith("unnamed"):
                continue
            for metric, keys in METRIC_KEYWORDS.items():
                if any(k in col for k in keys):
                    numeric = clean[col].map(_to_number).dropna()
                    if not numeric.empty:
                        candidates[metric].append(float(numeric.sum()))

        # Row-based extraction (best for summary tables)
        for _, row in clean.iterrows():
            string_cells = [_norm(v) for v in row.values if isinstance(v, str)]
            row_text = " ".join(string_cells)
            numeric_vals = [_to_number(v) for v in row.values]
            nums = [v for v in numeric_vals if v is not None]
            if not nums:
                continue

            for metric, keys in METRIC_KEYWORDS.items():
                if any(k in row_text for k in keys):
                    candidates[metric].append(max(nums, key=lambda x: abs(x)))

    result: dict[str, float] = {}
    for metric, values in candidates.items():
        if not values:
            continue
        # Prefer largest by absolute value to avoid double-counting across sheets.
        result[metric] = max(values, key=lambda x: abs(x))
    return result


def _build_notes(metrics: dict[str, float]) -> list[str]:
    notes: list[str] = []
    sales = metrics.get("sales")
    fines = metrics.get("fines")
    net = metrics.get("net_profit")

    if sales and fines and abs(fines) / abs(sales) > 0.05:
        notes.append("Высокие штрафы: более 5% от продаж.")
    if net is not None and net < 0:
        notes.append("Отрицательная чистая прибыль — проверьте удержания и себестоимость.")
    if not notes:
        notes.append("Критичных отклонений не обнаружено.")
    return notes


def parse_wb_report(path: str | Path) -> ParsedReport:
    xls = pd.ExcelFile(path)
    frames = [pd.read_excel(xls, sheet_name=sheet) for sheet in xls.sheet_names]

    start, end = _extract_period(frames)
    metrics = _extract_metrics(frames)

    if "net_profit" not in metrics:
        if "total_payment" in metrics:
            metrics["net_profit"] = (
                metrics["total_payment"]
                - metrics.get("tax", 0.0)
                - metrics.get("cost_price", 0.0)
            )
        else:
            metrics["net_profit"] = (
                metrics.get("payout_goods", 0.0)
                - metrics.get("fines", 0.0)
                - metrics.get("storage", 0.0)
                - metrics.get("logistics", 0.0)
                - metrics.get("deductions", 0.0)
                - metrics.get("tax", 0.0)
                - metrics.get("cost_price", 0.0)
            )

    notes = _build_notes(metrics)
    return ParsedReport(date_start=start, date_end=end, metrics=metrics, notes=notes)
