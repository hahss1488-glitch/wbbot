from __future__ import annotations

from io import BytesIO

import pandas as pd


class ValidationError(ValueError):
    pass


def _read_table(file_bytes: bytes, filename: str) -> pd.DataFrame:
    lower = filename.lower()
    bio = BytesIO(file_bytes)
    if lower.endswith(".csv"):
        return pd.read_csv(bio)
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return pd.read_excel(bio)
    raise ValidationError("Поддерживаются только CSV/XLSX файлы")


def parse_speeds(file_bytes: bytes, filename: str) -> list[dict]:
    df = _read_table(file_bytes, filename)
    expected = ["region_code", "region_name", "warehouse_id", "warehouse_name", "time_hours"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValidationError(f"В файле speeds не хватает колонок: {missing}")

    out = []
    for i, row in df.iterrows():
        time = row["time_hours"]
        if pd.isna(time):
            time = float("inf")
        try:
            time = float(time)
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(f"Строка {i + 2}: некорректное time_hours={time}") from exc
        if time <= 0:
            raise ValidationError(f"Строка {i + 2}: time_hours должен быть > 0")
        out.append(
            {
                "region_code": str(row["region_code"]).strip(),
                "region_name": str(row["region_name"]).strip(),
                "warehouse_id": int(row["warehouse_id"]),
                "warehouse_name": str(row["warehouse_name"]).strip(),
                "time_hours": time,
            }
        )

    if not out:
        raise ValidationError("Файл speeds пуст")
    return out


def parse_sales(file_bytes: bytes, filename: str) -> list[dict]:
    df = _read_table(file_bytes, filename)
    expected = ["region_code", "orders"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValidationError(f"В файле sales не хватает колонок: {missing}")

    out = []
    for i, row in df.iterrows():
        try:
            orders = float(row["orders"])
        except Exception as exc:  # noqa: BLE001
            raise ValidationError(f"Строка {i + 2}: orders должен быть числом") from exc
        if orders < 0:
            raise ValidationError(f"Строка {i + 2}: orders должен быть >= 0")
        out.append({"region_code": str(row["region_code"]).strip(), "orders": orders})

    if not out:
        raise ValidationError("Файл sales пуст")
    return out
