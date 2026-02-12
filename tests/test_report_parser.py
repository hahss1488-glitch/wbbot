from pathlib import Path

import pandas as pd

from wbbot.report_parser import parse_wb_report


def test_parse_wb_report_extracts_period_and_metrics(tmp_path: Path):
    df = pd.DataFrame(
        {
            "Дата": ["01.01.2026", "31.01.2026"],
            "Показатель": ["К перечислению за товар", "Штрафы"],
            "Сумма": [197_853.73, 1_500.00],
            "Реализовано": [219_312.00, None],
            "Хранение": [1_134.30, None],
            "Логистика": [33_424.56, None],
            "Удержания": [25_000.00, None],
            "Налог": [13_158.72, None],
            "Себестоимость": [92_470.00, None],
        }
    )

    file_path = tmp_path / "report.xlsx"
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Отчёт", index=False)

    report = parse_wb_report(file_path)

    assert report.date_start.strftime("%d.%m.%Y") == "01.01.2026"
    assert report.date_end.strftime("%d.%m.%Y") == "31.01.2026"
    assert report.metrics["sales"] == 219_312.00
    assert report.metrics["payout_goods"] == 197_853.73
    assert report.metrics["fines"] == 1_500.00
    assert round(report.metrics["net_profit"], 2) == 92_225.01
