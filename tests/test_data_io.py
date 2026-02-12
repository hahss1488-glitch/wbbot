import pandas as pd
import pytest

from bot.data_io import ValidationError, parse_speeds


def test_parse_speeds_rejects_unknown_format():
    df = pd.DataFrame([{"a": 1, "b": 2}])
    with pytest.raises(ValidationError):
        parse_speeds(df.to_csv(index=False).encode(), "bad.csv")


def test_parse_priority_wide_variants():
    df = pd.DataFrame(
        [
            {
                "region_name": "Москва",
                "1-й приоритет": "Коледино, 28 ч",
                "2-й приоритет": "Алексин 31.6",
            }
        ]
    )
    result = parse_speeds(df.to_csv(index=False).encode(), "priority.csv")
    assert result.detected_format == "priority_wide"
    assert len(result.records) == 2
    assert result.records[0]["time_hours"] == 28.0
    assert result.records[1]["time_hours"] == 31.6
