import pandas as pd
import pytest

from bot.data_io import ValidationError, parse_speeds


def test_parse_speeds_rejects_non_positive_time():
    df = pd.DataFrame(
        [{
            "region_code": "msk",
            "region_name": "Moscow",
            "warehouse_id": 1,
            "warehouse_name": "A",
            "time_hours": 0,
        }]
    )
    data = df.to_csv(index=False).encode()

    with pytest.raises(ValidationError):
        parse_speeds(data, "speeds.csv")
