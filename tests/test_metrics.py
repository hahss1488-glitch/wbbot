from bot.metrics import build_views, recommend_next


def test_recommendation_with_zero_baseline_has_none_pct():
    rows = [
        {"region_code": "msk", "region_name": "Moscow", "warehouse_id": "a", "warehouse_name": "A", "time_hours": 10},
        {"region_code": "msk", "region_name": "Moscow", "warehouse_id": "b", "warehouse_name": "B", "time_hours": 5},
    ]
    active = set()
    sales = [{"region_code": "msk", "orders": 100}]
    view = build_views(rows, active, sales)
    rec = recommend_next(view, active, top_n=1)[0]
    assert rec.marginal_pct is None
    assert rec.marginal_abs > 0
