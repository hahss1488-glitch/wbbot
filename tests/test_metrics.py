from bot.metrics import build_views, recommend_next


def test_recommendation_prefers_best_gain():
    rows = [
        {"region_code": "msk", "region_name": "Moscow", "warehouse_id": 1, "warehouse_name": "A", "time_hours": 10},
        {"region_code": "msk", "region_name": "Moscow", "warehouse_id": 2, "warehouse_name": "B", "time_hours": 5},
        {"region_code": "spb", "region_name": "SPB", "warehouse_id": 1, "warehouse_name": "A", "time_hours": 8},
        {"region_code": "spb", "region_name": "SPB", "warehouse_id": 2, "warehouse_name": "B", "time_hours": 20},
    ]
    active = {1}
    sales = [{"region_code": "msk", "orders": 100}, {"region_code": "spb", "orders": 100}]

    view = build_views(rows, active, sales)
    rec = recommend_next(view, active, top_n=1)[0]

    assert rec.warehouse_id == 2
    assert rec.marginal_abs > 0
