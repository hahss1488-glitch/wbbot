from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass


@dataclass
class RegionView:
    code: str
    name: str
    weight: float
    old_time: float
    new_time: float


@dataclass
class Recommendation:
    warehouse_id: str
    warehouse_name: str
    marginal_abs: float
    marginal_pct: float | None
    coverage_pct: float
    global_speed_current: float
    weighted_avg_time_old: float
    weighted_avg_time_new: float
    weighted_avg_time_delta: float
    region_changes: list[RegionView]


def _weights(regions: set[str], sales: dict[str, float]) -> dict[str, float]:
    if sales and sum(sales.values()) > 0:
        total = sum(sales.get(r, 0.0) for r in regions)
        if total > 0:
            return {r: sales.get(r, 0.0) / total for r in regions}
    n = len(regions)
    return {r: 1 / n for r in regions}


def _compute_global_speed(region_best_time: dict[str, float], weights: dict[str, float]) -> float:
    gs = 0.0
    for r, w in weights.items():
        t = region_best_time.get(r, float("inf"))
        speed = 0.0 if t == float("inf") else 1 / t
        gs += w * speed
    return gs


def _weighted_avg_time(region_best_time: dict[str, float], weights: dict[str, float]) -> float:
    if any(region_best_time.get(r, float("inf")) == float("inf") and w > 0 for r, w in weights.items()):
        return float("inf")
    return sum(weights[r] * region_best_time.get(r, float("inf")) for r in weights)


def build_views(speeds_rows: list, active_ids: set[str], sales_rows: list):
    region_name: dict[str, str] = {}
    best_all = defaultdict(lambda: float("inf"))
    best_active = defaultdict(lambda: float("inf"))
    best_by_wh = defaultdict(lambda: defaultdict(lambda: float("inf")))
    wh_name = {}

    for row in speeds_rows:
        r = row["region_code"]
        region_name[r] = row["region_name"]
        w_id = str(row["warehouse_id"])
        wh_name[w_id] = row["warehouse_name"]
        t = float(row["time_hours"]) if row["time_hours"] is not None else float("inf")
        if t < best_all[r]:
            best_all[r] = t
        if w_id in active_ids and t < best_active[r]:
            best_active[r] = t
        if t < best_by_wh[w_id][r]:
            best_by_wh[w_id][r] = t

    regions = set(region_name.keys())
    sales = {row["region_code"]: float(row["orders"]) for row in sales_rows}
    weights = _weights(regions, sales)

    global_current = _compute_global_speed(best_active, weights)
    global_opt = _compute_global_speed(best_all, weights)
    coverage = 0.0 if global_opt == 0 else global_current / global_opt * 100

    return {
        "region_name": region_name,
        "best_all": best_all,
        "best_active": best_active,
        "best_by_wh": best_by_wh,
        "weights": weights,
        "global_current": global_current,
        "global_opt": global_opt,
        "coverage": coverage,
        "warehouse_names": wh_name,
        "avg_time_current": _weighted_avg_time(best_active, weights),
    }


def recommend_next(view: dict, active_ids: set[str], top_n: int = 1) -> list[Recommendation]:
    recs: list[Recommendation] = []
    for w_id, by_region in view["best_by_wh"].items():
        if w_id in active_ids:
            continue
        new_best = {}
        for r in view["region_name"]:
            new_best[r] = min(view["best_active"][r], by_region.get(r, float("inf")))

        new_global = _compute_global_speed(new_best, view["weights"])
        marginal_abs = new_global - view["global_current"]
        marginal_pct = None if view["global_current"] == 0 else marginal_abs / view["global_current"] * 100
        old_avg = _weighted_avg_time(view["best_active"], view["weights"])
        new_avg = _weighted_avg_time(new_best, view["weights"])

        changes = []
        for r, nm in view["region_name"].items():
            old_t = view["best_active"][r]
            new_t = new_best[r]
            if new_t < old_t:
                changes.append(RegionView(code=r, name=nm, weight=view["weights"][r], old_time=old_t, new_time=new_t))

        recs.append(
            Recommendation(
                warehouse_id=w_id,
                warehouse_name=view["warehouse_names"][w_id],
                marginal_abs=marginal_abs,
                marginal_pct=marginal_pct,
                coverage_pct=0.0 if view["global_opt"] == 0 else new_global / view["global_opt"] * 100,
                global_speed_current=view["global_current"],
                weighted_avg_time_old=old_avg,
                weighted_avg_time_new=new_avg,
                weighted_avg_time_delta=(new_avg - old_avg) if old_avg != float("inf") and new_avg != float("inf") else float("inf"),
                region_changes=sorted(changes, key=lambda x: x.weight, reverse=True),
            )
        )
    recs.sort(key=lambda r: r.marginal_abs, reverse=True)
    return recs[:top_n]
