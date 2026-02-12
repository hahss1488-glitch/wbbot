from __future__ import annotations

import io

import pandas as pd
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import BufferedInputFile, Message

from bot.data_io import ValidationError, parse_sales, parse_speeds
from bot.db import Database
from bot.metrics import build_views, recommend_next


router = Router()
db = Database()


class UploadStates(StatesGroup):
    waiting_speeds = State()
    waiting_sales = State()


def _fmt_time(t: float) -> str:
    if t == float("inf"):
        return "∞"
    return f"{t:.2f}ч"


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await message.answer(
        "Привет! Я бот для подбора складов по скорости доставки.\n"
        "Команды: /upload_speeds /upload_sales /list_warehouses /set_active /add_active "
        "/recommend_next /simulate_add /report /export"
    )


@router.message(Command("upload_speeds"))
async def cmd_upload_speeds(message: Message, state: FSMContext) -> None:
    await state.set_state(UploadStates.waiting_speeds)
    await message.answer("Отправь файл speeds.csv или speeds.xlsx")


@router.message(Command("upload_sales"))
async def cmd_upload_sales(message: Message, state: FSMContext) -> None:
    await state.set_state(UploadStates.waiting_sales)
    await message.answer("Отправь файл sales.csv или sales.xlsx")


@router.message(UploadStates.waiting_speeds, F.document)
async def on_speeds_doc(message: Message, state: FSMContext) -> None:
    file = await message.bot.get_file(message.document.file_id)
    b = io.BytesIO()
    await message.bot.download_file(file.file_path, destination=b)
    try:
        records = parse_speeds(b.getvalue(), message.document.file_name)
        db.replace_speeds(records)
        db.add_upload("speeds", message.document.file_name)
        await message.answer(f"Загружено записей speeds: {len(records)}")
    except ValidationError as exc:
        await message.answer(f"Ошибка валидации: {exc}")
    finally:
        await state.clear()


@router.message(UploadStates.waiting_sales, F.document)
async def on_sales_doc(message: Message, state: FSMContext) -> None:
    file = await message.bot.get_file(message.document.file_id)
    b = io.BytesIO()
    await message.bot.download_file(file.file_path, destination=b)
    try:
        records = parse_sales(b.getvalue(), message.document.file_name)
        db.replace_sales(records)
        db.add_upload("sales", message.document.file_name)
        await message.answer(f"Загружено записей sales: {len(records)}")
    except ValidationError as exc:
        await message.answer(f"Ошибка валидации: {exc}")
    finally:
        await state.clear()


@router.message(Command("list_warehouses"))
async def cmd_list_warehouses(message: Message) -> None:
    rows = db.list_warehouses()
    if not rows:
        await message.answer("Сначала загрузи speeds через /upload_speeds")
        return
    lines = [f"{'✅' if r['active'] else '▫️'} {r['id']} — {r['name']}" for r in rows]
    await message.answer("\n".join(lines))


@router.message(Command("set_active"))
async def cmd_set_active(message: Message) -> None:
    args = (message.text or "").split()[1:]
    ids = [int(a) for a in args] if args else []
    db.set_active(ids)
    await message.answer(f"Активные склады установлены: {ids}")


@router.message(Command("add_active"))
async def cmd_add_active(message: Message) -> None:
    args = (message.text or "").split()[1:]
    if len(args) != 1:
        await message.answer("Используй: /add_active <id>")
        return
    w_id = int(args[0])
    db.add_active(w_id)
    await message.answer(f"Склад {w_id} добавлен в активные")


@router.message(Command("recommend_next"))
async def cmd_recommend_next(message: Message) -> None:
    if not db.has_data():
        await message.answer("Нет данных speeds. Загрузи через /upload_speeds")
        return

    args = (message.text or "").split()[1:]
    n = int(args[0]) if args else 1
    active = db.active_ids()
    view = build_views(db.speeds_rows(), active, db.sales_rows())
    recs = recommend_next(view, active, top_n=max(1, n))

    if not recs:
        await message.answer("Нет кандидатов: возможно, все склады уже активны")
        return

    out = [
        f"Текущее покрытие: {view['coverage']:.2f}%",
        f"Текущий global_speed: {view['global_current']:.6f}",
    ]
    for rec in recs:
        pct_text = (
            "baseline=0, показываю только absolute"
            if rec.marginal_pct is None
            else f"{rec.marginal_pct:.2f}%"
        )
        out.append(
            f"\n#{rec.warehouse_id} {rec.warehouse_name}\n"
            f"marginal_gain abs={rec.marginal_abs:.6f}, pct={pct_text}\n"
            f"coverage после добавления: {rec.coverage_pct:.2f}%"
        )
        if rec.region_changes:
            out.append("Изменения по регионам:")
            for ch in rec.region_changes[:10]:
                out.append(
                    f"- {ch.name} ({ch.weight:.3f}): {_fmt_time(ch.old_time)} -> {_fmt_time(ch.new_time)}"
                )
    await message.answer("\n".join(out))


@router.message(Command("simulate_add"))
async def cmd_simulate_add(message: Message) -> None:
    if not db.has_data():
        await message.answer("Нет данных speeds. Загрузи через /upload_speeds")
        return
    args = (message.text or "").split()[1:]
    if len(args) != 1:
        await message.answer("Используй: /simulate_add <id>")
        return

    w_id = int(args[0])
    active = db.active_ids()
    view = build_views(db.speeds_rows(), active, db.sales_rows())
    recs = recommend_next(view, active, top_n=10_000)
    rec = next((r for r in recs if r.warehouse_id == w_id), None)
    if not rec:
        await message.answer("Склад не найден среди кандидатов (возможно уже активен)")
        return

    pct_text = "N/A (baseline=0)" if rec.marginal_pct is None else f"{rec.marginal_pct:.2f}%"
    lines = [
        f"Симуляция добавления #{rec.warehouse_id} {rec.warehouse_name}",
        f"marginal_gain abs={rec.marginal_abs:.6f}, pct={pct_text}",
        f"coverage после добавления: {rec.coverage_pct:.2f}%",
    ]
    for ch in rec.region_changes[:30]:
        lines.append(f"- {ch.name}: {_fmt_time(ch.old_time)} -> {_fmt_time(ch.new_time)}; weight={ch.weight:.3f}")
    await message.answer("\n".join(lines))


@router.message(Command("report"))
async def cmd_report(message: Message) -> None:
    if not db.has_data():
        await message.answer("Нет данных speeds. Загрузи через /upload_speeds")
        return
    active = db.active_ids()
    view = build_views(db.speeds_rows(), active, db.sales_rows())
    await message.answer(
        f"Активные: {sorted(active)}\n"
        f"global_speed: {view['global_current']:.6f}\n"
        f"global_speed_optimal: {view['global_opt']:.6f}\n"
        f"coverage: {view['coverage']:.2f}%"
    )


@router.message(Command("export"))
async def cmd_export(message: Message) -> None:
    if not db.has_data():
        await message.answer("Нет данных speeds. Загрузи через /upload_speeds")
        return
    rows = db.speeds_rows()
    sales = {r["region_code"]: float(r["orders"]) for r in db.sales_rows()}
    active = db.active_ids()
    lines = []
    for row in rows:
        lines.append(
            {
                "region_code": row["region_code"],
                "region_name": row["region_name"],
                "warehouse_id": row["warehouse_id"],
                "warehouse_name": row["warehouse_name"],
                "time_hours": row["time_hours"],
                "orders": sales.get(row["region_code"], None),
                "is_active": int(row["warehouse_id"] in active),
            }
        )
    df = pd.DataFrame(lines)
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    await message.answer_document(BufferedInputFile(buf.getvalue(), filename="report_export.csv"))
