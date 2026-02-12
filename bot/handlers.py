from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from bot.data_io import ParseIssue, ValidationError, parse_sales, parse_speeds
from bot.db import Database
from bot.metrics import build_views, recommend_next

router = Router()
db = Database()
UPLOAD_DIR = Path("data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Загрузить скорости"), KeyboardButton(text="Загрузить продажи")],
        [KeyboardButton(text="Активные склады"), KeyboardButton(text="Рекомендация")],
        [KeyboardButton(text="Отчёт"), KeyboardButton(text="Экспорт")],
    ],
    resize_keyboard=True,
)


class UploadStates(StatesGroup):
    waiting_speeds = State()
    waiting_sales = State()


class EditStates(StatesGroup):
    waiting_region_column = State()


def _fmt_time(t: float) -> str:
    return "∞" if t == float("inf") else f"{t:.2f}ч"


def _issues_text(issues: list[ParseIssue]) -> str:
    if not issues:
        return ""
    lines = ["⚠️ Проблемные ячейки (до 10):"]
    for item in issues[:10]:
        lines.append(f"- строка {item.row}: «{item.value}» ({item.column}) — {item.problem}")
    return "\n".join(lines)


def _preview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data="speeds:confirm")],
            [InlineKeyboardButton(text="✏️ Выбрать другой лист", callback_data="speeds:sheet")],
            [InlineKeyboardButton(text="✏️ Поменять колонку региона", callback_data="speeds:regioncol")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="speeds:cancel")],
        ]
    )


def _recommend_keyboard(warehouse_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Добавить склад", callback_data=f"active:add:{warehouse_id}")],
            [InlineKeyboardButton(text="Показать топ5", callback_data="recommend:top5")],
        ]
    )


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await message.answer("Привет! Выбирай действие кнопками 👇", reply_markup=MAIN_MENU)


@router.message(F.text == "Загрузить скорости")
@router.message(Command("upload_speeds"))
async def cmd_upload_speeds(message: Message, state: FSMContext) -> None:
    await state.set_state(UploadStates.waiting_speeds)
    await message.answer("Отправь файл speeds в CSV/XLSX")


@router.message(F.text == "Загрузить продажи")
@router.message(Command("upload_sales"))
async def cmd_upload_sales(message: Message, state: FSMContext) -> None:
    await state.set_state(UploadStates.waiting_sales)
    await message.answer("Отправь файл sales в CSV/XLSX")


@router.message(UploadStates.waiting_speeds, F.document)
async def on_speeds_doc(message: Message, state: FSMContext) -> None:
    file = await message.bot.get_file(message.document.file_id)
    payload = io.BytesIO()
    await message.bot.download_file(file.file_path, destination=payload)

    save_path = UPLOAD_DIR / f"{message.from_user.id}_{message.document.file_name}"
    save_path.write_bytes(payload.getvalue())

    try:
        result = parse_speeds(payload.getvalue(), message.document.file_name)
    except ValidationError as exc:
        await message.answer(f"Ошибка парсинга: {exc}")
        await state.clear()
        return

    await state.update_data(
        pending_speeds=result.records,
        pending_file=str(save_path),
        pending_filename=message.document.file_name,
        pending_sheet=result.sheet_name,
    )

    preview_df = pd.DataFrame(result.preview_rows)
    preview_txt = preview_df.to_string(index=False) if not preview_df.empty else "(пусто)"
    info = [
        f"Формат: {result.detected_format}",
        f"Лист: {result.sheet_name or '-'}",
        f"Записей к загрузке: {len(result.records)}",
        "\nПревью:",
        f"<pre>{preview_txt[:3500]}</pre>",
    ]
    issues_txt = _issues_text(result.issues)
    if issues_txt:
        info.append(issues_txt)
    await message.answer("\n".join(info), reply_markup=_preview_keyboard())


@router.callback_query(F.data == "speeds:confirm")
async def cb_speeds_confirm(callback, state: FSMContext) -> None:
    data = await state.get_data()
    rows = data.get("pending_speeds")
    if not rows:
        await callback.message.answer("Нет данных для подтверждения")
        await callback.answer()
        return
    db.upsert_speeds(rows)
    db.add_upload(data.get("pending_filename", "unknown"), data.get("pending_file", ""), str(callback.from_user.id))
    await callback.message.answer(f"✅ Сохранено записей: {len(rows)}", reply_markup=MAIN_MENU)
    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "speeds:cancel")
async def cb_speeds_cancel(callback, state: FSMContext) -> None:
    await state.clear()
    await callback.message.answer("Загрузка отменена", reply_markup=MAIN_MENU)
    await callback.answer()


@router.callback_query(F.data == "speeds:sheet")
async def cb_speeds_sheet(callback) -> None:
    await callback.message.answer("Пока поддержан авто-выбор: лист result или первый лист.")
    await callback.answer()


@router.callback_query(F.data == "speeds:regioncol")
async def cb_speeds_regioncol(callback, state: FSMContext) -> None:
    await state.set_state(EditStates.waiting_region_column)
    await callback.message.answer("Отправь точное имя колонки региона (например: Регион)")
    await callback.answer()


@router.message(EditStates.waiting_region_column)
async def on_region_column(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Опция зарезервирована для расширенного маппинга колонок. Использован авто-режим.")


@router.message(UploadStates.waiting_sales, F.document)
async def on_sales_doc(message: Message, state: FSMContext) -> None:
    file = await message.bot.get_file(message.document.file_id)
    payload = io.BytesIO()
    await message.bot.download_file(file.file_path, destination=payload)
    try:
        records = parse_sales(payload.getvalue(), message.document.file_name)
    except ValidationError as exc:
        await message.answer(f"Ошибка валидации: {exc}")
        await state.clear()
        return

    db.replace_sales(records)
    await message.answer(f"Загружено sales: {len(records)}", reply_markup=MAIN_MENU)
    await state.clear()


@router.message(F.text == "Активные склады")
@router.message(Command("list_warehouses"))
async def cmd_list_warehouses(message: Message) -> None:
    rows = db.list_warehouses()
    if not rows:
        await message.answer("Сначала загрузи скорости")
        return
    for row in rows[:60]:
        buttons = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Добавить", callback_data=f"active:add:{row['warehouse_id']}"),
                    InlineKeyboardButton(text="Убрать", callback_data=f"active:remove:{row['warehouse_id']}"),
                    InlineKeyboardButton(text="Симулировать", callback_data=f"sim:{row['warehouse_id']}"),
                ]
            ]
        )
        await message.answer(f"{'✅' if row['active'] else '▫️'} {row['warehouse_id']} — {row['warehouse_name']}", reply_markup=buttons)


@router.callback_query(F.data.startswith("active:add:"))
async def cb_add_active(callback) -> None:
    w_id = callback.data.split(":", 2)[2]
    db.add_active(w_id)
    await callback.message.answer(f"Склад {w_id} добавлен в активные")
    await callback.answer()


@router.callback_query(F.data.startswith("active:remove:"))
async def cb_remove_active(callback) -> None:
    w_id = callback.data.split(":", 2)[2]
    db.remove_active(w_id)
    await callback.message.answer(f"Склад {w_id} убран из активных")
    await callback.answer()


async def _send_recommendation(message: Message, top_n: int = 1) -> None:
    if not db.has_data():
        await message.answer("Нет данных speeds. Загрузи файл.")
        return

    active = db.active_ids()
    view = build_views(db.speeds_rows(), active, db.sales_rows())
    recs = recommend_next(view, active, top_n=max(1, top_n))
    if not recs:
        await message.answer("Нет кандидатов — возможно, все склады уже активны")
        return

    first = recs[0]
    base_msg = [
        f"Coverage: {view['coverage']:.2f}% от оптимума",
        f"global_speed: {view['global_current']:.6f}",
        f"avg_time: {_fmt_time(view['avg_time_current'])}",
    ]
    if first.marginal_pct is None:
        base_msg.append("В данный момент нет активных складов — смотрите абсолютные значения.")
        pct_text = "N/A"
    else:
        pct_text = f"+{first.marginal_pct:.2f}%"
    delta_hours = "N/A" if first.weighted_avg_time_delta == float("inf") else f"{abs(first.weighted_avg_time_delta):.2f}ч"

    base_msg += [
        f"\nЛучший склад: {first.warehouse_id} — {first.warehouse_name}",
        f"Прирост: abs={first.marginal_abs:.6f}, pct={pct_text}, изменение avg_time: -{delta_hours}",
        "Изменения по регионам:",
    ]
    for ch in first.region_changes[:12]:
        delta = ch.old_time - ch.new_time
        base_msg.append(f"- {ch.name}: {_fmt_time(ch.old_time)} → {_fmt_time(ch.new_time)} (Δ {delta:.2f}ч), вес {ch.weight:.2%}")

    await message.answer("\n".join(base_msg), reply_markup=_recommend_keyboard(first.warehouse_id))

    if top_n > 1:
        lines = ["TOP рекомендации:"]
        for i, rec in enumerate(recs[:top_n], start=1):
            pct = "N/A" if rec.marginal_pct is None else f"+{rec.marginal_pct:.2f}%"
            lines.append(f"{i}. {rec.warehouse_id} {rec.warehouse_name} — abs {rec.marginal_abs:.6f}, pct {pct}")
        await message.answer("\n".join(lines))


@router.message(F.text == "Рекомендация")
@router.message(Command("recommend_next"))
async def cmd_recommend_next(message: Message) -> None:
    await _send_recommendation(message, top_n=1)


@router.callback_query(F.data == "recommend:top5")
async def cb_recommend_top5(callback) -> None:
    await _send_recommendation(callback.message, top_n=5)
    await callback.answer()


@router.callback_query(F.data.startswith("sim:"))
async def cb_simulate(callback) -> None:
    w_id = callback.data.split(":", 1)[1]
    if not db.has_data():
        await callback.message.answer("Нет данных speeds. Загрузи через кнопку.")
        await callback.answer()
        return
    active = db.active_ids()
    view = build_views(db.speeds_rows(), active, db.sales_rows())
    recs = recommend_next(view, active, top_n=10_000)
    rec = next((r for r in recs if r.warehouse_id == w_id), None)
    if not rec:
        await callback.message.answer("Склад не найден среди кандидатов")
    else:
        pct = "N/A" if rec.marginal_pct is None else f"+{rec.marginal_pct:.2f}%"
        await callback.message.answer(f"Симуляция {rec.warehouse_name}: abs={rec.marginal_abs:.6f}, pct={pct}, coverage={rec.coverage_pct:.2f}%")
    await callback.answer()


@router.message(Command("simulate_add"))
async def cmd_simulate_add(message: Message) -> None:
    args = (message.text or "").split()[1:]
    if not args:
        await message.answer("Используй /simulate_add <warehouse_id>")
        return
    w_id = args[0]
    if not db.has_data():
        await message.answer("Нет данных speeds. Загрузи через кнопку.")
        return
    active = db.active_ids()
    view = build_views(db.speeds_rows(), active, db.sales_rows())
    recs = recommend_next(view, active, top_n=10_000)
    rec = next((r for r in recs if r.warehouse_id == w_id), None)
    if not rec:
        await message.answer("Склад не найден среди кандидатов")
        return
    pct = "N/A" if rec.marginal_pct is None else f"+{rec.marginal_pct:.2f}%"
    await message.answer(f"Симуляция {rec.warehouse_name}: abs={rec.marginal_abs:.6f}, pct={pct}, coverage={rec.coverage_pct:.2f}%")


@router.message(F.text == "Отчёт")
@router.message(Command("report"))
async def cmd_report(message: Message) -> None:
    if not db.has_data():
        await message.answer("Нет данных speeds")
        return
    active = db.active_ids()
    view = build_views(db.speeds_rows(), active, db.sales_rows())
    await message.answer(
        f"Активные: {sorted(active)}\n"
        f"global_speed: {view['global_current']:.6f}\n"
        f"global_speed_optimal: {view['global_opt']:.6f}\n"
        f"coverage: {view['coverage']:.2f}%\n"
        f"avg_time: {_fmt_time(view['avg_time_current'])}"
    )


@router.message(F.text == "Экспорт")
@router.message(Command("export"))
async def cmd_export(message: Message) -> None:
    if not db.has_data():
        await message.answer("Нет данных speeds")
        return
    rows = db.speeds_rows()
    sales = {r["region_code"]: int(r["orders"]) for r in db.sales_rows()}
    active = db.active_ids()

    payload = []
    for row in rows:
        payload.append(
            {
                "region_code": row["region_code"],
                "region_name": row["region_name"],
                "warehouse_id": row["warehouse_id"],
                "warehouse_name": row["warehouse_name"],
                "time_hours": row["time_hours"],
                "orders": sales.get(row["region_code"]),
                "is_active": int(row["warehouse_id"] in active),
            }
        )
    df = pd.DataFrame(payload)
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    await message.answer_document(BufferedInputFile(buf.getvalue(), filename="report_export.csv"))
