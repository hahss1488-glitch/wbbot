from __future__ import annotations

import logging
import os
from tempfile import NamedTemporaryFile
from typing import Any

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from wbbot.report_parser import ParsedReport, parse_wb_report


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

LAST_REPORT_BY_CHAT: dict[int, ParsedReport] = {}


def _fmt_money(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:,.2f}".replace(",", " ") + " ₽"


def _fmt_date(value) -> str:
    return value.strftime("%d.%m.%Y") if value else "не определён"


def _compose_message(report: ParsedReport, previous: ParsedReport | None = None) -> str:
    m = report.metrics
    lines = [
        f"📊 Принят отчёт за период {_fmt_date(report.date_start)} — {_fmt_date(report.date_end)}",
        f"✔️ Реализовано: {_fmt_money(m.get('sales'))}",
        f"📦 К перечислению за товар: {_fmt_money(m.get('payout_goods'))}",
        f"🚚 Логистика: {_fmt_money(m.get('logistics'))}",
        f"📦 Хранение: {_fmt_money(m.get('storage'))}",
        f"⚠️ Штрафы: {_fmt_money(m.get('fines'))}",
        f"💸 Удержания: {_fmt_money(m.get('deductions'))}",
        f"🧾 Налог: {_fmt_money(m.get('tax'))}",
        f"👟 Себестоимость: {_fmt_money(m.get('cost_price'))}",
        "",
        f"💰 ЧИСТАЯ ПРИБЫЛЬ: {_fmt_money(m.get('net_profit'))}",
        "",
        "🧠 Анализ:",
    ]
    lines.extend([f"• {note}" for note in report.notes])

    if previous:
        old_total = previous.metrics.get("total_payment") or previous.metrics.get("sales")
        new_total = report.metrics.get("total_payment") or report.metrics.get("sales")
        if old_total and new_total:
            change = ((new_total - old_total) / abs(old_total)) * 100
            lines.append(f"• Изменение к прошлому отчёту: {change:+.2f}%")

    return "\n".join(lines)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Отправьте Excel-файл отчёта Wildberries (.xlsx), и я сделаю расшифровку."
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.document:
        return

    doc = update.message.document
    filename = (doc.file_name or "").lower()
    if not filename.endswith(".xlsx"):
        await update.message.reply_text("Пожалуйста, отправьте файл .xlsx")
        return

    tg_file = await doc.get_file()
    with NamedTemporaryFile(suffix=".xlsx", delete=True) as tmp:
        await tg_file.download_to_drive(custom_path=tmp.name)
        report = parse_wb_report(tmp.name)

    previous = LAST_REPORT_BY_CHAT.get(update.effective_chat.id)
    LAST_REPORT_BY_CHAT[update.effective_chat.id] = report

    await update.message.reply_text(_compose_message(report, previous))


def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN environment variable")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    logger.info("WB bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
