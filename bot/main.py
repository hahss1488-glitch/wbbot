from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from bot.handlers import router


def load_bot_token() -> str:
    env_token = os.getenv("BOT_TOKEN")
    if env_token:
        return env_token

    file_path = Path("config/bot_token.txt")
    if file_path.exists():
        token = file_path.read_text(encoding="utf-8").strip()
        if token:
            return token

    raise RuntimeError(
        "Bot token not provided. Set BOT_TOKEN env var or place token in config/bot_token.txt"
    )


async def main() -> None:
    token = load_bot_token()
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
