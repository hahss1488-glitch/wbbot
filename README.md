# wbbot

Telegram-бот для расшифровки Excel-отчётов Wildberries (`.xlsx`).

## Что умеет

- Принимает Excel-файл отчёта в Telegram.
- Автоматически определяет период отчёта.
- Извлекает ключевые метрики (продажи, к перечислению, штрафы, логистика, хранение, удержания, налог, себестоимость, итого к оплате).
- Считает чистую прибыль.
- Показывает простой текстовый анализ и сравнение с предыдущим отчётом в чате.

## Запуск

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN="<ваш токен>"
python -m wbbot.bot
```

## Тесты

```bash
pytest
```
