# WB Warehouse Optimizer Bot (MVP)

Telegram-бот, который рекомендует, какой склад добавить следующим, чтобы максимизировать скорость доставки.

## Что умеет
- Загрузка матрицы скоростей (`/upload_speeds`) из CSV/XLSX
- Загрузка весов продаж (`/upload_sales`) из CSV/XLSX (опционально)
- Управление активными складами (`/set_active`, `/add_active`)
- Рекомендации следующего склада (`/recommend_next [N]`)
- Симуляция конкретного склада (`/simulate_add <id>`)
- Сводный отчет (`/report`)
- Экспорт данных (`/export`)

## Формат входных файлов
### speeds.csv / speeds.xlsx
Обязательные колонки:
- `region_code`
- `region_name`
- `warehouse_id`
- `warehouse_name`
- `time_hours`

Правила:
- `time_hours` > 0
- пустой `time_hours` трактуется как бесконечность (`+∞`)

### sales.csv / sales.xlsx (опционально)
Колонки:
- `region_code`
- `orders`

Если sales не загружен, бот использует равные веса регионов.

## Метрика
- `w_r = orders_r / sum_orders` (или равные веса)
- `best_time_r = min(time_hours)` по активным складам
- `speed_r = 1 / best_time_r`, если склада нет -> `0`
- `global_speed = Σ(w_r * speed_r)`
- `global_speed_optimal` — аналогично, но по всем складам
- `coverage% = global_speed / global_speed_optimal * 100%`

## Локальный запуск
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
export BOT_TOKEN=<your_token>
python -m bot.main
```

## Docker
```bash
docker build -t wb-bot .
docker run --rm -e BOT_TOKEN=$BOT_TOKEN -v $(pwd)/data:/app/data wb-bot
```

## VPS quickstart
1. Поставьте Docker и Git на VPS.
2. Клонируйте репозиторий.
3. `docker build -t wb-bot .`
4. `docker run -d --name wb-bot --restart unless-stopped -e BOT_TOKEN=<TOKEN> -v $(pwd)/data:/app/data wb-bot`
5. Проверка логов: `docker logs -f wb-bot`

## Команды в Telegram
- `/upload_speeds`
- `/upload_sales`
- `/list_warehouses`
- `/set_active <id1> <id2> ...`
- `/add_active <id>`
- `/recommend_next [N]`
- `/simulate_add <id>`
- `/report`
- `/export`
