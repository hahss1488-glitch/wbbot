# WB Warehouse Optimizer Bot

Telegram-бот с кнопочным интерфейсом, который:
- принимает реальные Excel/CSV файлы со скоростями,
- считает покрытие и метрики,
- рекомендует следующий склад для подключения,
- позволяет управлять активными складами прямо из кнопок.

## 1) Как один раз настроить токен

Бот читает токен в таком порядке:
1. `BOT_TOKEN` из переменных окружения,
2. `config/bot_token.txt`,
3. иначе ошибка `RuntimeError("Bot token not provided...")`.

### Linux / macOS
```bash
export BOT_TOKEN=123456:ABCDEF
python -m bot.main
```

### Windows PowerShell
```powershell
$env:BOT_TOKEN="123456:ABCDEF"
python -m bot.main
```

### Файл (удобно для VPS)
```bash
mkdir -p config
echo "123456:ABCDEF" > config/bot_token.txt
python -m bot.main
```

### Docker
```bash
docker build -t wb-bot .
docker run -d --name wb-bot --restart unless-stopped \
  -e BOT_TOKEN="$BOT_TOKEN" \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/config:/app/config \
  wb-bot
```

## 2) GitHub Secrets (без ручного редактирования на VPS)

Один раз в репозитории:
- **Settings → Secrets and variables → Actions → New repository secret**
- имя: `BOT_TOKEN`
- значение: токен Telegram-бота

Дальше workflow (`.github/workflows/deploy.yml`) сам передаст токен в docker и запишет его в `config/bot_token.txt` на VPS.

Также добавьте:
- `VPS_HOST`
- `VPS_USER`
- `VPS_SSH_KEY`

## 3) Поддерживаемые форматы speeds

Функция: `parse_speeds_file(filepath)`/`parse_speeds(bytes, filename)`.

### Long format
Колонки:
- `region_code`, `region_name`, `warehouse_id`, `warehouse_name`, `time_hours`

### Priority wide format (реальный)
Колонки вида:
- `region_name`, `1-й приоритет`, `2-й приоритет`, ...
- ячейки: `"Склад X, 28 ч"`

### Wide matrix
- `region_name` + колонки-склады с числовыми значениями времени.

Если формат не распознан — бот вернёт понятную ошибку и превью первых строк.

## 4) Как загрузка работает в Telegram

1. Нажми **«Загрузить скорости»** и отправь файл.
2. Бот покажет:
   - определённый формат,
   - выбранный лист (`result` или первый),
   - превью 8-10 строк,
   - проблемные ячейки.
3. Нажми:
   - ✅ Подтвердить (сохранить),
   - ✏️ Выбрать другой лист,
   - ✏️ Поменять колонку региона,
   - ❌ Отменить.

## 5) Кнопки меню

- `Загрузить скорости`
- `Загрузить продажи`
- `Активные склады`
- `Рекомендация`
- `Отчёт`
- `Экспорт`

## 6) Примеры файлов

Смотри папку `examples/`:
- `example_long.csv`
- `example_priority_wide.csv`
- `example_wide_matrix.csv`

## 7) Метрика

- `w_r = orders_r / sum_orders` (или равные веса)
- `speed_r = 1 / best_time_r`, если нет активного склада → `0`
- `global_speed = Σ(w_r * speed_r)`
- `coverage = global_speed / global_speed_optimal`

В рекомендации бот показывает:
- coverage,
- текущий `global_speed`,
- альтернативу `avg_time`,
- лучший склад, прирост в абсолюте и %,
- изменения по регионам (`old -> new`, дельта, вес).
