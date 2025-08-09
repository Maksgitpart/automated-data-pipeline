# Data Pipeline Project

## Опис
Цей проєкт реалізує автоматизований дата-пайплайн, який:
- Завантажує дані з CSV, API, та Wikipedia (web scraping)
- Зберігає ці дані у MongoDB та SQLite

## Як запустити

1. Встановити Docker та Docker Compose
2. Відкрити термінал у цій папці
3. Запустити команду:
```bash
docker-compose up --build
```
4. Після завершення виводу з'явиться `Pipeline completed successfully!`

## Структура
- `pipeline.py` — головний скрипт
- `docker-compose.yml` — налаштування сервісів
- `requirements.txt` — залежності Python
- `Dockerfile` — образ Python додатку
