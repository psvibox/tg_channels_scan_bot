# Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Системные зависимости (в т.ч. для telethon/aiohttp)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# зависимости проекта
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# код и данные
COPY . /app

# создаём папку для БД
RUN mkdir -p /app/data

# порт для Render
ENV PORT=10000
EXPOSE 10000

# команда запуска: FastAPI (webhook) + фоновый краулер в том же процессе
CMD ["uvicorn", "bot_with_crawler:app", "--host", "0.0.0.0", "--port", "10000"]
