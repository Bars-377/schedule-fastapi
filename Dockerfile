# Используем официальный образ Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Устанавливаем системные зависимости для mysqlclient
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libmariadb-dev \
    pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Копируем файлы проекта в контейнер
COPY . /app

# Убедитесь, что venv доступен
RUN python -m venv /venv

# Убедитесь, что venv доступен
ENV PATH="/venv/bin:$PATH"

# Устанавливаем зависимости
RUN pip install --upgrade pip
# RUN pip install -r requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Открываем порт для приложения
EXPOSE 8444
