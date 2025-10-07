# Инструкция

## Запуск

1. Создать виртуальное окружение:

         python -m venv venv

2. Запустить виртуальное окружение:

         .\venv\Scripts\activate

4. Запустить приложение:

   Стандартный запуск:

         python main.py

   Нестанларнтынй запуск через Docker

## Дополнительно:

Создаёт requirements.txt:

      python -m pip freeze > requirements.txt

Установить requirements.txt:

      python -m pip install -r requirements.txt

PowerShell:

      python -m pip freeze | ForEach-Object { python -m pip uninstall -y $_ }

PEP 8:

      python -m pip install -r dev-requirements.txt

      pre-commit autoupdate
      pre-commit uninstall
      pre-commit install

      pre-commit run --all-files
