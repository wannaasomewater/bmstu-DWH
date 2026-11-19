# Домашнее задание 2.2

## Структура решения
* `hw2_2.ipynb` — Jupyter Notebook с кодом и выполненными ячейками для всех трех заданий.
* `README.md` — данная инструкция.

## Инструкция по запуску
1. Запустите окружение через Docker:
   `docker compose -f docker/docker-compose.yml up -d`
2. Откройте Jupyter Notebook по адресу `http://localhost:8888`.
3. Запустите ноутбук `hw2_2.ipynb` сверху вниз (Kernel -> Restart & Run All).

## Пути к входным данным (Inputs)
* Локальные файлы:
  - `data4hw2_2/task1/campaigns.csv`
  - `data4hw2_2/task2/catalogue.json`
  - `data4hw2_2/task2/skew_transactions.csv`
  - `data4hw2_2/task3/cut_ratings.csv`
  - `data4hw2_2/task3/cut_transactions.csv`
  - `data4hw2_2/task3/ids.csv`
* Данные в S3:
  - События для СD2: `s3a://dwh/task1/events/`
  - Готовая SCD2: `s3a://dwh/task1/scd2/`

## Пути к результатам (Outputs)
* **Задание 1:** `s3a://dwh/task1/res_{auto, merge, broadcast, shuffle_hash}/`
* **Задание 2:** `s3a://dwh/task2/res_orig/`, `s3a://dwh/task2/res_bcast/`, `s3a://dwh/task2/res_salt/`
* **Задание 3:** `s3a://dwh/task3/result/`

## Сравнение вариантов и метрики
* **Задание 1:** Физические планы, время выполнения и количество строк выводятся прямо в ячейках ноутбука.
* **Задание 2:** Таблица измерений, диагностика перекоса и проверка равенства описаны в Markdown-ячейке после кода.
* **Задание 3:** Таблица с признаками, проверки на NULL, уникальность и min <= max`выведены в конце ноутбука.
