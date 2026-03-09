# ETL Pipeline: MongoDB → PostgreSQL (Airflow)

Небольшой учебный проект по реализации ETL-процесса с использованием **MongoDB, PostgreSQL и Apache Airflow**.

Проект демонстрирует полный поток данных:

1. генерация данных,
2. загрузка в MongoDB,
3. ETL в PostgreSQL,
4. построение аналитических витрин.

---

## Архитектура

```
Data generator
      ↓
JSON files
      ↓
MongoDB (source)
      ↓
Airflow ETL
      ↓
PostgreSQL (DWH)
      ↓
Analytical marts
```

---

## Структура проекта

```
airflow/dags/           DAG'и Airflow
scripts/                скрипты генерации и ETL
init/                   SQL инициализации PostgreSQL
data/                   временные JSON файлы (не коммитятся)
docker-compose.yml      запуск всей инфраструктуры
```

---

## Используемые технологии

* Apache Airflow
* MongoDB
* PostgreSQL
* Docker / Docker Compose
* Python

---

## Запуск проекта

Запуск инфраструктуры:

```
docker compose up
```

Airflow будет доступен:

```
http://localhost:8080
login: admin
password: admin
```

---

## Пайплайны Airflow

### 1. generate_mongo_data_dag

Генерация тестовых данных и загрузка их в MongoDB.

### 2. etl_mongo_to_postgres_dag

ETL-процесс:

* чтение данных из MongoDB
* трансформация
* загрузка в PostgreSQL.

### 3. build_marts_dag

Построение аналитических витрин.

---

## Аналитические витрины

### mart_user_activity

Поведенческая аналитика пользователей:

* количество сессий
* средняя длительность
* распределение по устройствам

### mart_support_stats

Статистика работы поддержки:

* количество тикетов
* статус обращений
* среднее время решения

---

## Примечание

Папка `data/` содержит временные файлы генерации данных, которые не добавляются в репозиторий.
Возможно, для успешного запуска DAG в Airflow потребуется изменить права этой папки с помощью chmod.

