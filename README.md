# SQL labs 1-2

Проект:
- ЛР1: ETL
- ЛР2: DWH + витрина + перекладка в MySQL

## Что уже сделано в проекте

- Python-пайплайн:
  - `get_dataset()` — генерация «сломанного» датасета (10 полей)
  - `load_data_to_db()` — загрузка в `t_sql_source_unstructured`
  - `fill_structured_table()` — запуск `fn_etl_data_load(start_date, end_date)`
  - `fill_dm_table()` — запуск `fn_dm_data_load(start_dt, end_dt)`
  - `transfer_dm_to_mysql()` — перенос `v_dm_task` в MySQL
  - `etl()` — верхнеуровневая функция без параметров
- SQL для PostgreSQL и MySQL

---

## Рекомендуемый запуск (минимум установок)

Тебе нужно только:
1. Docker Desktop
2. DBeaver

Локально PostgreSQL/MySQL/Python ставить не нужно.

### 1) Запуск инфраструктуры

В корне проекта:

```powershell
docker compose up -d postgres mysql
```

Это поднимет:
- PostgreSQL: `localhost:15432`
- MySQL: `localhost:13306`

Схемы, таблицы, функции и view создаются автоматически init-скриптами при первом старте.

### 2) Запуск пайплайна (ЛР1 + ЛР2)

```powershell
docker compose run --rm app --step all
```

Что выполнится:
- генерация и загрузка сырых данных
- очистка и запись в structured-таблицу
- формирование DWH/витрины
- перенос витрины в MySQL

---

## Проверка в DBeaver

### Подключение PostgreSQL

- Host: `localhost`
- Port: `15432`
- DB: `sql_labs`
- User: `postgres`
- Password: `postgres`

Проверки:

```sql
select count(*) from s_sql_dds.t_sql_source_unstructured;
select count(*) from s_sql_dds.t_sql_source_structured;
select min(report_date), max(report_date) from s_sql_dds.t_sql_source_structured;
select count(*) from s_sql_dds.v_dm_task;
select * from s_sql_dds.v_dm_task limit 20;
```

### Подключение MySQL

- Host: `localhost`
- Port: `13306`
- DB: `sql_dm`
- User: `root`
- Password: `root`

Проверки:

```sql
select count(*) from t_dm_task;
select * from t_dm_task limit 20;
```

---

## Если нужно запускать шагами

```powershell
docker compose run --rm app --step etl
docker compose run --rm app --step dm
docker compose run --rm app --step transfer
```

---

## Важно про init-скрипты Docker

SQL-инициализация контейнеров срабатывает только при **первом** создании volume.
Если нужно «пересоздать с нуля»:

```powershell
docker compose down -v
docker compose up -d postgres mysql
```

---

## Альтернатива без Docker (не рекомендуется)

Можно запускать локально через Python и локальные БД, но это требует больше ручной настройки.

---

## Файлы, которые важны для ЛР

- Python: `data-pipeline/src/*.py`, `data-pipeline/main.py`
- PostgreSQL SQL: `sql/dds/...`, `sql/dm/s_sql_dds/...`
- MySQL SQL: `sql/dm/mysql/t_dm_task.sql`
- Docker init:
  - `docker/postgres/init/01_init_all.sql`
  - `docker/mysql/init/01_init_mysql.sql`
- Отчет: `report_template_lr1_lr2.tex`
