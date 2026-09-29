# ClickHouse для BI-аналитика: шпаргалка по стеку Kafka + ClickHouse + DataLens + Airflow

Кому: аналитику, который на проекте один — и данные, и DWH, и ETL, и дашборды.
Стек: **Kafka → ClickHouse → витрины → DataLens**, оркестрация — **Airflow**.
Окружение примеров: docker-compose, ClickHouse **24.8.14.39**, Kafka **3.9.1**, Airflow **2.10.5**, DataLens.

Формат — как в моей шпаргалке по BigQuery: **задача → код → результат → практическая заметка**.
Всё, что ниже, прогнано на живой базе `cheat` (демо-данные: 1 557 369 событий, 202 000 заказов, 40 000 пользователей).
Где в примере есть реальный вывод — он приведён как есть, вместе с граблями, которые вылезли при прогоне.

## Оглавление

| Раздел | О чём | Файл |
|---|---|---|
| 1 | [Подключение и первые грабли](#1-подключение-и-первые-грабли) | — |
| 2 | [ClickHouse vs BigQuery: что переучивать](#2-clickhouse-vs-bigquery-что-переучивать) | — |
| 3 | [Модель данных: движки, ключ сортировки, партиции](#3-модель-данных-движки-ключ-сортировки-партиции) | — |
| 4 | [DDL и фишки схемы](#4-ddl-и-фишки-схемы) | `sql/01_ddl_and_engines.sql` |
| 5 | [Kafka → ClickHouse](#5-kafka--clickhouse) | `sql/02_kafka_ingest.sql`, `kafka/producer_events.py` |
| 6 | [Витрины (марты)](#6-витрины-марты) | `sql/03_marts.sql` |
| 7 | [Airflow: ETL и DQ-гейты](#7-airflow-etl-и-dq-гейты) | `etl/airflow/` |
| 8 | [BI-запросы: рецепты](#8-bi-запросы-рецепты) | `sql/04_bi_queries.sql` |
| 9 | [Оптимизация запросов](#9-оптимизация-запросов) | — |
| 10 | [Эксплуатация и диагностика](#10-эксплуатация-и-диагностика) | `sql/05_ops_diagnostics.sql` |
| 11 | [DataLens поверх ClickHouse](#11-datalens-поверх-clickhouse) | — |
| 12 | [Список грабель одним экраном](#12-список-грабель-одним-экраном) | — |

Демо-база поднимается одним файлом: `sql/00_setup_demo.sql` (создаёт БД `cheat` и наполняет её).

---

## 1. Подключение и первые грабли

```bash
# нативный клиент внутри контейнера
docker exec -i dwh-clickhouse clickhouse-client

# с хоста по HTTP (8123) — так же ходят Airflow и DataLens
curl 'http://localhost:8123/' --data-binary 'SELECT version(), currentUser(), timezone()'
```

**Грабли, которые ловятся сразу:**

* **HTTP-интерфейс не принимает несколько запросов в одном теле.** `clickhouse-client --multiquery` ест пачку стейтментов, HTTP — только один. Отсюда классика: «в DBeaver/скрипте скрипт падает на второй строке» → шлите по одному запросу на вызов.
* **Часовой пояс сервера — UTC.** `SELECT timezone()` вернул `UTC`, `now()` — UTC. В DataLens/BI даты поедут, если об этом не помнить: для отчётов либо настраивайте TZ сервера, либо явно `toTimeZone(now(), 'Europe/Moscow')`.
* Порт **9000** — нативный протокол, **8123** — HTTP. DataLens/Airflow/`curl` — это 8123.

---

## 2. ClickHouse vs BigQuery: что переучивать

| Задача | BigQuery | ClickHouse |
|---|---|---|
| Партиционирование | `PARTITION BY DATE(ts)` | `PARTITION BY toYYYYMM(ts)` (чаще месяц, а не день!) |
| Кластеризация | `CLUSTER BY user_id` | `ORDER BY (user_id, ts)` — **это и есть ключ сортировки, он же первичный** |
| Индексы | автоматические | нет обычных индексов: разреженный primary key по `ORDER BY` + skip-индексы (`minmax`, `set`, `bloom_filter`) |
| Дедупликация | `MERGE`/`ROW_NUMBER` | движок `ReplacingMergeTree` + `FINAL` |
| Расписание запросов | scheduled queries | refreshable MV, MV + Airflow, TTL |
| Агрегаты | `SUM`, `COUNT` | те же + `-State`/`-Merge` (агрегатные состояния в таблицах) |
| Массивы | `UNNEST(arr)` | `arrayJoin(arr)` / `ARRAY JOIN arr` |
| Оконная функция | `QUALIFY` | `QUALIFY` есть, `EXCLUDE` в окне — **нет** |
| `date_trunc(d, month)` | `DATE_TRUNC` | `toStartOfMonth(d)` / `date_trunc('month', d)` |
| `ARRAY_AGG` | есть | `groupArray(x)` |
| `SAFE_CAST` | есть | `toInt64OrNull(x)`, `accurateCastOrNull` |
| Пустая оконная рамка | `NULL` | **`0`** — см. раздел 8, рецепт 8.14 |
| Time travel (as-of-запрос к прошлому) | есть | **нет**; ближайшее — `BACKUP`/`FREEZE` и версионные таблицы |
| UPDATE/DELETE | DML как обычно | мутации (`ALTER ... UPDATE`), переписывают парты; есть lightweight `DELETE FROM` |

---

## 3. Модель данных: движки, ключ сортировки, партиции

Три вещи, которые в ClickHouse решают всё:

1. **`ORDER BY` (ключ сортировки)** — по нему строится разреженный первичный индекс: одна запись индекса на ~8192 строки (`index_granularity`). Фильтр режет данные, только если он **по префиксу** ключа.
2. **`PARTITION BY`** — физическое разделение на парты. Мельчить нельзя: партиция на каждый день = тысячи партов и тормоза на мержах. Месяц/неделя — рабочая норма.
3. **Движок** — определяет, что происходит при вставке: дубликаты (`MergeTree`), дедуп (`ReplacingMergeTree`), суммирование (`SummingMergeTree`), агрегатные состояния (`AggregatingMergeTree`).

Проверка на живых данных — почему порядок колонок в `ORDER BY` критичен:

```sql
EXPLAIN indexes = 1 SELECT count() FROM cheat.events WHERE event_type = 'purchase';
-- PrimaryKey: Granules 16/192  → индекс срезал почти всё

EXPLAIN indexes = 1 SELECT count() FROM cheat.events WHERE user_id = 42;
-- PrimaryKey: Granules 192/192 → полный скан!
```

`user_id` есть в ключе сортировки `(event_type, event_time, user_id)`, но стоит **третьим**: без фильтра по `event_type` разреженный индекс бесполезен. Кладите в начало ключа то, по чему фильтруете всегда.

А вот так работает партиционирование (`PARTITION BY toYYYYMM(event_time)`):

```sql
EXPLAIN indexes = 1 SELECT count() FROM cheat.events WHERE event_time >= today() - 1;
-- Partition: Parts 2/2, Granules 100/100   → из 4 партов прочитано 2
-- MinMax:    Parts 2/4
-- PrimaryKey: Granules 19/100
```

Фильтр по `event_date` (материализованная колонка, **не** ключ партиционирования) партиции не режет — только `event_time`/`toYYYYMM(event_time)`.

---

## 4. DDL и фишки схемы

Полный файл: `sql/01_ddl_and_engines.sql` (идемпотентный, можно гонять повторно).

```sql
CREATE TABLE cheat.events
(
    event_id   UInt64,
    event_time DateTime,
    event_date Date MATERIALIZED toDate(event_time),  -- считается на лету, на диске не хранится
    user_id    UInt64,
    event_type LowCardinality(String),               -- словарь на лету: экономит память и диск
    page       LowCardinality(String),
    device     LowCardinality(String),
    country    LowCardinality(String),
    revenue    Decimal(18,2),
    props      String DEFAULT '{}' CODEC(ZSTD(3)),   -- JSON кладём строкой (см. ниже)
    src        String EPHEMERAL                       -- только для вставки, в таблице не живёт
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time, user_id)
TTL event_time + INTERVAL 24 MONTH DELETE;
```

**Что реально пригодилось из DDL:**

* `ALTER TABLE ... ADD COLUMN` — метаданные, мгновенно. **Но `ADD COLUMN ... MATERIALIZED` не пересчитывает данные задним числом** — значение считается на чтении, старые строки отдают корректный результат, потому что выражение детерминированное.
* `EXCHANGE TABLES a AND b` — **атомарная замена двух таблиц**. Главный инструмент пересборки витрины без простоя (см. раздел 7).
* `ALTER TABLE ... ADD INDEX` (skip-индекс) → `MATERIALIZE INDEX` строит его по существующим данным. **Грабля:** `MATERIALIZE INDEX` — асинхронная мутация, и `DROP INDEX` сразу за ней падает с «mutation is not finished yet». Лечится `SET mutations_sync = 1`.
* Проекции (`ALTER TABLE ... ADD PROJECTION ... MATERIALIZE PROJECTION`) — альтернативная сортировка внутри той же таблицы. Проверено: `system.projection_parts` показывает парты проекции (по 100 000 строк, 1.69 MiB).
* `CREATE TABLE x AS y` копирует структуру, `CREATE TABLE x AS y ENGINE=... AS SELECT` — структуру по запросу. `CREATE TABLE ... EMPTY AS SELECT` — только схема, без данных.
* **JSON в 24.8:** тип `JSON` есть, но экспериментальный (`allow_experimental_json_type = 1`), и подколонки имеют тип `Dynamic` — `WHERE props.a = 1` **молча вернёт 0 строк**, нужно `props.a::Int64 = 1`. Рабочий вариант без сюрпризов — `String` + `JSONExtractString/JSONExtractInt`.
* Refreshable MV (`REFRESH EVERY 10 MINUTE`) требует `allow_experimental_refreshable_materialized_view = 1` в 24.8, статус — в `system.view_refreshes`.
* Словари: `CREATE DICTIONARY ... SOURCE(CLICKHOUSE(...))` — загрузилось 40 000 элементов, `dictGetOrDefault` отдаёт дефолт на промахе. Проверять — `system.dictionaries`.

**Про `ReplacingMergeTree` (дедуп) — измерено на 202 000 заказов:**

```sql
SELECT count() FROM cheat.orders;                    -- 201 803 (вставки + обновления)
SELECT count() FROM cheat.orders FINAL;              -- 199 960 (дедуп + удалённые)
SELECT uniqExact(order_id) FROM cheat.orders;        -- 200 000
OPTIMIZE TABLE cheat.orders FINAL DEDUPLICATE BY order_id, created_at;
SELECT count() FROM cheat.orders;                    -- 201 335 (старые версии выброшены)
```

Заметки: **`FINAL` — не «удалить дубли», а «прочитать с дедупом»** (медленнее, зато всегда корректно); физически старые версии выбрасываются мержами или `OPTIMIZE`. Колонка-флаг `is_deleted = 1` превращает `ReplacingMergeTree` в «мягкое удаление»: такие строки `FINAL` не отдаёт. `DEDUPLICATE BY` обязан покрывать все колонки `ORDER BY`/`PRIMARY KEY`/`PARTITION BY`, иначе ошибка.

---

## 5. Kafka → ClickHouse

Файлы: `sql/02_kafka_ingest.sql`, генератор `kafka/producer_events.py`.

Схема конвейера: **Kafka-топик → таблица с движком `Kafka` → MATERIALIZED VIEW → таблица в `MergeTree` (+ отдельная MV в DLQ)**.

```bash
# 1. топик создаёт не ClickHouse, а Kafka
docker exec dwh-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic cheat.events --partitions 3 --replication-factor 1
```

```sql
CREATE TABLE cheat.events_kafka
(
    event_id UInt64, event_time DateTime, user_id UInt64,
    event_type LowCardinality(String), page LowCardinality(String),
    revenue Decimal(18,2), props String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',          -- имя сервиса из docker-сети, не localhost
    kafka_topic_list = 'cheat.events',
    kafka_group_name = 'ch_cheat_events',      -- это consumer group: меняешь — читаешь с нуля
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 3,                   -- ≤ числа партиций топика, иначе лишние простаивают
    kafka_max_block_size = 65536,
    kafka_poll_timeout_ms = 1000,
    kafka_skip_broken_messages = 0,            -- 0 = битые сообщения уходят в MV (видно в DLQ)
    kafka_handle_error_mode = 'stream';        -- вместо падения отдать строку с _error/_raw_message
```

```sql
-- сырьё + виртуальные колонки Kafka: _topic, _partition, _offset — храним для дедупа и трассировки
CREATE MATERIALIZED VIEW cheat.mv_events_stream TO cheat.events_stream AS
SELECT event_id, event_time, user_id, event_type, page, revenue, props,
       _topic AS kafka_topic, _partition AS kafka_partition, _offset AS kafka_offset,
       now() AS loaded_at
FROM cheat.events_kafka;

-- DLQ: сюда попадают только сообщения с ошибкой разбора
CREATE MATERIALIZED VIEW cheat.mv_events_dlq TO cheat.events_dlq AS
SELECT now() AS received_at, _topic, _partition, _offset, _error, _raw_message
FROM cheat.events_kafka WHERE _error != '';
```

**Проверено живьём** (20 005 сообщений, 7 500 msg/s):

```
events_stream: 20 004 строки, дублей по (partition, offset) — 0
DLQ:           4 строки с текстом ошибки и сырым сообщением
партиции:      offsets 0..6574 / 0..6772 / 0..6651
```

**Грабли, ради которых стоит читать этот раздел:**

* **`kafka_skip_broken_messages > 0` молча теряет битые сообщения.** С `= 100` пять невалидных JSON-ов исчезли бесследно (DLQ пуст, в хранилище ровно 20 000 «хороших»). С `= 0` они прилетели в DLQ с `_error`. Нужен контроль входа — ставьте `0` + `handle_error_mode = 'stream'`.
* **`ALTER TABLE ... MODIFY SETTING` для Kafka-движка запрещён** («table engine doesn't support settings changes»). Менять параметры — только `DROP`/`CREATE` таблицы (или `DETACH`/`ATTACH`). Оффсеты при этом не теряются: они в Kafka, по имени группы.
* **Проверено перебором в 24.8 — этих настроек НЕ существует:** `kafka_auto_offset_reset`, `kafka_compression_codec`, `kafka_compression_level`, `kafka_security_protocol`, `kafka_debug`, `kafka_retry_backoff_ms`. `kafka_max_wait_ms` — серверная настройка (`SET`), а не параметр движка. Если скопировали конфиг из статьи 2021 года — часть строк просто не применится.
* **Порядок важен:** MV нужно создать до того, как в топик пойдут данные. MV — это триггер на вставку, он не видит то, что вставилось раньше (`POPULATE` для Kafka-таблиц не работает).
* **`SELECT * FROM cheat.events_kafka` выгребает сообщения** из топика в этот запрос — для отладки ок, но не делайте так на боевом конвейере.
* Текущий лаг консьюмера из ClickHouse не видно (`num_messages_read - num_commits` — это счётчики за всю жизнь, не лаг):

```bash
docker exec dwh-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group ch_cheat_events
```

---

## 6. Витрины (марты)

Файл: `sql/03_marts.sql`. Правило: **DataLens и аналитик не ходят в сырой факт** на миллионы строк — им отдаём маленькую таблицу с готовыми агрегатами.

Инкрементальный март на агрегатных состояниях:

```sql
CREATE TABLE cheat.mart_daily_state
(
    event_date Date, country LowCardinality(String), device LowCardinality(String),
    event_type LowCardinality(String),
    users_state    AggregateFunction(uniq, UInt64),
    events_state   AggregateFunction(count),
    revenue_state  AggregateFunction(sum, Decimal(18,2)),
    p95_state      AggregateFunction(quantile(0.95), Decimal(18,2)),
    first_seen_state AggregateFunction(min, DateTime),
    last_seen_state  AggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, country, device, event_type);

-- MV считает состояния на каждой вставке в факт
CREATE MATERIALIZED VIEW cheat.mv_mart_daily TO cheat.mart_daily_state AS
SELECT toDate(event_time) AS event_date, country, device, event_type,
       uniqState(user_id) AS users_state, countState() AS events_state,
       sumState(revenue) AS revenue_state, quantileState(0.95)(revenue) AS p95_state,
       minState(event_time) AS first_seen_state, maxState(event_time) AS last_seen_state
FROM cheat.events
GROUP BY event_date, country, device, event_type;
```

Читаем через `-Merge` (в BI слой отдаём `View`, чтобы аналитик не думал про состояния):

```sql
CREATE VIEW cheat.mart_daily AS
SELECT event_date, country, device, event_type,
       uniqMerge(users_state) AS users, countMerge(events_state) AS events,
       sumMerge(revenue_state) AS revenue, quantileMerge(0.95)(p95_state) AS revenue_p95
FROM cheat.mart_daily_state
GROUP BY event_date, country, device, event_type;
```

**Грабли:** MV ловит **только новые вставки**. Данные, которые уже лежали в факте до создания MV, надо залить руками — тем же `SELECT`, что внутри MV, но с `INSERT INTO ... SELECT`. Это называется backfill, и забывают про него постоянно.

`SummingMergeTree` — когда нужны простые суммы (проверено: 3 строки → 2 после `OPTIMIZE`, потому что суммы по одинаковому ключу сложились). Хорош для «выручка по дням», плох для `uniq` (для уникальных — только `AggregatingMergeTree` с `uniqState`).

Пересборка витрины без простоя (и это же — рецепт для Airflow):

```sql
CREATE TABLE cheat.mart_daily_state_tmp AS cheat.mart_daily_state;
INSERT INTO cheat.mart_daily_state_tmp SELECT * FROM cheat.mart_daily_state WHERE event_date < today() - 7;
INSERT INTO cheat.mart_daily_state_tmp SELECT /* пересчёт окна за 7 дней */ ...;
EXCHANGE TABLES cheat.mart_daily_state_tmp AND cheat.mart_daily_state;  -- атомарно
DROP TABLE cheat.mart_daily_state_tmp;
```

**Сверка (обязательный шаг любой витрины):** агрегат из марта обязан совпасть с фактом.
`заказов в марте 49 578` = `paid-заказов в факте 49 578`. Не сходится — витрина врёт, в дашборд нельзя.

---

## 7. Airflow: ETL и DQ-гейты

Файлы: `etl/airflow/clickhouse_http.py` (мини-клиент), `etl/airflow/cheat_events_pipeline_dag.py` (DAG).

**Почему HTTP, а не `clickhouse-connect`:** в образе `apache/airflow:2.10.5` этого пакета нет (проверено: `ModuleNotFoundError`), а `requests` есть. Плюс HTTP-порт 8123 понимает те же настройки. Клиент — 60 строк: `query()` (SELECT → список словарей), `execute()` (DDL/DML), `insert_json()`, и в `execute()` по умолчанию `mutations_sync = 1`.

Конфиг — переменные окружения контейнера Airflow: `CLICKHOUSE_HOST=clickhouse`, `CLICKHOUSE_PORT=8123`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB`.

DAG `cheat_events_pipeline` (4 задачи, `schedule=None` — запускается вручную):

1. `kafka_health` — гейт на входе: консьюмеры есть, свежих битых сообщений в DLQ нет;
2. `load_incremental` — догрузка из стрима в факт, **идемпотентно** через `LEFT ANTI JOIN` по `event_id` + обогащение из `cheat.users` (`argMax(device, signup_date)`);
3. `rebuild_mart` — пересборка витрины за окно: `tmp`-таблица → `EXCHANGE TABLES` → `DROP`;
4. `dq_checks` — гейт после расчёта: дубли `event_id`, отрицательная выручка, покупки без пользователя, пустая/отставшая витрина. Не прошло — падаем, витрина не поедет в дашборд.

Деплой (два файла рядом, `clickhouse_http` импортируется по имени):

```bash
cp etl/airflow/clickhouse_http.py etl/airflow/cheat_events_pipeline_dag.py <папка_dags>/
# новый DAG-файл планировщик подхватит при следующем сканировании (dag_dir_list_interval),
# если ждать не хочется — перезапустить планировщик
docker restart dwh-airflow-scheduler
docker exec dwh-airflow-scheduler airflow dags list | grep cheat
```

**Две грабли, поймавшие меня на этом DAG (обе — про то, как писать DQ-гейты):**

* **`system.kafka_consumers` — серверная таблица: в ней видны консьюмеры ВСЕХ пайплайнов.** Первая версия гейта валилась на транзиентном `Broker: Coordinator load in progress` у чужой таблицы (`raw.prices_kafka`). Всегда фильтруйтесь по своей: `WHERE database = 'cheat' AND table = 'events_kafka'`. И не делайте «исторические ошибки консьюмера» жёстким гейтом — они транзиентные; жёсткий сигнал это отсутствие консьюмера и свежие строки в DLQ.
* **`last_poll_time` не годится как жёсткий гейт.** Наблюдал ложную тревогу «консьюмер не опрашивает топик > 5 минут» при полностью живом конвейере (через пару минут те же консьюмеры опрашивались нормально). В лог — да, в `raise` — нет.

---

## 8. BI-запросы: рецепты

Файл: `sql/04_bi_queries.sql` — 32 запроса, все прогнаны на базе `cheat`. Ниже — что именно пригодится в ежедневной работе.

**8.1. DAU/WAU/MAU и sticky factor** — `uniqExactIf(user_id, event_date = today())` и т.д., отношение DAU/MAU как «липкость».

**8.2. Динамика по дням с дырами** — `GROUP BY event_date ... ORDER BY event_date WITH FILL FROM today() - 14 TO today() STEP 1`. Пропущенные дни приходят нулями, а не выпадают из отчёта (в BigQuery это `GENERATE_DATE_ARRAY` + `LEFT JOIN`).

**8.3. Воронка** — `windowFunnel(86400)(event_time, event_type='view', event_type='search', ...)`, затем разворот в шаги через `UNION ALL` + оконные доли:

```sql
WITH funnel AS (
    SELECT countIf(level >= 1) AS s1, countIf(level >= 2) AS s2, countIf(level >= 3) AS s3,
           countIf(level >= 4) AS s4, countIf(level >= 5) AS s5
    FROM (SELECT user_id, windowFunnel(86400)(event_time,
              event_type = 'view', event_type = 'search', event_type = 'cart',
              event_type = 'checkout', event_type = 'purchase') AS level
          FROM cheat.events GROUP BY user_id)
)
SELECT *, round(100 * users / max(users) OVER (), 2) AS pct_of_start,
          round(100 * users / lagInFrame(users, 1, users) OVER (ORDER BY step_no), 2) AS pct_of_prev_step
FROM (SELECT 1 AS step_no, 'view' AS step, s1 AS users FROM funnel
      UNION ALL SELECT 2, 'search', s2 FROM funnel
      UNION ALL SELECT 3, 'cart', s3 FROM funnel
      UNION ALL SELECT 4, 'checkout', s4 FROM funnel
      UNION ALL SELECT 5, 'purchase', s5 FROM funnel)
ORDER BY step_no;
```

**8.4. Когортное удержание** — `age('month', signup_date, event_date)` + `uniqExact(user_id)` в разрезе когорта × период. **Грабля данных:** если события не покрывают день регистрации, `day0` в когорте будет меньше числа пользователей (у меня так и вышло: 877 против 2 653) — это не баг запроса, это дыра в данных.

**8.5. Retention-кривая** — агрегатная функция `retention(cond1, cond2, ...)` возвращает массив 0/1 по пользователю: `sum(r[1])`, `sum(r[2])`, ... по когортам.

**8.6. Сессии из «голых» событий** (если `session_id` не приходит из источника): две вложенности, потому что алиас окна нельзя переиспользовать в том же `SELECT`:

```sql
WITH ev AS (
    SELECT user_id, event_time,
           event_time - lagInFrame(event_time, 1, event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS gap
    FROM cheat.events WHERE event_date >= today() - 3
), marked AS (
    SELECT *, sum(if(gap > 1800, 1, 0)) OVER (PARTITION BY user_id ORDER BY event_time) AS session_no
    FROM ev
)
SELECT user_id, session_no, min(event_time) AS started, max(event_time) AS ended,
       count() AS events_in_session,
       dateDiff('second', min(event_time), max(event_time)) AS duration_sec
FROM marked GROUP BY user_id, session_no ORDER BY events_in_session DESC LIMIT 10;
```

**8.7. Накопительный итог и DoD/MoM** — `sum(x) OVER (ORDER BY d)` и `lagInFrame(x, 1, x) OVER (ORDER BY d)`; месячная динамика — тот же приём с `toStartOfMonth`.

**8.8. ASOF JOIN: пересчёт валют по курсу на дату заказа** — главный пример, зачем в ClickHouse есть `ASOF`:

```sql
SELECT o.order_id, o.created_at, o.currency, o.amount, r.rate,
       round(o.amount * r.rate, 2) AS amount_rub
FROM cheat.orders AS o FINAL
ASOF LEFT JOIN cheat.fx_rates AS r
   ON o.currency = r.currency AND toDate(o.created_at) >= r.rate_date
WHERE o.status = 'paid' ORDER BY o.created_at DESC LIMIT 10;
```

Почему нельзя обычным `LEFT JOIN`: тот же запрос без `ASOF` даёт **17 996 400 строк вместо 199 960** — каждая покупка размножилась на 90 курсов. Если видите «выручка выросла в 90 раз» — вот причина.
**Требование `ASOF`:** обязательна колонка равенства (equi-условие) плюс одна колонка неравенства. Чистое `ON a.x > b.y` не скомпилируется.

**8.9. Последнее состояние записи** — три способа, выбирать по ситуации: `argMax(status, updated_at)` в `GROUP BY`; `ReplacingMergeTree` + `FINAL`; `row_number() OVER (PARTITION BY id ORDER BY ts DESC) = 1`.

**8.10. TOP-N внутри группы** — `LIMIT 2 BY country`, `topK(3)(page)` (возвращает массив), `row_number()`.

**8.11. Pivot** — `sumIf(revenue, device = 'desktop') AS desktop_rev, ...` по одной строке на день; или `sumMap([device], [revenue])` для «свёртки в мапу»; или `groupArray`/`groupUniqArray`.

**8.12. Доли от итога** — `round(100 * users / sum(users) OVER (), 2)`: алиас из того же `SELECT` в оконной функции ClickHouse подставляет корректно.

**8.13. Квантили и средний чек** — `median()`, `quantile(0.9)()`, `quantileExact(0.95)()`, плюс `-If` комбинаторы: `quantileIf(0.5)(revenue, device = 'mobile' AND event_type = 'purchase')`.

**8.14. Пустая оконная рамка: `0` вместо `NULL`** — отличие от BigQuery, на котором легко получить неверный отчёт:

```sql
SELECT d,
       min(v)        OVER (ORDER BY d ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS m,        -- 0
       minOrNull(v)  OVER (ORDER BY d ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS m_or_null -- NULL
FROM (SELECT 1 AS d, 5 AS v UNION ALL SELECT 2, 7) ORDER BY d;
```

В BigQuery та же конструкция даёт `NULL`, в ClickHouse `min` от пустого набора отдаёт дефолт типа (**0**), и «минимум за прошлые 3 дня» молча превращается в ноль и ломает средние. Нужен `NULL` — берите `-OrNull` вариант.

**8.15. Разбор строки в строки (аналог `UNNEST`)** — `splitByChar('/', page)` + `arrayJoin`; с сохранением позиции (аналог `UNNEST ... WITH OFFSET`) — `ARRAY JOIN parts AS path_part, positions AS pos`, где `positions = arrayEnumerate(parts)`.

**8.16. JSON из строки** — `JSONExtractString(props, 'utm_source')`, `JSONExtractInt`, `JSONHas`; группировка по вытащенному полю работает как по обычной колонке.

**8.17. Атрибуция (first/last touch)** — `argMin(JSONExtractString(props,'utm_source'), event_time)` и `argMax(...)` в `GROUP BY user_id`.

**8.18. Анти-джойн** — `LEFT ANTI JOIN` (кого нет в заказах), `LEFT SEMI JOIN` (кто есть). Читается лучше, чем `NOT IN`.

**8.19. Комбинаторы, к которым стоит привыкнуть:** `-If` (агрегат по условию), `-Array` (по массиву: `sumArray([1,2,3])`), `-State`/`-Merge` (для витрин), `-OrNull` (NULL вместо дефолта), `-Distinct`.

---

## 9. Оптимизация запросов

В ClickHouse «оптимизация» — это в 80% случаев **правильный `ORDER BY` при создании таблицы**, а не переписывание запроса.

1. **Порядок колонок в `ORDER BY` = порядок, в котором вы фильтруете.** Измерено выше: `event_type = 'purchase'` режет гранулы 16/192, а `user_id = 42` — 192/192 (полный скан), хотя `user_id` в ключе есть. То, по чему фильтруете всегда (и то, что в джойнах), — в начало.
2. **Партиции — по фильтру, который есть во всех запросах.** `event_time >= today() - 1` → прочитано 2 парта из 4. Фильтр по материализованной `event_date` партиции не режет вообще.
3. **`EXPLAIN indexes = 1` перед тем, как жаловаться на скорость.** Смотрите `Partition`/`PrimaryKey`: если `Granules 192/192` — индекс не работает, и никакие настройки не помогут.
4. **`LowCardinality(String)` для «перечислений»** (страна, устройство, тип события) — словарь на лету, экономит и диск, и время на группировках.
5. **Кодеки под данные:** `ZSTD(3)` для JSON-строк, `Delta`/`DoubleDelta` для монотонных чисел и дат, `T64` для счётчиков.
6. **Предагрегация вместо тяжёлых запросов на лету.** Дашборд должен читать витрину из раздела 6 (тысячи строк), а не факт из миллионов.
7. **Не тащите `SELECT *`** из широких таблиц: ClickHouse читает колонками, лишние колонки — лишние байты.
8. **`FINAL` стоит дорого.** Если нужен дедуп в постоянном отчёте — материализуйте результат в витрину, а не считайте `FINAL` на каждый чих.
9. **Мелкие вставки = мелкие парты.** Вставлять по одной строке из ETL — верный способ утопить сервер в мержах; копите пачки (`kafka_max_block_size`, батчи в Airflow).
10. **`PREWHERE`** для селективных фильтров (ClickHouse часто сам его выводит, но если фильтр «тяжёлый» по вычислению — пишите явно).
11. **Профилируйте по факту:** `system.query_log` (раздел 10) покажет, что именно жрёт чтение, и сколько раз это запускалось.
12. **Time travel в ClickHouse нет.** Откат «на вчера» — только через бэкап/`FREEZE` или версионные таблицы (`ReplacingMergeTree` + колонка версии). Планируйте это заранее.

---

## 10. Эксплуатация и диагностика

Файл: `sql/05_ops_diagnostics.sql` (21 запрос, прогнаны на живом сервере).

**Что занимает место и как сжато:**

```sql
SELECT table, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS disk,
       round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.parts WHERE database = 'cheat' AND active GROUP BY table ORDER BY sum(bytes_on_disk) DESC;
```

Факт по демо-базе: `events` — 1 557 369 строк на 36.1 MiB (партиции 202608: 752 792 строки / 17.43 MiB, 202609: 804 577 / 18.67 MiB).

**Мутации (`ALTER UPDATE`/`DELETE`) — это перезапись партов, не мгновенная операция.** В скриптах ставьте `SET mutations_sync = 1`, иначе следующий DDL упадёт на «mutation is not finished yet». Контроль — `system.mutations` (`is_done`), при необходимости `KILL MUTATION`. В 24.8 работает и лёгкое `DELETE FROM t WHERE ...` (`allow_experimental_lightweight_delete = 1` по умолчанию): строка помечается мгновенно, физически удаляется мержами.

**`system.query_log` — главный инструмент разбора тормозов** (не забудьте `SYSTEM FLUSH LOGS` перед чтением):

```sql
SELECT event_time, query_duration_ms, read_rows, formatReadableSize(memory_usage) AS peak_mem,
       user, substring(query, 1, 80) AS query
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Select' AND event_time > now() - 86400
ORDER BY query_duration_ms DESC LIMIT 10;
```

Ошибки за сутки — `type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart')`. Кто кого убил — `system.processes` + `KILL QUERY WHERE query_id = '...' SYNC`.

**Бэкап:** `BACKUP TABLE cheat.users TO Disk('backups', 'cheat_users.zip')`, `BACKUP DATABASE cheat TO Disk('backups', 'cheat_full.zip')`, либо `ALTER TABLE cheat.events FREEZE` (жёсткие ссылки на парты в `shadow`). Список дисков — `SELECT * FROM system.disks`.

**TTL:** объявляется в `CREATE TABLE ... TTL event_time + INTERVAL 24 MONTH DELETE`. **Грабля 24.8: таблицы `system.table_ttl_infos` не существует** — TTL смотрите в `system.tables.create_table_query` и в `system.parts` (`delete_ttl_info_min/max`).

**Права для BI:** DataLens подключается с `readonly = 2`, поэтому заводите отдельного пользователя и давайте только чтение:

```sql
CREATE USER IF NOT EXISTS cheat_reader IDENTIFIED WITH sha256_password BY '<пароль>';
GRANT SELECT ON cheat.* TO cheat_reader;
```

**Health-check одной таблицей** (последний запрос в `sql/05_ops_diagnostics.sql`): консьюмеры Kafka и свежие строки в DLQ, незавершённые мутации, активные мержи, количество запросов дольше 5 секунд за сутки. Прогон: `events_stream_rows = 20 004`, `dlq_rows_24h = 4`.

**Грабля при диагностике:** запрос к `system.merges` **в момент реально идущего мержа** падал с `Code: 1001, std::length_error` (в `query_log` остаётся как `ExceptionWhileProcessing`). На синтетическом мерже небольшой таблицы не воспроизвелось — похоже на баг 24.8 на таблицах с проекциями. Если наткнулись: состояние мержей/партов смотрите через `system.parts` и `system.mutations`, они стабильны.

---

## 11. DataLens поверх ClickHouse

**Как DataLens реально ходит в ClickHouse** — вытащено из логов `datalens-data-api` на живом стенде (это не догадка, это те самые запросы):

```
ch_url:  http://clickhouse:8123/
ch_params: {"database": "cheat", "join_use_nulls": 1, "readonly": 2, "send_progress_in_http_headers": 0}
ch_query: SELECT t1.event_date AS res_0, t1.country AS res_1, SUM(t1.revenue) AS res_2
          FROM ( /* ваш SQL датасета */ ) AS t1
          GROUP BY res_0, res_1 ORDER BY res_2 DESC LIMIT 100 FORMAT JSONCompact
```

Что из этого следует практически:

* **Подключение.** Хост — `clickhouse`, порт **8123**, HTTP. Если DataLens и ClickHouse в одной docker-сети, имя сервиса работает как есть; иначе — проброс порта и внешний адрес.
* **`readonly = 2`.** DataLens физически не может ничего создавать/менять в ClickHouse. Значит: **все витрины, `View` и индексы готовит аналитик заранее** (SQL-файлы + Airflow), а в DataLens остаётся только чтение. Это не ограничение DataLens, а правильная архитектура — но знать надо: «сделаю таблицу прямо из датасета» не получится.
* **`join_use_nulls = 1`.** В DataLens `LEFT JOIN` отдаёт `NULL` (а не дефолт типа, как в чистом ClickHouse). Расхождение с тем, что вы видите в `clickhouse-client`, — вот откуда.
* **Датасет оборачивается в подзапрос `t1` + `GROUP BY res_N + LIMIT`.** Отсюда: ваш SQL датасета должен быть **плоским `SELECT` с уже посчитанными метриками**. Оконные функции, `ORDER BY` внутри, «умные» вычисления — либо не доживут, либо посчитаются не так, как вы ждёте. Всё, что сложнее «выбери и сгруппируй», считайте в витрине.
* **`LIMIT` без `ORDER BY`** в обёртке — это недетерминированная выборка (ClickHouse отдаёт то, что попалось первым). Для проверки «какие вообще есть значения» — ок, для отчёта — нет.
* **Часовой пояс UTC.** Даты в дашборде поедут относительно московского времени, если в витрине не привели к нужной зоне явно.

**Рабочий рецепт:** в ClickHouse — база с витринами и `View`-ями «BI-слой» (`cheat.mart_daily`), в DataLens — датасет из `SELECT` по этому `View`, без вычислений на стороне дашборда. Тогда данные в дашборде всегда совпадают с тем, что вы видите в SQL.

---

## 12. Список грабель одним экраном

1. HTTP (8123) не принимает multi-statement; `--multiquery` есть только у нативного клиента.
2. `ORDER BY` — не «сортировка», а первичный ключ: фильтр не по префиксу = полный скан.
3. Партиции режутся только фильтром по ключу партиционирования (`toYYYYMM(event_time)`), не по `event_date`.
4. `MATERIALIZE INDEX/PROJECTION` — асинхронная мутация → `SET mutations_sync = 1`.
5. `kafka_skip_broken_messages > 0` молча съедает битые сообщения (нужен `0` + `handle_error_mode = 'stream'`).
6. `ALTER ... MODIFY SETTING` для Kafka-движка запрещён — только пересоздание таблицы.
7. Часть «популярных» Kafka-настроек в 24.8 не существует (`kafka_auto_offset_reset` и др.).
8. MV ловит только новые вставки — про backfill помнить.
9. `system.kafka_consumers` показывает чужие пайплайны → фильтруйте по своей таблице.
10. `last_poll_time` и «исторические ошибки» консьюмера — не жёсткие гейты, они транзиентные.
11. `SELECT * FROM kafka_table` выгребает сообщения из топика.
12. `FINAL` = чтение с дедупом, не удаление; `DEDUPLICATE BY` обязан покрывать ключ.
13. Обычный `LEFT JOIN` к курсам размножил строки в 90 раз (17 996 400 vs 199 960) — для «значение на дату» нужен `ASOF JOIN` с equi-колонкой.
14. Пустая оконная рамка: `min` → **0**, не `NULL` (в BigQuery — `NULL`). Берите `minOrNull`.
15. `EXCLUDE` в оконной функции не поддерживается; алиас окна нельзя переиспользовать в том же `SELECT`.
16. Тип `JSON` в 24.8 экспериментальный, подколонки `Dynamic`: `WHERE props.a = 1` молча даёт 0 строк → `props.a::Int64 = 1`.
17. `system.table_ttl_infos` в 24.8 нет; `system.merges` может упасть с `Code 1001 std::length_error` во время мержа.
18. Часовой пояс сервера — UTC.
19. DataLens: `readonly = 2`, `join_use_nulls = 1`, обёртка `t1 ... GROUP BY res_N LIMIT N` → датасет = плоский агрегирующий `SELECT`, вся логика — в витрине.
20. Time travel в ClickHouse нет — бэкапы (`BACKUP`/`FREEZE`) и версии в таблицах.

---

## Как этим пользоваться

```bash
# 1. поднять демо-базу (создаст cheat: события, заказы, пользователи, курсы)
docker exec -i dwh-clickhouse clickhouse-client --multiquery < sql/00_setup_demo.sql

# 2. DDL и фишки схемы
docker exec -i dwh-clickhouse clickhouse-client --multiquery < sql/01_ddl_and_engines.sql

# 3. Kafka-конвейер (создаёт топик + Kafka-таблицу + MV + DLQ)
docker exec dwh-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic cheat.events --partitions 3 --replication-factor 1
docker exec -i dwh-clickhouse clickhouse-client --multiquery < sql/02_kafka_ingest.sql

# 4. наполнить топик событиями (в т.ч. битыми — для проверки DLQ)
pip install kafka-python-ng
python kafka/producer_events.py --count 20000 --broken 5

# 5. витрины и BI-запросы
docker exec -i dwh-clickhouse clickhouse-client --multiquery < sql/03_marts.sql
docker exec -i dwh-clickhouse clickhouse-client --multiquery < sql/04_bi_queries.sql

# 6. эксплуатация
docker exec -i dwh-clickhouse clickhouse-client --multiquery < sql/05_ops_diagnostics.sql
```

Структура репозитория:

```
sql/      00_setup_demo · 01_ddl_and_engines · 02_kafka_ingest · 03_marts · 04_bi_queries · 05_ops_diagnostics
kafka/    producer_events.py — генератор событий в топик (с режимом битых сообщений)
etl/airflow/  clickhouse_http.py — мини-клиент ClickHouse по HTTP для Airflow
              cheat_events_pipeline_dag.py — пайплайн: гейт → догрузка → витрина → DQ
```

Все примеры выполнены на ClickHouse 24.8.14.39. На других версиях возможны отличия (особенно по экспериментальным фичам: JSON, refreshable MV, lightweight delete).


