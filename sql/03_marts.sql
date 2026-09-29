-- =============================================================================
-- 03. Витрины (марты): pre-aggregation, MV, состояния, атомарная замена
--
-- Идея: DataLens и аналитики не должны ходить в сырой факт на миллионы строк.
-- Март = маленькая таблица, где агрегаты уже посчитаны.
-- Три способа наполнения мартов — от самого простого к самому управляемому:
--   3.1  MATERIALIZED VIEW на сырой факт (реалтайм, но только новые вставки)
--   3.2  INSERT SELECT по расписанию из Airflow (идемпотентно, можно backfill)
--   3.3  REFRESHABLE MATERIALIZED VIEW (ClickHouse сам пересчитывает по таймеру)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1. Март на AggregatingMergeTree + MATERIALIZED VIEW
-- Держим не готовые числа, а "состояния" агрегатов: uniqState/sumState/quantileState.
-- Плюс: состояния разных дней можно досчитывать инкрементально, merge их склеивает.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.mart_daily_state;

CREATE TABLE cheat.mart_daily_state
(
    event_date      Date,
    country         LowCardinality(String),
    device          LowCardinality(String),
    event_type      LowCardinality(String),
    users_state     AggregateFunction(uniq, UInt64),
    sessions_state  AggregateFunction(uniq, UInt64),
    events_state    AggregateFunction(count),
    revenue_state   AggregateFunction(sum, Decimal(18, 2)),
    revenue_p95_state AggregateFunction(quantile(0.95), Decimal(18, 2)),
    first_seen_state AggregateFunction(min, DateTime),
    last_seen_state  AggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, country, device, event_type);

DROP VIEW IF EXISTS cheat.mv_mart_daily;

CREATE MATERIALIZED VIEW cheat.mv_mart_daily TO cheat.mart_daily_state AS
SELECT
    toDate(event_time) AS event_date,
    country,
    device,
    event_type,
    uniqState(user_id)              AS users_state,
    uniqState(session_id)           AS sessions_state,
    countState()                    AS events_state,
    sumState(revenue)               AS revenue_state,
    quantileState(0.95)(revenue)    AS revenue_p95_state,
    minState(event_time)            AS first_seen_state,
    maxState(event_time)            AS last_seen_state
FROM cheat.events
GROUP BY event_date, country, device, event_type;

-- ВАЖНО: MV видит только те вставки, что случились ПОСЛЕ его создания.
-- Данные, которые уже лежат в cheat.events, надо долить руками — тем же SELECT.
-- (в проде это делает Airflow-таск backfill, см. etl/airflow/)
INSERT INTO cheat.mart_daily_state
SELECT
    toDate(event_time) AS event_date,
    country,
    device,
    event_type,
    uniqState(user_id),
    uniqState(session_id),
    countState(),
    sumState(revenue),
    quantileState(0.95)(revenue),
    minState(event_time),
    maxState(event_time)
FROM cheat.events
GROUP BY event_date, country, device, event_type;

-- -----------------------------------------------------------------------------
-- 3.2. View-обёртка с -Merge: то, что читают аналитик и DataLens
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS cheat.mart_daily;

CREATE VIEW cheat.mart_daily AS
SELECT
    event_date,
    country,
    device,
    event_type,
    uniqMerge(users_state)              AS users,
    uniqMerge(sessions_state)           AS sessions,
    countMerge(events_state)            AS events,
    sumMerge(revenue_state)             AS revenue,
    quantileMerge(0.95)(revenue_p95_state) AS revenue_p95,
    minMerge(first_seen_state)          AS first_seen,
    maxMerge(last_seen_state)           AS last_seen
FROM cheat.mart_daily_state
GROUP BY event_date, country, device, event_type;

-- -----------------------------------------------------------------------------
-- 3.3. SummingMergeTree: самый дешёвый март, когда нужны только суммы
-- Заполняется не MV, а INSERT SELECT по расписанию: MV на ReplacingMergeTree
-- посчитал бы обе версии строки (дедуп делает только FINAL / merge).
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.mart_revenue_sum;

CREATE TABLE cheat.mart_revenue_sum
(
    day        Date,
    country    LowCardinality(String),
    currency   LowCardinality(String),
    orders     UInt64,
    revenue    Decimal(18, 2)
)
ENGINE = SummingMergeTree((orders, revenue))
PARTITION BY toYYYYMM(day)
ORDER BY (day, country, currency);

-- пересчёт "с нуля" идемпотентно: считаем из FINAL (одна версия на заказ)
INSERT INTO cheat.mart_revenue_sum
SELECT
    toDate(o.created_at) AS day,
    u.country            AS country,
    o.currency           AS currency,
    count()              AS orders,
    sum(o.amount)        AS revenue
FROM cheat.orders AS o FINAL
INNER JOIN cheat.users AS u ON u.user_id = o.user_id
WHERE o.status = 'paid' AND o.is_deleted = 0
GROUP BY day, country, currency;

SELECT 'март: строк' AS metric, count() AS value FROM cheat.mart_revenue_sum
UNION ALL
SELECT 'март: заказов', sum(orders) FROM cheat.mart_revenue_sum
UNION ALL
SELECT 'факт: paid-заказов', count() FROM cheat.orders FINAL WHERE status = 'paid' AND is_deleted = 0;

-- -----------------------------------------------------------------------------
-- 3.4. Атомарная замена марта (blue/green): считаем в temp, потом EXCHANGE TABLES
-- Так витрина никогда не бывает "полупустой" в момент перезагрузки.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.mart_revenue_sum_tmp;

CREATE TABLE cheat.mart_revenue_sum_tmp AS cheat.mart_revenue_sum;

INSERT INTO cheat.mart_revenue_sum_tmp
SELECT toDate(created_at), 'ZZ', currency, count(), sum(amount)
FROM cheat.orders FINAL
WHERE status = 'paid' AND is_deleted = 0
GROUP BY toDate(created_at), currency;

EXCHANGE TABLES cheat.mart_revenue_sum AND cheat.mart_revenue_sum_tmp;

DROP TABLE cheat.mart_revenue_sum_tmp;

-- -----------------------------------------------------------------------------
-- 3.5. Что читает DataLens: плоский SELECT из view, без окон и подзапросов
-- DataLens оборачивает датасет в подзапрос и добавляет GROUP BY ... LIMIT,
-- поэтому отдаём ему уже агрегированный и плоский результат.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS cheat.dl_daily_kpi;

CREATE VIEW cheat.dl_daily_kpi AS
SELECT
    event_date,
    toStartOfWeek(event_date, 1) AS week_start,
    toStartOfMonth(event_date)   AS month_start,
    country,
    device,
    event_type,
    users,
    sessions,
    events,
    revenue,
    if(events = 0, 0, revenue / events) AS revenue_per_event
FROM cheat.mart_daily;
