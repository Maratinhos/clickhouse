-- =============================================================================
-- 01. DDL, движки, фишки схемы. Всё создаётся в базе cheat и проверено живьём.
-- Файл идемпотентен: можно запускать повторно.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1. Каркас MergeTree: что за что отвечает
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.demo_mergetree;

CREATE TABLE cheat.demo_mergetree
(
    -- типы: чем точнее, тем меньше байт. LowCardinality — для строк с малым числом
    -- уникальных значений (страна, тип события): экономит в разы и ускоряет GROUP BY
    dt          DateTime CODEC(DoubleDelta, ZSTD(1)),   -- монотонное время -> DoubleDelta
    country     LowCardinality(String),
    user_id     UInt64,
    amount      Decimal(18, 2) CODEC(ZSTD(3)),
    cnt         UInt32 DEFAULT 1,
    -- MATERIALIZED: считается на вставке, в INSERT не передаётся, читается бесплатно
    day         Date MATERIALIZED toDate(dt),
    -- ALIAS: вычисляется на чтении, в таблице не хранится (удобно для формул витрины)
    amount_rub  Float64 ALIAS amount * 1.0,
    -- EPHEMERAL: не хранится, доступна только внутри запроса (для INSERT ... SELECT)
    src         String EPHEMERAL,
    -- обычная колонка с дефолтом
    comment     String DEFAULT '',
    INDEX idx_user user_id TYPE bloom_filter(0.01) GRANULARITY 4   -- пропускающий индекс
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)          -- партиция = единица удаления/перемещения данных
ORDER BY (country, dt)             -- ключ сортировки = физический порядок + первичный индекс
PRIMARY KEY country                -- можно сузить: индекс только по country
TTL day + INTERVAL 1 YEAR          -- автоудаление старых партиций
SETTINGS index_granularity = 8192; -- 8192 строк в грануле по умолчанию

INSERT INTO cheat.demo_mergetree (dt, country, user_id, amount)
SELECT now() - toIntervalDay(number % 30),
       ['RU', 'KZ', 'BY'][1 + number % 3],
       number % 1000,
       number / 100
FROM numbers(100000);

SELECT count() AS rows, uniqExact(country) AS countries FROM cheat.demo_mergetree;

-- -----------------------------------------------------------------------------
-- 1.2. Как посмотреть, что реально получилось
-- -----------------------------------------------------------------------------
SELECT database, table, engine, partition_key, sorting_key, primary_key, total_rows, comment
FROM system.tables
WHERE database = 'cheat' AND table = 'demo_mergetree'
FORMAT Vertical;

SELECT name, type, default_kind, default_expression, compression_codec
FROM system.columns
WHERE database = 'cheat' AND table = 'demo_mergetree'
ORDER BY position;

-- -----------------------------------------------------------------------------
-- 1.3. DDL: добавить/изменить/удалить колонку, индекс, проекцию
-- -----------------------------------------------------------------------------
-- ВАЖНО: MATERIALIZE INDEX/PROJECTION и ALTER UPDATE/DELETE — это асинхронные
-- мутации. Без mutations_sync = 1 следующий DDL упадёт с "Cannot drop index ...
-- it's affected by mutation ... which is not finished yet".
SET mutations_sync = 1;

ALTER TABLE cheat.demo_mergetree ADD COLUMN IF NOT EXISTS channel LowCardinality(String) DEFAULT 'unknown';
ALTER TABLE cheat.demo_mergetree COMMENT COLUMN channel 'источник привлечения';
ALTER TABLE cheat.demo_mergetree MODIFY COLUMN cnt UInt64;                     -- расширение типа — метаданные, без перезаписи
ALTER TABLE cheat.demo_mergetree ADD INDEX IF NOT EXISTS idx_amount amount TYPE minmax GRANULARITY 4;
ALTER TABLE cheat.demo_mergetree MATERIALIZE INDEX idx_amount;                 -- построить индекс по уже лежащим данным
ALTER TABLE cheat.demo_mergetree DROP INDEX IF EXISTS idx_amount;
-- ALTER ... MODIFY TTL меняет правило хранения без перезаписи данных
ALTER TABLE cheat.demo_mergetree MODIFY TTL day + INTERVAL 2 YEAR;

-- -----------------------------------------------------------------------------
-- 1.4. Проекция: альтернатива второй таблице-витрине (агрегат по другой колонке)
-- -----------------------------------------------------------------------------
ALTER TABLE cheat.demo_mergetree
    ADD PROJECTION IF NOT EXISTS p_country_amount (SELECT country, sum(amount), count() GROUP BY country);
ALTER TABLE cheat.demo_mergetree MATERIALIZE PROJECTION p_country_amount;

SELECT partition, name, part_type, rows, formatReadableSize(bytes_on_disk) AS size
FROM system.projection_parts
WHERE database = 'cheat' AND table = 'demo_mergetree' AND active
LIMIT 5;

-- -----------------------------------------------------------------------------
-- 1.5. Копирование структуры и данных
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.demo_copy_like;
DROP TABLE IF EXISTS cheat.demo_copy_as;
DROP TABLE IF EXISTS cheat.demo_copy_empty;

CREATE TABLE cheat.demo_copy_like AS cheat.demo_mergetree;                    -- только структура + движок
CREATE TABLE cheat.demo_copy_as ENGINE = MergeTree ORDER BY user_id AS
    SELECT user_id, amount FROM cheat.demo_mergetree;                          -- структура из SELECT + данные
CREATE TABLE cheat.demo_copy_empty AS cheat.demo_mergetree;                    -- структура
INSERT INTO cheat.demo_copy_empty SELECT * FROM cheat.demo_mergetree WHERE 0;  -- EMPTY AS SELECT

SELECT 'copy_like' AS t, count() FROM cheat.demo_copy_like
UNION ALL SELECT 'copy_as', count() FROM cheat.demo_copy_as
UNION ALL SELECT 'copy_empty', count() FROM cheat.demo_copy_empty;

-- -----------------------------------------------------------------------------
-- 1.6. RENAME / EXCHANGE / TRUNCATE / DETACH
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.demo_a;
DROP TABLE IF EXISTS cheat.demo_b;
CREATE TABLE cheat.demo_a (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE cheat.demo_b (id UInt64, note String) ENGINE = MergeTree ORDER BY id;

INSERT INTO cheat.demo_a VALUES (1), (2);
INSERT INTO cheat.demo_b VALUES (3, 'b');

-- EXCHANGE TABLES — атомарная подмена: собрали витрину в новой таблице и переключили
EXCHANGE TABLES cheat.demo_a AND cheat.demo_b;

SELECT 'a' AS table_name, count() AS rows FROM cheat.demo_a
UNION ALL SELECT 'b', count() FROM cheat.demo_b;

RENAME TABLE cheat.demo_a TO cheat.demo_a_renamed;
DROP TABLE cheat.demo_a_renamed;
DROP TABLE cheat.demo_b;

-- DETACH/ATTACH: таблица исчезает из БД, но данные остаются на диске
DETACH TABLE cheat.demo_copy_empty;
ATTACH TABLE cheat.demo_copy_empty;

-- -----------------------------------------------------------------------------
-- 1.7. ReplacingMergeTree: дедуп и «мягкое удаление» (проверено на cheat.orders)
-- -----------------------------------------------------------------------------
-- В cheat.orders две загрузки: 200 000 строк + 2 000 строк с тем же order_id,
-- но более новым updated_at (часть с is_deleted = 1). Смотрим разницу.
SELECT 'все строки (сырые)' AS metric, count() AS value FROM cheat.orders
UNION ALL SELECT 'FINAL (после дедупа)', count() FROM cheat.orders FINAL
UNION ALL SELECT 'уникальных order_id', uniqExact(order_id) FROM cheat.orders;

-- что именно перезаписалось: старые версии отфильтрованы
SELECT order_id, status, updated_at
FROM cheat.orders FINAL
WHERE order_id IN (1, 8, 15)
ORDER BY order_id;

-- ручной дедуп (обычно не нужен — merge делает это сам).
-- Две грабли:
--   1) DEDUPLICATE BY должен покрывать все колонки ORDER BY/PRIMARY KEY/PARTITION BY;
--   2) в PARTITION можно передать только константу: PARTITION 202609 или
--      PARTITION ID '202609'. Выражение вида toYYYYMM(now()) — синтаксическая ошибка.
OPTIMIZE TABLE cheat.orders FINAL DEDUPLICATE BY order_id, created_at;

-- дедуп одной партиции (id партиции смотрим в system.parts)
OPTIMIZE TABLE cheat.orders PARTITION ID '202609' FINAL DEDUPLICATE BY order_id, created_at;

SELECT 'после дедупа' AS metric, count() AS value FROM cheat.orders
UNION ALL SELECT 'FINAL', count() FROM cheat.orders FINAL;

-- -----------------------------------------------------------------------------
-- 1.8. SummingMergeTree и AggregatingMergeTree: агрегаты, которые переживают merge
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.demo_summing;

CREATE TABLE cheat.demo_summing
(
    day      Date,
    country  LowCardinality(String),
    orders   UInt64,
    revenue  Decimal(18, 2)
)
ENGINE = SummingMergeTree
ORDER BY (day, country);

INSERT INTO cheat.demo_summing VALUES
    (today(), 'RU', 10, 1000), (today(), 'RU', 5, 500), (today(), 'KZ', 3, 300);

SELECT day, country, orders, revenue FROM cheat.demo_summing;                    -- пока 3 строки
OPTIMIZE TABLE cheat.demo_summing FINAL;                                         -- merge сложил дубли
SELECT day, country, orders, revenue FROM cheat.demo_summing;                    -- осталось 2 строки

-- -----------------------------------------------------------------------------
-- 1.9. Словарь: маленькое измерение для dictGet (быстрее, чем JOIN)
-- -----------------------------------------------------------------------------
DROP DICTIONARY IF EXISTS cheat.users_dict;
DROP TABLE IF EXISTS cheat.users_dict_src;

CREATE TABLE cheat.users_dict_src
(
    user_id UInt64,
    country LowCardinality(String),
    plan    LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO cheat.users_dict_src SELECT user_id, country, plan FROM cheat.users;

-- ВАЖНО: если SOURCE(CLICKHOUSE(...)) не указать host/user/password/db явно,
-- сервер пойдёт в localhost:9000 под пользователем default и получит
-- "Authentication failed". Креды указываются в SOURCE.
CREATE DICTIONARY cheat.users_dict
(
    user_id UInt64,
    country String,
    plan    String
)
PRIMARY KEY user_id
SOURCE(CLICKHOUSE(
    host 'clickhouse' port 9000 user 'bi_admin' password '<пароль из .env>'
    db 'cheat' table 'users_dict_src'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 600);

SELECT dictGet('cheat.users_dict', 'country', toUInt64(1))       AS country,
       dictGet('cheat.users_dict', 'plan', toUInt64(1))          AS plan,
       dictGetOrDefault('cheat.users_dict', 'plan', toUInt64(999999), 'нет такого') AS fallback;

SELECT name, status, element_count, last_successful_update_time
FROM system.dictionaries WHERE database = 'cheat' FORMAT Vertical;

-- -----------------------------------------------------------------------------
-- 1.10. Refreshable materialized view (аналог scheduled query в BigQuery)
-- В 24.8 требует настройки allow_experimental_refreshable_materialized_view = 1
-- -----------------------------------------------------------------------------
SET allow_experimental_refreshable_materialized_view = 1;

DROP VIEW IF EXISTS cheat.demo_refreshable;

CREATE MATERIALIZED VIEW cheat.demo_refreshable
REFRESH EVERY 1 MINUTE
ENGINE = MergeTree
ORDER BY (event_type)
AS
SELECT event_type, count() AS events, uniqExact(user_id) AS users
FROM cheat.events
GROUP BY event_type;

SELECT * FROM cheat.demo_refreshable ORDER BY events DESC;

-- состояние обновления: system.view_refreshes
SELECT database, view, status, last_refresh_result, last_success_time, next_refresh_time, refresh_count
FROM system.view_refreshes FORMAT Vertical;

-- -----------------------------------------------------------------------------
-- 1.11. JSON: два варианта — String + JSONExtract* (рабочий) и тип JSON (24.8: экспериментальный)
-- -----------------------------------------------------------------------------
SET allow_experimental_json_type = 1;   -- тип JSON в 24.8 выключен по умолчанию

-- (а) рабочий вариант: JSON хранится как String, разбирается функциями на чтении
SELECT props,
       JSONExtractString(props, 'utm_source')                       AS utm_source,
       JSONExtractInt(props, 'step')                                AS step,
       JSONExtractString(props, 'utm_source', 'campaign')           AS nested_path  -- путь через запятую
FROM cheat.events
LIMIT 3;

-- (б) тип JSON: требует allow_experimental_json_type = 1 на уровне запроса,
--     а подколонки имеют тип Dynamic — сравнение без приведения типа молча вернёт 0 строк
DROP TABLE IF EXISTS cheat.demo_json;

CREATE TABLE cheat.demo_json (id UInt64, props JSON) ENGINE = MergeTree ORDER BY id;

INSERT INTO cheat.demo_json
SELECT number, '{"a":1,"b":{"c":"x"},"tags":["p","q"]}' FROM numbers(3);

SELECT props.a AS a, toTypeName(props.a) AS type_of_subcolumn FROM cheat.demo_json LIMIT 1;
SELECT countIf(props.a = 1)        AS wrong_way,   -- 0: Dynamic нельзя сравнить с числом напрямую
       countIf(props.a::Int64 = 1) AS right_way    -- 3: сначала приводим тип
FROM cheat.demo_json;

-- какие пути и типы ClickHouse вывел сам
SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(props)) AS path_and_type FROM cheat.demo_json;

-- -----------------------------------------------------------------------------
-- 1.12. Пользователь для BI-инструмента: read-only (DataLens сам шлёт readonly=2)
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS cheat_reader IDENTIFIED WITH sha256_password BY 'cheat_reader_pwd'
    SETTINGS readonly = 1, max_execution_time = 60, max_memory_usage = 4000000000;

GRANT SELECT ON cheat.* TO cheat_reader;

SHOW GRANTS FOR cheat_reader;

-- -----------------------------------------------------------------------------
-- 1.13. Уборка временных демо-таблиц
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.demo_copy_like;
DROP TABLE IF EXISTS cheat.demo_copy_as;
DROP TABLE IF EXISTS cheat.demo_copy_empty;
DROP TABLE IF EXISTS cheat.demo_json;
DROP TABLE IF EXISTS cheat.demo_summing;
DROP TABLE IF EXISTS cheat.demo_mergetree;
DROP VIEW IF EXISTS cheat.demo_refreshable;
DROP DICTIONARY IF EXISTS cheat.users_dict;
DROP TABLE IF EXISTS cheat.users_dict_src;
