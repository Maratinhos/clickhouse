-- =============================================================================
-- 00. Демо-база `cheat` — на ней проверены все рецепты шпаргалки.
--
-- Запуск:  clickhouse-client --multiquery < sql/00_setup_demo.sql
-- Удалить: clickhouse-client --multiquery < sql/99_teardown.sql
--
-- Данные генерируются относительно now(), поэтому "последние 7 дней",
-- "текущий месяц" и т.п. в рецептах работают всегда, а не только сегодня.
-- Генерация детерминированная (cityHash64 вместо rand()) — прогон за прогоном
-- цифры совпадают, значит результаты в документации воспроизводимы.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS cheat;

-- -----------------------------------------------------------------------------
-- 1. Измерение пользователей: ReplacingMergeTree + MATERIALIZED-колонка
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.users;

CREATE TABLE cheat.users
(
    user_id     UInt64,
    signup_dt   DateTime,
    signup_date Date MATERIALIZED toDate(signup_dt),   -- вычисляется на вставке, читается бесплатно
    country     LowCardinality(String),
    device      LowCardinality(String),
    channel     LowCardinality(String),                -- источник привлечения
    plan        LowCardinality(String),
    updated_at  DateTime DEFAULT now()                 -- версия для ReplacingMergeTree
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(signup_date)
ORDER BY user_id;

INSERT INTO cheat.users (user_id, signup_dt, country, device, channel, plan)
SELECT
    number + 1                                                                          AS user_id,
    toDateTime(toStartOfDay(now()) - toIntervalDay(cityHash64(number, 'sd') % 90)
                                    - toIntervalSecond(cityHash64(number, 'ss') % 86400)) AS signup_dt,
    ['RU', 'KZ', 'BY', 'AM', 'GE'][1 + cityHash64(number, 'co') % 5]                    AS country,
    ['desktop', 'mobile', 'tablet'][1 + cityHash64(number, 'de') % 3]                   AS device,
    ['organic', 'paid_search', 'email', 'referral'][1 + cityHash64(number, 'ch') % 4]   AS channel,
    if(cityHash64(number, 'pl') % 10 = 0, 'pro', 'free')                                AS plan
FROM numbers(40000);

-- -----------------------------------------------------------------------------
-- 2. Факт событий: партиции по месяцам, ключ сортировки (тип события, время, юзер)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.events;

CREATE TABLE cheat.events
(
    event_id   UInt64,
    event_time DateTime,
    event_date Date MATERIALIZED toDate(event_time),   -- MATERIALIZED: считается на лету, в файлах не хранится
    user_id    UInt64,
    session_id UInt64,
    event_type LowCardinality(String),      -- view / search / cart / checkout / purchase
    page       LowCardinality(String),
    device     LowCardinality(String),
    country    LowCardinality(String),
    revenue    Decimal(18, 2) DEFAULT 0,
    props      String DEFAULT '{}' CODEC(ZSTD(3))   -- "JSON в String" — рабочий вариант без экспериментального типа
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time, user_id)
SETTINGS index_granularity = 8192;

-- 40 000 юзеров x 60 дней x до 5 шагов воронки, с затуханием активности по возрасту
-- аккаунта — чтобы когортное удержание выглядело как в жизни, а не как 100%.
INSERT INTO cheat.events
WITH toStartOfDay(now()) AS anchor
SELECT
    g.row_no + 1                                                                    AS event_id,
    toDateTime(g.event_date) + toIntervalSecond(cityHash64(g.row_no, 'sec') % 86400) AS event_time,
    g.user_id                                                                       AS user_id,
    cityHash64(concat(toString(g.user_id), '-', toString(g.day_no)))                 AS session_id,
    ['view', 'search', 'cart', 'checkout', 'purchase'][g.step + 1]                   AS event_type,
    ['/home', '/catalog', '/product', '/cart', '/checkout'][g.step + 1]              AS page,
    u.device                                                                        AS device,
    u.country                                                                       AS country,
    if(g.step = 4,
       toDecimal64(100 + (cityHash64(g.row_no, 'rev') % 90000) / 100, 2),
       toDecimal64(0, 2))                                                           AS revenue,
    concat('{"utm_source":"', u.channel, '","step":', toString(g.step), '}')         AS props
FROM
(
    SELECT
        number                                                  AS row_no,
        intDiv(number, 300) + 1                                 AS user_id,   -- 300 = 60 дней * 5 шагов
        intDiv(number % 300, 5)                                 AS day_no,    -- 0..59
        number % 5                                              AS step,      -- 0..4
        anchor - toIntervalDay(59 - intDiv(number % 300, 5))     AS event_date
    FROM numbers(40000 * 300)
) AS g
INNER JOIN cheat.users AS u ON u.user_id = g.user_id
WHERE g.event_date >= u.signup_date
  -- глубина воронки у каждого юзера своя (1..5 шагов в день)
  AND g.step < 1 + (cityHash64(g.user_id, g.day_no) % 5)
  -- затухание: чем старше аккаунт, тем меньше шанс активности
  AND (cityHash64(g.user_id, g.day_no, 'keep') % 100)
      < greatest(8, 100 - 3 * dateDiff('day', u.signup_date, g.event_date));

-- -----------------------------------------------------------------------------
-- 3. Заказы: ReplacingMergeTree(версия, флаг удаления) — дедуп и мягкое удаление
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.orders;

CREATE TABLE cheat.orders
(
    order_id   UInt64,
    user_id    UInt64,
    created_at DateTime,
    updated_at DateTime,
    status     LowCardinality(String),   -- new / paid / shipped / cancelled
    amount     Decimal(18, 2),
    currency   LowCardinality(String),
    is_deleted UInt8 DEFAULT 0           -- 1 = строка "удалена" (второй аргумент движка)
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
PARTITION BY toYYYYMM(created_at)
ORDER BY order_id;

-- первичная загрузка
INSERT INTO cheat.orders (order_id, user_id, created_at, updated_at, status, amount, currency)
SELECT
    number + 1                                                       AS order_id,
    cityHash64(number, 'ou') % 40000 + 1                             AS user_id,
    toDateTime(toStartOfDay(now()) - toIntervalDay(cityHash64(number, 'od') % 90))
        + toIntervalSecond(cityHash64(number, 'ot') % 86400)         AS created_at,
    toDateTime(toStartOfDay(now()) - toIntervalDay(cityHash64(number, 'od') % 90))
        + toIntervalSecond(cityHash64(number, 'ot') % 86400)         AS updated_at,
    ['new', 'paid', 'shipped', 'cancelled'][1 + cityHash64(number, 'os') % 4] AS status,
    toDecimal64(100 + (cityHash64(number, 'oa') % 990000) / 100, 2)  AS amount,
    ['RUB', 'USD', 'EUR', 'KZT'][1 + cityHash64(number, 'oc') % 4]   AS currency
FROM numbers(200000);

-- вторая загрузка: тот же order_id, но новее updated_at + пара "удалённых" строк.
-- Так выглядит CDC/повторный ETL-прогон — именно от этого спасает ReplacingMergeTree.
INSERT INTO cheat.orders (order_id, user_id, created_at, updated_at, status, amount, currency, is_deleted)
SELECT
    number * 7 + 1                                                   AS order_id,
    cityHash64(number * 7 + 1, 'ou') % 40000 + 1                     AS user_id,
    toDateTime(toStartOfDay(now()) - toIntervalDay(cityHash64(number * 7 + 1, 'od') % 90))
        + toIntervalSecond(cityHash64(number * 7 + 1, 'ot') % 86400) AS created_at,
    now()                                                            AS updated_at,   -- версия новее
    if(number % 20 = 0, 'cancelled', 'shipped')                      AS status,
    toDecimal64(100 + (cityHash64(number * 7 + 1, 'oa') % 990000) / 100, 2) AS amount,
    ['RUB', 'USD', 'EUR', 'KZT'][1 + cityHash64(number * 7 + 1, 'oc') % 4]  AS currency,
    if(number % 50 = 0, 1, 0)                                        AS is_deleted
FROM numbers(2000);

-- -----------------------------------------------------------------------------
-- 4. Курсы валют по дням — для ASOF JOIN (пересчёт заказов в RUB на дату заказа)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.fx_rates;

CREATE TABLE cheat.fx_rates
(
    currency  LowCardinality(String),
    rate_date Date,
    rate      Float64
)
ENGINE = MergeTree
ORDER BY (currency, rate_date);

INSERT INTO cheat.fx_rates
SELECT
    currency                                                                          AS currency,
    toDate(toStartOfDay(now())) - toIntervalDay(89 - d.day_no)                        AS rate_date,
    round(multiIf(currency = 'RUB', 1.0, currency = 'USD', 80.0, currency = 'EUR', 90.0, 0.17)
          + (cityHash64(currency, d.day_no) % 1000) / 100.0, 4)                       AS rate
FROM (SELECT ['RUB', 'USD', 'EUR', 'KZT'] AS currencies) AS cur
ARRAY JOIN cur.currencies AS currency
CROSS JOIN (SELECT number AS day_no FROM numbers(90)) AS d;
