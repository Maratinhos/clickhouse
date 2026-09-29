-- =============================================================================
-- 04. BI-запросы: то, что реально спрашивают у аналитика
-- Все примеры проверены на базе cheat (см. sql/00_setup_demo.sql).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1. DAU / WAU / MAU и sticky factor одним запросом
-- -----------------------------------------------------------------------------
SELECT uniqExactIf(user_id, event_date = today() - 1)  AS dau,
       uniqExactIf(user_id, event_date >= today() - 7) AS wau,
       uniqExactIf(user_id, event_date >= today() - 30) AS mau,
       round(dau / mau, 3)                             AS sticky_dau_mau
FROM cheat.events
WHERE event_date >= today() - 30;

-- -----------------------------------------------------------------------------
-- 4.2. Динамика по дням + WITH FILL: пустые дни не выпадают из графика
-- (в BigQuery приходилось делать GENERATE_DATE_ARRAY + LEFT JOIN)
-- -----------------------------------------------------------------------------
SELECT event_date,
       count()            AS events,
       uniqExact(user_id) AS users,
       sum(revenue)       AS revenue
FROM cheat.events
WHERE event_date >= today() - 14
GROUP BY event_date
ORDER BY event_date WITH FILL FROM today() - 14 TO today() STEP 1;

-- -----------------------------------------------------------------------------
-- 4.3. Воронка: windowFunnel + конверсия по шагам
-- windowFunnel(window)(time, cond1, cond2, ...) возвращает максимальный
-- достигнутый шаг на пользователя в пределах окна (86400 сек).
-- -----------------------------------------------------------------------------
WITH funnel AS (
    SELECT countIf(level >= 1) AS s1,
           countIf(level >= 2) AS s2,
           countIf(level >= 3) AS s3,
           countIf(level >= 4) AS s4,
           countIf(level >= 5) AS s5
    FROM (
        SELECT user_id,
               windowFunnel(86400)(event_time,
                   event_type = 'view',
                   event_type = 'search',
                   event_type = 'cart',
                   event_type = 'checkout',
                   event_type = 'purchase') AS level
        FROM cheat.events
        GROUP BY user_id
    )
)
SELECT step_no,
       step,
       users,
       round(100 * users / max(users) OVER (), 2)                                  AS pct_of_start,
       round(100 * users / lagInFrame(users, 1, users) OVER (ORDER BY step_no), 2) AS pct_of_prev_step
FROM (
    SELECT 1 AS step_no, 'view'     AS step, s1 AS users FROM funnel
    UNION ALL SELECT 2, 'search',   s2 FROM funnel
    UNION ALL SELECT 3, 'cart',     s3 FROM funnel
    UNION ALL SELECT 4, 'checkout', s4 FROM funnel
    UNION ALL SELECT 5, 'purchase', s5 FROM funnel
)
ORDER BY step_no;

-- -----------------------------------------------------------------------------
-- 4.4. Когортное удержание: матрица "неделя регистрации x неделя активности"
-- age('week', signup, activity) = сколько недель прошло
-- -----------------------------------------------------------------------------
SELECT toStartOfWeek(u.signup_date, 1)                    AS cohort_week,
       dateDiff('week', u.signup_date, e.event_date)      AS week_no,
       uniqExact(u.user_id)                               AS users
FROM cheat.users AS u
INNER JOIN (SELECT DISTINCT user_id, event_date FROM cheat.events) AS e
        ON e.user_id = u.user_id
WHERE u.signup_date >= today() - 42
GROUP BY cohort_week, week_no
ORDER BY cohort_week, week_no
LIMIT 40;

-- -----------------------------------------------------------------------------
-- 4.5. Retention curve одной функцией: retention(cond1, cond2, ...) -> массив 0/1
-- -----------------------------------------------------------------------------
SELECT cohort,
       count()    AS users,
       sum(r[1])  AS day0,
       sum(r[2])  AS day1,
       sum(r[3])  AS day3,
       sum(r[4])  AS day7,
       sum(r[5])  AS day14,
       sum(r[6])  AS day30,
       round(100 * sum(r[4]) / sum(r[1]), 1) AS d7_rate_pct
FROM (
    SELECT u.user_id,
           toStartOfWeek(u.signup_date, 1) AS cohort,
           retention(e.event_date = u.signup_date,
                     e.event_date = u.signup_date + 1,
                     e.event_date = u.signup_date + 3,
                     e.event_date = u.signup_date + 7,
                     e.event_date = u.signup_date + 14,
                     e.event_date = u.signup_date + 30) AS r
    FROM cheat.users AS u
    LEFT JOIN cheat.events AS e ON e.user_id = u.user_id
    WHERE u.signup_date >= today() - 63
    GROUP BY u.user_id, cohort
)
GROUP BY cohort
ORDER BY cohort;

-- -----------------------------------------------------------------------------
-- 4.6. Сессии из событий (аналог sessionize, которого в 24.8 нет)
-- Две вложенности обязательны: алиас session_no нельзя использовать в окне
-- того же SELECT (ошибка NOT_FOUND_COLUMN_IN_BLOCK).
-- -----------------------------------------------------------------------------
WITH ev AS (
    SELECT user_id,
           event_time,
           event_time - lagInFrame(event_time, 1, event_time)
                        OVER (PARTITION BY user_id ORDER BY event_time) AS gap
    FROM cheat.events
    WHERE event_date >= today() - 2
),
marked AS (
    SELECT user_id, event_time,
           sum(if(gap > 1800, 1, 0)) OVER (PARTITION BY user_id ORDER BY event_time) AS session_no
    FROM ev
)
SELECT user_id,
       session_no,
       min(event_time)                                          AS session_start,
       max(event_time)                                          AS session_end,
       count()                                                  AS events_in_session,
       dateDiff('second', min(event_time), max(event_time))     AS duration_sec
FROM marked
GROUP BY user_id, session_no
ORDER BY events_in_session DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 4.7. Накопительный итог + DoD/MoM через lagInFrame
-- -----------------------------------------------------------------------------
WITH daily AS (
    SELECT event_date,
           sum(revenue) AS revenue
    FROM cheat.events
    WHERE event_type = 'purchase' AND event_date >= today() - 30
    GROUP BY event_date
)
SELECT event_date,
       revenue,
       sum(revenue) OVER (ORDER BY event_date)                                   AS revenue_cum,
       lagInFrame(revenue, 1, 0) OVER (ORDER BY event_date)                      AS revenue_prev_day,
       round(100 * (revenue - lagInFrame(revenue, 1, 0) OVER (ORDER BY event_date))
             / nullIf(lagInFrame(revenue, 1, 0) OVER (ORDER BY event_date), 0), 2) AS dod_pct,
       round(avg(revenue) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ma7
FROM daily
ORDER BY event_date;

-- -----------------------------------------------------------------------------
-- 4.8. ASOF JOIN: пересчёт заказов в рубли по курсу, действовавшему на дату заказа
-- ASOF JOIN в ClickHouse 24.8 обязан иметь хотя бы одну колонку равенства.
-- -----------------------------------------------------------------------------
SELECT o.order_id,
       o.created_at,
       o.currency,
       o.amount,
       r.rate,
       round(o.amount * r.rate, 2) AS amount_rub
FROM cheat.orders AS o FINAL
ASOF LEFT JOIN cheat.fx_rates AS r
     ON o.currency = r.currency AND toDate(o.created_at) >= r.rate_date
WHERE o.status = 'paid'
ORDER BY o.created_at DESC
LIMIT 10;

-- для сравнения: обычный LEFT JOIN на справочник курсов раздувает строки
SELECT 'ASOF JOIN' AS how, count() AS rows FROM
    (SELECT o.order_id FROM cheat.orders AS o FINAL
     ASOF LEFT JOIN cheat.fx_rates AS r ON o.currency = r.currency AND toDate(o.created_at) >= r.rate_date)
UNION ALL
SELECT 'обычный LEFT JOIN', count() FROM
    (SELECT o.order_id FROM cheat.orders AS o FINAL
     LEFT JOIN cheat.fx_rates AS r ON o.currency = r.currency);

-- -----------------------------------------------------------------------------
-- 4.9. Последнее состояние строки: три способа
-- -----------------------------------------------------------------------------
-- (а) argMax — если версии лежат в одной таблице и нужен произвольный срез
SELECT order_id,
       argMax(status, updated_at) AS last_status,
       argMax(amount, updated_at) AS last_amount,
       max(updated_at)            AS last_update
FROM cheat.orders
GROUP BY order_id
ORDER BY order_id
LIMIT 5;

-- (б) FINAL — если таблица на ReplacingMergeTree и нужна одна строка на ключ
SELECT status, count() AS orders, sum(amount) AS amount
FROM cheat.orders FINAL
WHERE is_deleted = 0
GROUP BY status
ORDER BY orders DESC;

-- (в) row_number — универсальный вариант, работает и на MergeTree
SELECT order_id, status, amount, updated_at
FROM (
    SELECT order_id, status, amount, updated_at,
           row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) AS rn
    FROM cheat.orders
)
WHERE rn = 1
ORDER BY order_id
LIMIT 5;

-- -----------------------------------------------------------------------------
-- 4.10. TOP-N по группам: LIMIT BY, topK, row_number
-- -----------------------------------------------------------------------------
SELECT country, event_date, revenue
FROM cheat.mart_daily
WHERE event_type = 'purchase' AND event_date >= today() - 7
ORDER BY revenue DESC
LIMIT 2 BY country;

SELECT topK(3)(page) AS top3_pages, topK(3)(country) AS top3_countries
FROM cheat.events
WHERE event_date >= today() - 7;

-- -----------------------------------------------------------------------------
-- 4.11. Pivot и "массив значений": sumIf / sumMap / groupArray
-- -----------------------------------------------------------------------------
SELECT event_date,
       sumIf(revenue, device = 'desktop') AS rev_desktop,
       sumIf(revenue, device = 'mobile')  AS rev_mobile,
       sumIf(revenue, device = 'tablet')  AS rev_tablet
FROM cheat.events
WHERE event_type = 'purchase' AND event_date >= today() - 7
GROUP BY event_date
ORDER BY event_date;

SELECT sumMap([device], [revenue]) AS revenue_by_device_map
FROM cheat.events
WHERE event_type = 'purchase' AND event_date >= today() - 7;

SELECT country,
       groupUniqArray(3)(page)  AS some_pages,
       groupArray(5)(event_type) AS first_5_events
FROM cheat.events
WHERE event_date >= today() - 7
GROUP BY country
ORDER BY country;

-- -----------------------------------------------------------------------------
-- 4.12. Доли от итога: оконная сумма по алиасу в том же SELECT
-- -----------------------------------------------------------------------------
SELECT country,
       uniqExact(user_id)                          AS users,
       round(100 * users / sum(users) OVER (), 2)  AS share_pct,
       round(sum(revenue), 2)                      AS revenue
FROM cheat.events
WHERE event_date >= today() - 30
GROUP BY country
ORDER BY users DESC;

-- -----------------------------------------------------------------------------
-- 4.13. Средний чек, медиана, квантили
-- -----------------------------------------------------------------------------
SELECT count()                        AS purchases,
       round(avg(revenue), 2)         AS avg_check,
       median(revenue)                AS median_check,
       quantile(0.90)(revenue)        AS p90,
       quantileExact(0.95)(revenue)   AS p95_exact,
       max(revenue)                   AS max_check
FROM cheat.events
WHERE event_type = 'purchase' AND event_date >= today() - 30;

-- -----------------------------------------------------------------------------
-- 4.14. Разбор строки на части: splitByChar + arrayJoin (аналог UNNEST)
-- С сохранением позиции части — через arrayEnumerate (аналог WITH OFFSET в BigQuery)
-- -----------------------------------------------------------------------------
SELECT arrayJoin(splitByChar('/', page)) AS path_part,
       count()                           AS hits
FROM cheat.events
WHERE event_date >= today() - 7
GROUP BY path_part
ORDER BY hits DESC;

SELECT path_part, pos
FROM (
    SELECT splitByChar('/', page) AS parts,
           arrayEnumerate(parts)  AS positions
    FROM cheat.events
    WHERE event_date >= today() - 7
    LIMIT 3
)
ARRAY JOIN parts AS path_part, positions AS pos;

-- -----------------------------------------------------------------------------
-- 4.15. JSON, который лежит в String
-- -----------------------------------------------------------------------------
SELECT JSONExtractString(props, 'utm_source') AS utm_source,
       count()                                AS events,
       sum(JSONExtractInt(props, 'step'))      AS steps_total
FROM cheat.events
WHERE event_date >= today() - 7
GROUP BY utm_source
ORDER BY events DESC;

-- безопасные касты: аналог SAFE_CAST в BigQuery
SELECT JSONExtractString(props, 'utm_source')             AS raw_value,
       toInt64OrNull(JSONExtractString(props, 'step'))     AS step_as_int,
       accurateCastOrNull(JSONExtractString(props, 'step'), 'UInt8') AS step_uint8
FROM cheat.events
LIMIT 3;

-- -----------------------------------------------------------------------------
-- 4.16. Атрибуция first touch / last touch
-- -----------------------------------------------------------------------------
SELECT user_id,
       argMin(utm, event_time) AS first_touch,
       argMax(utm, event_time) AS last_touch,
       count()                 AS events
FROM (
    SELECT user_id,
           event_time,
           JSONExtractString(props, 'utm_source') AS utm
    FROM cheat.events
    WHERE event_date >= today() - 30
)
GROUP BY user_id
ORDER BY events DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 4.17. Пользователи без покупок: LEFT ANTI JOIN (в 24.8 есть и SEMI)
-- -----------------------------------------------------------------------------
SELECT count() AS users_without_purchase
FROM cheat.users AS u
LEFT ANTI JOIN (SELECT DISTINCT user_id FROM cheat.events WHERE event_type = 'purchase') AS p
     ON p.user_id = u.user_id;

SELECT count() AS users_with_purchase
FROM cheat.users AS u
LEFT SEMI JOIN (SELECT DISTINCT user_id FROM cheat.events WHERE event_type = 'purchase') AS p
     ON p.user_id = u.user_id;

-- -----------------------------------------------------------------------------
-- 4.18. Портирование рецептов из bigquery-шпаргалки
-- -----------------------------------------------------------------------------

-- (а) BQ: min(price) over(partition by grp order by dt rows between 3 preceding and 1 preceding)
--     ClickHouse: синтаксис оконной рамки тот же
SELECT event_date,
       revenue,
       min(revenue) OVER (ORDER BY event_date ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS prev3_min_revenue
FROM (
    SELECT event_date, sum(revenue) AS revenue
    FROM cheat.events
    WHERE event_type = 'purchase' AND event_date >= today() - 14
    GROUP BY event_date
)
ORDER BY event_date;

-- (б) BQ: date_trunc(current_date('Europe/Moscow'), week(monday))
SELECT toMonday(today())                          AS monday_via_toMonday,
       toStartOfWeek(today(), 1)                  AS monday_via_toStartOfWeek,
       date_trunc('week', toDate(now('Europe/Moscow'))) AS monday_via_date_trunc,
       today()                                    AS server_today_utc,
       toDate(now('Europe/Moscow'))               AS today_moscow;

-- (в) BQ: QUALIFY row_number() over(...) = 1 — в ClickHouse работает так же
SELECT user_id, event_date, revenue
FROM cheat.events
WHERE event_type = 'purchase'
QUALIFY row_number() OVER (PARTITION BY user_id ORDER BY revenue DESC) = 1
ORDER BY revenue DESC
LIMIT 10;

-- (г) BQ: разделитель слов с offset -> splitByNonAlpha + arrayJoin с позицией
SELECT word, pos
FROM (
    SELECT splitByNonAlpha(page) AS words,
           arrayEnumerate(words) AS positions
    FROM cheat.events
    LIMIT 2
)
ARRAY JOIN words AS word, positions AS pos;

-- (д) список дат без дыр (аналог GENERATE_DATE_ARRAY)
SELECT arrayJoin(arrayMap(x -> today() - 6 + x, range(7))) AS dt;

-- -----------------------------------------------------------------------------
-- 4.19. Комбинаторы агрегатных функций: -If, -Array, -State, -OrNull, -Distinct
-- -----------------------------------------------------------------------------
SELECT sumIf(revenue, device = 'mobile')                        AS revenue_mobile,
       countIf(event_type = 'purchase')                         AS purchases,
       uniqExactIf(user_id, country = 'RU')                     AS users_ru,
       sumArray([1, 2, 3])                                      AS sum_array,
       avgOrNull(revenue)                                       AS avg_or_null,
       sumDistinct(revenue)                                     AS sum_distinct_revenue,
       round(quantileIf(0.5)(revenue, device = 'mobile' AND event_type = 'purchase'), 2) AS median_mobile_check
FROM cheat.events
WHERE event_date >= today() - 7;


