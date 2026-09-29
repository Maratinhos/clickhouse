"""Демо-пайплайн ClickHouse-стека: Kafka -> raw -> факт -> витрина -> DQ.

Что показывает:
  1) как Airflow ходит в ClickHouse (HTTP + requests, без доп. пакетов);
  2) идемпотентную догрузку из стрима в факт (ANTI JOIN по event_id);
  3) пересборку витрины через временную таблицу + EXCHANGE TABLES (атомарная замена,
     никаких ALTER DELETE и «дырок» в витрине на время пересчёта);
  4) DQ-гейт в конце: если данные плохие — падаем, а не отдаём мусор в DataLens.

Запуск: Airflow UI -> cheat_events_pipeline -> Trigger DAG.
Расписание намеренно schedule=None, чтобы демо не крутилось само.

Кладём в Airflow два файла: этот и clickhouse_http.py (он импортируется по имени).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from clickhouse_http import execute, query, scalar

DB = "cheat"
LOOKBACK_DAYS = 7

default_args = {
    "owner": "bi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def task_kafka_health(**_):
    """Гейт №1: прежде чем считать, убеждаемся, что входной поток здоров.

    ГРАБЛЯ (поймана живьём): system.kafka_consumers — серверная таблица, в ней видны
    ВСЕ Kafka-консьюмеры ClickHouse, включая чужие пайплайны. Первая версия проверки
    валилась на транзиентном "Broker: Coordinator load in progress" у соседней таблицы
    raw.prices_kafka. Поэтому фильтруемся по своей таблице, а «ошибки консьюмера» не
    делаем жёстким гейтом (они транзиентные) — жёсткий сигнал это: консьюмер перестал
    опрашивать топик, и/или в DLQ прилетают свежие битые сообщения.
    """
    mine = f"database = '{DB}' AND table = 'events_kafka'"

    consumers = scalar(f"SELECT count() FROM system.kafka_consumers WHERE {mine}")
    if not consumers:
        raise ValueError(
            f"для {DB}.events_kafka нет ни одного консьюмера: таблица не создана, "
            f"движок не поднялся или у неё нет MATERIALIZED VIEW"
        )

    fresh_dlq = scalar(
        f"SELECT count() FROM {DB}.events_dlq WHERE received_at > now() - 900"
    )
    if fresh_dlq:
        raise ValueError(f"за 15 минут в DLQ {fresh_dlq} битых сообщений — разбирай входной поток")

    # Предупреждения (в лог, но не падаем):
    # last_poll_time НЕ годится как жёсткий гейт — наблюдалось, что он отстаёт на минуты,
    # пока в топике нет новых сообщений, при полностью живом конвейере.
    stale = query(
        f"SELECT consumer_id, last_poll_time, now() - last_poll_time AS lag_s "
        f"FROM system.kafka_consumers WHERE {mine} AND last_poll_time < now() - 300"
    )
    if stale:
        print(f"ВНИМАНИЕ: консьюмеры давно не опрашивали топик (простой или нет данных): {stale}")

    errs = query(
        f"SELECT consumer_id, exceptions.text AS error FROM system.kafka_consumers "
        f"WHERE {mine} AND length(exceptions.text) > 0"
    )
    if errs:
        print(f"ВНИМАНИЕ: у консьюмеров есть исторические ошибки: {len(errs)}")

    # Настоящий лаг смотри в Kafka (из CH его не видно):
    #   kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    #     --describe --group ch_cheat_events
    print("входной поток жив: консьюмеры есть, свежих битых сообщений нет")


def task_load_incremental(**_):
    """Догрузка из стрима в факт. Идемпотентно: повторный запуск не создаст дублей."""
    before = scalar(f"SELECT count() FROM {DB}.events")
    execute(
        f"""
        INSERT INTO {DB}.events
            (event_id, event_time, user_id, session_id, event_type, page, device, country, revenue, props)
        SELECT
            s.event_id,
            s.event_time,
            s.user_id,
            cityHash64(concat(toString(s.user_id), toString(toDate(s.event_time)))) AS session_id,
            s.event_type,
            s.page,
            coalesce(u.device, 'unknown')  AS device,
            coalesce(u.country, 'unknown') AS country,
            s.revenue,
            s.props
        FROM {DB}.events_stream AS s
        LEFT ANTI JOIN (SELECT DISTINCT event_id FROM {DB}.events) AS e ON e.event_id = s.event_id
        LEFT JOIN (
            SELECT user_id, argMax(device, signup_date) AS device, argMax(country, signup_date) AS country
            FROM {DB}.users GROUP BY user_id
        ) AS u ON u.user_id = s.user_id
        WHERE s.event_time >= today() - {LOOKBACK_DAYS}
        """
    )
    after = scalar(f"SELECT count() FROM {DB}.events")
    print(f"факт: было {before}, стало {after}, догружено {after - before}")


def task_rebuild_mart(**_):
    """Пересборка витрины за последние LOOKBACK_DAYS дней: tmp + EXCHANGE TABLES."""
    main, tmp = f"{DB}.mart_daily_state", f"{DB}.mart_daily_state_tmp"

    execute(f"DROP TABLE IF EXISTS {tmp}")
    execute(f"CREATE TABLE {tmp} AS {main}")

    # 1. переносим историю, которую не пересчитываем
    execute(f"INSERT INTO {tmp} SELECT * FROM {main} WHERE event_date < today() - {LOOKBACK_DAYS}")

    # 2. считаем окно заново (тот же SELECT, что и в MV из sql/03_marts.sql)
    execute(
        f"""
        INSERT INTO {tmp}
        SELECT
            toDate(event_time)                       AS event_date,
            country,
            device,
            event_type,
            uniqState(user_id)                       AS users_state,
            uniqState(session_id)                    AS sessions_state,
            countState()                             AS events_state,
            sumState(revenue)                        AS revenue_state,
            quantileState(0.95)(revenue)             AS p95_state,
            minState(event_time)                     AS first_seen_state,
            maxState(event_time)                     AS last_seen_state
        FROM {DB}.events
        WHERE event_date >= today() - {LOOKBACK_DAYS}
        GROUP BY event_date, country, device, event_type
        """
    )

    # 3. атомарная подмена: читатели видят либо старую, либо новую витрину, никогда — половину
    execute(f"EXCHANGE TABLES {tmp} AND {main}")
    execute(f"DROP TABLE {tmp}")

    rows = scalar(f"SELECT count() FROM {main}")
    print(f"витрина пересобрана, строк: {rows}")


def task_dq_checks(**_):
    """Гейт №2: проверки после пересчёта. Падаем — значит витрина не поедет в DataLens."""
    checks = {
        "дубли event_id в факте": f"SELECT count() - uniqExact(event_id) FROM {DB}.events",
        "отрицательная выручка": f"SELECT count() FROM {DB}.events WHERE revenue < 0",
        "покупки без пользователя": (
            f"SELECT count() FROM {DB}.events AS e "
            f"LEFT ANTI JOIN {DB}.users AS u ON u.user_id = e.user_id"
        ),
        "витрина пуста": f"SELECT count() = 0 FROM {DB}.mart_daily_state",
        "витрина отстала больше чем на 2 дня": (
            f"SELECT max(event_date) < today() - 2 FROM {DB}.mart_daily_state"
        ),
    }
    failed = {}
    for name, sql in checks.items():
        value = scalar(sql)
        print(f"{name}: {value}")
        if value:
            failed[name] = value
    if failed:
        raise ValueError(f"DQ не пройден: {failed}")


with DAG(
    dag_id="cheat_events_pipeline",
    description="Kafka -> ClickHouse -> витрина -> DQ (демо-пайплайн из шпаргалки)",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["clickhouse", "cheat", "demo"],
) as dag:
    kafka_health = PythonOperator(task_id="kafka_health", python_callable=task_kafka_health)
    load_incremental = PythonOperator(task_id="load_incremental", python_callable=task_load_incremental)
    rebuild_mart = PythonOperator(task_id="rebuild_mart", python_callable=task_rebuild_mart)
    dq_checks = PythonOperator(task_id="dq_checks", python_callable=task_dq_checks)

    kafka_health >> load_incremental >> rebuild_mart >> dq_checks
