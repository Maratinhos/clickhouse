"""Минимальный HTTP-клиент ClickHouse для Airflow-задач.

Почему HTTP, а не clickhouse-connect:
  - в образе Airflow (apache/airflow:2.10.5) нет clickhouse-connect, а requests есть;
  - HTTP-порт 8123 понимает те же настройки, что и нативный клиент;
  - НО: HTTP-интерфейс не принимает несколько запросов в одном теле — только один
    стейтмент за вызов (в отличие от clickhouse-client --multiquery).

Настройки берутся из переменных окружения контейнера:
  CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB
"""
from __future__ import annotations

import json
import os

import requests

URL = "http://{host}:{port}/".format(
    host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    port=os.getenv("CLICKHOUSE_PORT", "8123"),
)
AUTH = (os.getenv("CLICKHOUSE_USER", "bi_admin"), os.getenv("CLICKHOUSE_PASSWORD", ""))
DEFAULT_DB = os.getenv("CLICKHOUSE_DB", "cheat")


def _params(database: str | None = None, settings: dict | None = None) -> dict:
    p = {"database": database or DEFAULT_DB}
    if settings:
        p.update({k: v for k, v in settings.items()})
    return p


def query(sql: str, settings: dict | None = None, database: str | None = None, timeout: int = 600):
    """SELECT -> список словарей. FORMAT JSON добавляется сам, если его нет."""
    body = sql if "FORMAT " in sql.upper() else sql.rstrip().rstrip(";") + "\nFORMAT JSON"
    r = requests.post(URL, params=_params(database, settings), data=body.encode(),
                      auth=AUTH, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse: {r.text.strip()[:500]}\nSQL: {sql[:300]}")
    return json.loads(r.text or "{}").get("data", [])


def scalar(sql: str, **kw):
    """SELECT одного значения -> скаляр."""
    rows = query(sql, **kw)
    return next(iter(rows[0].values())) if rows else None


def execute(sql: str, settings: dict | None = None, database: str | None = None, timeout: int = 3600):
    """DDL/DML. Для ALTER ... UPDATE/DELETE по умолчанию ждём завершения мутации."""
    st = {"mutations_sync": 1}
    if settings:
        st.update(settings)
    body = sql.rstrip().rstrip(";")
    r = requests.post(URL, params=_params(database, st), data=body.encode(),
                      auth=AUTH, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse: {r.text.strip()[:500]}\nSQL: {sql[:300]}")
    return r.text.strip()


def insert_json(table: str, rows: list[dict], database: str | None = None):
    """Вставка списка словарей через JSONEachRow (без внешних библиотек)."""
    if not rows:
        return 0
    body = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in rows)
    sql = f"INSERT INTO {table} FORMAT JSONEachRow"
    r = requests.post(URL, params={"query": sql, "database": database or DEFAULT_DB},
                      data=body.encode(), auth=AUTH, timeout=600)
    if r.status_code != 200:
        raise RuntimeError(f"ClickHouse insert: {r.text.strip()[:500]}")
    return len(rows)
