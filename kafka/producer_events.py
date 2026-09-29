#!/usr/bin/env python3
"""Генератор событий в Kafka для конвейера Kafka -> ClickHouse.

Зачем: наполнить топик реалистичным потоком и проверить, что Kafka engine,
MATERIALIZED VIEW и дедуп по offset работают как ожидается.

Установка и запуск (с хоста, где Kafka проброшена на localhost:29092):
    pip install kafka-python-ng
    python producer_events.py --count 20000 --rate 2000

Полезные режимы:
    --broken 5          добавить 5 заведомо битых JSON — проверка DLQ
    --bootstrap localhost:29092   адрес брокера (по умолчанию он же)
    --dry-run           ничего не отправлять, только показать пример сообщения

Важно: изнутри docker-сети адрес брокера другой — kafka:9092.
Именно его указывает ClickHouse в kafka_broker_list.
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone

FUNNEL = ["view", "search", "cart", "checkout", "purchase"]
PAGES = ["/home", "/catalog", "/product", "/cart", "/checkout"]
COUNTRIES = ["RU", "KZ", "BY", "AM", "GE"]
CHANNELS = ["organic", "paid_search", "email", "referral"]


def make_event(event_id: int, rnd: random.Random) -> dict:
    """Одно событие воронки. Схема совпадает с колонками cheat.events_kafka."""
    user_id = rnd.randint(1, 40_000)
    step = rnd.randint(0, 4)
    event_time = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
        seconds=rnd.randint(0, 3600)
    )
    return {
        "event_id": event_id,
        "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": user_id,
        "event_type": FUNNEL[step],
        "page": PAGES[step],
        "revenue": round(rnd.uniform(100, 1000), 2) if step == 4 else 0,
        "props": json.dumps(
            {"utm_source": rnd.choice(CHANNELS), "step": step}, ensure_ascii=False
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Producer событий в Kafka")
    parser.add_argument("--bootstrap", default="localhost:29092", help="адрес брокера")
    parser.add_argument("--topic", default="cheat.events")
    parser.add_argument("--count", type=int, default=20_000, help="сколько событий отправить")
    parser.add_argument("--rate", type=int, default=0, help="событий в секунду (0 = без задержек)")
    parser.add_argument("--broken", type=int, default=0, help="сколько битых сообщений подмешать")
    parser.add_argument("--seed", type=int, default=42, help="сид генератора")
    parser.add_argument("--dry-run", action="store_true", help="показать пример и выйти")
    args = parser.parse_args()

    rnd = random.Random(args.seed)

    if args.dry_run:
        print(json.dumps(make_event(1, rnd), ensure_ascii=False, indent=2))
        return 0

    try:
        from kafka import KafkaProducer
    except ImportError:
        print("Нет kafka-python: pip install kafka-python-ng", file=sys.stderr)
        return 1

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        value_serializer=lambda v: v.encode() if isinstance(v, str) else json.dumps(v).encode(),
        key_serializer=lambda k: str(k).encode(),
        acks="all",          # подтверждение от всех реплик
        linger_ms=50,        # батчинг: 50 мс накопления перед отправкой
        retries=5,
    )

    started = time.time()
    sent = 0
    for i in range(args.count):
        event = make_event(i + 1, rnd)
        # ключ = user_id: события одного пользователя всегда в одной партиции,
        # значит порядок по пользователю сохраняется
        producer.send(args.topic, key=event["user_id"], value=event)
        sent += 1
        if args.rate and sent % args.rate == 0:
            time.sleep(1)

    # битые сообщения: проверяем, что DLQ их поймает, а конвейер не встанет
    for i in range(args.broken):
        producer.send(args.topic, key="broken", value='{"event_id": "не число", "event_time": "вчера"}')
        sent += 1

    producer.flush()
    producer.close()

    elapsed = time.time() - started
    print(
        f"отправлено {sent} сообщений в топик {args.topic} за {elapsed:.1f} c "
        f"({sent / max(elapsed, 1e-9):.0f} msg/s), битых: {args.broken}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
