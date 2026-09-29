-- =============================================================================
-- 02. Ингест из Kafka: Kafka engine -> MATERIALIZED VIEW -> MergeTree
--
-- Порядок: сначала этот файл, потом продюсер (kafka/producer_events.py),
-- потом секция 2.5 «проверки» (или просто запустите файл ещё раз).
--
-- Топик ClickHouse НЕ создаёт — это делает Kafka:
--   docker exec dwh-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
--     --create --if-not-exists --topic cheat.events --partitions 3 --replication-factor 1
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1. Таблица с движком Kafka — это не хранилище, а «розетка»:
--      данные через неё только протекают, SELECT из неё вычитывает и теряет их.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.events_kafka;

CREATE TABLE cheat.events_kafka
(
    event_id   UInt64,
    event_time DateTime,
    user_id    UInt64,
    event_type LowCardinality(String),
    page       LowCardinality(String),
    revenue    Decimal(18, 2),
    props      String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list      = 'kafka:9092',        -- изнутри docker-сети, не localhost
    kafka_topic_list       = 'cheat.events',
    kafka_group_name       = 'ch_cheat_events',   -- consumer group: смещения хранит Kafka
    kafka_format           = 'JSONEachRow',
    kafka_num_consumers    = 3,                   -- <= число партиций топика
    kafka_max_block_size   = 65536,               -- сколько сообщений забирать за раз (батч)
    kafka_poll_timeout_ms  = 1000,
    kafka_poll_max_batch_size = 1000,
    kafka_flush_interval_ms = 1000,               -- как часто MV сбрасывает батч в таблицу
    kafka_commit_every_batch = 1,                 -- коммитить после каждого батча (см. заметку ниже)
    kafka_skip_broken_messages = 0,               -- ВАЖНО: 0 = битые сообщения уходят в MV (видно в DLQ).
                                                  -- Любое значение > 0 = битые строки молча пропускаются,
                                                  -- в cheat.events_dlq при этом пусто (проверено: 5 битых
                                                  -- сообщений при skip=100 -> DLQ пуст, при skip=0 -> 4 строки в DLQ)
    kafka_handle_error_mode = 'stream';           -- вместо падения отдать строку в MV с _error/_raw_message

-- Полный список настроек, которые Kafka engine принимает в 24.8 (проверено перебором):
--   kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format, kafka_schema,
--   kafka_num_consumers, kafka_max_block_size, kafka_max_rows_per_message,
--   kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms,
--   kafka_skip_broken_messages, kafka_handle_error_mode, kafka_commit_every_batch,
--   kafka_commit_on_select, kafka_thread_per_consumer, kafka_client_id
-- НЕ существуют (в 24.8): kafka_auto_offset_reset, kafka_compression_codec,
--   kafka_compression_level, kafka_security_protocol, kafka_debug, kafka_retry_backoff_ms,
--   kafka_consumer_reset_config_service_timeout_ms.
--   Отдельно: kafka_max_wait_ms — это НЕ настройка движка, а серверная
--   (system.settings), задаётся глобально в config.xml.

-- -----------------------------------------------------------------------------
-- 2.2. Хранилище: MergeTree + MATERIALIZED VIEW, который переливает из Kafka
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.events_stream;

CREATE TABLE cheat.events_stream
(
    event_id        UInt64,
    event_time      DateTime,
    user_id         UInt64,
    event_type      LowCardinality(String),
    page            LowCardinality(String),
    revenue         Decimal(18, 2),
    props           String,
    kafka_topic     String,       -- виртуальная колонка _topic
    kafka_partition Int64,        -- _partition — нужна вместе с offset для дедупа
    kafka_offset    Int64,        -- _offset
    loaded_at       DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time, user_id);

DROP VIEW IF EXISTS cheat.mv_events_stream;

-- MV видит только те сообщения, которые пришли ПОСЛЕ его создания.
-- Порядок «сначала MV, потом данные» — иначе первые сообщения потеряются.
CREATE MATERIALIZED VIEW cheat.mv_events_stream TO cheat.events_stream AS
SELECT
    event_id, event_time, user_id, event_type, page, revenue, props,
    _topic     AS kafka_topic,
    _partition AS kafka_partition,
    _offset    AS kafka_offset,
    now()      AS loaded_at
FROM cheat.events_kafka;

-- -----------------------------------------------------------------------------
-- 2.3. Dead letter queue: битые сообщения не останавливают конвейер.
--      Работает только вместе с kafka_handle_error_mode = 'stream'.
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS cheat.events_dlq;

CREATE TABLE cheat.events_dlq
(
    received_at     DateTime DEFAULT now(),
    kafka_topic     String,
    kafka_partition Int64,
    kafka_offset    Int64,
    error           String,
    raw_message     String
)
ENGINE = MergeTree
ORDER BY (received_at, kafka_partition, kafka_offset)
TTL received_at + INTERVAL 30 DAY;

DROP VIEW IF EXISTS cheat.mv_events_dlq;

CREATE MATERIALIZED VIEW cheat.mv_events_dlq TO cheat.events_dlq AS
SELECT
    now()      AS received_at,
    _topic     AS kafka_topic,
    _partition AS kafka_partition,
    _offset    AS kafka_offset,
    _error     AS error,
    _raw_message AS raw_message
FROM cheat.events_kafka
WHERE _error != '';

-- -----------------------------------------------------------------------------
-- 2.4. Диагностика консьюмера: что реально происходит внутри ClickHouse
-- -----------------------------------------------------------------------------
-- Сколько сообщений прочитано, сколько коммитов, какие ошибки (реальные имена
-- колонок: exceptions.text / exceptions.time, а не last_exception)
SELECT database, table, consumer_id, num_messages_read, num_commits,
       last_poll_time, exceptions.text AS errors, is_currently_used
FROM system.kafka_consumers
WHERE database = 'cheat'
FORMAT Vertical;

-- -----------------------------------------------------------------------------
-- 2.5. Проверки после продуцирования (запускать после kafka/producer_events.py)
-- -----------------------------------------------------------------------------
-- сколько долетело и нет ли дублей по (партиция, смещение)
SELECT count()                                        AS rows_in_storage,
       uniqExact(event_id)                            AS unique_events,
       uniqExact((kafka_partition, kafka_offset))     AS unique_offsets,
       min(loaded_at)                                 AS first_load,
       max(loaded_at)                                 AS last_load
FROM cheat.events_stream;

-- распределение по партициям Kafka
SELECT kafka_partition,
       count()                AS rows,
       min(kafka_offset)      AS min_offset,
       max(kafka_offset)      AS max_offset
FROM cheat.events_stream
GROUP BY kafka_partition
ORDER BY kafka_partition;

-- что попало в DLQ
SELECT kafka_partition, kafka_offset, error, raw_message
FROM cheat.events_dlq
ORDER BY received_at DESC
LIMIT 5;

-- -----------------------------------------------------------------------------
-- 2.6. Дедуп на случай повторной доставки: offset — уникальный ключ сообщения
-- -----------------------------------------------------------------------------
-- ReplacingMergeTree по (partition, offset) даёт идемпотентность: одно и то же
-- сообщение, доставленное дважды, схлопнется при merge/FINAL.
DROP TABLE IF EXISTS cheat.events_stream_dedup;

CREATE TABLE cheat.events_stream_dedup
(
    event_id   UInt64,
    event_time DateTime,
    user_id    UInt64,
    event_type LowCardinality(String),
    kafka_partition Int64,
    kafka_offset    Int64,
    loaded_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (kafka_partition, kafka_offset);
