-- =============================================================================
-- 05. Эксплуатация и диагностика: что смотреть, когда что-то не так
-- Все запросы проверены на живом сервере ClickHouse 24.8.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 5.1. Сколько занимает база, как сжато, сколько партиций
-- -----------------------------------------------------------------------------
SELECT table,
       sum(rows)                                            AS rows,
       formatReadableSize(sum(bytes_on_disk))               AS disk,
       formatReadableSize(sum(data_uncompressed_bytes))     AS uncompressed,
       round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.parts
WHERE database = 'cheat' AND active
GROUP BY table
ORDER BY sum(bytes_on_disk) DESC;

-- партиции конкретной таблицы
SELECT partition_id,
       count()                          AS parts,
       sum(rows)                        AS rows,
       formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE database = 'cheat' AND table = 'events' AND active
GROUP BY partition_id
ORDER BY partition_id;

-- -----------------------------------------------------------------------------
-- 5.2. Что прямо сейчас делает сервер: мержи, мутации, запросы
-- -----------------------------------------------------------------------------
-- ГРАБЛЯ (наблюдалось на 24.8.14): пока мерж реально идёт, запрос к system.merges
-- может упасть с Code: 1001 std::length_error. В query_log он останется как
-- ExceptionWhileProcessing. Если так — смотри состояние через system.parts и
-- system.mutations, они стабильны:
SELECT database, table, elapsed, num_parts, rows_read, total_size_bytes_compressed
FROM system.merges;

SELECT database, table, mutation_id, command, create_time, is_done, parts_to_do, latest_failed_part
FROM system.mutations
WHERE is_done = 0
ORDER BY create_time DESC;

SELECT query_id, user, elapsed, read_rows, formatReadableSize(memory_usage) AS mem, substring(query, 1, 60) AS q
FROM system.processes
WHERE query NOT LIKE '%system.processes%'
ORDER BY elapsed DESC;

-- убить зависший запрос (осторожно: убивает без подтверждения)
-- KILL QUERY WHERE query_id = '<query_id>' SYNC;
-- убить мутацию
-- KILL MUTATION WHERE database = 'cheat' AND table = 'orders';

-- -----------------------------------------------------------------------------
-- 5.3. UPDATE / DELETE: это мутации, они переписывают парты
-- mutations_sync = 1 заставляет ждать завершения (в скриптах обязательно,
-- иначе следующий DDL упадёт на "mutation is not finished yet")
-- -----------------------------------------------------------------------------
SET mutations_sync = 1;

-- (а) обычная мутация: помечает старые версии
ALTER TABLE cheat.orders UPDATE status = 'cancelled' WHERE order_id = 2;

-- (б) лёгкое удаление (в 24.8 включено по умолчанию: allow_experimental_lightweight_delete = 1)
--     внутри превращается в мутацию UPDATE _row_exists = 0, но помечает строки мгновенно
DELETE FROM cheat.orders WHERE order_id = 3;

SELECT '5.3 мутации' AS section, mutation_id, is_done FROM system.mutations
WHERE database = 'cheat' AND table = 'orders' ORDER BY create_time DESC LIMIT 3;

-- -----------------------------------------------------------------------------
-- 5.4. OPTIMIZE: склеить парты, дедуплицировать, пересобрать партицию
-- -----------------------------------------------------------------------------
-- OPTIMIZE TABLE cheat.events PARTITION ID '202609' FINAL;      -- одна партиция
-- OPTIMIZE TABLE cheat.orders FINAL DEDUPLICATE BY order_id, created_at;

SELECT partition, count() AS parts_before, sum(rows) AS rows
FROM system.parts
WHERE database = 'cheat' AND table = 'events' AND active
GROUP BY partition
ORDER BY partition;

-- -----------------------------------------------------------------------------
-- 5.5. query_log: кто, что и сколько жрал. Главный инструмент разбора тормозов
-- -----------------------------------------------------------------------------
SYSTEM FLUSH LOGS;

-- топ-10 самых долгих запросов за сутки
SELECT event_time,
       query_duration_ms,
       read_rows,
       formatReadableSize(read_bytes)   AS read_size,
       formatReadableSize(memory_usage) AS peak_mem,
       user,
       substring(query, 1, 80)          AS query
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_time > now() - 86400
  AND query_kind = 'Select'
ORDER BY query_duration_ms DESC
LIMIT 10;

-- самые тяжёлые по чтению (кандидаты на оптимизацию ORDER BY/партиционирования)
SELECT substring(query, 1, 80) AS query,
       count()                 AS runs,
       sum(read_rows)          AS total_rows_read,
       formatReadableSize(sum(read_bytes)) AS total_read,
       round(avg(query_duration_ms))       AS avg_ms
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Select' AND event_time > now() - 86400
GROUP BY query
ORDER BY sum(read_bytes) DESC
LIMIT 10;

-- ошибки: что падало за сутки
SELECT event_time, exception_code, substring(exception, 1, 120) AS error, substring(query, 1, 60) AS query
FROM system.query_log
WHERE type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart')
  AND event_time > now() - 86400
ORDER BY event_time DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 5.6. Kafka-конвейер: читает ли, коммитит ли, где ошибки
-- -----------------------------------------------------------------------------
SELECT database, table, consumer_id, num_messages_read, num_commits, last_poll_time, is_currently_used
FROM system.kafka_consumers
ORDER BY database, table;

SELECT database, table, consumer_id, assignments.partition_id AS part,
       assignments.current_offset AS cur_offset, exceptions.text AS error
FROM system.kafka_consumers
WHERE length(exceptions.text) > 0
FORMAT Vertical;

-- -----------------------------------------------------------------------------
-- 5.7. Словари: загрузились ли
-- -----------------------------------------------------------------------------
SELECT database, name, status, element_count, last_successful_update_time, last_exception
FROM system.dictionaries
ORDER BY database, name;

-- -----------------------------------------------------------------------------
-- 5.8. Здоровье сервера: ошибки и ключевые события
-- -----------------------------------------------------------------------------
SELECT name, value, last_error_time, substring(last_error_message, 1, 80) AS last_error
FROM system.errors
WHERE value > 0
ORDER BY value DESC
LIMIT 10;

SELECT event, value, description
FROM system.events
WHERE event IN ('Query', 'InsertQuery', 'SelectQuery', 'FailedQuery', 'MergeTreeDataWriterRows',
                'MergeTreeDataWriterUncompressedBytes', 'KafkaMessagesPolled', 'KafkaMessagesRead')
ORDER BY event;

-- -----------------------------------------------------------------------------
-- 5.9. Бэкап: BACKUP в файл/диск + FREEZE как "снимок партов"
-- -----------------------------------------------------------------------------
-- BACKUP TABLE cheat.users TO Disk('backups', 'cheat_users.zip');
-- BACKUP DATABASE cheat TO Disk('backups', 'cheat_full.zip');
-- ALTER TABLE cheat.events FREEZE;   -- жёсткие ссылки на парты в /var/lib/clickhouse/shadow

SELECT name, path, type FROM system.disks;

-- -----------------------------------------------------------------------------
-- 5.10. TTL: что и когда сервер удалит сам
-- -----------------------------------------------------------------------------
-- в 24.8 таблицы system.table_ttl_infos нет — TTL видно в system.tables и в партах
SELECT database, table, name, engine
FROM system.tables
WHERE database = 'cheat' AND create_table_query LIKE '%TTL%';

SELECT table, extract(create_table_query, 'TTL[^)]*') AS ttl_expr
FROM system.tables
WHERE database = 'cheat' AND create_table_query LIKE '%TTL%';

SELECT partition_id, rows, delete_ttl_info_min, delete_ttl_info_max
FROM system.parts
WHERE database = 'cheat' AND table = 'events' AND active
ORDER BY partition_id;

-- -----------------------------------------------------------------------------
-- 5.11. Пользователи и права: DataLens ходит с readonly = 2
-- -----------------------------------------------------------------------------
SELECT name, storage, auth_type, host_ip, default_database FROM system.users ORDER BY name;
SELECT user_name, role_name, access_type, database, table FROM system.grants WHERE user_name = 'cheat_reader';

-- -----------------------------------------------------------------------------
-- 5.12. Health-check одной таблицей: что проверять перед "всё сломалось"
-- -----------------------------------------------------------------------------
-- ВАЖНО: system.kafka_consumers — серверная таблица, видны ВСЕ консьюмеры (и чужие
-- пайплайны тоже). В health-check фильтруйся по своей таблице:
--   WHERE database = 'cheat' AND table = 'events_kafka'
-- Иначе транзиентная ошибка соседнего консьюмера завалит твою проверку.
-- Ещё: num_messages_read/num_commits — счётчики за всю жизнь консьюмера,
-- их разность НЕ равна текущему лагу. Настоящий лаг смотри в Kafka:
--   docker exec dwh-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
--     --bootstrap-server localhost:9092 --describe --group ch_cheat_events
SELECT 'kafka_consumers_total' AS metric, toInt64(count()) AS value FROM system.kafka_consumers
UNION ALL
SELECT 'kafka_consumers_with_errors', toInt64(count()) FROM system.kafka_consumers WHERE length(exceptions.text) > 0
UNION ALL
SELECT 'kafka_consumers_stale_poll', toInt64(count()) FROM system.kafka_consumers
    WHERE last_poll_time < now() - 300
UNION ALL
SELECT 'unfinished_mutations', toInt64(count()) FROM system.mutations WHERE is_done = 0
UNION ALL
SELECT 'active_merges', toInt64(count()) FROM system.merges
UNION ALL
SELECT 'dlq_rows_24h', toInt64(count()) FROM cheat.events_dlq WHERE received_at > now() - 86400
UNION ALL
SELECT 'events_stream_rows', toInt64(count()) FROM cheat.events_stream
UNION ALL
SELECT 'slow_queries_24h', toInt64(count()) FROM system.query_log
    WHERE type = 'QueryFinish' AND query_duration_ms > 5000 AND event_time > now() - 86400;
