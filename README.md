# tz-kafka

Kafka и Clickhouse находятся в одной сети, поэтому используем внутреннюю адресацию.
Используем Kraft режим кластера, никаких Zookeepers

Схема:
3 Kafka-ноды;
Kafka KRaft;
каждая нода одновременно broker + controller;
replication factor = 3;
min.insync.replicas = 2;
внутренний listener для Kafka-клиентов;
отдельный listener для межброкерного взаимодействия;
подготовлено для дальнейшей интеграции с ClickHouse через Kafka Engine или Kafka Connect.

В ходе работы получаем стабильный UUID - перманентно
kafka-storage.sh random-uuid

env kafka_cluster_id: "..."

ansible-playbook \
  -i inventory/hosts.ini \
  playbook.yml
  
Проверяем кворум
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server 10.10.10.11:9092 \
  describe --status
  
Создание топика для аналитики
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.10.10.11:9092 \
  --create \
  --topic user-events \
  --partitions 12 \
  --replication-factor 3 \
  --config min.insync.replicas=2
  
Проверяем топик 
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.10.10.11:9092 \
  --describe \
  --topic user-events

Создал 5 топиков типовых


Предполагаем что структура таблицы примерно
CREATE TABLE analytics.user_events
(
    event_id UUID,

    event_type LowCardinality(String),

    event_time DateTime64(3, 'UTC'),

    user_id UInt64,

    session_id UUID,

    platform LowCardinality(String),

    payload String,

    kafka_partition UInt32,

    kafka_offset UInt64,

    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time, user_id)
TTL event_time + INTERVAL 2 YEAR;

такая

тогда MV
CREATE MATERIALIZED VIEW analytics.user_events_mv
TO analytics.user_events
AS
SELECT
    event_id,
    event_type,
    event_time,
    user_id,
    session_id,
    platform,
    payload,

    _partition AS kafka_partition,
    _offset AS kafka_offset,

    now() AS inserted_at
FROM analytics.user_events_queue;


Итог:
 - можно усовершенствовать, но это по желанию: plaintext поменять на ssl или добавить несколько вариантов коннектов
 - сделать отдельные kafka users
 - acl на топики
 - алерты
 
ГЛОБАЛЬНО
 - заменить docker && legacy на k8s cluster + operator
 - взять не Kafka, а Redpanda - она гораздо быстрее, имеет оператор в k8s и не только и быстрее настраивается, совсем другой уровень отказоустойчивости
 
