# Pipeline Loader Configuration Baseline
# Parallel Ingestion + Robust DLQ/Replay Controls

Status: Reference baseline  
Scope: `Sources -> Kafka -> (parallel) ClickHouse + S3`  
Goal: Failure-resilient ingestion with deterministic recovery

## 1) Control objectives

- No silent drops at any stage
- Deterministic replay by immutable `event_id`
- Bounded eventual consistency across Kafka/S3/ClickHouse
- Fast incident triage through stage-specific DLQs

## 2) Topic and DLQ topology

## 2.1 Primary topics

- `ops.worker-events.v1`
- `ops.document-events.v1`
- `ops.compliance-events.v1`

Partitioning key:

- `tenant_id + entity_id`

## 2.2 DLQ topics (one class per failure domain)

- `dlq.producer.validation.v1`
- `dlq.sink.s3.v1`
- `dlq.sink.clickhouse.v1`
- `dlq.transform.clickhouse.v1`
- `dlq.replay.loader.v1`

Retention baseline:

- DLQ topics: 14-30 days
- Primary topics: 7-14 days (longer if replay-from-kafka SLA needs it)

## 3) Producer configuration (application/services)

Required producer settings:

```text
acks=all
enable.idempotence=true
retries=2147483647
max.in.flight.requests.per.connection=5
compression.type=zstd
linger.ms=5-20
batch.size=131072-524288
delivery.timeout.ms=120000
request.timeout.ms=30000
```

Validation behavior:

- Reject invalid payloads before publish
- Write rejected payload metadata to `dlq.producer.validation.v1`

Envelope requirements:

- `event_id` (UUID, immutable)
- `tenant_id`, `entity_id`, `event_time`, `event_type`, `event_version`

## 4) Kafka -> S3 sink loader configuration

Example baseline (Kafka Connect style semantics):

```json
{
  "connector.class": "S3SinkConnector",
  "topics": "ops.worker-events.v1,ops.document-events.v1,ops.compliance-events.v1",
  "format.class": "ParquetFormat",
  "storage.class": "S3Storage",
  "partitioner.class": "TimeBasedPartitioner",
  "path.format": "'tenant'=\\${key.tenant_id}/'year'=YYYY/'month'=MM/'day'=dd",
  "partition.duration.ms": "86400000",
  "rotate.interval.ms": "30000",
  "flush.size": "50000",
  "s3.compression.type": "snappy",
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "dlq.sink.s3.v1",
  "errors.deadletterqueue.context.headers.enable": "true",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "tasks.max": "4"
}
```

Notes:

- Prefer 32-128 MB effective object size for request-cost efficiency
- Use time-bounded prefixes to reduce replay scan cost

## 5) Kafka -> ClickHouse loader configuration

## 5.1 Kafka source table (ClickHouse)

```sql
CREATE TABLE worker_events_kafka
(
  tenant_id String,
  worker_id String,
  event_time DateTime64(3, 'UTC'),
  event_type String,
  event_id UUID,
  event_version UInt16,
  payload_json String
)
ENGINE = Kafka('kafka-broker:9092', 'ops.worker-events.v1', 'ch-worker-events-group', 'JSONEachRow')
SETTINGS
  kafka_num_consumers = 4,
  kafka_thread_per_consumer = 1,
  kafka_max_block_size = 65536,
  kafka_handle_error_mode = 'stream';
```

## 5.2 Raw target and ingestion MV

```sql
CREATE TABLE worker_events_local
(
  tenant_id LowCardinality(String),
  worker_id String,
  event_time DateTime64(3, 'UTC'),
  event_type LowCardinality(String),
  event_id UUID,
  event_version UInt16,
  payload_json String,
  ingest_time DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/worker_events_local', '{replica}')
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, worker_id, event_time, event_id);

CREATE MATERIALIZED VIEW worker_events_kafka_mv
TO worker_events_local
AS
SELECT
  tenant_id,
  worker_id,
  event_time,
  event_type,
  event_id,
  event_version,
  payload_json,
  now64() AS ingest_time
FROM worker_events_kafka;
```

## 5.3 ClickHouse ingest DLQ table

```sql
CREATE TABLE ingest_dlq_clickhouse
(
  dlq_time DateTime64(3, 'UTC') DEFAULT now64(),
  source_topic String,
  source_partition UInt64,
  source_offset UInt64,
  error String,
  raw_message String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dlq_time)
ORDER BY (dlq_time, source_topic, source_partition, source_offset);

CREATE MATERIALIZED VIEW worker_events_kafka_dlq_mv
TO ingest_dlq_clickhouse
AS
SELECT
  _topic AS source_topic,
  _partition AS source_partition,
  _offset AS source_offset,
  _error AS error,
  _raw_message AS raw_message
FROM worker_events_kafka
WHERE length(_error) > 0;
```

## 6) Checkpoint store configuration

Recommended table (DynamoDB or equivalent):

```text
PK: stage#tenant_id#topic#partition
SK: watermark_type (committed_offset | processed_offset | replay_offset)
Attributes:
  offset (number)
  event_time_max (timestamp)
  updated_at (timestamp)
  status (healthy | lagging | error)
```

Update cadence:

- Every 5-15 seconds per active partition consumer

## 7) Reconciliation job configuration

Windowing:

- 5-15 minute windows
- Keyed by `(tenant_id, event_date, event_hour, event_minute_bucket)`

Comparisons:

- record count parity
- hash parity (`xxhash64(event_id)` aggregate)
- max offset/time watermark parity

Mismatch policy:

- Critical entities: zero tolerance
- Non-critical entities: bounded tolerance with delayed escalation

## 8) Replay loader configuration

Replay sources (priority):

1. Kafka (if within retention)
2. S3 archive

Replay worker baseline:

```text
worker_concurrency=4-8
batch_rows=50k-200k
max_inflight_batches=4
retry_backoff=2s,4s,8s,16s,32s
max_retries=8
idempotent_key=event_id
```

Replay DLQ rules:

- Deterministic parse/schema errors -> `dlq.replay.loader.v1`
- Transient sink errors -> retry then DLQ after retry cap

## 9) Quarantine mechanism

Quarantine destinations:

- Topic: `dlq.*` classes
- S3 prefix: `s3://firstwork-events/quarantine/<stage>/<yyyy>/<mm>/<dd>/`

Required quarantine metadata:

- `event_id` (if available)
- stage name
- error class and stack
- source topic/partition/offset
- first_seen_at and retry_count

## 10) Failure handling matrix

| Failure type | Stage | Action |
|---|---|---|
| Schema validation failure | producer | route to `dlq.producer.validation.v1` |
| S3 sink transient failure | Kafka->S3 | retry + checkpoint hold |
| S3 sink permanent failure | Kafka->S3 | route to `dlq.sink.s3.v1` + quarantine |
| CH parse/format failure | Kafka->CH | write to CH ingest DLQ + `dlq.sink.clickhouse.v1` |
| Reconciliation mismatch | control plane | trigger replay job |
| Replay deterministic failure | replay | route to `dlq.replay.loader.v1` |

## 11) Operational alerts

Page-level alerts:

- Kafka lag breach beyond SLO window
- DLQ rate spikes by class
- reconciliation mismatch above threshold
- replay backlog growth beyond threshold

Ticket-level alerts:

- transient error bursts with auto-recovery

