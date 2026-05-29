# my-docker-desktop-configs

# Real-Time Change Data Capture (CDC) Pipeline: Postgres to MinIO S3 via KRaft Kafka

A robust, streaming data engineering platform designed to intercept transactional mutations from an active PostgreSQL core, route them through a high-throughput, native KRaft-mode Apache Kafka cluster, and flush them cleanly as flattened, date-formatted JSON strings into a MinIO Object Storage bucket.

---

## Infrastructure Overview

The local topology runs entirely inside a isolated Docker Desktop container ecosystem, composed of the following services:

* **Kafka Control Plane:** A 3-Broker Native KRaft Cluster (`controller-1`, `broker-1`, `broker-2`, `broker-3`) completely stripped of ZooKeeper dependencies.
* **Schema Registry:** Confluent Schema Registry tracking schema metadata and checking internal format compatibility layers.
* **Source Database:** A PostgreSQL instances (`postgres-db`) with Logical Replication enabled (`wal_level=logical`).
* **Streaming Engine:** Debezium Connect Cluster worker framework (`kafka-connect`) handling runtime source and sink connector class loops.
* **Target Infrastructure:** A local MinIO S3 API Emulator (`minio-s3`) automated via a configuration sidecar container (`minio-init`) to generate target storage folders on startup.

---

## Getting Started: Docker Desktop Deployment

### 1. Initialize the Cluster Stack
To launch the complete cluster infrastructure along with clean storage mounts, execute:
```bash
docker compose up -d

```

### 2. Verify System Status

Ensure all services are running and the internal health checks are settled:

```bash
docker compose ps

```

---

## Connector Implementations

Both connectors explicitly implement **Schema Validation Consistency** to manage data type transformations cleanly across boundaries.

### 1. PostgreSQL Debezium Source Connector

Pulls transactions straight out of the PostgreSQL WAL log. Note that the `table.include.list` must explicitly contain the fully qualified `schema.table_name`.

* **Endpoint:** `http://localhost:8083/connectors/postgres-db-source/config`
* **Payload Format:**

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "plugin.name": "pgoutput",
  "database.hostname": "postgres-db",
  "database.port": "5432",
  "database.dbname": "ordersdb",
  "database.user": "kafka_user",
  "database.password": "kafka_password",
  "topic.prefix": "orders_",
  "table.include.list": "public.orders",
  "poll.interval.ms": "2000",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "true",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",
  "decimal.handling.mode": "string",
  "time.precision.mode": "connect"
}

```

### 2. MinIO S3 Sink Connector

Reads records from Kafka, applies a Debezium SMT unwrap block to isolate the payload, and transforms epoch-day values into a standard `yyyy-MM-dd` date format.

* **Endpoint:** `http://localhost:8083/connectors/minio-s3-sink-connector/config`
* **Payload Format:**

```json
{
  "connector.class": "io.confluent.connect.s3.S3SinkConnector",
  "tasks.max": "1",
  "topics": "orders_.public.orders",
  "schema.compatibility": "NONE",
  "s3.bucket.name": "kafka-sink-bucket",
  "s3.region": "us-east-1",
  "store.url": "http://minio-s3:9000",
  "aws.access.key.id": "minio_admin",
  "aws.secret.access.key": "minio_secret_key",
  "storage.class": "io.confluent.connect.s3.storage.S3Storage",
  "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
  "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
  "flush.size": "1",
  "rotate.interval.ms": "10000",
  "s3.part.size": "5242880",
  "consumer.max.poll.interval.ms": "300000",
  "consumer.session.timeout.ms": "45000",
  "transforms": "unwrap,formatDate",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "true",
  "transforms.formatDate.type": "org.apache.kafka.connect.transforms.TimestampConverter$Value",
  "transforms.formatDate.field": "order_date", 
  "transforms.formatDate.target.type": "string",
  "transforms.formatDate.format.string": "yyyy-MM-dd",
  "transforms.formatDate.type.name": "Date",
  "transforms.formatDate.format": "yyyy-MM-dd"
}

```

Deploy either file from your terminal by using standard `PUT` requests:

```bash
curl -X PUT -H "Content-Type: application/json" --data @<filename>.json http://localhost:8083/connectors/<connector-name>/config

```

---

## Validation & Administrative Commands

### Check Connector Running Status

```bash
curl -s http://localhost:8083/connectors?expand=status | python -m json.tool

```

### Native Container Kafka Log Stream Verification

To trace raw message transmission directly out of the cluster brokers without running into local machine path syntax issues, step into the container directly:

```bash
docker exec -it broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders_.public.orders \
  --from-beginning

```

### Scale Task Pool Allocation On-The-Fly (Git Bash / Windows)

If `jq` is not installed on your machine, you can scale the task workers directly from your local terminal using this inline Python string modification script. This parses a nested repository JSON wrapper layout, extracts the raw properties map, updates `tasks.max` directly, and feeds it safely through the container loop:

```bash
python -c "import sys, json; data = json.load(sys.stdin); data['config']['tasks.max'] = '5'; print(json.dumps(data['config']))" < minio-s3-sink.json | \
docker exec -i kafka-connect curl -X PUT -H "Content-Type: application/json" \
--data @- http://localhost:8083/connectors/minio-s3-sink-connector/config

```

---

## Troubleshooting Layout Notes

* **Empty Topic Streams:** Ensure your Postgres table mutations are running *after* the initial data capture snapshot process completes.
* **TimestampConverter String Errors:** If Kafka Connect complains about missing parameters when converting dates, confirm both `"transforms.formatDate.type.name": "Date"` and `"transforms.formatDate.format": "yyyy-MM-dd"` are explicitly applied to your sink runtime instance.

```