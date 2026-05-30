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
```markdown
## Operational Reference & Cluster Administration

This section details the explicit commands required to seed the source engine, manage connector lifecycles, and validate real-time data streaming across the platform.

---

### 1. PostgreSQL Database Initialization & Data Seeding

#### Create the Application Target Database
```bash
docker exec -it postgres-db psql -U kafka_user -d postgres -c "CREATE DATABASE ordersdb;"

```

#### Provision the Orders Table Schema

```bash
docker exec -it postgres-db psql -U kafka_user -d ordersdb -c "
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer VARCHAR(255) NOT NULL,
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    amount NUMERIC(10, 2) NOT NULL
);
"

```

#### Build a Formatted Date Representation View

```bash
docker exec -it postgres-db psql -U kafka_user -d ordersdb -c "
CREATE VIEW public.orders_formatted_vw AS 
SELECT 
    id,
    customer,
    amount,
    to_char(order_date, 'YYYY-MM-DD') AS order_date
FROM public.orders;
"

```

#### Seed Initial Transaction Records (10-Row Mock Data Generation)

```bash
docker exec -it postgres-db psql -U kafka_user -d ordersdb -c "
INSERT INTO orders (customer, order_date, amount)
SELECT 
    (ARRAY['Alice Smith', 'Bob Jones', 'Charlie Brown', 'Diana Prince', 'Evan Wright', 'Fiona Gallagher', 'George Clark', 'Hannah Abbot', 'Ian Malcolm', 'Julia Roberts'])[i] AS customer,
    (CURRENT_DATE - (i || ' days')::INTERVAL)::DATE AS order_date,
    ROUND((RANDOM() * 450 + 50)::NUMERIC, 2) AS amount
FROM generate_series(1, 10) AS i;
"

```

#### Verify Source Rows Directly in Database

```bash
docker exec -it postgres-db psql -U kafka_user ordersdb -c "SELECT * FROM public.orders LIMIT 3;"

```

#### Insert an On-The-Fly Test Mutation

```bash
docker exec -it postgres-db psql -U kafka_user -d ordersdb -c "INSERT INTO orders (customer, amount) VALUES ('Bondey Jones', 22.75);"

```

---

### 2. Debezium PostgreSQL Source Connector Administration

#### Initialize/Register the Source Connector

```bash
curl -X POST -H "Content-Type: application/json" --data @postgres-source.json http://localhost:8083/connectors | python -m json.tool

```

#### Validate Task Runtime Health Status

```bash
curl -s http://localhost:8083/connectors/postgres-db-source/status | python -m json.tool

```

#### View Active Cluster Configuration States

```bash
curl -s http://localhost:8083/connectors/postgres-db-source/config | python -m json.tool

```

#### Update Configurations Dynamic Sync (Bypasses Windows Wrapper Layers)

```bash
python -c "import sys, json; data = json.load(sys.stdin); print(json.dumps(data['config']))" < postgres-source.json | \
docker exec -i kafka-connect curl -X PUT -H "Content-Type: application/json" \
--data @- http://localhost:8083/connectors/postgres-db-source/config | python -m json.tool

```

#### Restart the Source Task Instances

```bash
curl -X POST http://localhost:8083/connectors/postgres-db-source/restart

```

#### Delete/Deregister the Source Instance

```bash
curl -i -X DELETE http://localhost:8083/connectors/postgres-db-source

```

---

### 3. Kafka Broker Verification & Message Consumption

#### Inspect Registered Topics in Cluster Space

```bash
docker exec -it broker-1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

```

#### Trace Event Packets Streaming from Worker Natively

```bash
docker exec -it broker-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders_.public.orders \
  --from-beginning \
  --max-messages 1

```

---

### 4. MinIO S3 Sink Connector Administration

#### Initialize/Register the Sink Connector

```bash
curl -X POST -H "Content-Type: application/json" --data @minio-s3-sink.json http://localhost:8083/connectors | python -m json.tool

```

#### Validate Task Runtime Health Status

```bash
curl -s http://localhost:8083/connectors/minio-s3-sink-connector/status | python -m json.tool

```

#### View Active Cluster Configuration States

```bash
curl -s http://localhost:8083/connectors/minio-s3-sink-connector/config | python -m json.tool

```

#### Scale Task Concurrency Allocations Instantly (`tasks.max = 3`)

```bash
python -c "import sys, json; data = json.load(sys.stdin); data['config']['tasks.max'] = '3'; print(json.dumps(data['config']))" < minio-s3-sink.json | \
docker exec -i kafka-connect curl -X PUT -H "Content-Type: application/json" \
--data @- http://localhost:8083/connectors/minio-s3-sink-connector/config | python -m json.tool

```

#### Update Configurations Dynamic Sync (Bypasses Windows Wrapper Layers)

```bash
python -c "import sys, json; data = json.load(sys.stdin); print(json.dumps(data['config']))" < minio-s3-sink.json | \
docker exec -i kafka-connect curl -X PUT -H "Content-Type: application/json" \
--data @- http://localhost:8083/connectors/minio-s3-sink-connector/config | python -m json.tool

```

#### Restart the Sink Task Instances

```bash
curl -X POST http://localhost:8083/connectors/minio-s3-sink-connector/restart

```

#### Delete/Deregister the Sink Instance

```bash
curl -i -X DELETE http://localhost:8083/connectors/minio-s3-sink-connector

```

#### Force Immediate Memory-to-S3 Buffer Flush (`flush.size = 1`)

```bash
curl -X PUT -H "Content-Type: application/json" \
  -d '{
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
    "rotate.interval.ms": "10000"
  }' http://localhost:8083/connectors/minio-s3-sink-connector/config | python -m json.tool

```
---

## Troubleshooting Layout Notes

* **Empty Topic Streams:** Ensure your Postgres table mutations are running *after* the initial data capture snapshot process completes.
* **TimestampConverter String Errors:** If Kafka Connect complains about missing parameters when converting dates, confirm both `"transforms.formatDate.type.name": "Date"` and `"transforms.formatDate.format": "yyyy-MM-dd"` are explicitly applied to your sink runtime instance.

```
