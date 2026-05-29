# my-docker-desktop-configs



\# Real-Time Change Data Capture (CDC) Pipeline: Postgres to MinIO S3 via KRaft Kafka



A robust, streaming data engineering platform designed to intercept transactional mutations from an active PostgreSQL core, route them through a high-throughput, native KRaft-mode Apache Kafka cluster, and flush them cleanly as flattened, date-formatted JSON strings into a MinIO Object Storage bucket.



\---



\## Infrastructure Overview



The local topology runs entirely inside a isolated Docker Desktop container ecosystem, composed of the following services:



\* \*\*Kafka Control Plane:\*\* A 3-Broker Native KRaft Cluster (`controller-1`, `broker-1`, `broker-2`, `broker-3`) completely stripped of ZooKeeper dependencies.

\* \*\*Schema Registry:\*\* Confluent Schema Registry tracking schema metadata and checking internal format compatibility layers.

\* \*\*Source Database:\*\* A PostgreSQL instances (`postgres-db`) with Logical Replication enabled (`wal\_level=logical`).

\* \*\*Streaming Engine:\*\* Debezium Connect Cluster worker framework (`kafka-connect`) handling runtime source and sink connector class loops.

\* \*\*Target Infrastructure:\*\* A local MinIO S3 API Emulator (`minio-s3`) automated via a configuration sidecar container (`minio-init`) to generate target storage folders on startup.



\---



\## Getting Started: Docker Desktop Deployment



\### 1. Initialize the Cluster Stack

To launch the complete cluster infrastructure along with clean storage mounts, execute:

```bash

docker compose up -d

