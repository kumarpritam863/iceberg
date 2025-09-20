# Iceberg Kafka Connect Coordinator Architecture

## Overview Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            Kubernetes Cluster                                   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                              Job A                                      │   │
│  │                                                                         │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │                    Job Manager Pod                              │   │   │
│  │  │              (IcebergJobManagerApplication)                     │   │   │
│  │  │                                                                 │   │   │
│  │  │  ┌─────────────────────────────────────────────────────────┐   │   │   │
│  │  │  │              IcebergJobManager                          │   │   │   │
│  │  │  │                                                         │   │   │   │
│  │  │  │  • Commit coordination                                  │   │   │   │
│  │  │  │  • Event processing (DATA_WRITTEN, DATA_COMPLETE)      │   │   │   │
│  │  │  │  • Table commits (AppendFiles, RowDelta)               │   │   │   │
│  │  │  │  • Catalog management                                  │   │   │   │
│  │  │  │  • JobCommitState tracking                             │   │   │   │
│  │  │  └─────────────────────────────────────────────────────────┘   │   │   │
│  │  │                              │                                  │   │   │
│  │  │                              │ Kafka Events                     │   │   │
│  │  │                              ▼                                  │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                         │   │
│  │  ConfigMap: iceberg-job-config-job-a                                   │   │
│  │  - CONNECT_GROUP_ID=job-a                                              │   │
│  │  - CONTROL_TOPIC=iceberg-control-topic-a                               │   │
│  │  - CATALOG_* properties                                                │   │
│  │                                                                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                        Kafka Connect Workers                           │   │
│  │                         (Shared across jobs)                           │   │
│  │                                                                         │   │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                │   │
│  │  │   Worker 1  │    │   Worker 2  │    │   Worker 3  │                │   │
│  │  │             │    │             │    │             │                │   │
│  │  │ Connector A │    │ Connector A │    │ Connector A │                │   │
│  │  │ ┌─────────┐ │    │ ┌─────────┐ │    │ ┌─────────┐ │                │   │
│  │  │ │ Task 1  │ │    │ │ Task 2  │ │    │ │ Task 3  │ │                │   │
│  │  │ │ Part0,3 │ │    │ │ Part1,4 │ │    │ │ Part2,5 │ │                │   │
│  │  │ └─────────┘ │    │ └─────────┘ │    │ └─────────┘ │                │   │
│  │  │             │    │             │    │             │                │   │
│  │  │ Connector B │    │ Connector B │    │ Connector B │                │   │
│  │  │ ┌─────────┐ │    │ ┌─────────┐ │    │ ┌─────────┐ │                │   │
│  │  │ │ Task 1  │ │    │ │ Task 2  │ │    │ │ Task 3  │ │                │   │
│  │  │ │ Part0,3 │ │    │ │ Part1,4 │ │    │ │ Part2,5 │ │                │   │
│  │  │ └─────────┘ │    │ └─────────┘ │    │ └─────────┘ │                │   │
│  │  └─────────────┘    └─────────────┘    └─────────────┘                │   │
│  │                              │                                          │   │
│  │                              │ Events                                   │   │
│  │                              ▼                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                              Job B                                      │   │
│  │                                                                         │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │                    Job Manager Pod                              │   │   │
│  │  │              (IcebergJobManagerApplication)                     │   │   │
│  │  │                                                                 │   │   │
│  │  │  ┌─────────────────────────────────────────────────────────┐   │   │   │
│  │  │  │              IcebergJobManager                          │   │   │   │
│  │  │  │                                                         │   │   │   │
│  │  │  │  • Independent commit coordination                      │   │   │   │
│  │  │  │  • Separate control topic                               │   │   │   │
│  │  │  │  • Different catalog/table configuration               │   │   │   │
│  │  │  └─────────────────────────────────────────────────────────┘   │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                         │   │
│  │  ConfigMap: iceberg-job-config-job-b                                   │   │
│  │  - CONNECT_GROUP_ID=job-b                                              │   │
│  │  - CONTROL_TOPIC=iceberg-control-topic-b                               │   │
│  │  - Different CATALOG_* properties                                      │   │
│  │                                                                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                            External Dependencies                                 │
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│  │     Kafka       │    │  Iceberg Tables │    │   Object Store  │             │
│  │                 │    │                 │    │                 │             │
│  │ • Source topics │    │ • Data files    │    │ • S3/GCS/ADLS   │             │
│  │ • Control topics│    │ • Metadata      │    │ • Warehouse     │             │
│  │ • Offset topics │    │ • Manifests     │    │ • Catalog DB    │             │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘             │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Event Flow Diagram

```
┌─────────────────┐                    ┌─────────────────┐                    ┌─────────────────┐
│  Kafka Connect  │                    │  Job Manager    │                    │ Iceberg Tables  │
│    Workers      │                    │ (Coordinator)   │                    │                 │
└─────────────────┘                    └─────────────────┘                    └─────────────────┘
         │                                        │                                        │
         │                                        │                                        │
         │ 1. Process data & write files          │                                        │
         ├────────────────────────────────────────▶                                        │
         │                                        │                                        │
         │ 2. Send DATA_WRITTEN event             │                                        │
         ├────────────────────────────────────────▶                                        │
         │                                        │                                        │
         │ 3. Send DATA_COMPLETE event            │                                        │
         ├────────────────────────────────────────▶                                        │
         │                                        │                                        │
         │                                        │ 4. Aggregate events                    │
         │                                        │    & start commit                      │
         │                                        │                                        │
         │                                        │ 5. Commit to Iceberg tables           │
         │                                        ├────────────────────────────────────────▶
         │                                        │                                        │
         │                                        │ 6. Send COMMIT_TO_TABLE event          │
         │                                        │                                        │
         │ 7. Receive COMMIT_COMPLETE event       │                                        │
         ◀────────────────────────────────────────┤                                        │
         │                                        │                                        │

Timeline:
├─ t0: Commit interval reached OR timeout
├─ t1: Job Manager sends START_COMMIT
├─ t2: Workers send DATA_WRITTEN events
├─ t3: Workers send DATA_COMPLETE events
├─ t4: Job Manager commits to Iceberg
└─ t5: Job Manager sends COMMIT_COMPLETE
```

## Component Interaction Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Single Job Architecture                                  │
│                                                                                 │
│ Job Manager Pod                                                                 │
│ ┌─────────────────────────────────────────────────────────────────────────┐     │
│ │ IcebergJobManagerApplication                                            │     │
│ │                                                                         │     │
│ │ ┌─────────────────────────────────────────┐                             │     │
│ │ │         IcebergJobManager               │                             │     │
│ │ │                                         │                             │     │
│ │ │ ┌─────────────────────────────────────┐ │   ┌───────────────────────┐ │     │
│ │ │ │         JobCommitState              │ │   │    Catalog Loader     │ │     │
│ │ │ │                                     │ │   │                       │ │     │
│ │ │ │ • Commit interval tracking         │ │   │ • JDBC Catalog        │ │     │
│ │ │ │ • Event aggregation                │ │   │ • Hive Catalog        │ │     │
│ │ │ │ • Timeout management               │ │   │ • REST Catalog        │ │     │
│ │ │ │ • Data/Delete file collections    │ │   │ • Custom catalogs     │ │     │
│ │ │ └─────────────────────────────────────┘ │   └───────────────────────┘ │     │
│ │ │                                         │                             │     │
│ │ │ ┌─────────────────────────────────────┐ │   ┌───────────────────────┐ │     │
│ │ │ │       Kafka Producer/Consumer       │ │   │   Commit Executor     │ │     │
│ │ │ │                                     │ │   │                       │ │     │
│ │ │ │ • Control topic subscription       │ │   │ • Thread pool         │ │     │
│ │ │ │ • Event publishing                 │ │   │ • Parallel commits    │ │     │
│ │ │ │ • Group ID isolation               │ │   │ • Table operations    │ │     │
│ │ │ └─────────────────────────────────────┘ │   └───────────────────────┘ │     │
│ │ └─────────────────────────────────────────┘                             │     │
│ │                                                                         │     │
│ │ Configuration Sources:                                                  │     │
│ │ • Environment Variables (KAFKA_*, CATALOG_*)                           │     │
│ │ • ConfigMap mounting                                                    │     │
│ │ • Secret mounting (for credentials)                                     │     │
│ └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

                                        │
                                        │ Kafka Events
                                        │ (Control Topic)
                                        ▼

┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Kafka Connect Workers                                   │
│                                                                                 │
│ ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐             │
│ │   Worker Pod 1  │    │   Worker Pod 2  │    │   Worker Pod 3  │             │
│ │                 │    │                 │    │                 │             │
│ │ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │             │
│ │ │Sink Task    │ │    │ │Sink Task    │ │    │ │Sink Task    │ │             │
│ │ │             │ │    │ │             │ │    │ │             │ │             │
│ │ │• Partitions │ │    │ │• Partitions │ │    │ │• Partitions │ │             │
│ │ │  0, 3       │ │    │ │  1, 4       │ │    │ │  2, 5       │ │             │
│ │ │• Write data │ │    │ │• Write data │ │    │ │• Write data │ │             │
│ │ │• Send events│ │    │ │• Send events│ │    │ │• Send events│ │             │
│ │ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │             │
│ └─────────────────┘    └─────────────────┘    └─────────────────┘             │
│                                                                                 │
│ Shared Configuration:                                                           │
│ • Same CONNECT_GROUP_ID                                                         │
│ • Same CONTROL_TOPIC                                                            │
│ • Iceberg connector JARs                                                       │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Deployment Sequence Diagram

```
┌─────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Operator   │    │   Kubernetes    │    │  Job Manager    │    │ Kafka Connect  │
│   /User     │    │     API         │    │                 │    │   Workers      │
└─────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
       │                     │                       │                       │
       │                     │                       │                       │
       │ 1. Set env vars     │                       │                       │
       │    (KAFKA_*, etc)   │                       │                       │
       │                     │                       │                       │
       │ 2. ./deploy-job-    │                       │                       │
       │    manager.sh       │                       │                       │
       ├────────────────────▶│                       │                       │
       │                     │                       │                       │
       │                     │ 3. Create ConfigMap   │                       │
       │                     │    & Deployment       │                       │
       │                     ├──────────────────────▶│                       │
       │                     │                       │                       │
       │                     │                       │ 4. Start Job Manager  │
       │                     │                       │    Process             │
       │                     │                       │                       │
       │ 5. Deploy workers   │                       │                       │
       ├────────────────────▶│                       │                       │
       │                     │                       │                       │
       │                     │ 6. Create Worker      │                       │
       │                     │    Deployment         │                       │
       │                     ├──────────────────────────────────────────────▶│
       │                     │                       │                       │
       │                     │                       │                       │ 7. Start Connect
       │                     │                       │                       │    Workers
       │                     │                       │                       │
       │ 8. Create connector │                       │                       │
       │    via REST API     │                       │                       │
       ├─────────────────────────────────────────────────────────────────────▶│
       │                     │                       │                       │
       │                     │                       │                       │ 9. Start tasks
       │                     │                       │                       │    & begin
       │                     │                       │                       │    processing
       │                     │                       │                       │
       │                     │                       │◀─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┤
       │                     │                       │     Events flow       │
       │                     │                       │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─▶│
       │                     │                       │                       │

Steps:
1. Operator/User sets environment variables for job configuration
2. Runs deployment script for Job Manager
3. Kubernetes creates ConfigMap and Deployment for Job Manager
4. Job Manager pod starts and begins listening for events
5. Deploy Kafka Connect workers (shared across all jobs)
6. Workers start and register with Kafka Connect framework
7. Create specific connector configuration via REST API
8. Workers start processing partitions and sending events to Job Manager
9. Job Manager coordinates commits independently of partition assignments
```

## Transactional Event Processing

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Transactional Coordination                               │
│                                                                                 │
│ Job Manager (Transactional Producer/Consumer)                                  │
│ ┌─────────────────────────────────────────────────────────────────────────┐     │
│ │                       Transaction Boundary                              │     │
│ │                                                                         │     │
│ │ BEGIN TRANSACTION                                                       │     │
│ │ ├─ 1. Consume events (DATA_WRITTEN, DATA_COMPLETE)                     │     │
│ │ ├─ 2. Validate and aggregate data                                       │     │
│ │ ├─ 3. Commit to Iceberg tables                                          │     │
│ │ ├─ 4. Send COMMIT_COMPLETE event                                        │     │
│ │ └─ 5. COMMIT TRANSACTION                                                │     │
│ │                                                                         │     │
│ │ On Error: ABORT TRANSACTION                                             │     │
│ │ - Rolls back all operations                                             │     │
│ │ - Events are re-consumed                                                │     │
│ │ - No partial state                                                      │     │
│ └─────────────────────────────────────────────────────────────────────────┘     │
│                                                                                 │
│ Consumer Configuration:                                                         │
│ • isolation.level = read_committed                                              │
│ • enable.auto.commit = false                                                   │
│ • manual offset commits after transaction success                              │
│                                                                                 │
│ Producer Configuration:                                                         │
│ • transactional.id = iceberg-coordinator-{job-id}                              │
│ • enable.idempotence = true                                                    │
│ • acks = all                                                                   │
│ • max.in.flight.requests.per.connection = 1                                   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

Timeline with Transactions:
├─ t0: Begin transaction
├─ t1: Consume events (only committed events visible)
├─ t2: Process and validate events
├─ t3: Commit to Iceberg tables
├─ t4: Send COMMIT_COMPLETE event (within transaction)
├─ t5: Commit transaction (atomically commits event + consumer offsets)
└─ t6: Consumer offsets committed (exactly-once processing)

Failure Scenarios:
├─ Network failure → Abort transaction, retry from last committed offset
├─ Iceberg commit failure → Abort transaction, retry entire operation
├─ Producer failure → Abort transaction, no partial events sent
└─ Consumer rebalance → Transaction aborted, new consumer continues from committed offset
```

## Exactly-Once Semantics Guarantees

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          Exactly-Once Flow                                      │
│                                                                                 │
│ Workers                    Job Manager                     Iceberg Tables      │
│ ┌─────────────────┐       ┌─────────────────┐            ┌─────────────────┐   │
│ │  Task 1         │       │  Transactional  │            │  Table A        │   │
│ │  writes data    │──────▶│  Coordinator    │───────────▶│  Committed      │   │
│ │  sends events   │       │                 │            │  Snapshot       │   │
│ └─────────────────┘       │  BEGIN TX       │            └─────────────────┘   │
│                           │  │              │                                  │
│ ┌─────────────────┐       │  ├─consume       │            ┌─────────────────┐   │
│ │  Task 2         │       │  ├─validate     │            │  Control Topic  │   │
│ │  writes data    │──────▶│  ├─commit       │───────────▶│  COMMIT_COMPLETE│   │
│ │  sends events   │       │  ├─send event   │            │  Event          │   │
│ └─────────────────┘       │  │              │            └─────────────────┘   │
│                           │  COMMIT TX      │                                  │
│ ┌─────────────────┐       │                 │            ┌─────────────────┐   │
│ │  Task 3         │       │  Offsets        │            │  Consumer       │   │
│ │  writes data    │──────▶│  Committed      │───────────▶│  Offsets        │   │
│ │  sends events   │       │                 │            │  Committed      │   │
│ └─────────────────┘       └─────────────────┘            └─────────────────┘   │
│                                                                                 │
│ Guarantees:                                                                     │
│ ✅ Events processed exactly once (no duplicates)                               │
│ ✅ Iceberg commits are atomic (all or nothing)                                 │
│ ✅ Control events sent exactly once                                             │
│ ✅ Consumer offsets track processed events exactly                              │
│ ✅ Failures result in retry, not partial state                                 │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```
BEFORE (Partition-Based Coordinator):
┌─────────────────────────────────────────────────────────────────┐
│                        Single Job                               │
│                                                                 │
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐    │
│ │   Task 0        │ │   Task 1        │ │   Task 2        │    │
│ │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │    │
│ │ │Coordinator  │ │ │ │   Worker    │ │ │ │   Worker    │ │    │
│ │ │Partition 0  │ │ │ │ Partition 1 │ │ │ │ Partition 2 │ │    │
│ │ │Partition 3  │ │ │ │ Partition 4 │ │ │ │ Partition 5 │ │    │
│ │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │    │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘    │
│         │                   │                   │              │
│         └─── Rebalance ──────┴─── Disrupts ─────┘              │
│              Coordinator moves between tasks                   │
└─────────────────────────────────────────────────────────────────┘

AFTER (Per-Job Coordinator):
┌─────────────────────────────────────────────────────────────────┐
│                        Single Job                               │
│                                                                 │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │                 Job Manager                                 │ │
│ │              (Dedicated Pod)                                │ │
│ │                                                             │ │
│ │  • Independent of partition assignment                      │ │
│ │  • Survives rebalances                                      │ │
│ │  • Dedicated resources                                      │ │
│ │  • Single point of coordination                             │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                              │                                 │
│                              │ Events                          │
│                              ▼                                 │
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐   │
│ │    Worker 1     │ │    Worker 2     │ │    Worker 3     │   │
│ │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │   │
│ │ │ Partition 0 │ │ │ │ Partition 1 │ │ │ │ Partition 2 │ │   │
│ │ │ Partition 3 │ │ │ │ Partition 4 │ │ │ │ Partition 5 │ │   │
│ │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │   │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘   │
│                                                               │
│ Rebalance only affects workers, not coordination!             │
└─────────────────────────────────────────────────────────────────┘
```