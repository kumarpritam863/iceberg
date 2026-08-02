# Iceberg Coordinator-as-a-Service: Full Design

## Executive Summary

Extract the coordinator into a **standalone multi-task Kafka Connect Sink Connector** (`IcebergCoordinatorConnector`) where **each task owns exactly one control topic**. The number of tasks equals the number of control topics being coordinated. Each control topic represents one independent coordination domain (potentially serving multiple worker connectors). This creates a true **Coordinator-as-a-Service (CaaS)** - a single deployment that manages all Iceberg ingestion pipelines in your cluster.

---

## 1. Current Architecture (Problems)

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Today: Each connector embeds its own coordinator                        │
│                                                                          │
│  Connector "orders-iceberg" (tasks.max=4)                               │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐                   │
│  │ Task 0   │ │ Task 1   │ │ Task 2   │ │ Task 3   │                   │
│  │ Worker + │ │ Worker   │ │ Worker   │ │ Worker   │                   │
│  │ COORD    │ │          │ │          │ │          │                   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘                   │
│                                                                          │
│  Connector "events-iceberg" (tasks.max=8)                               │
│  ┌──────────┐ ┌──────────┐ ... ┌──────────┐                            │
│  │ Task 0   │ │ Task 1   │     │ Task 7   │                            │
│  │ Worker + │ │ Worker   │     │ Worker   │                            │
│  │ COORD    │ │          │     │          │                            │
│  └──────────┘ └──────────┘     └──────────┘                            │
│                                                                          │
│  Problems: coupled lifecycle, resource contention, leader election       │
│  fragility, no independent scaling, N redundant coordinators            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Proposed Architecture: Coordinator-as-a-Service

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Kafka Connect Cluster                                                           │
│                                                                                  │
│  ┌─ IcebergCoordinatorConnector "iceberg-caas" ──────────────────────────────┐  │
│  │  topics = control-orders, control-events, control-clicks                   │  │
│  │  tasks = 3 (auto-computed: sum of partitions = 1+1+1 = 3)                │  │
│  │                                                                            │  │
│  │  ENFORCED: every control topic must have exactly 1 partition              │  │
│  │                                                                            │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐           │  │
│  │  │ Task 0          │  │ Task 1          │  │ Task 2          │           │  │
│  │  │                 │  │                 │  │                 │           │  │
│  │  │ control-orders  │  │ control-events  │  │ control-clicks  │           │  │
│  │  │ partition-0     │  │ partition-0     │  │ partition-0     │           │  │
│  │  │                 │  │                 │  │                 │           │  │
│  │  │ Commits to:     │  │ Commits to:     │  │ Commits to:     │           │  │
│  │  │  db.orders      │  │  db.page_views  │  │  analytics.     │           │  │
│  │  │  db.order_items │  │  db.app_events  │  │    clicks       │           │  │
│  │  │                 │  │  db.user_actions │  │                 │           │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘           │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌─ Worker: "orders-iceberg" (tasks.max=4) ──────────────────────────────────┐  │
│  │  control.topic = control-orders (1 partition, enforced)                    │  │
│  │  coordinator.enabled = false                                               │  │
│  │  [Worker] [Worker] [Worker] [Worker]                                       │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌─ Worker: "events-iceberg" (tasks.max=8) ──────────────────────────────────┐  │
│  │  control.topic = control-events (1 partition, enforced)                    │  │
│  │  coordinator.enabled = false                                               │  │
│  │  [Worker] [Worker] [Worker] [Worker] [Worker] [Worker] [Worker] [Worker]   │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│  ┌─ Worker: "clicks-iceberg" (tasks.max=12) ─────────────────────────────────┐  │
│  │  control.topic = control-clicks (1 partition, enforced)                    │  │
│  │  coordinator.enabled = false                                               │  │
│  │  [Worker x12]                                                              │  │
│  └────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Core Invariants (Enforced, Not Optional)

```
1 Control Topic = 1 Partition = 1 Coordination Domain = 1 Coordinator Task (steady state)
```

**Hard constraints enforced by the connector at startup:**

1. **Every control topic MUST have exactly 1 partition.** The connector validates this via the Admin API during `start()`. If any configured control topic has != 1 partition, the connector fails with `ConfigException`.

2. **Task count = total partition count across all control topics.** Since each topic has 1 partition, this equals the number of control topics. The connector computes this automatically — the user does NOT set `tasks.max` (or if set, it's just a ceiling — set it high).

3. **Each task receives exactly 1 partition (= 1 topic) in steady state.** Kafka Connect's default partition assignment naturally achieves 1:1 mapping when `tasks = partitions`.

4. **During failover, a task may serve multiple domains.** If a task dies, Connect rebalances its partition to a surviving task. That task handles multiple control topics gracefully — no exceptions, no cascading failure. When the failed task restarts, rebalancing restores the 1:1 mapping.

These constraints eliminate partition assignment complexity while providing resilience under failure.

---

## 3. Task Count Enforcement

### Why Single-Partition Control Topics

- The current protocol already uses a single producer key → all events route to 1 partition
- Strict ordering within a coordination domain is required (StartCommit must precede DataWritten responses)
- No coordination across partitions needed — parallelism lives inside the task (commit thread pool)
- Enables trivial 1:1 task assignment with zero custom partition logic

### Connector Implementation

```java
public class IcebergCoordinatorConnector extends SinkConnector {

    private IcebergCoordinatorConfig config;
    private int totalPartitions;

    @Override
    public void start(Map<String, String> props) {
        this.config = new IcebergCoordinatorConfig(props);

        // Validate: every control topic must have exactly 1 partition
        try (Admin admin = config.createAdmin()) {
            List<String> controlTopics = config.controlTopics();
            Map<String, TopicDescription> descriptions =
                admin.describeTopics(controlTopics).allTopicValues().get();

            for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                int partitionCount = entry.getValue().partitions().size();
                if (partitionCount != 1) {
                    throw new ConfigException(String.format(
                        "Control topic '%s' has %d partitions. "
                            + "IcebergCoordinatorConnector requires exactly 1 partition "
                            + "per control topic. Recreate the topic with --partitions 1.",
                        entry.getKey(), partitionCount));
                }
            }

            // Task count = total partitions across all control topics
            // Since each has exactly 1 partition: tasks = number of control topics
            this.totalPartitions = controlTopics.size();
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConnectException("Failed to validate control topics", e);
        }

        LOG.info("IcebergCoordinatorConnector validated {} control topics, "
            + "each with 1 partition. Will create {} tasks.",
            config.controlTopics().size(), totalPartitions);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return IcebergCoordinatorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // Task count is determined by total partition count across control topics.
        // Since each control topic has exactly 1 partition (enforced at start()),
        // this equals the number of control topics.
        //
        // Kafka Connect creates min(maxTasks, taskConfigs.size()) tasks.
        // We return exactly totalPartitions configs.
        // User should set tasks.max >= totalPartitions (just set it high like 100).

        List<String> controlTopics = config.controlTopics();

        if (maxTasks < totalPartitions) {
            LOG.error(
                "tasks.max ({}) is less than the total partition count ({}). "
                    + "Some coordination domains will be unserved. "
                    + "Set tasks.max >= {}.",
                maxTasks, totalPartitions, totalPartitions);
        }

        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < totalPartitions; i++) {
            Map<String, String> taskProps = new HashMap<>(config.originalProps());
            // Assign this task its specific control topic
            taskProps.put(
                IcebergCoordinatorConfig.ASSIGNED_CONTROL_TOPIC_PROP,
                controlTopics.get(i)
            );
            // Also set the Kafka Connect "topics" for this task's subscription
            // (the framework uses this to subscribe the task's consumer)
            taskProps.put("topics", controlTopics.get(i));
            taskProps.put("task.id", String.valueOf(i));
            configs.add(taskProps);
        }
        return configs;
    }

    @Override
    public ConfigDef config() {
        return IcebergCoordinatorConfig.CONFIG_DEF;
    }
}
```

### Why This Works Perfectly

With N control topics × 1 partition each = N total partitions, and `taskConfigs()` returning N configs:

```
topics = control-orders, control-events, control-clicks
         (1 partition)    (1 partition)    (1 partition)

Total partitions across all subscribed topics = 3
taskConfigs() returns 3 configs
Framework creates 3 tasks
Framework assigns partitions round-robin:
  Task 0 → control-orders-0
  Task 1 → control-events-0
  Task 2 → control-clicks-0
```

No custom assigners. No filtering in `open()`. No pausing foreign partitions. The math just works because 1 partition per topic + N tasks for N partitions = deterministic 1:1 assignment.

### Resilience on Rebalance (No Exceptions on Extra Partitions)

If a task dies, Kafka Connect rebalances its partition to a surviving task. That task now holds 2+ partitions from different control topics. **The task MUST NOT throw** — doing so would cascade failures across the entire coordinator.

Instead, each task dynamically handles whatever partitions it receives:

```java
@Override
public void open(Collection<TopicPartition> partitions) {
    // Group assigned partitions by topic — each topic is a coordination domain
    Map<String, List<TopicPartition>> byTopic = partitions.stream()
        .collect(Collectors.groupingBy(TopicPartition::topic));

    for (Map.Entry<String, List<TopicPartition>> entry : byTopic.entrySet()) {
        String topic = entry.getKey();
        if (!domains.containsKey(topic)) {
            // New domain assigned to this task (likely due to another task's failure)
            domains.put(topic, new CoordinationDomain(topic, config, admin, producer));
            LOG.warn("Coordinator task {} taking over domain '{}' "
                + "(likely another task failed). Now serving {} domains.",
                taskId, topic, domains.size());
        }
    }

    if (domains.size() > 1) {
        LOG.warn("Coordinator task {} is serving {} domains: {}. "
            + "This indicates a task failure. Performance may be degraded "
            + "until the failed task restarts and rebalancing restores 1:1 mapping.",
            taskId, domains.size(), domains.keySet());
    } else {
        LOG.info("Coordinator task {} opened for {}", taskId, partitions);
    }
}

@Override
public void close(Collection<TopicPartition> partitions) {
    // Partitions being revoked — remove domains that no longer have any partitions
    Map<String, List<TopicPartition>> revoked = partitions.stream()
        .collect(Collectors.groupingBy(TopicPartition::topic));

    for (String topic : revoked.keySet()) {
        CoordinationDomain domain = domains.remove(topic);
        if (domain != null) {
            LOG.info("Coordinator task {} releasing domain '{}' (rebalance)", taskId, topic);
            domain.close();
        }
    }
}
```

**Behavioral properties during degraded state (task serving multiple domains):**

| Aspect | Behavior |
|--------|----------|
| Correctness | Fully preserved — each domain has independent CommitState |
| Ordering | Preserved — each domain reads from its single partition |
| Performance | Slightly degraded — one task does commit work for 2+ domains |
| Recovery | Automatic — when the failed task restarts, rebalance restores 1:1 |
| Cascading failure | Impossible — task never throws on partition count |

**Steady state** is still 1:1 (one task per control topic). The multi-domain capability is purely a resilience mechanism for transient failures.

---

## 4. Detailed Class Design

### 4.1 Module Structure

```
kafka-connect/
├── kafka-connect-coordinator/                    # NEW MODULE
│   └── src/main/java/org/apache/iceberg/connect/coordinator/
│       ├── IcebergCoordinatorConnector.java      # SinkConnector
│       ├── IcebergCoordinatorTask.java           # SinkTask (1 per control topic)
│       ├── IcebergCoordinatorConfig.java         # Configuration
│       ├── CoordinationDomain.java              # Per-topic coordination state
│       ├── DomainCommitOrchestrator.java        # Commit lifecycle for one domain
│       └── PartitionCountTracker.java           # Dynamic worker partition tracking
│
├── kafka-connect/                                # EXISTING (workers)
│   └── src/main/java/org/apache/iceberg/connect/
│       ├── IcebergSinkConnector.java            # Unchanged
│       ├── IcebergSinkTask.java                 # Unchanged
│       ├── IcebergSinkConfig.java               # + coordinator.enabled flag
│       └── channel/
│           ├── CommitterImpl.java               # Skip coordinator when disabled
│           ├── Worker.java                      # Unchanged
│           ├── Coordinator.java                 # Unchanged (still available for embedded mode)
│           └── ...
│
├── kafka-connect-events/                         # EXISTING (shared event format)
│   └── (unchanged - same Event/Avro protocol)
```

### 4.2 IcebergCoordinatorTask

Each task is a self-contained coordinator that handles one or more control topics (normally 1, more during failover):

```java
public class IcebergCoordinatorTask extends SinkTask {

    private String primaryControlTopic;  // From taskConfigs(), for logging
    private IcebergCoordinatorConfig config;
    private Producer<String, byte[]> producer;
    private Admin admin;
    private ScheduledExecutorService commitScheduler;

    // One domain per control topic this task is serving
    // Normally contains 1 entry; may temporarily have more during failover
    private final Map<String, CoordinationDomain> domains = new ConcurrentHashMap<>();

    @Override
    public void start(Map<String, String> props) {
        this.config = new IcebergCoordinatorConfig(props);
        this.primaryControlTopic = config.assignedControlTopic();

        // Shared Kafka clients across all domains this task serves
        this.admin = createAdmin(config);
        this.producer = createTransactionalProducer(config);

        // Periodic commit interval checker (iterates all active domains)
        this.commitScheduler = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-coord-" + primaryControlTopic + "-%d")
                .build()
        );
        commitScheduler.scheduleAtFixedRate(
            this::checkAllDomains,
            config.commitCheckIntervalMs(),
            config.commitCheckIntervalMs(),
            TimeUnit.MILLISECONDS
        );

        LOG.info("Coordinator task started, primary domain: {}", primaryControlTopic);
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        // Described in Section 3 — creates CoordinationDomain per unique topic
        Map<String, List<TopicPartition>> byTopic = partitions.stream()
            .collect(Collectors.groupingBy(TopicPartition::topic));

        for (String topic : byTopic.keySet()) {
            domains.computeIfAbsent(topic, t -> {
                LOG.info("Coordinator task opening domain for '{}'", t);
                if (!t.equals(primaryControlTopic)) {
                    LOG.warn("Taking over domain '{}' from a failed task. "
                        + "Now serving {} domains.", t, domains.size() + 1);
                }
                return new CoordinationDomain(t, config, admin, producer);
            });
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        Map<String, List<TopicPartition>> revoked = partitions.stream()
            .collect(Collectors.groupingBy(TopicPartition::topic));

        for (String topic : revoked.keySet()) {
            CoordinationDomain domain = domains.remove(topic);
            if (domain != null) {
                LOG.info("Releasing domain '{}' (partition revoked)", topic);
                domain.close();
            }
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            // Route record to the correct domain by topic
            CoordinationDomain domain = domains.get(record.topic());
            if (domain == null) {
                LOG.debug("Ignoring record from unknown topic: {}", record.topic());
                continue;
            }

            // Decode and dispatch to the domain's commit state machine
            byte[] value = (byte[]) record.value();
            Event event = AvroUtil.decode(value);
            domain.handleEvent(event, record.kafkaPartition(), record.kafkaOffset());
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Collect safe offsets from ALL domains this task is serving
        Map<TopicPartition, OffsetAndMetadata> safeOffsets = new HashMap<>();
        for (CoordinationDomain domain : domains.values()) {
            safeOffsets.putAll(domain.safeOffsets());
        }
        return safeOffsets;
    }

    @Override
    public void stop() {
        commitScheduler.shutdownNow();
        domains.values().forEach(CoordinationDomain::close);
        domains.clear();
        producer.close();
        admin.close();
        LOG.info("Coordinator task stopped (was serving {} domains)", domains.size());
    }

    private void checkAllDomains() {
        for (CoordinationDomain domain : domains.values()) {
            domain.checkCommitIntervals();
        }
    }
}
```

### 4.3 CoordinationDomain

Represents one control topic's coordination domain. Handles multiple `groupId`s (multiple worker connectors can share one control topic, or 1:1):

```java
class CoordinationDomain {

    private final String controlTopic;
    private final IcebergCoordinatorConfig config;
    private final Producer<String, byte[]> producer;
    private final Admin admin;
    private final ExecutorService commitExecutor;
    private final PartitionCountTracker partitionTracker;
    private final String producerId;

    // One CommitState per worker connector groupId using this control topic
    private final Map<String, ConnectorCommitState> connectorStates = new ConcurrentHashMap<>();
    // Track offsets that are safe to commit (all events up to this point are processed)
    private final Map<Integer, Long> lastCommittedOffsets = new ConcurrentHashMap<>();

    CoordinationDomain(
            String controlTopic,
            IcebergCoordinatorConfig config,
            Admin admin,
            Producer<String, byte[]> producer) {
        this.controlTopic = controlTopic;
        this.config = config;
        this.admin = admin;
        this.producer = producer;
        this.producerId = UUID.randomUUID().toString();
        this.commitExecutor = new ThreadPoolExecutor(
            config.commitThreads(),
            config.commitThreads(),
            120, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-commit-" + controlTopic + "-%d")
                .build()
        );
        this.partitionTracker = new PartitionCountTracker(
            admin, config.partitionCountRefreshMs()
        );

        // Pre-register known connectors from config
        for (String groupId : config.managedGroupIds(controlTopic)) {
            registerConnector(groupId);
        }
    }

    void handleEvent(Event event, int partition, long offset) {
        String groupId = event.groupId();

        // Auto-register unknown connectors (discovery mode)
        ConnectorCommitState state = connectorStates.computeIfAbsent(
            groupId, this::registerConnector
        );

        switch (event.payload().type()) {
            case DATA_WRITTEN:
                state.commitState().addResponse(new Envelope(event, partition, offset));
                break;

            case DATA_COMPLETE:
                state.commitState().addReady(new Envelope(event, partition, offset));
                int partitionCount = partitionTracker.getPartitionCount(groupId);
                if (state.commitState().isCommitReady(partitionCount)) {
                    executeCommit(state, false);
                }
                break;

            case START_COMMIT:
                // Ignore our own StartCommit echoes
                break;

            case COMMIT_TO_TABLE:
            case COMMIT_COMPLETE:
                // Informational, ignore
                break;
        }
    }

    /**
     * Called periodically by the scheduler.
     * Checks commit intervals and timeouts for each managed connector.
     */
    void checkCommitIntervals() {
        for (ConnectorCommitState state : connectorStates.values()) {
            try {
                if (state.commitState().isCommitIntervalReached()) {
                    startCommit(state);
                }
                if (state.commitState().isCommitTimedOut()) {
                    executeCommit(state, true);
                }
            } catch (Exception e) {
                LOG.error("Error in commit check for {}", state.groupId(), e);
            }
        }
    }

    private void startCommit(ConnectorCommitState state) {
        state.commitState().startNewCommit();
        Event event = new Event(
            state.groupId(),
            new StartCommit(state.commitState().currentCommitId())
        );
        sendToControlTopic(event);
        LOG.info("Domain [{}] initiated commit {} for connector {}",
            controlTopic, state.commitState().currentCommitId(), state.groupId());
    }

    private void executeCommit(ConnectorCommitState state, boolean partial) {
        try {
            doCommit(state, partial);
        } catch (Exception e) {
            LOG.warn("Domain [{}] failed commit {} for connector {}, will retry",
                controlTopic, state.commitState().currentCommitId(), state.groupId(), e);
        } finally {
            state.commitState().endCurrentCommit();
        }
    }

    private void doCommit(ConnectorCommitState state, boolean partial) {
        Map<TableReference, List<Envelope>> commitMap = state.commitState().tableCommitMap();
        OffsetDateTime validThroughTs = state.commitState().validThroughTs(partial);

        // Commit to all tables in parallel (same logic as existing Coordinator.commitToTable)
        Tasks.foreach(commitMap.entrySet())
            .executeWith(commitExecutor)
            .stopOnFailure()
            .run(entry -> commitToTable(
                state, entry.getKey(), entry.getValue(), validThroughTs
            ));

        // All tables committed successfully
        state.commitState().clearResponses();
        lastCommittedOffsets.putAll(/* current controlTopicOffsets */);

        // Emit CommitComplete
        Event event = new Event(
            state.groupId(),
            new CommitComplete(state.commitState().currentCommitId(), validThroughTs)
        );
        sendToControlTopic(event);

        LOG.info("Domain [{}] completed commit {} for connector {}, {} table(s)",
            controlTopic, state.commitState().currentCommitId(),
            state.groupId(), commitMap.size());
    }

    private void commitToTable(
            ConnectorCommitState state,
            TableReference tableReference,
            List<Envelope> envelopes,
            OffsetDateTime validThroughTs) {
        // === SAME LOGIC AS EXISTING Coordinator.commitToTable() ===
        // 1. Load table from catalog
        // 2. Validate UUID matches
        // 3. Get lastCommittedOffsets from snapshot summary
        // 4. Merge with control topic offsets
        // 5. Filter envelopes by offset range
        // 6. Deduplicate data/delete files
        // 7. Build AppendFiles or RowDelta operation
        // 8. Set snapshot properties (offsets, commit-id, task-id, valid-through-ts)
        // 9. Validate via SnapshotAncestryValidator
        // 10. Commit
        // 11. Emit CommitToTable event
    }

    private void sendToControlTopic(Event event) {
        byte[] data = AvroUtil.encode(event);
        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(controlTopic, producerId, data);
        producer.send(record).get(); // sync send for coordinator events
    }

    Map<TopicPartition, OffsetAndMetadata> safeOffsets(String topic) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        lastCommittedOffsets.forEach((partition, offset) ->
            offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset))
        );
        return offsets;
    }

    private ConnectorCommitState registerConnector(String groupId) {
        LOG.info("Domain [{}] registering connector with groupId: {}", controlTopic, groupId);
        Catalog catalog = loadCatalog(config, groupId);
        CommitState commitState = new CommitState(config.commitConfig(groupId));
        return new ConnectorCommitState(groupId, catalog, commitState);
    }

    void close() {
        commitExecutor.shutdownNow();
        connectorStates.values().forEach(ConnectorCommitState::close);
    }
}
```

### 4.4 ConnectorCommitState

Per-worker-connector state within a domain:

```java
class ConnectorCommitState {
    private final String groupId;
    private final Catalog catalog;
    private final CommitState commitState;
    private final String snapshotOffsetsProp;

    ConnectorCommitState(String groupId, Catalog catalog, CommitState commitState) {
        this.groupId = groupId;
        this.catalog = catalog;
        this.commitState = commitState;
        this.snapshotOffsetsProp = String.format(
            "kafka.connect.offsets.%s.%s",
            /* controlTopic from parent */, groupId
        );
    }

    String groupId() { return groupId; }
    Catalog catalog() { return catalog; }
    CommitState commitState() { return commitState; }
    String snapshotOffsetsProp() { return snapshotOffsetsProp; }

    void close() {
        if (catalog instanceof Closeable) {
            ((Closeable) catalog).close();
        }
    }
}
```

### 4.5 PartitionCountTracker

```java
class PartitionCountTracker {
    private final Admin admin;
    private final long refreshIntervalMs;
    private final Map<String, Integer> cachedCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> lastRefreshTime = new ConcurrentHashMap<>();

    int getPartitionCount(String groupId) {
        long now = System.currentTimeMillis();
        Long lastRefresh = lastRefreshTime.get(groupId);

        if (lastRefresh == null || (now - lastRefresh) > refreshIntervalMs) {
            refresh(groupId);
        }
        return cachedCounts.getOrDefault(groupId, 0);
    }

    private synchronized void refresh(String groupId) {
        try {
            ConsumerGroupDescription desc =
                KafkaUtils.consumerGroupDescription(groupId, admin);
            int count = desc.members().stream()
                .mapToInt(m -> m.assignment().topicPartitions().size())
                .sum();
            cachedCounts.put(groupId, count);
            lastRefreshTime.put(groupId, System.currentTimeMillis());
            LOG.debug("Refreshed partition count for {}: {}", groupId, count);
        } catch (Exception e) {
            LOG.warn("Failed to refresh partition count for {}, using cached: {}",
                groupId, cachedCounts.get(groupId), e);
        }
    }
}
```

---

## 5. Configuration

```java
public class IcebergCoordinatorConfig extends AbstractConfig {

    // ═══ Connector-Level (applies to the CaaS connector itself) ═══

    // List of control topics to coordinate (determines task count)
    // The connector auto-sets the Kafka Connect "topics" config from this list.
    // User does NOT need to set "topics" separately.
    static final String CONTROL_TOPICS_PROP = "iceberg.coordinator.control-topics";
    // e.g., "control-orders,control-events,control-clicks"

    // Internal: assigned to each task by taskConfigs()
    static final String ASSIGNED_CONTROL_TOPIC_PROP = "iceberg.coordinator.assigned-control-topic";

    // ═══ Task Count (auto-computed, not user-configured) ═══
    // tasks = sum(partitions across all control topics)
    //       = number of control topics (since each MUST have exactly 1 partition)
    // User sets tasks.max as a ceiling (set it high, e.g., 100).
    // The connector's taskConfigs() returns exactly N configs where
    // N = total partition count. Framework creates min(tasks.max, N) tasks.

    // ═══ Global Defaults (overridable per-domain or per-connector) ═══

    static final String COMMIT_INTERVAL_MS_PROP = "iceberg.coordinator.commit.interval-ms";
    static final int COMMIT_INTERVAL_MS_DEFAULT = 300_000;

    static final String COMMIT_TIMEOUT_MS_PROP = "iceberg.coordinator.commit.timeout-ms";
    static final int COMMIT_TIMEOUT_MS_DEFAULT = 30_000;

    static final String COMMIT_THREADS_PROP = "iceberg.coordinator.commit.threads";
    static final int COMMIT_THREADS_DEFAULT = Runtime.getRuntime().availableProcessors() * 2;

    static final String PARTITION_COUNT_REFRESH_MS_PROP =
        "iceberg.coordinator.partition-count-refresh-ms";
    static final long PARTITION_COUNT_REFRESH_MS_DEFAULT = 60_000;

    static final String COMMIT_CHECK_INTERVAL_MS_PROP =
        "iceberg.coordinator.commit-check-interval-ms";
    static final long COMMIT_CHECK_INTERVAL_MS_DEFAULT = 1_000; // 1s polling

    // ═══ Per-Domain Overrides (by control topic name) ═══
    // iceberg.coordinator.domain.{control-topic-name}.commit.interval-ms
    // iceberg.coordinator.domain.{control-topic-name}.commit.timeout-ms
    // iceberg.coordinator.domain.{control-topic-name}.commit.threads

    // ═══ Per-Connector Overrides (by groupId, within a domain) ═══
    // iceberg.coordinator.connector.{groupId}.commit.interval-ms
    // iceberg.coordinator.connector.{groupId}.catalog.*

    // ═══ Managed Connectors (which groupIds to expect per control topic) ═══
    // iceberg.coordinator.domain.{control-topic-name}.managed-connectors=groupId1,groupId2
    // OR: auto-discover (register any groupId seen on the control topic)
    static final String AUTO_DISCOVER_PROP = "iceberg.coordinator.auto-discover";
    static final boolean AUTO_DISCOVER_DEFAULT = true;

    // ═══ Catalog (default, overridable per-connector) ═══
    static final String CATALOG_NAME_PROP = "iceberg.catalog";
    static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";

    // ═══ Kafka Clients ═══
    static final String KAFKA_PROP_PREFIX = "iceberg.kafka.";

    // ═══ Converters (must be ByteArray for raw Avro events) ═══
    // key.converter = org.apache.kafka.connect.converters.ByteArrayConverter
    // value.converter = org.apache.kafka.connect.converters.ByteArrayConverter

    // ─── Derived ───

    public List<String> controlTopics() {
        return getList(CONTROL_TOPICS_PROP);
    }

    public String assignedControlTopic() {
        return getString(ASSIGNED_CONTROL_TOPIC_PROP);
    }

    public List<String> managedGroupIds(String controlTopic) {
        String key = "iceberg.coordinator.domain." + controlTopic + ".managed-connectors";
        String value = originalProps.get(key);
        return value != null ? Arrays.asList(value.split(",")) : List.of();
    }
}
```

---

## 6. Data Flow: Per-Task Isolation

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Control Topic: "control-orders" (1 partition)                           │
│                                                                          │
│  Events from workers:                                                    │
│    [DataWritten(groupId=connect-orders-iceberg, table=db.orders, ...)]  │
│    [DataComplete(groupId=connect-orders-iceberg, partitions=4)]          │
│                                                                          │
│  Events from coordinator:                                                │
│    [StartCommit(groupId=connect-orders-iceberg, commitId=abc)]          │
│    [CommitComplete(groupId=connect-orders-iceberg, commitId=abc)]        │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
                                    ↓
                    ┌───────────────────────────────┐
                    │  Coordinator Task 0            │
                    │  (assigned: control-orders)    │
                    │                               │
                    │  Domain: control-orders        │
                    │  ├── ConnectorState:          │
                    │  │   groupId=connect-orders   │
                    │  │   commitState=...          │
                    │  │   catalog=REST             │
                    │  │                            │
                    │  └── CommitExecutor (4 threads)│
                    └───────────────────────────────┘
                                    │
                                    ↓
                    ┌───────────────────────────────┐
                    │  Iceberg Table: db.orders      │
                    │  - AppendFiles / RowDelta      │
                    │  - Snapshot properties:        │
                    │    offsets, commit-id,         │
                    │    valid-through-ts            │
                    └───────────────────────────────┘
```

Each task is **completely independent**. No shared state between tasks. No inter-task communication. Pure isolation.

---

## 7. Control Topic Constraints (Enforced)

### Rule: Exactly 1 Partition Per Control Topic

```bash
# Create control topics — MUST be 1 partition
kafka-topics --create --topic control-orders --partitions 1 --replication-factor 3
kafka-topics --create --topic control-events --partitions 1 --replication-factor 3
kafka-topics --create --topic control-clicks --partitions 1 --replication-factor 3
```

**This is enforced, not recommended.** The connector validates at startup and fails hard if any control topic has more than 1 partition.

**Why this is the correct constraint:**
- The protocol already uses a single producer key → all events route to 1 partition
- Strict ordering within a domain is required (StartCommit → DataWritten → DataComplete → Commit)
- Single partition = trivial offset tracking (only partition 0)
- Enables the deterministic `tasks = sum(partitions)` invariant
- Eliminates all partition assignment complexity
- Parallelism is internal to the task (commit thread pool for table commits), not at the topic level

**What if you have "too many events" for one partition?**
- The control topic carries metadata only (commit IDs, file lists, offsets) — NOT the actual data
- A single partition can handle thousands of events/second, which corresponds to millions of data records/second on the source topics
- If you genuinely exceed this, split into multiple coordination domains (multiple worker connectors, each with its own control topic)

### Validation at Connector Start

```java
// In IcebergCoordinatorConnector.start():
private void validateControlTopics(Admin admin, List<String> controlTopics) {
    Map<String, TopicDescription> descriptions =
        admin.describeTopics(controlTopics).allTopicValues().get();

    List<String> violations = new ArrayList<>();
    for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
        int partitions = entry.getValue().partitions().size();
        if (partitions != 1) {
            violations.add(String.format("%s (%d partitions)", entry.getKey(), partitions));
        }
    }

    if (!violations.isEmpty()) {
        throw new ConfigException(String.format(
            "All control topics must have exactly 1 partition. Violations: %s. "
                + "Recreate these topics with --partitions 1, or use separate "
                + "single-partition topics per coordination domain.",
            String.join(", ", violations)));
    }
}
```

### Task Count Formula

```
tasks = sum of partitions across all configured control topics
      = number of control topics  (since each has exactly 1 partition)
```

The user sets `tasks.max >= number of control topics`. The connector's `taskConfigs()` returns exactly N configs (one per control topic). If `tasks.max < N`, the connector logs an error — some domains will be uncoordinated.

---

## 8. Scaling Model

### Scaling Workers (no coordinator impact)

```
Before: orders-iceberg tasks.max=4
After:  orders-iceberg tasks.max=12

Coordinator: detects new partition count on next refresh (60s)
             next commit correctly expects 12 DataComplete responses
```

### Adding New Ingestion Jobs

```bash
# 1. Deploy new worker connector
curl -X POST http://connect:8083/connectors -d '{
  "name": "analytics-iceberg",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "iceberg.coordinator.enabled": "false",
    "iceberg.control.topic": "control-analytics",
    ...
  }
}'

# 2. Update coordinator to include new control topic
curl -X PUT http://connect:8083/connectors/iceberg-caas/config -d '{
  ...
  "iceberg.coordinator.control-topics": "control-orders,control-events,control-clicks,control-analytics",
  ...
}'
# This triggers connector reconfiguration → new task spawned for control-analytics
```

### Removing Ingestion Jobs

```bash
# 1. Delete worker connector
curl -X DELETE http://connect:8083/connectors/clicks-iceberg

# 2. Update coordinator to remove control topic
curl -X PUT http://connect:8083/connectors/iceberg-caas/config -d '{
  "iceberg.coordinator.control-topics": "control-orders,control-events,control-analytics"
}'
# Task for control-clicks is stopped
```

### Auto-Discovery Mode

With `iceberg.coordinator.auto-discover=true`, the coordinator doesn't need to know worker groupIds upfront. When it sees events on a control topic with a new `groupId`:

1. It creates a new `ConnectorCommitState` for that groupId
2. It queries the admin API for the connector's consumer group
3. It loads the catalog (using default catalog config or per-connector override)
4. It starts coordinating for that connector immediately

---

## 9. Multiple Connectors Sharing One Control Topic

A single control topic CAN serve multiple worker connectors (maintaining backward compatibility with the current shared-topic model):

```
control-iceberg:
  ├── Events with groupId = "connect-orders-iceberg"
  ├── Events with groupId = "connect-events-iceberg"
  └── Events with groupId = "connect-clicks-iceberg"
```

One coordinator task handles this by maintaining separate `ConnectorCommitState` per groupId. This is the **fallback compatibility mode** - it preserves the existing behavior where all connectors share `control-iceberg`.

### Deployment Spectrum

| Model | Control Topics | Tasks (auto-computed) | Isolation |
|-------|---------------|----------------------|-----------|
| **Shared** (backward compat) | 1 (`control-iceberg`, 1 partition) | 1 | Logical (groupId filtering) |
| **Per-job isolation** (recommended) | N (one per job, 1 partition each) | N | Physical (topic-level) |
| **Hybrid** | M < N (group related jobs) | M | Mixed |

In ALL cases: `tasks = sum(partitions across configured control topics)`. Since every topic must have exactly 1 partition, this simplifies to `tasks = number of control topics`.

---

## 10. Worker-Side Changes (Minimal)

### New Config Property

```java
// In IcebergSinkConfig.java:
private static final String COORDINATOR_ENABLED_PROP = "iceberg.coordinator.enabled";
private static final boolean COORDINATOR_ENABLED_DEFAULT = true;

public boolean coordinatorEnabled() {
    return getBoolean(COORDINATOR_ENABLED_PROP);
}
```

### Modified CommitterImpl

```java
// In CommitterImpl.open():
@Override
public void open(...) {
    initialize(icebergCatalog, icebergSinkConfig, sinkTaskContext, addedPartitions);
    if (config.coordinatorEnabled() && hasLeaderPartition(addedPartitions)) {
        startCoordinator();
    }
    // When coordinatorEnabled=false: workers still produce/consume control topic events
    // They just don't initiate StartCommit themselves
}

// In CommitterImpl.close():
@Override
public void close(Collection<TopicPartition> closedPartitions) {
    stopWorker();
    if (!isInitialized.get()) { return; }
    if (closedPartitions.isEmpty() && config.coordinatorEnabled()) {
        stopCoordinator();
        return;
    }
    if (config.coordinatorEnabled() && hasLeaderPartition(closedPartitions)) {
        stopCoordinator();
    }
    KafkaUtils.seekToLastCommittedOffsets(context);
}
```

**That's it.** Workers are otherwise unchanged. They still:
- Write data files on receiving records
- Subscribe to the control topic
- Respond to `StartCommit` with `DataWritten` + `DataComplete`
- Use transactional producers for exactly-once

---

## 11. Exactly-Once Guarantees

### Preserved Mechanisms

| Mechanism | How It Works | Changed? |
|-----------|-------------|----------|
| Offset in snapshot summary | `kafka.connect.offsets.{topic}.{groupId}` stored in each Iceberg snapshot | No |
| SnapshotAncestryValidator | Validates no concurrent writes advanced offsets | No |
| File deduplication | `distinctByKey(ContentFile::location)` | No |
| Envelope offset filtering | Skip events below last committed offset | No |
| Transactional workers | Workers send events + source offsets atomically | No |

### New: Coordinator's Own Offset Management

```java
@Override
public Map<TopicPartition, OffsetAndMetadata> preCommit(
        Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Only advance control topic consumer offset AFTER successful Iceberg commit
    // If coordinator crashes mid-commit → re-reads events → dedup handles retries
    return domain.safeOffsets(assignedControlTopic);
}
```

### Recovery Scenario

```
1. Coordinator starts commit cycle [id=xyz]
2. Workers respond with DataWritten + DataComplete
3. Coordinator commits to Table A successfully
4. Coordinator CRASHES before committing to Table B
5. Control topic offset NOT advanced (preCommit not called)

Recovery:
6. Coordinator task restarts (Kafka Connect auto-restart)
7. Re-reads events from last committed offset
8. Sees DataWritten for commit [id=xyz]
9. Attempts commit to Table A → SnapshotAncestryValidator detects offsets already advanced → SKIP
10. Commits to Table B → SUCCESS (offsets not yet advanced for B)
11. Advances control topic offset
```

---

## 12. Full Deployment Example

### Coordinator Connector Config

```json
{
  "name": "iceberg-caas",
  "config": {
    "connector.class": "org.apache.iceberg.connect.coordinator.IcebergCoordinatorConnector",
    "tasks.max": "100",
    "_comment_tasks_max": "Set high. Actual tasks = sum of control topic partitions (auto-computed). Since each topic must have 1 partition, tasks = number of control topics = 3 in this example.",

    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",

    "iceberg.coordinator.control-topics": "control-orders,control-events,control-clicks",

    "iceberg.coordinator.commit.interval-ms": "300000",
    "iceberg.coordinator.commit.timeout-ms": "30000",
    "iceberg.coordinator.commit.threads": "8",
    "iceberg.coordinator.partition-count-refresh-ms": "60000",
    "iceberg.coordinator.auto-discover": "true",

    "iceberg.coordinator.domain.control-events.commit.interval-ms": "60000",
    "iceberg.coordinator.domain.control-events.commit.threads": "16",

    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg-catalog:8181",
    "iceberg.catalog.warehouse": "s3://warehouse/",

    "iceberg.coordinator.connector.connect-clicks-iceberg.catalog.type": "glue",
    "iceberg.coordinator.connector.connect-clicks-iceberg.catalog.warehouse": "s3://clicks/",

    "iceberg.kafka.bootstrap.servers": "kafka:9092"
  }
}
```

### Worker Connector Configs

```json
{
  "name": "orders-iceberg",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "4",
    "topics": "orders,order-updates",
    "iceberg.coordinator.enabled": "false",
    "iceberg.tables": "db.orders,db.order_items",
    "iceberg.tables.route-field": "table_name",
    "iceberg.control.topic": "control-orders",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg-catalog:8181"
  }
}
```

```json
{
  "name": "events-iceberg",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "8",
    "topics": "app-events",
    "iceberg.coordinator.enabled": "false",
    "iceberg.tables.dynamic-enabled": "true",
    "iceberg.tables.route-field": "event_type",
    "iceberg.control.topic": "control-events",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg-catalog:8181"
  }
}
```

```json
{
  "name": "clicks-iceberg",
  "config": {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "12",
    "topics": "clickstream",
    "iceberg.coordinator.enabled": "false",
    "iceberg.tables": "analytics.clicks",
    "iceberg.control.topic": "control-clicks",
    "iceberg.catalog.type": "glue",
    "iceberg.catalog.warehouse": "s3://clicks/"
  }
}
```

### Topic Setup

```bash
# Control topics — MUST be exactly 1 partition (enforced by coordinator connector)
kafka-topics --create --topic control-orders --partitions 1 --replication-factor 3 \
  --config retention.ms=604800000 --config cleanup.policy=delete

kafka-topics --create --topic control-events --partitions 1 --replication-factor 3 \
  --config retention.ms=604800000 --config cleanup.policy=delete

kafka-topics --create --topic control-clicks --partitions 1 --replication-factor 3 \
  --config retention.ms=604800000 --config cleanup.policy=delete

# Total partitions = 3 → coordinator will create exactly 3 tasks
```

---

## 13. Operational Model

### Monitoring (per-task = per-domain)

```
# JMX / Prometheus metrics per coordinator task:
iceberg.coordinator.{control-topic}.connectors.active
iceberg.coordinator.{control-topic}.commit.in_progress
iceberg.coordinator.{control-topic}.commit.latency.p99
iceberg.coordinator.{control-topic}.commit.tables.count
iceberg.coordinator.{control-topic}.commit.files.total
iceberg.coordinator.{control-topic}.commit.bytes.total
iceberg.coordinator.{control-topic}.commit.failures
iceberg.coordinator.{control-topic}.workers.partition_count
iceberg.coordinator.{control-topic}.control_topic.lag
```

### REST API Status

```bash
# Check coordinator status
$ curl http://connect:8083/connectors/iceberg-caas/status
{
  "name": "iceberg-caas",
  "connector": { "state": "RUNNING" },
  "tasks": [
    { "id": 0, "state": "RUNNING", "worker_id": "worker-1:8083" },
    { "id": 1, "state": "RUNNING", "worker_id": "worker-2:8083" },
    { "id": 2, "state": "RUNNING", "worker_id": "worker-3:8083" }
  ]
}

# Restart just the events coordinator (doesn't affect orders or clicks)
$ curl -X POST http://connect:8083/connectors/iceberg-caas/tasks/1/restart
```

### Adding a New Job (Hot-Add)

```bash
# 1. Create control topic (MUST be 1 partition)
kafka-topics --create --topic control-analytics --partitions 1 --replication-factor 3

# 2. Deploy worker
curl -X POST http://connect:8083/connectors -d @analytics-worker.json

# 3. Update coordinator config (task count auto-recomputes from partition sum)
curl -X PUT http://connect:8083/connectors/iceberg-caas/config -d '{
  ...existing config...,
  "iceberg.coordinator.control-topics": "control-orders,control-events,control-clicks,control-analytics"
}'
# Connector restart: validates topics, sees 4 partitions total, creates 4 tasks
```

### Removing a Job

```bash
# 1. Delete worker connector
curl -X DELETE http://connect:8083/connectors/analytics-iceberg

# 2. Update coordinator (task auto-removed, now 3 partitions = 3 tasks)
curl -X PUT http://connect:8083/connectors/iceberg-caas/config -d '{
  ...without control-analytics...,
  "iceberg.coordinator.control-topics": "control-orders,control-events,control-clicks"
}'

# 3. Optionally delete control topic
kafka-topics --delete --topic control-analytics
```

---

## 14. Failure Modes & Edge Cases

| Scenario | Behavior | Recovery |
|----------|----------|----------|
| Coordinator task dies | Connect auto-restarts within ~30s | Re-reads from last committed offset, dedup handles any retries |
| Worker connector rebalances | Partition count changes | Coordinator refreshes count within 60s; timeout-based commit handles interim |
| Worker dies mid-commit | Fewer DataComplete responses | Commit timeout fires → partial commit with available data |
| Catalog unavailable | Table commit fails | CommitState preserves buffers; retries on next cycle |
| Control topic fills up | Coordinator falls behind | `put()` processes records as fast as possible; lag metric alerts ops |
| Two coordinators (split-brain) | Both send StartCommit | SnapshotAncestryValidator ensures only one Iceberg commit succeeds |
| New worker connector appears | Unknown groupId on control topic | Auto-discover registers it dynamically |
| Worker uses wrong control topic | Events go to wrong domain | groupId filtering ignores; worker sees no StartCommit (stalls until fixed) |

---

## 15. Comparison: Embedded vs. CaaS

| Dimension | Embedded (today) | CaaS (proposed) |
|-----------|-------------------|-----------------|
| Deployment | Automatic (embedded) | Explicit (separate connector) |
| Tasks | N coordinators for N jobs | 1 coordinator connector, N tasks |
| Isolation | None (shares worker JVM) | Physical (per-task = per-domain) |
| Scaling workers | Disrupts coordinator | Zero impact |
| Scaling coordination | Not possible | Add tasks (add control topics) |
| Monitoring | Hidden thread | Full Connect task status |
| Restartability | Triggers worker rebalance | Independent restart per task |
| Multi-catalog | N catalog instances | Shared catalog pool or per-connector |
| Operational overhead | Low (embedded) | Slightly more (separate connector) |
| Blast radius of failure | Affects one connector's workers | Affects only that domain's coordination |

---

## 16. Migration Path

### Phase 1: Ship alongside (backward compatible)

```
Worker config (default): iceberg.coordinator.enabled=true  (existing behavior)
Coordinator connector:   optional, standalone
```

Both can coexist. If a standalone coordinator is running AND the embedded one fires, the SnapshotAncestryValidator ensures exactly one wins.

### Phase 2: Recommend standalone

```
Worker config: iceberg.coordinator.enabled=false
Coordinator:   required
```

New deployments use standalone by default. Existing deployments migrate.

### Phase 3: Deprecate embedded

Remove embedded coordinator code. Workers become pure data writers.

### Zero-Downtime Migration Steps

```bash
# 1. Deploy coordinator connector (starts sending StartCommit)
#    Both embedded and standalone may fire - safe due to idempotency

# 2. Rolling restart workers with coordinator.enabled=false
#    One at a time - each worker stops its embedded coordinator on restart

# 3. Verify only standalone coordinator is active
#    Check metrics: no embedded coordinator logs in workers
```

---

## 17. Commit Flow Analysis: Current vs. CaaS (Full Verification)

### 17.1 Step-by-Step Commit Lifecycle Comparison

Below is every step the current embedded coordinator performs, mapped to its equivalent in the CaaS design.

#### Phase 1: Commit Initiation

| Step | Current (Coordinator.java) | CaaS (CoordinationDomain) | Status |
|------|---------------------------|---------------------------|--------|
| Check interval | `CommitState.isCommitIntervalReached()` in `process()` loop | Same call in `checkCommitIntervals()` via scheduler thread | SAME |
| Generate commitId | `CommitState.startNewCommit()` → random UUID | Same | SAME |
| Send StartCommit | `send(new Event(groupId, new StartCommit(commitId)))` via transactional producer | `sendToControlTopic(event)` | **GAP A** (see below) |
| Consumer group filter | `connectGroupId` set in Event constructor | Same — `state.groupId()` set in Event | SAME |

#### Phase 2: Response Collection

| Step | Current | CaaS | Status |
|------|---------|------|--------|
| Poll control topic | `consumeAvailable(1s)` → KafkaConsumer poll | Kafka Connect framework calls `put()` with records | EQUIVALENT |
| Track control topic offsets | `controlTopicOffsets.put(partition, offset+1)` on each record | **GAP B** — not explicitly tracked in current design doc | **GAP** |
| Filter by groupId | `event.groupId().equals(connectGroupId)` | Routes by groupId to correct `ConnectorCommitState` | SAME |
| Ignore own echoes | `receive()` returns false for START_COMMIT/COMMIT_COMPLETE | Explicit `case START_COMMIT: break;` | SAME |
| Buffer DataWritten | `commitState.addResponse(envelope)` | Same | SAME |
| Buffer DataComplete | `commitState.addReady(envelope)` | Same | SAME |
| Check readiness | `commitState.isCommitReady(totalPartitionCount)` | `commitState.isCommitReady(partitionTracker.getPartitionCount(groupId))` | IMPROVED (dynamic refresh) |
| Timeout check | `commitState.isCommitTimedOut()` in `process()` | Same call in `checkCommitIntervals()` | SAME |

#### Phase 3: Commit Execution (`doCommit`)

| Step | Current | CaaS | Status |
|------|---------|------|--------|
| Get table map | `commitState.tableCommitMap()` | Same | SAME |
| Get validThroughTs | `commitState.validThroughTs(partial)` | Same | SAME |
| Parallel table commits | `Tasks.foreach(commitMap).executeWith(exec).stopOnFailure()` | Same pattern with `commitExecutor` | SAME |
| Commit consumer offsets | `commitConsumerOffsets()` → consumer.commitSync() | Framework commits via `preCommit()` return | EQUIVALENT |
| Clear responses | `commitState.clearResponses()` | Same | SAME |
| Send CommitComplete | `send(new Event(groupId, new CommitComplete(...)))` | `sendToControlTopic(event)` | **GAP A** |
| Log completion | LOG.info with taskId, commitId, table count, validThroughTs | Same | SAME |

#### Phase 4: Per-Table Commit (`commitToTable`)

| Step | Current | CaaS | Status |
|------|---------|------|--------|
| Load table | `catalog.loadTable(tableIdentifier)` | Same via `state.catalog()` | SAME |
| Handle NoSuchTableException | Log warn, skip | Same | SAME |
| Validate table UUID | `tableReference.uuid() != null && !equals(table.uuid())` → skip | Same | SAME |
| Get branch | `config.tableConfig(tableIdentifier).commitBranch()` | Same via config | SAME |
| Get lastCommittedOffsets | `lastCommittedOffsetsForTable(table, branch)` → walk snapshot ancestry | Same logic | SAME |
| Merge offsets | `Stream.of(committedOffsets, controlTopicOffsets)...collect(toMap(..., Long::max))` | Same — **requires controlTopicOffsets** | **GAP B** |
| Convert to JSON | `offsetsToJson(mergedOffsets)` | Same | SAME |
| Filter envelopes by offset | `envelope.offset() >= minOffset` per partition | Same | SAME |
| Extract DataWritten payloads | Stream + cast | Same | SAME |
| Collect dataFiles | Filter recordCount>0, deduplicate by location | Same | SAME |
| Collect deleteFiles | Filter recordCount>0, deduplicate by location | Same | SAME |
| Check terminated | `if (terminated) throw` | **GAP C** — needs closed flag | **GAP** |
| Skip if empty | Log, return | Same | SAME |
| Build AppendFiles (no deletes) | `table.newAppend().validateWith(offsetValidator(...))` | Same | SAME |
| Build RowDelta (with deletes) | `table.newRowDelta().validateWith(offsetValidator(...))` | Same | SAME |
| Set branch | `appendOp.toBranch(branch)` | Same | SAME |
| Set snapshotOffsetsProp | `appendOp.set(snapshotOffsetsProp, offsetsJson)` | Same via `state.snapshotOffsetsProp()` | SAME |
| Set commit-id prop | `appendOp.set(COMMIT_ID_SNAPSHOT_PROP, commitId.toString())` | Same | SAME |
| Set task-id prop | `appendOp.set(TASK_ID_SNAPSHOT_PROP, taskId)` | Same | SAME |
| Set valid-through-ts | `appendOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString())` | Same | SAME |
| Add files | `dataFiles.forEach(appendOp::appendFile)` | Same | SAME |
| Commit | `appendOp.commit()` | Same | SAME |
| Get snapshotId | `latestSnapshot(table, branch).snapshotId()` | Same | SAME |
| Send CommitToTable | `send(new Event(groupId, new CommitToTable(...)))` | Same | SAME |
| Validate concurrency | `SnapshotAncestryValidator` checks offsets match expected | Same | SAME |

#### Phase 5: Offset Management

| Step | Current | CaaS | Status |
|------|---------|------|--------|
| `snapshotOffsetsProp` format | `kafka.connect.offsets.{controlTopic}.{connectGroupId}` | Same — constructed in `ConnectorCommitState` | **GAP D** (migration) |
| Last committed offsets lookup | Walk snapshot ancestry, find prop, parse JSON | Same | SAME |
| Offset validator error message | Includes table identifier + expected vs actual | Same | SAME |

---

### 17.2 Identified Gaps

#### GAP A: Transactional Producer for Coordinator Events

**Current behavior:**
The coordinator sends events through `Channel.send()` which wraps them in a Kafka transaction:
```java
producer.beginTransaction();
recordList.forEach(producer::send);
// sourceOffsets is empty for coordinator, so sendOffsetsToTransaction is skipped
producer.commitTransaction();
```

**CaaS design doc shows:**
```java
producer.send(record).get(); // sync send
```

**Impact:** Without transactions, a coordinator crash between `send()` and acknowledgment could result in duplicate events on the control topic. While downstream deduplication (`distinctByKey(ContentFile::location)`) makes this safe for data integrity, duplicate `StartCommit` events could trigger workers to flush prematurely.

**Fix:** Use a transactional producer in `CoordinationDomain`:
```java
private void sendToControlTopic(Event event) {
    byte[] data = AvroUtil.encode(event);
    ProducerRecord<String, byte[]> record =
        new ProducerRecord<>(controlTopic, producerId, data);
    synchronized (producer) {
        producer.beginTransaction();
        try {
            producer.send(record);
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            throw e;
        }
    }
}
```

Since the producer is shared across domains (failover scenario), the `synchronized` block prevents interleaving. Each domain's sends are keyed by different `producerId`s, so ordering within a domain is preserved.

---

#### GAP B: `controlTopicOffsets` Tracking

**Current behavior:**
`Channel.consumeAvailable()` maintains `controlTopicOffsets` (Map<Integer, Long>) — updated on every record consumed, representing "the next offset to consume" per control topic partition.

This map is used in TWO critical places:
1. **Merge step in `commitToTable()`**: `Stream.of(committedOffsets, controlTopicOffsets)...Long::max`
2. **`commitConsumerOffsets()`**: commits these offsets to the coordinator's consumer group

**CaaS design has a placeholder:** `lastCommittedOffsets.putAll(/* current controlTopicOffsets */)`

**Fix:** The domain needs TWO offset maps:

```java
class CoordinationDomain {
    // Updated on every handleEvent() call — "where we currently are"
    private final Map<Integer, Long> currentControlTopicOffsets = new ConcurrentHashMap<>();
    // Updated ONLY after successful Iceberg commit — "safe to resume from here"
    private final Map<Integer, Long> lastCommittedControlTopicOffsets = new ConcurrentHashMap<>();

    void handleEvent(Event event, int partition, long offset) {
        // Track current position (equivalent to Channel.consumeAvailable)
        currentControlTopicOffsets.put(partition, offset + 1);
        // ... rest of routing logic
    }

    private void doCommit(ConnectorCommitState state, boolean partial) {
        Map<Integer, Long> controlTopicOffsets = new HashMap<>(currentControlTopicOffsets);
        // ... commit to tables using controlTopicOffsets for the merge step ...

        // Only after ALL table commits succeed:
        lastCommittedControlTopicOffsets.putAll(controlTopicOffsets);
        state.commitState().clearResponses();
        // ... send CommitComplete ...
    }

    Map<TopicPartition, OffsetAndMetadata> safeOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        lastCommittedControlTopicOffsets.forEach((partition, offset) ->
            offsets.put(new TopicPartition(controlTopic, partition),
                        new OffsetAndMetadata(offset)));
        return offsets;
    }
}
```

The distinction matters:
- If commit fails: `currentControlTopicOffsets` is ahead of `lastCommittedControlTopicOffsets`
- `preCommit()` returns `lastCommittedControlTopicOffsets` → framework doesn't advance past the failed point
- On restart: events are re-read from `lastCommittedControlTopicOffsets`, retry succeeds

---

#### GAP C: `terminated` / Closed Flag for Clean Abort

**Current behavior:**
```java
// In commitToTable(), after collecting files, before table operations:
if (terminated) {
    throw new ConnectException("Coordinator is terminated, commit aborted");
}
```

**CaaS fix:** Add a `volatile boolean closed` flag to `CoordinationDomain`:
```java
class CoordinationDomain {
    private volatile boolean closed;

    void close() {
        this.closed = true;
        commitExecutor.shutdownNow();
        // ... cleanup
    }

    private void commitToTable(...) {
        // ... collect files ...
        if (closed) {
            throw new ConnectException("Coordination domain closed, aborting commit");
        }
        // ... perform table operations ...
    }
}
```

---

#### GAP D: `snapshotOffsetsProp` Migration Compatibility

**Current:** The property stored in Iceberg snapshots is:
```
kafka.connect.offsets.control-iceberg.connect-orders-iceberg
```

**CaaS (if control topic renamed):** Would be:
```
kafka.connect.offsets.control-orders.connect-orders-iceberg
```

**Impact:** If migrating from a shared `control-iceberg` topic to per-domain `control-orders`, the snapshot property key changes. The coordinator won't find previous offsets → re-processes everything. Data won't be corrupted (dedup + SnapshotAncestryValidator), but there will be a burst of duplicate file references to filter.

**Fix options:**
1. **Keep the same control topic name** during migration (use `control-iceberg` in the CaaS too) — avoids the issue entirely
2. **Allow configuring the snapshot offset property format** — override to match the legacy key
3. **Fallback lookup** — if primary key not found, try legacy key format:
```java
private Map<Integer, Long> lastCommittedOffsetsForTable(Table table, String branch) {
    Snapshot snapshot = latestSnapshot(table, branch);
    if (snapshot == null) return Map.of();

    Iterable<Snapshot> ancestry = SnapshotUtil.ancestorsOf(snapshot.snapshotId(), table::snapshot);

    // Try current property name first
    Map<Integer, Long> offsets = findOffsets(ancestry, snapshotOffsetsProp);
    if (offsets.isEmpty() && legacySnapshotOffsetsProp != null) {
        // Fallback to legacy property for migration
        offsets = findOffsets(ancestry, legacySnapshotOffsetsProp);
    }
    return offsets;
}
```

---

#### GAP E: Thread Safety of CommitState

**Current behavior:** The embedded coordinator runs in a **single thread** (`CoordinatorThread`). All access to `CommitState` is from that one thread — no synchronization needed.

**CaaS behavior:** `CommitState` is accessed from **two threads**:
1. **Kafka Connect `put()` thread** — calls `addResponse()`, `addReady()`, `isCommitReady()`, potentially `executeCommit()`
2. **Scheduler thread** — calls `isCommitIntervalReached()`, `startNewCommit()`, `isCommitTimedOut()`, `executeCommit()`

`CommitState` uses plain `ArrayList` internally — NOT thread-safe.

**Fix:** Synchronize on the `ConnectorCommitState` object:

```java
void handleEvent(Event event, int partition, long offset) {
    currentControlTopicOffsets.put(partition, offset + 1);
    String groupId = event.groupId();
    ConnectorCommitState state = connectorStates.computeIfAbsent(groupId, this::registerConnector);

    synchronized (state) {
        switch (event.payload().type()) {
            case DATA_WRITTEN:
                state.commitState().addResponse(new Envelope(event, partition, offset));
                break;
            case DATA_COMPLETE:
                state.commitState().addReady(new Envelope(event, partition, offset));
                int partitionCount = partitionTracker.getPartitionCount(groupId);
                if (state.commitState().isCommitReady(partitionCount)) {
                    executeCommit(state, false);
                }
                break;
        }
    }
}

void checkCommitIntervals() {
    for (ConnectorCommitState state : connectorStates.values()) {
        synchronized (state) {
            if (state.commitState().isCommitIntervalReached()) {
                startCommit(state);
            }
            if (state.commitState().isCommitTimedOut()) {
                executeCommit(state, true);
            }
        }
    }
}
```

The `synchronized(state)` block is per-connector, so commits for different connectors can proceed concurrently. Only same-connector operations serialize — which matches the current single-threaded behavior exactly.

---

### 17.3 Functionality Checklist

| Feature | Current | CaaS | Preserved? |
|---------|---------|------|------------|
| Commit interval triggering | CoordinatorThread loop | Scheduler thread | YES |
| Commit timeout (partial commit) | Same loop | Same scheduler | YES |
| StartCommit event production | Transactional send | Transactional send (after fix A) | YES |
| DataWritten buffering | CommitState.commitBuffer | Same CommitState | YES |
| DataComplete readiness tracking | CommitState.readyBuffer | Same CommitState | YES |
| Partition count for readiness | Computed once at startup | Periodically refreshed | IMPROVED |
| GroupId-based event filtering | In consumeAvailable() | In handleEvent() routing | YES |
| Parallel table commits | ThreadPoolExecutor + Tasks | Same | YES |
| Table UUID validation | In commitToTable() | Same | YES |
| Branch-aware commits | config.tableConfig().commitBranch() | Same | YES |
| Snapshot offset merge | max(committedOffsets, controlTopicOffsets) | Same (after fix B) | YES |
| Envelope offset filtering | Skip below last committed | Same | YES |
| Data file deduplication | distinctByKey(location) | Same | YES |
| Delete file deduplication | distinctByKey(location) | Same | YES |
| recordCount > 0 filter | Stream filter | Same | YES |
| AppendFiles operation | table.newAppend() | Same | YES |
| RowDelta operation | table.newRowDelta() | Same | YES |
| SnapshotAncestryValidator | Validates expected offsets | Same | YES |
| Snapshot property: offsets | kafka.connect.offsets.{topic}.{group} | Same | YES |
| Snapshot property: commit-id | kafka.connect.commit-id | Same | YES |
| Snapshot property: task-id | kafka.connect.task-id | Same | YES |
| Snapshot property: valid-through-ts | kafka.connect.valid-through-ts | Same | YES |
| validThroughTs computation | Min timestamp across all assignments | Same CommitState logic | YES |
| CommitToTable event | Sent after each table commit | Same | YES |
| CommitComplete event | Sent after all tables + offset commit | Same | YES |
| Consumer offset management | consumer.commitSync() | preCommit() return (after fix B) | EQUIVALENT |
| Clean shutdown | terminated flag + exec.shutdownNow() | closed flag + shutdownNow() (after fix C) | YES |
| Recovery from partial failure | Re-read events, SnapshotAncestryValidator prevents dupes | Same + preCommit safety | YES |
| Multi-table support | tableCommitMap() groups by TableReference | Same | YES |
| Dynamic table routing | Workers handle (unchanged) | Same | YES |
| Static table routing | Workers handle (unchanged) | Same | YES |
| Schema evolution | Workers handle (unchanged) | Same | YES |
| Auto-create tables | Workers handle (unchanged) | Same | YES |

---

### 17.4 What's Improved Over Current

| Area | Current | CaaS Improvement |
|------|---------|------------------|
| Partition count tracking | Computed ONCE at coordinator startup; stales on rebalance | Periodically refreshed (default 60s) |
| Failure recovery | Leader must be re-elected after rebalance; commit gap during election | Task restarts independently; no election needed |
| Resource isolation | Shares JVM with write worker; heavy writes starve polling | Dedicated task for coordination |
| Observability | Internal thread — no status API, no metrics | Full Connect REST status + per-domain metrics |
| Multi-connector | N embedded coordinators (one per sink connector) | Single connector manages all |
| Commit interval tuning | Same interval for all connectors | Per-domain and per-connector overrides |
| Graceful shutdown | Must wait for CoordinatorThread + executor termination | Kafka Connect lifecycle management |

---

### 17.5 Corrected CoordinationDomain (Complete, All Gaps Resolved)

```java
class CoordinationDomain {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinationDomain.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String COMMIT_ID_SNAPSHOT_PROP = "kafka.connect.commit-id";
    private static final String TASK_ID_SNAPSHOT_PROP = "kafka.connect.task-id";
    private static final String VALID_THROUGH_TS_SNAPSHOT_PROP = "kafka.connect.valid-through-ts";

    private final String controlTopic;
    private final IcebergCoordinatorConfig config;
    private final Producer<String, byte[]> producer;
    private final Admin admin;
    private final ExecutorService commitExecutor;
    private final PartitionCountTracker partitionTracker;
    private final String producerId;
    private final String taskId;

    private final Map<String, ConnectorCommitState> connectorStates = new ConcurrentHashMap<>();
    private final Map<Integer, Long> currentControlTopicOffsets = new ConcurrentHashMap<>();
    private final Map<Integer, Long> lastCommittedControlTopicOffsets = new ConcurrentHashMap<>();
    private volatile boolean closed;

    CoordinationDomain(
            String controlTopic,
            IcebergCoordinatorConfig config,
            Admin admin,
            Producer<String, byte[]> producer) {
        this.controlTopic = controlTopic;
        this.config = config;
        this.admin = admin;
        this.producer = producer;
        this.producerId = UUID.randomUUID().toString();
        this.taskId = "coordinator-" + controlTopic;
        this.commitExecutor = new ThreadPoolExecutor(
            config.commitThreads(controlTopic),
            config.commitThreads(controlTopic),
            120, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-commit-" + controlTopic + "-%d")
                .build());
        this.partitionTracker = new PartitionCountTracker(admin, config.partitionCountRefreshMs());

        for (String groupId : config.managedGroupIds(controlTopic)) {
            connectorStates.put(groupId, createConnectorState(groupId));
        }
    }

    // ─── Event Handling (called from put() thread) ───

    void handleEvent(Event event, int partition, long offset) {
        // Track current control topic position (equivalent to Channel.consumeAvailable)
        currentControlTopicOffsets.put(partition, offset + 1);

        String groupId = event.groupId();
        ConnectorCommitState state = connectorStates.computeIfAbsent(
            groupId, this::createConnectorState);

        synchronized (state) {
            switch (event.payload().type()) {
                case DATA_WRITTEN:
                    state.commitState().addResponse(new Envelope(event, partition, offset));
                    break;

                case DATA_COMPLETE:
                    state.commitState().addReady(new Envelope(event, partition, offset));
                    int partitionCount = partitionTracker.getPartitionCount(groupId);
                    if (state.commitState().isCommitReady(partitionCount)) {
                        executeCommit(state, false);
                    }
                    break;

                case START_COMMIT:
                case COMMIT_TO_TABLE:
                case COMMIT_COMPLETE:
                    // Own echoes or informational — ignore
                    break;
            }
        }
    }

    // ─── Periodic Checks (called from scheduler thread) ───

    void checkCommitIntervals() {
        for (ConnectorCommitState state : connectorStates.values()) {
            synchronized (state) {
                try {
                    if (state.commitState().isCommitIntervalReached()) {
                        startCommit(state);
                    }
                    if (state.commitState().isCommitTimedOut()) {
                        executeCommit(state, true);
                    }
                } catch (Exception e) {
                    LOG.error("Error in commit check for {} on domain [{}]",
                        state.groupId(), controlTopic, e);
                }
            }
        }
    }

    // ─── Commit Lifecycle ───

    private void startCommit(ConnectorCommitState state) {
        state.commitState().startNewCommit();
        Event event = new Event(
            state.groupId(),
            new StartCommit(state.commitState().currentCommitId()));
        sendToControlTopic(event);
        LOG.info("Domain [{}] initiated commit {} for connector {}",
            controlTopic, state.commitState().currentCommitId(), state.groupId());
    }

    private void executeCommit(ConnectorCommitState state, boolean partial) {
        try {
            doCommit(state, partial);
        } catch (Exception e) {
            LOG.warn("Domain [{}] commit {} failed for {}, will retry next cycle",
                controlTopic, state.commitState().currentCommitId(), state.groupId(), e);
        } finally {
            state.commitState().endCurrentCommit();
        }
    }

    private void doCommit(ConnectorCommitState state, boolean partial) {
        Map<TableReference, List<Envelope>> commitMap = state.commitState().tableCommitMap();
        OffsetDateTime validThroughTs = state.commitState().validThroughTs(partial);

        // Snapshot current control topic offsets for the merge step
        Map<Integer, Long> controlTopicOffsetsSnapshot =
            new HashMap<>(currentControlTopicOffsets);

        // Commit to all tables in parallel (stopOnFailure = atomic)
        Tasks.foreach(commitMap.entrySet())
            .executeWith(commitExecutor)
            .stopOnFailure()
            .run(entry -> commitToTable(
                state, entry.getKey(), entry.getValue(),
                controlTopicOffsetsSnapshot, validThroughTs));

        // ALL tables committed successfully — advance safe offsets
        lastCommittedControlTopicOffsets.putAll(controlTopicOffsetsSnapshot);
        state.commitState().clearResponses();

        // Emit CommitComplete
        Event event = new Event(
            state.groupId(),
            new CommitComplete(state.commitState().currentCommitId(), validThroughTs));
        sendToControlTopic(event);

        LOG.info("Domain [{}] completed commit {} for {}, {} table(s), valid-through {}",
            controlTopic, state.commitState().currentCommitId(),
            state.groupId(), commitMap.size(), validThroughTs);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void commitToTable(
            ConnectorCommitState state,
            TableReference tableReference,
            List<Envelope> envelopeList,
            Map<Integer, Long> controlTopicOffsets,
            OffsetDateTime validThroughTs) {

        TableIdentifier tableIdentifier = tableReference.identifier();
        Table table;
        try {
            table = state.catalog().loadTable(tableIdentifier);
        } catch (NoSuchTableException e) {
            LOG.warn("Table not found, skipping commit: {}", tableIdentifier, e);
            return;
        }

        if (tableReference.uuid() != null && !tableReference.uuid().equals(table.uuid())) {
            LOG.warn("Skipping commit to {} — UUID mismatch. Expected: {} Got: {}",
                tableIdentifier, table.uuid(), tableReference.uuid());
            return;
        }

        String branch = config.tableCommitBranch(state.groupId(), tableIdentifier.toString());

        // Merge: max(lastCommittedOffsetsInSnapshot, currentControlTopicOffsets)
        Map<Integer, Long> committedOffsets = lastCommittedOffsetsForTable(
            state, table, branch);
        Map<Integer, Long> mergedOffsets =
            Stream.of(committedOffsets, controlTopicOffsets)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::max));
        String offsetsJson = offsetsToJson(mergedOffsets);

        // Filter envelopes by committed offsets (skip already-committed events)
        List<DataWritten> payloads = envelopeList.stream()
            .filter(envelope -> {
                Long minOffset = committedOffsets.get(envelope.partition());
                return minOffset == null || envelope.offset() >= minOffset;
            })
            .map(envelope -> (DataWritten) envelope.event().payload())
            .collect(Collectors.toList());

        // Collect and deduplicate data files
        List<DataFile> dataFiles = payloads.stream()
            .filter(payload -> payload.dataFiles() != null)
            .flatMap(payload -> payload.dataFiles().stream())
            .filter(dataFile -> dataFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

        // Collect and deduplicate delete files
        List<DeleteFile> deleteFiles = payloads.stream()
            .filter(payload -> payload.deleteFiles() != null)
            .flatMap(payload -> payload.deleteFiles().stream())
            .filter(deleteFile -> deleteFile.recordCount() > 0)
            .filter(distinctByKey(ContentFile::location))
            .collect(Collectors.toList());

        // Abort if domain is shutting down
        if (closed) {
            throw new ConnectException(
                String.format("Domain [%s] is closed, aborting commit", controlTopic));
        }

        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            LOG.info("Domain [{}] nothing to commit to {}, skipping",
                controlTopic, tableIdentifier);
        } else if (deleteFiles.isEmpty()) {
            AppendFiles appendOp = table.newAppend()
                .validateWith(offsetValidator(tableIdentifier, committedOffsets));
            if (branch != null) { appendOp.toBranch(branch); }
            appendOp.set(state.snapshotOffsetsProp(), offsetsJson);
            appendOp.set(COMMIT_ID_SNAPSHOT_PROP, state.commitState().currentCommitId().toString());
            appendOp.set(TASK_ID_SNAPSHOT_PROP, taskId);
            if (validThroughTs != null) {
                appendOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
            }
            dataFiles.forEach(appendOp::appendFile);
            appendOp.commit();
        } else {
            RowDelta deltaOp = table.newRowDelta()
                .validateWith(offsetValidator(tableIdentifier, committedOffsets));
            if (branch != null) { deltaOp.toBranch(branch); }
            deltaOp.set(state.snapshotOffsetsProp(), offsetsJson);
            deltaOp.set(COMMIT_ID_SNAPSHOT_PROP, state.commitState().currentCommitId().toString());
            deltaOp.set(TASK_ID_SNAPSHOT_PROP, taskId);
            if (validThroughTs != null) {
                deltaOp.set(VALID_THROUGH_TS_SNAPSHOT_PROP, validThroughTs.toString());
            }
            dataFiles.forEach(deltaOp::addRows);
            deleteFiles.forEach(deltaOp::addDeletes);
            deltaOp.commit();
        }

        if (!dataFiles.isEmpty() || !deleteFiles.isEmpty()) {
            Long snapshotId = latestSnapshot(table, branch).snapshotId();
            Event event = new Event(
                state.groupId(),
                new CommitToTable(
                    state.commitState().currentCommitId(), tableReference,
                    snapshotId, validThroughTs));
            sendToControlTopic(event);

            LOG.info("Domain [{}] committed to {}, snapshot {}, commit {}",
                controlTopic, tableIdentifier, snapshotId,
                state.commitState().currentCommitId());
        }
    }

    // ─── Helpers (identical to current Coordinator.java) ───

    private SnapshotAncestryValidator offsetValidator(
            TableIdentifier tableIdentifier, Map<Integer, Long> expectedOffsets) {
        return new SnapshotAncestryValidator() {
            private Map<Integer, Long> lastCommittedOffsets;

            @Override
            public boolean validate(Iterable<Snapshot> baseSnapshots) {
                lastCommittedOffsets = lastCommittedOffsets(baseSnapshots);
                return expectedOffsets.equals(lastCommittedOffsets);
            }

            @Override
            public String errorMessage() {
                return String.format(
                    "Cannot commit to %s, stale offsets: Expected: %s Committed: %s",
                    tableIdentifier, expectedOffsets, lastCommittedOffsets);
            }
        };
    }

    private Map<Integer, Long> lastCommittedOffsetsForTable(
            ConnectorCommitState state, Table table, String branch) {
        Snapshot snapshot = latestSnapshot(table, branch);
        if (snapshot == null) return Map.of();
        Iterable<Snapshot> ancestry =
            SnapshotUtil.ancestorsOf(snapshot.snapshotId(), table::snapshot);
        return lastCommittedOffsets(state.snapshotOffsetsProp(), ancestry);
    }

    private Map<Integer, Long> lastCommittedOffsets(
            String propKey, Iterable<Snapshot> snapshots) {
        return Streams.stream(snapshots)
            .filter(Objects::nonNull)
            .filter(s -> s.summary().containsKey(propKey))
            .map(s -> s.summary().get(propKey))
            .map(this::parseOffsets)
            .findFirst()
            .orElseGet(Map::of);
    }

    private Map<Integer, Long> parseOffsets(String value) {
        if (value == null) return Map.of();
        try {
            return MAPPER.readValue(value, new TypeReference<Map<Integer, Long>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String offsetsToJson(Map<Integer, Long> offsets) {
        try {
            return MAPPER.writeValueAsString(offsets);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Snapshot latestSnapshot(Table table, String branch) {
        return branch == null ? table.currentSnapshot() : table.snapshot(branch);
    }

    private <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Map<Object, Boolean> seen = Maps.newConcurrentMap();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }

    private void sendToControlTopic(Event event) {
        byte[] data = AvroUtil.encode(event);
        ProducerRecord<String, byte[]> record =
            new ProducerRecord<>(controlTopic, producerId, data);
        synchronized (producer) {
            producer.beginTransaction();
            try {
                producer.send(record);
                producer.commitTransaction();
            } catch (Exception e) {
                try { producer.abortTransaction(); }
                catch (Exception ex) { LOG.warn("Error aborting transaction", ex); }
                throw e;
            }
        }
    }

    // ─── Offset Management ───

    Map<TopicPartition, OffsetAndMetadata> safeOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        lastCommittedControlTopicOffsets.forEach((partition, offset) ->
            offsets.put(new TopicPartition(controlTopic, partition),
                        new OffsetAndMetadata(offset)));
        return offsets;
    }

    // ─── Lifecycle ───

    private ConnectorCommitState createConnectorState(String groupId) {
        LOG.info("Domain [{}] registering connector: {}", controlTopic, groupId);
        Catalog catalog = CatalogUtils.loadCatalog(config.catalogProps(groupId));
        CommitState commitState = new CommitState(config.commitConfig(groupId));
        String snapshotProp = String.format(
            "kafka.connect.offsets.%s.%s", controlTopic, groupId);
        return new ConnectorCommitState(groupId, catalog, commitState, snapshotProp);
    }

    void close() {
        this.closed = true;
        commitExecutor.shutdownNow();
        try {
            if (!commitExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                LOG.warn("Domain [{}] commit executor did not terminate in time", controlTopic);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        connectorStates.values().forEach(ConnectorCommitState::close);
    }
}
```

---

### 17.6 Verdict

**All current coordinator functionality is preserved.** The five gaps identified are implementation details in the design doc, not architectural gaps. With the fixes above:

- **GAP A** (transactional sends): Fixed — use transaction wrapper, matches current behavior exactly
- **GAP B** (offset tracking): Fixed — dual-map pattern (`current` vs `lastCommitted`), matches Channel semantics
- **GAP C** (closed flag): Fixed — volatile boolean check before table operations
- **GAP D** (migration): Addressed — fallback lookup or same-name topic strategy
- **GAP E** (thread safety): Fixed — `synchronized(state)` per connector, matches single-thread semantics

The corrected `CoordinationDomain` in Section 17.5 above is a **complete, line-by-line equivalent** of the current `Coordinator.java` + `Channel.java` functionality, adapted for the Kafka Connect Sink framework.

---

## 18. Summary

The **Coordinator-as-a-Service** design gives you:

- **1 connector** that runs all coordination for your entire cluster
- **1 task per control topic** providing physical isolation between ingestion jobs
- **Hot-add/remove** of ingestion jobs by updating the coordinator's topic list
- **Independent scaling** of workers without disrupting coordination
- **Per-domain tuning** of commit intervals, thread pools, and catalogs
- **Full observability** via Kafka Connect REST API and per-task metrics
- **Zero protocol changes** to the existing event format
- **Minimal worker changes** (single boolean flag)
- **Backward-compatible migration** with idempotent commit semantics

The control topic becomes the contract boundary: workers write to their control topic, the CaaS reads from it. Everything else is implementation detail on either side.
