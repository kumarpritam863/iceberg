# Coordinator redesign: level-triggered leadership, control-topic liveness, CAS-enforced safety

**Status:** proposed design, ready to drive implementation.
**Target commit:** `f70ddecf52` (branch `bugfix/split_brain_fix`). All Iceberg citations below were read with `git show f70ddecf52:<path>` and are line-accurate for **that commit**. **The working tree is NOT on that commit** — it is on `parallel_control_processing` (`ca1fe06257`, a merge of `main`), which is not a descendant of `f70ddecf52`. Pin the target branch before implementing; see §12 for what changes if the target is `parallel_control_processing`.

**Citation legend** (short forms used throughout):

| Short form | Absolute path prefix |
|---|---|
| `CommitterImpl.java`, `Coordinator.java`, `Channel.java`, `CommitState.java`, `CoordinatorThread.java`, `Worker.java`, `KafkaUtils.java`, `KafkaClientFactory.java`, `NotRunningException.java`, `Envelope.java` | `/Users/pritamkumar863/Desktop/apdp/work/personal/open_source_forks/iceberg/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/channel/` |
| `IcebergSinkTask.java`, `IcebergSinkConfig.java`, `IcebergSinkConnector.java`, `Committer.java`, `CommitterFactory.java` | `/Users/pritamkumar863/Desktop/apdp/work/personal/open_source_forks/iceberg/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/` |
| `SinkWriter.java`, `IcebergWriter.java` | `.../kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/` |
| `SnapshotProducer.java`, `SnapshotUtil.java`, `Tasks.java`, `TableProperties.java`, `BaseTaskWriter.java`, `ErrorHandlers.java` | `/Users/pritamkumar863/Desktop/apdp/work/personal/open_source_forks/iceberg/core/src/main/java/org/apache/iceberg/` (+ `util/`, `io/`, `rest/`) |
| `SnapshotAncestryValidator.java` | `/Users/pritamkumar863/Desktop/apdp/work/personal/open_source_forks/iceberg/api/src/main/java/org/apache/iceberg/` |
| `DynamicCommitter.java` | `/Users/pritamkumar863/Desktop/apdp/work/personal/open_source_forks/iceberg/flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/` |
| Kafka `WorkerSinkTask.java`, `WorkerTask.java`, `WorkerConfig.java`, `Worker.java (Connect)`, `WorkerSinkTaskContext.java`, `DistributedConfig.java`, `DistributedHerder.java` | `/Users/pritamkumar863/desktop/apdp/work/official/my_aci_fork_kafka/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/` (+ `distributed/`) |
| Kafka `ConsumerConfig.java`, `Consumer.java`, `ConsumerCoordinator.java`, `ClassicKafkaConsumer.java`, `AbstractCoordinator.java`, `ConsumerRebalanceListenerInvoker.java`, `TransactionManager.java`, `ProducerConfig.java`, `HeartbeatRequestManager.java` | `/Users/pritamkumar863/desktop/apdp/work/official/my_aci_fork_kafka/kafka/clients/src/main/java/org/apache/kafka/clients/...` |
| Kafka `SinkTaskContext.java` | `.../kafka/connect/api/src/main/java/org/apache/kafka/connect/sink/` |
| Kafka fork version | `.../kafka/gradle.properties:25` — 3.9.2 |

---

## 1. Executive summary (act on this)

1. **The bug is a type error, not a race.** Coordinator leadership is a *level* ("am I the coordinator now?"); `SinkTask.open()/close()` are *edges* that fire on every group generation change. Under the EAGER protocol — which Connect resolves to by default (`Worker.java (Connect):882-935` sets no `partition.assignment.strategy`; `ConsumerConfig.java:419-424` defaults to `[RangeAssignor, CooperativeStickyAssignor]`; `ConsumerCoordinator.java:209-233` intersects to `[EAGER]`) — **every** rebalance delivers `close(FULL)` then `open(FULL)`, so the coordinator is destroyed and rebuilt once per generation for reasons that have nothing to do with the leader.
2. **The user-visible outage is the commit clock.** A rebuilt `CommitState` has `startTime == 0` and `isCommitIntervalReached()` seeds it to *now* on first call (`CommitState.java:81-88`), so a connector whose sink group rebalances more often than `iceberg.control.commit.interval-ms` (default 300 000, `IcebergSinkConfig.java:85`) **never commits, silently**: no task fails, and Connect reports 100 % offset-commit success because `preCommit` returns an empty map (`IcebergSinkTask.java:100-105`; framework short-circuit `WorkerSinkTask.java:467-471`).
3. **Fix #1 — leadership becomes a level read on the task thread.** `open()/close()` become O(1), non-throwing bookkeeping. `save()` computes `isPrimary = context.assignment().contains(leaderKey)` (`WorkerSinkTaskContext.java:102-106` returns `consumer.assignment()`) and reconciles start/stop. A rebalance that leaves TP0 with this task becomes a **literal no-op**: no teardown, no election, no clock reset, no epoch steal, no blocking inside the callback.
4. **Fix #2 — safety comes from the Iceberg CAS, never from leadership.** A lost offsets CAS becomes a benign `SUPERSEDED` outcome (Flink's `DynamicCommitter` mechanic, `DynamicCommitter.java:335-407`) instead of `ValidationException → coordinator death → FAILED task Connect never restarts`. Overlapping coordinators degrade to wasted work.
5. **Fix #3 — absence becomes bounded by a control-topic watchdog.** Every worker already consumes the control topic. Task 0 promotes itself if it sees no *foreign* `StartCommit` for K intervals; a primary whose own coordinator stops progressing for K intervals restarts it. **Zero new topics, groups, threads, clients or dependencies.**
6. **Fix #4 — delete the coordinator's transaction.** The coordinator's producer sends only `StartCommit`/`CommitToTable`/`CommitComplete`, never source offsets (`Channel.java:98-100`), so its transaction buys nothing for the table (catalog commits precede it: `Coordinator.java:340-366` before `375`) and its `initTransactions()` is an *unconditional epoch bump*, not a CAS (`TransactionManager.java:280-301` → `KafkaApis.scala:2349-2353` → `TransactionMetadata.scala:281-286`). Making it a plain idempotent producer deletes root cause #4 outright.
7. **Fix #5 — the control-topic consumers stop being group members** (`assign()` instead of `subscribe()`). This deletes the rebalance listener, the seek-forward hack, a consumer group per task per rebalance, the "slow catalog evicts the `-coord` member" failure, and — critically — the **split-view** hazard where two coordinators each see only some control partitions.
8. **Fix #6 — the only unrecoverable data-loss path is closed.** `commitToTable` returns *normally* on `NoSuchTableException` and on a table-UUID mismatch (`Coordinator.java:275-289`), after which `doCommit` advances the `-coord` watermark and clears the buffer (`242-243`). For a REST catalog any unrecognised 404 becomes `NoSuchTableException` (`ErrorHandlers.java:141-155`). New rule: **the watermark advances and the buffer clears only for envelopes actually appended or provably below that table's floor.**
9. **Fix #7 — readiness becomes set coverage** over `(topic,partition)` from `DataComplete.assignments()` with a **refreshed** expected set, so `kafka.connect.valid-through-ts` can no longer overstate completeness. No Avro change.
10. **Fix #8 — memory is bounded by backpressure, not by dropping.** Above a high-water mark the coordinator stops polling the control topic and stops emitting `StartCommit`; workers stop advancing source offsets; source lag grows visibly. Nothing is lost.
11. **Deleted concepts:** edge-triggered election, "coordinator death = task death" (`NotRunningException` file removed), the fixed coordinator `transactional.id` and `isProducerFenced`, the frozen `totalPartitionCount`, the 60 s `join`/1 min `awaitTermination` inside a rebalance callback, `Channel implements ConsumerRebalanceListener`, `seekToTrackedOffsets`, the per-rebalance worker consumer group, the unused per-`Channel` `Admin`, `sourcePartitionCount`'s `partitionsFor` loop, `IcebergSinkTask.flush`.
12. **Net code:** ≈ −360 lines of lifecycle/plumbing, ≈ +500 lines of which ~150 is a reinstated metrics MBean and ~120 is the tri-state commit-outcome plumbing. **Net LOC is roughly flat to slightly positive; net concepts are strongly negative.** §9 accounts for this honestly and justifies the one non-deletion (observability is axis-3 mandatory).
13. **Upstreamability:** no phase changes a `Committer` SPI signature, no phase touches the `kafka-connect-events` Avro protocol, no new runtime dependency. One config is deprecated (`iceberg.control.group-id-prefix`), one default changes (`max-consecutive-failures` 1 → 5).
14. **Rollout:** 7 independently shippable, default-safe phases (§10), each with a one-file revert. Phase 1 (benign CAS) must land before Phase 3 (watchdog), because the watchdog deliberately manufactures overlap.
15. **Judge overrides:** I overruled the liveness judge's winner (a dedicated election topic/group) — §4 — because the level read already delivers its main benefit at zero added machinery, and the control-topic watchdog delivers its remaining benefit (bounded absence, hung-owner detection) without a group, a thread or a client.

---

## 2. Root cause, argued

### 2.1 The signal cannot supply the properties the design demands of it

The design derives a cluster-wide singleton owning durable state — leader identity, commit-round buffers, the readiness quorum, the commit clock, a Kafka transactional identity — from `SinkTask.open()/close()`. Five properties are required and none exists.

**(1) Uniqueness across instances.** The predicate is "my `open()` received partition 0 of the lexicographically lowest subscribed topic" (`CommitterImpl.java:67-71, 89-96`). Kafka guarantees at most one owner of a partition *per group, per generation, among current members*. It does not guarantee at most one live `SinkTask` **instance** with that assignment. Connect says so: "Zombie sink tasks are handled naturally because requests to alter consumer group offsets / delete consumer groups will fail if there are still active members in the group" (`DistributedHerder.java:1724-1726`) — a statement about the offsets REST API. `Worker.awaitStopTasks` derives **one** deadline of `task.shutdown.graceful.timeout.ms` for the whole collection ("This is the total amount of time, not per task", `WorkerConfig.java:95-100`, default 5000; `Worker.java (Connect):1089-1096`), removes the task from `Worker.tasks` *before* awaiting (`1066-1079`), and then calls `cancel()` which does **not** interrupt (`WorkerTask.java:145-148`). `IncrementalCooperativeAssignor.duplicatedAssignments` exists precisely because two workers can run the same `ConnectorTaskId`.

**(2) Stability.** Coordinator lifetime is bounded by the inter-rebalance interval. Verified chain (this is the fact that corrects the earlier belief that only *relevant* rebalances mattered): `Worker.baseConsumerConfigs` sets no `partition.assignment.strategy` and no `group.protocol` (`Worker.java (Connect):882-935`, sink consumer built at `1851-1854`); the client default is `[RangeAssignor, CooperativeStickyAssignor]` (`ConsumerConfig.java:419-424`); `RangeAssignor` inherits `supportedProtocols() == [EAGER]`, so `ConsumerCoordinator`'s `retainAll` intersection yields EAGER (`ConsumerCoordinator.java:209-233`); `onJoinPrepare` EAGER revokes **all** then `assignFromSubscribed(emptySet())` (`786-791`); `onJoinComplete` therefore computes `addedPartitions == the full assignment` (`377-380, 421-424`); `WorkerSinkTask` forwards both sets verbatim (`689-690, 781`; `461-463` via `698, 813`). Any member in `STABLE` rejoins on `REBALANCE_IN_PROGRESS` (`AbstractCoordinator.java:1251-1257`), so **an unrelated task's join drags every task through the full cycle**. On the Iceberg side both `closedPartitions.contains(leaderTP)` (`CommitterImpl.java:134`) and `addedPartitions.contains(leaderTP)` (`96`) are therefore true for the TP0 owner on every rebalance, and the `coordinatorThread != null` idempotence guard (`188`) cannot help because `stopCoordinator` nulled the field first (`232`).

Four bounds a designer must carry, all verified: a task's **first** join gets `open(FULL)` with no preceding `close` (`WorkerSinkTask.java:809` early-returns); a task with an **empty** assignment gets **neither** callback (`772`, `809`), so a readiness quorum expressed over "tasks that called open()" is wrong when `tasks.max > partitions`; a task whose *new* assignment is empty gets `close(FULL)` and **no** `open()` — coordinator torn down with no matching restart; and the close callback is not always the "revoked" one — on generation reset it is `invokePartitionsLost(ALL)` (`ConsumerCoordinator.java:773-784` → `WorkerSinkTask.java:697-709`), i.e. `close(FULL)` with no `preCommit` and no offset commit. Finally, EAGER is a **default, not an invariant**: `connector.client.config.override.policy` defaults to `All` (`WorkerConfig.java:149-155`) and `consumer.override.*` is applied last (`Worker.java (Connect):910`), so one connector-config line silently converts open/close into deltas.

**(3) Level semantics.** The commit history is literally an oscillation between the two encodings: `bbdc525aee`/`32ead2d099` moved reconciliation to a level-triggered `reconcileCoordinator()` in `save()`; `a50470cc0c` reverted it, reopening the hole `ae79032753` had closed. The consequence is visible on HEAD: `processControlEvents` clears a fenced coordinator (`CommitterImpl.java:156-167`) and the **only** path that can ever start one again is a later `open()` carrying the leader TP (`96`).

**(4) Cross-JVM ordering.** `close()` on task A in JVM 1 and `open()` on task B in JVM 2 are unordered. The only cross-JVM primitive is the fixed coordinator `transactional.id` (`Channel.java:92-93`), and it is not a CAS: the client sends `(-1,-1)` (`TransactionManager.java:280-301`), the broker maps it to `None` (`KafkaApis.scala:2349-2353`), the `PRODUCER_FENCED` gate is vacuously satisfied (`TransactionCoordinator.scala:230-231`) and `prepareIncrementProducerEpoch(_, None, _)` bumps with **no failure branch** (`TransactionMetadata.scala:281-286`). Kafka's own `Admin.fenceProducers` sends the identical request and its source comment states `PRODUCER_FENCED` "will never be returned" for it (`FenceProducersHandler.java:83-93`). **This corrects the earlier belief** that `startCoordinator`'s `catch (RuntimeException e) { if (isProducerFenced(e)) return false; }` (`CommitterImpl.java:196-214`) guards the concurrent-election race: it cannot fire for that race. Two coordinators both succeed; every rebuild *steals* the id from a healthy predecessor, so (2)'s frequent rebuilds are a self-inflicted mutual-fencing flap.

**(5) State durability.** `commitBuffer`, `readyBuffer`, `receivedPartitionCount`, `startTime`, `currentCommitId` are process-local (`CommitState.java:40-45`) and `totalPartitionCount` is a one-shot snapshot (`CommitterImpl.java:101, 305-314`). Combined with (2) this resets the commit clock, and **that is the outage**: `isCommitIntervalReached()` seeds `startTime = now` on first call then requires a full interval (`CommitState.java:81-88`), and `new StartCommit(...)` appears exactly once in the module (`Coordinator.java:126`, inside that branch). `isCommitTimedOut()` and `isCommitReady()` are both gated on `isCommitInProgress()` (`CommitState.java:105-108, 117-120`), so there is no escape hatch.

**Two symptom corrections the earlier belief got wrong** (verified): control-topic lag does **not** grow during the stall — with zero `StartCommit`s the control topic is completely idle — and worker heap does **not** grow, because `close()` calls `stopWorker()` unconditionally (`CommitterImpl.java:122-124`) → `SinkWriter.close()` → `RecordWriter::close` (`SinkWriter.java:50-52`) with no `abort()` (`BaseTaskWriter.java:135-141`). What actually grows without bound is source-topic lag and **orphan data files**, superlinearly with stall duration.

### 2.2 The second, independent structural error

The design conflates *who runs the aggregator* (liveness) with *what makes a commit correct* (safety), and spends its whole patch budget on liveness — while safety is already solved elsewhere. Safety is three composed Iceberg-side mechanisms: the offsets CAS `validateWith(offsetValidator(...))` (`Coordinator.java:340, 354`) whose body is exact map equality (`394-398`), re-evaluated after `refresh()` inside every retry attempt (`SnapshotProducer.java:293-300, 371-382, 485-496`); the per-table offset floor (`Coordinator.java:303-311, 421-441`); and `distinctByKey(ContentFile::location)` (`318, 326`).

The inversion is fatal: `ValidationException` is not in `Tasks.onlyRetryOn` (`SnapshotProducer.java:492`; `Tasks.java:430-441` rethrows unmatched) and not in `Coordinator.isRetryable` (`221-227`) → `throw e` (`178`) → `CoordinatorThread` terminates (`CoordinatorThread.java:48-56`) → `processControlEvents` throws `NotRunningException` (`CommitterImpl.java:164`) → `deliverMessages` wraps it in `ConnectException` (`WorkerSinkTask.java:653-665`) → task FAILED, never auto-restarted. **The one place the architecture is already tolerant of two coordinators is the place the code chooses to die.** And `iceberg.control.commit.max-consecutive-failures` defaults to 1 (`IcebergSinkConfig.java:110`) with `consecutiveCommitFailures++; if (>= max) throw` (`184-192`), so the entire `isRetryable` allowance is **dead code at default config**.

One further correction that reshapes the fix: the fatal path is only the *quorum* path. `commit(true)` — the timeout-driven path (`133-134`) — swallows **every** `RuntimeException` into a counter and returns *before* the classifier (`167-176`). So in the churn regime the coordinator usually does not die; it silently never commits. A benign-CAS fix installed on one path only is invisible in production.

---

## 3. Design principles (the whole design in five sentences)

1. **Leadership is a level, read from current state on the task thread.** Never from a callback argument, never as an edge.
2. **Election need only *converge*, never be *correct*.** All safety is enforced inside the Iceberg table CAS, the only atomic check-and-act living in the same state machine as the durable data.
3. **A rebalance callback does O(1), non-throwing, memory-only work.** Everything blocking happens on the task thread in `save()` or on the coordinator's own thread.
4. **Coordinator absence is bounded and observable by a mechanism the connector already owns** — the control topic — not by a new group, thread, topic, client or dependency.
5. **The `-coord` watermark advances and the buffer clears only for envelopes actually appended, or provably below that table's floor.** Everything else is retained and re-driven.

---

## 4. Judge disagreements and overrides

| Judge lens | Winner | Verdict |
|---|---|---|
| Exactly-once / data safety | invariant-repair layer + corrected benign-CAS | **Absorbed whole.** Its winner is orthogonal to election and is Phases 1, 5, 6 here. Its `SUPERSEDED`-not-`skip` correction is the load-bearing graft (§5.6.3). |
| Liveness / operability | dedicated single-partition **election topic in its own group** | **Overruled.** See below. |
| Simplicity / upstreamability | **level read from the current assignment, reconciled in `save()`** + benign CAS + repair subset | **Adopted as the spine.** |

**Why the election topic loses.** Its stated benefit is "leadership independent of source rebalances", and the level read delivers exactly that for free: once `isPrimary` is `assignment.contains(leaderKey)` evaluated on the task thread, a rebalance that does not move TP0 never flips the boolean, so nothing is torn down. What the election group additionally buys is (a) leadership for a task with an empty source assignment, (b) a connector-owned `session.timeout.ms` for tunable failover, and (c) a watchdog hook. (a) is not needed — TP0 of the lowest subscribed topic is always owned by *some* member, and the standby rule covers the residue. (b) is worth less than it looks: A's failover already rides the sink group's `session.timeout.ms` (default 45 000, `ConsumerConfig.java:409-411`), which is better than the Connect-worker-group path that a static-task-index design would depend on (`scheduled.rebalance.max.delay.ms` default 300 000, `DistributedConfig.java:187-192`). (c) is delivered here by the control-topic watchdog. Against that, the election topic **adds** a third consumer group per connector, a fourth client, at least two configs, and a partition-count validator (partition growth silently breaks single-ownership), and it *still* needs the benign-CAS graft because partition ownership is a liveness signal, not a fence — a hung-but-heartbeating owner holds it forever. It adds a subsystem to fix a subsystem and therefore loses on the user's first axis.

**Why the static-task-index design loses.** It is simpler still (the election key disappears; `task.id` is already injected, `IcebergSinkConnector.java:58`), but coordinator absence becomes *unbounded*: a FAILED task 0 is never auto-restarted in this fork and no peer is eligible. That directly violates the "absence must be bounded" requirement. Its good idea — a deterministic, operator-legible holder — is grafted as the **standby** role only.

**Why the coordinator-free design loses.** Largest deletion, but it concentrates 100 % of exactly-once on a primitive verified to **fail open**: once the snapshot carrying `kafka.connect.offsets.<controlTopic>.<groupId>` leaves the branch ancestry, `SnapshotUtil.ancestorsOf` silently ends the walk (`SnapshotUtil.java:245-260`), `lastCommittedOffsets` returns `Map.of()` (`Coordinator.java:433-441`), the floor filter admits everything (`303-311`) **and** the validator degenerates to `{}.equals({})` and PASSES (`394-398`). It also dissolves the atomicity of source-offset advance with control-event durability (`Channel.java:118-137`) with nothing named to replace it, removes `kafka.connect.valid-through-ts` with no owner, and cannot ship incrementally (the whole `kafka-connect-events` Avro contract becomes dead). Two of its concepts are grafted: "election need only converge" and "delete the frozen partition count".

**Standalone coordinator connector / external lease:** rejected. The former *loses* guarantees (framework consumer defaults to `read_uncommitted` — `Worker.java (Connect):882-935` never sets `isolation.level`; no `transactional.id` specified for its producer) while adding a module, config namespace and cutover. The latter adds a hard runtime dependency for zero safety gain, since a lease in another state machine cannot be checked atomically with a catalog commit.

---

## 5. The design

### 5.1 Component diagram

```
┌───────────────────────── Connect worker JVM ──────────────────────────┐
│ IcebergSinkTask (one per task)          [Connect task thread only]    │
│   start()   -> CommitterImpl.configure + validateEnvironment          │
│   open(tps)  -> committer.open(...)     O(1), logs, never throws      │
│   close(tps) -> committer.close(...)    O(1)+bounded rewind, no throw │
│   put(recs)  -> committer.save(recs)    <-- THE ONLY RECONCILE POINT  │
│   stop()     -> committer.close(List.of())                            │
│                                                                       │
│ CommitterImpl                                                          │
│  ├─ RoleDecider  (pure function: assignment x subscription x clock)    │
│  ├─ Worker  (always, if assigned or task 0)                            │
│  │    control-topic consumer  assign()  read_committed  latest         │
│  │    transactional producer  (worker txn.id, KIP-447 offsets)  KEEP   │
│  │    SinkWriter                                                       │
│  └─ CoordinatorThread  (only while role != FOLLOWER)   [own thread]    │
│       Coordinator extends Channel                                      │
│         control-topic consumer assign()  read_committed  earliest      │
│         IDEMPOTENT producer (no transaction, no txn.id)     <-- NEW    │
│         CommitState (set-coverage readiness, durable-ish clock)        │
│         exec: iceberg-committer-N pool  [per-table commits]           │
│ SinkMetrics (JMX MBean, one per task)                        <-- NEW  │
└───────────────────────────────────────────────────────────────────────┘
                │ control topic (assign(); N partitions OK)
                │ `<connectGroupId>-coord` = offset STORE only, no membership
                v
        Iceberg catalog  <- the ONLY place safety is enforced (CAS)
```

Key structural changes visible here: no consumer group membership for either control-topic client; no coordinator transaction; no `Admin` per `Channel`; one reconcile point.

### 5.2 Thread model

| Thread | Owns | Blocking allowed? | Bound |
|---|---|---|---|
| Connect task thread | `open/close/put/preCommit/stop`, role decision, `Worker.save`, `Worker.process()` (`consumeAvailable(Duration.ZERO)`, non-blocking), the bounded rewind | Only in `put()`, and only bounded | Reconcile tick ≤ `offset.flush.interval.ms` (default 60 000, `WorkerConfig.java:102-105`) because `iteration()` polls with `timeoutMs = max(nextCommit - now, 0)` (`WorkerSinkTask.java:248, 262-263`) and `deliverMessages()` → `task.put()` runs unconditionally even with an empty batch (`347-362, 629-635`) |
| `iceberg-coord` (leader/standby only) | client construction (**moved out of the constructor**), `assign()`, the `process()` loop, `doCommit` fan-out driver, `commitConsumerOffsets` | Yes | `POLL_DURATION` 1 s per tick; per-table commit bounded by §5.8 |
| `iceberg-committer-N` pool | one `commitToTable` per table | Yes | Iceberg `commit.retry.total-timeout-ms`, overridden per §5.8 |

**Rule R1 (hard):** `open()` and `close()` may only set fields, read `context.assignment()`/`consumer().subscription()` (both local, no RPC), log, and — in `close()` only — perform one bounded `committed(partitions, 5s)` inside a total try/catch. Forbidden: constructing a Kafka client, `initTransactions()`, `partitionsFor()`, `Thread.join`, `awaitTermination`, any catalog call, and letting any `Throwable` escape.

Why R1's justification is *not* an eviction argument (this corrects an earlier belief): at defaults, `rebalance.timeout.ms == max.poll.interval.ms == 300 000` (`ClassicKafkaConsumer.java:303`, `ConsumerConfig.java:605-607`), the poll clock is reset immediately before each callback (`ConsumerCoordinator.java:482`), and `close()`/`open()` land in *different* `poll()` calls because Connect passes `waitForJoinGroup=false` (`ClassicKafkaConsumer.java:602-621` → `ConsumerCoordinator.java:511`). So today's ~180 s of callback work evicts nothing at defaults. The three real reasons are: (i) the **shutdown** budget is 5000 ms **total for all tasks** (`WorkerConfig.java:95-100`, one shared deadline at `Worker.java (Connect):1089-1096`), which today's ~120 s teardown (`Coordinator.java:463-467` + `CommitterImpl.java:246`) blows by 24×, leaving a heartbeating, partition-owning zombie; (ii) every millisecond in a callback is added to the wall-clock of a rebalance during which **every other task of the connector is stalled** in its own poll; (iii) crossing `max.poll.interval.ms` — one config operators commonly lower — converts a revoke into `onPartitionsLost` (`AbstractCoordinator.java:1147-1188` → `ConsumerCoordinator.java:773-784`) plus an extra rebalance.

**Rule R2:** callbacks are abortable mid-body. `consumer.wakeup()` is called from `WorkerSinkTask.stop()` (`183-184`) and `transitionTo()` (`221-223`), and `ConsumerRebalanceListenerInvoker` **rethrows** `WakeupException` rather than capturing it (`67-68, 93-94`). Therefore teardown must be idempotent, re-drivable from `save()`, and must never drop the reference to a coordinator not yet known dead — today `stopCoordinator` nulls `coordinatorThread` at `CommitterImpl.java:232` *before* `terminate()`.

### 5.3 Role state machine

Evaluated once per `save()` on the Connect task thread. Three states.

| State | Meaning | Coordinator running? |
|---|---|---|
| `FOLLOWER` | not coordinating | no |
| `PRIMARY` | owns the leader key | yes |
| `STANDBY` | task 0, has seen no foreign `StartCommit` for K intervals | yes |

Inputs (all local, zero RPC, task thread):

| Input | Source |
|---|---|
| `assignment` | `context.assignment()` → `consumer.assignment()` (`WorkerSinkTaskContext.java:102-106`; interface `SinkTaskContext.java:76`) |
| `subscription` | `consumer().subscription()` via the retained reflective handle (`KafkaUtils.java:69-79`) |
| `leaderKey` | `min(subscription) -> new TopicPartition(topic, 0)` — unchanged from `CommitterImpl.java:67-71` |
| `taskIndex` | `config.taskId()` parsed (`IcebergSinkConfig.java:308`; injected at `IcebergSinkConnector.java:58`) |
| `lastForeignStartCommitMs` | maintained by `Worker` when a `StartCommit` arrives whose `commitId` is not one this task's own coordinator emitted |
| `coordinatorState` | `{absent, running, terminated}` + `lastProgressMs` from `CoordinatorThread` |

Transitions (target computed, then reconciled):

| # | Guard | Target | Thread | Time bound |
|---|---|---|---|---|
| T1 | `leaderKey != null && assignment.contains(leaderKey)` | `PRIMARY` | task | O(1) |
| T2 | `!T1 && taskIndex == 0 && now - max(lastForeignStartCommitMs, roleEnteredMs) > promotionGapMs` | `STANDBY` | task | O(1) |
| T3 | otherwise | `FOLLOWER` | task | O(1) |
| A1 | target ∈ {PRIMARY, STANDBY} ∧ coordinator absent | start: allocate + `Thread.start()` | task | O(1) — **no client construction here** |
| A2 | target ∈ {PRIMARY, STANDBY} ∧ coordinator terminated | log ERROR + metric, then A1 | task | O(1) |
| A3 | target ∈ {PRIMARY, STANDBY} ∧ running ∧ `now - lastProgressMs > promotionGapMs` | stop then A1 (watchdog restart) | task | stop ≤ `stop-timeout-ms` (default 2000) |
| A4 | target == FOLLOWER ∧ coordinator running | stop | task | ≤ `stop-timeout-ms` |
| A5 | `assignment` non-empty ∨ `taskIndex == 0` ∧ no `Worker` | start `Worker` | task | client construction; bounded by `max.block.ms` on the worker producer only (unchanged from today) |

`promotionGapMs = standbyPromotionIntervals × commitIntervalMs` (default `3 × 300 000` = 900 000; set intervals to `0` to disable the standby role entirely).

**Convergence argument.** A `STANDBY`'s own `StartCommit`s do not refresh `lastForeignStartCommitMs` (it filters its own `commitId`s), so as soon as a `PRIMARY` exists and emits one, the standby's guard T2 goes false on the next tick and A4 stops it. A `PRIMARY` never demotes for liveness reasons — only T1 going false demotes it. Two `STANDBY`s cannot coexist because only `taskIndex == 0` is eligible; two `PRIMARY`s can coexist transiently (zombie instance, or divergent `subscription()` under `topics.regex`) and that is *tolerated by design* — see §6/I7.

**What T1–T3 buy, restated against the four verified EAGER bounds:** identical under EAGER and under a `CooperativeStickyAssignor` override (no callback argument is consulted); unaffected by close-without-open and open-without-close; unaffected by a task that receives *neither* callback; and it never treats "revoked ⇒ close() ran" as a framework guarantee.

**Absence bound, per cause:**

| Cause of absence | Bounded by |
|---|---|
| Leader worker crashed | sink-group `session.timeout.ms` (45 000) + rebalance → TP0 moves → T1 on a survivor |
| Leader task FAILED | its consumer closes (`WorkerSinkTask.java:188-196`), it leaves the group → TP0 moves → T1 |
| Leader task's new assignment empty (`close` with no `open`) | next `save()` tick ≤ 60 s: T1 false → A4; and T1 true on the new owner |
| Leader's coordinator hung in a catalog commit | T2 on task 0 after `promotionGapMs`, and A3 self-restart on the primary |
| `leaderKey` unresolvable (empty subscription) / lowest configured topic does not exist | T2 on task 0 after `promotionGapMs` |
| Coordinator thread died from any cause | A2 on the next tick ≤ 60 s (never a FAILED task) |
| Both the leader owner and task 0 gone | Connect reassignment (`scheduled.rebalance.max.delay.ms`, `DistributedConfig.java:187-192`) — **explicitly accepted**, §6/I4 |

### 5.4 Commit-round state machine (coordinator thread)

```
IDLE ──[isCommitIntervalReached() && !bufferPaused]──> emit StartCommit ──> IN_PROGRESS
IN_PROGRESS ──[receivedSet ⊇ expectedSet]──────────> commit(FULL_QUORUM)  ──> IDLE
IN_PROGRESS ──[now - startedMs > commitTimeoutMs]──> commit(PARTIAL)      ──> IDLE
IDLE ──[bufferPaused && no round in flight]────────> commit(DRAIN)        ──> IDLE
```

`isCommitIntervalReached()` becomes: `false` while a round is in progress; **`true` immediately when `lastCommitStartedMs == 0`** (freshly promoted, after a settle of `min(commitIntervalMs, 5000)` ms since the coordinator's first `process()`); otherwise `now - lastCommitStartedMs >= commitIntervalMs`. This is the whole fix for §2.1(5): a promoted coordinator commits at once instead of one full interval later, so leadership churn can no longer starve commits. `lastCommitStartedMs` is set in `startNewCommit()`, preserving today's interval semantics.

### 5.5 Per-table commit outcomes

`commitToTable` returns an enum instead of `void`, and `doCommit` gates its two side effects on it.

| Outcome | Cause | `-coord` watermark | `commitBuffer` | Metric |
|---|---|---|---|---|
| `COMMITTED` | append/rowDelta succeeded | may advance | may clear | `commit-count` |
| `NOTHING_TO_COMMIT` | all envelopes filtered by the floor (`Coordinator.java:334-336`) | may advance | may clear | — |
| `SUPERSEDED` | offsets CAS lost (another coordinator committed a ≥ floor) | **must not advance** | **must not clear** | `superseded-count` |
| `TABLE_UNAVAILABLE` | `NoSuchTableException` or UUID mismatch (`275-289`) | **must not advance** | **must not clear** | `table-unavailable-count` |
| (throws) | anything else | not reached | not reached | `commit-failure-count` |

`doCommit` advances the watermark and clears the buffer **iff every** table returned `COMMITTED` or `NOTHING_TO_COMMIT`. Otherwise it logs, meters, and returns without clearing — the next round re-reads each table's floor and the existing per-partition filter (`303-311`) drops exactly the dominated envelopes and re-drives the rest. **This is why `SUPERSEDED` needs no dominance arithmetic:** the floor filter *is* the dominance test, and it is already per control-topic partition. It is also why "skip" is the wrong word — Flink can skip because `MAX_COMMITTED_CHECKPOINT_ID` proves the *identical* change set landed (`DynamicCommitter.java:189-208`); a losing Kafka Connect coordinator's file set can be a strict superset of the winner's (different buffered window, different `distinctByKey` outcome), so "skip + advance the watermark" would convert today's loud crash into **silent unrecoverable loss**. That is the single most important correction in this document.

`TABLE_UNAVAILABLE` gets an explicit terminal policy: after `table-unavailable-rounds` consecutive rounds for the same table (default = `standbyPromotionIntervals`), log ERROR naming the table and the retained envelope count. It never advances the watermark and never fails the task.

### 5.6 Java sketches (load-bearing only)

#### 5.6.1 `CommitterImpl` — the level read and the reconcile

```java
public class CommitterImpl implements Committer {

  enum Role { FOLLOWER, PRIMARY, STANDBY }

  private CoordinatorThread coordinatorThread;
  private Worker worker;
  private Role role = Role.FOLLOWER;
  private long roleEnteredMs;
  private Consumer<byte[], byte[]> consumer;      // reflective handle, RETAINED (KIP-447)
  private SinkMetrics metrics;
  private Set<TopicPartition> pendingRewind = Set.of();
  private long promotionGapMs;
  private int taskIndex;

  // ---------- callbacks: O(1), total, never throw (Rule R1/R2) ----------

  @Override
  public void open(Catalog c, IcebergSinkConfig cfg, SinkTaskContext ctx,
                   Collection<TopicPartition> addedPartitions) {
    // Deliberately does NOT decide leadership. Under EAGER this set is the FULL
    // assignment on every group generation, so it carries no information about
    // whether THIS task's ownership changed (ConsumerCoordinator.java:786-791, 377-380).
    LOG.info("Committer {} open, {} partition(s) added; leadership is reconciled in save()",
        taskId, addedPartitions.size());
  }

  @Override
  public void close(Collection<TopicPartition> closedPartitions) {
    try {
      stopWorker();                                  // unchanged: drop buffered writers
      this.pendingRewind = Set.copyOf(closedPartitions);
      rewindClosedPartitions();                      // bounded 5s, total try/catch
      if (closedPartitions.isEmpty()) {
        // task shutdown: no further save() will reconcile. Bounded, best-effort.
        stopCoordinator("task-stop");
      }
    } catch (RuntimeException | Error e) {
      // An escaped exception both kills the task permanently (no auto-restart) and
      // SKIPS the paired open() (WorkerSinkTask.java:779). Never let one escape.
      LOG.error("Committer {} close failed; continuing", taskId, e);
    }
  }

  // ---------- the only reconcile point ----------

  @Override
  public void save(Collection<SinkRecord> sinkRecords) {
    reconcile();                                     // leadership + coordinator lifecycle
    if (sinkRecords != null && !sinkRecords.isEmpty()) {
      worker.save(sinkRecords);                      // worker guaranteed non-null by reconcile
    }
    if (worker != null) {
      worker.process();                              // non-blocking control-topic drain
    }
  }

  private void reconcile() {
    Set<TopicPartition> assignment = context.assignment();
    Set<String> subscription = consumer().subscription();
    TopicPartition leaderKey = leaderPartition(subscription).orElse(null);

    // A5: every ASSIGNED task must be able to answer StartCommit (fixes I4c), and
    // task 0 needs the control-topic view to act as standby.
    if (!assignment.isEmpty() || taskIndex == 0) {
      startWorker();
    }

    long now = clock.millis();
    Role target = decide(leaderKey, assignment, taskIndex,
        worker == null ? 0L : worker.lastForeignStartCommitMs(), roleEnteredMs, now, promotionGapMs);

    if (target == Role.FOLLOWER) {
      stopCoordinator("not-leader");
    } else {
      CoordinatorThread t = coordinatorThread;
      if (t == null) {
        startCoordinator(subscription);                             // A1
      } else if (t.isTerminated()) {
        LOG.error("Committer {} coordinator terminated ({}); re-electing", taskId, t.exception());
        metrics.coordinatorRestart();
        stopCoordinator("terminated");
        startCoordinator(subscription);                             // A2
      } else if (now - t.lastProgressMs() > promotionGapMs) {
        LOG.error("Committer {} coordinator made no progress for {} ms; restarting",
            taskId, now - t.lastProgressMs());
        metrics.coordinatorRestart();
        stopCoordinator("no-progress");
        startCoordinator(subscription);                             // A3
      } else {
        t.updateSourceTopics(subscription);   // refreshes the expected-set basis (H2)
      }
    }
    if (target != role) {
      role = target;
      roleEnteredMs = now;
      LOG.info("Committer {} role -> {} (leaderKey={}, assigned={})",
          taskId, target, leaderKey, assignment.size());
    }
    metrics.role(role, leaderKey, worker);
  }

  /** Pure, unit-testable. This function IS the design. */
  @VisibleForTesting
  static Role decide(TopicPartition leaderKey, Set<TopicPartition> assignment, int taskIndex,
                     long lastForeignStartCommitMs, long roleEnteredMs, long nowMs, long gapMs) {
    if (leaderKey != null && assignment.contains(leaderKey)) {
      return Role.PRIMARY;                                          // T1
    }
    if (gapMs > 0 && taskIndex == 0
        && nowMs - Math.max(lastForeignStartCommitMs, roleEnteredMs) > gapMs) {
      return Role.STANDBY;                                          // T2
    }
    return Role.FOLLOWER;                                           // T3
  }

  private void startCoordinator(Set<String> sourceTopics) {
    // O(1): no clients are constructed here. Coordinator.start() runs on the new
    // thread and builds the producer/consumer there, so save() never blocks on
    // initTransactions()/metadata and no fenced-construction catch is needed.
    Coordinator coordinator =
        new Coordinator(catalog, config, clientFactory, context, sourceTopics, metrics);
    coordinatorThread = new CoordinatorThread(coordinator);
    coordinatorThread.start();
    metrics.promotion(role);
  }

  private void stopCoordinator(String reason) {
    CoordinatorThread t = coordinatorThread;
    if (t == null) {
      return;
    }
    LOG.info("Committer {} stopping coordinator ({})", taskId, reason);
    try {
      t.terminate();                       // always drains the pool; see 5.6.5
      t.join(config.coordinatorStopTimeoutMs());   // default 2000, not 60000
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      LOG.warn("Committer {} coordinator teardown error, abandoning", taskId, e);
    }
    // Reference dropped only after terminate() was signalled (R2), and correctness
    // never depends on the join succeeding: an abandoned coordinator is neutralised
    // by the Iceberg offsets CAS, not by this wait.
    coordinatorThread = null;
    if (t.isAlive()) {
      metrics.abandonedCoordinator();
      LOG.warn("Committer {} coordinator did not exit in {} ms; abandoned (CAS-safe)",
          taskId, config.coordinatorStopTimeoutMs());
    }
  }

  private void rewindClosedPartitions() {
    if (pendingRewind.isEmpty()) {
      return;
    }
    try {
      Set<TopicPartition> still = new HashSet<>(pendingRewind);
      still.retainAll(consumer().assignment());   // only retained partitions need it
      if (!still.isEmpty()) {
        // Bounded (5s) instead of default.api.timeout.ms=60000, and total:
        // a TimeoutException here used to become rebalanceException -> FAILED task
        // (WorkerSinkTask.java:818, 517-521).
        consumer().committed(still, REWIND_TIMEOUT).forEach((tp, om) -> {
          if (om != null) {
            consumer().seek(tp, om.offset());
          }
        });
      }
      pendingRewind = Set.of();
    } catch (RuntimeException e) {
      metrics.rewindFailure();
      LOG.error("Committer {} could not rewind {} to committed offsets; under the EAGER "
          + "protocol positions are reset from committed offsets anyway, but under a "
          + "cooperative assignor override this risks losing buffered records",
          taskId, pendingRewind, e);
      pendingRewind = Set.of();
    }
  }
}
```

Note what disappeared: `processControlEvents`'s `throw new NotRunningException(...)` (`CommitterImpl.java:164`), `isProducerFenced` (`281-291`), the fenced-construction catch (`196-214`), `COORDINATOR_STOP_TIMEOUT_MS = 60_000` (`49`), `sourcePartitionCount` and its `partitionsFor` loop (`301-314`), and the `contains(leaderTopicPartition.get())` calls with a possibly-null key against the framework's comparator-based `TreeSet` (`96, 134`) that were an NPE source.

#### 5.6.2 The benign-CAS validator (Flink's mechanic, verbatim shape)

```java
/** Thrown from inside validate() so it survives runValidations() uncast and is precisely
 *  catchable. Signalling by returning false is NOT usable: SnapshotProducer converts false
 *  into a generic ValidationException (SnapshotProducer.java:371-382), indistinguishable
 *  from a real schema/partition validation failure. */
private static class StaleOffsetsException extends ValidationException {
  private StaleOffsetsException(String msg) { super(msg); }
}

private SnapshotAncestryValidator offsetValidator(
    TableIdentifier id, Map<Integer, Long> expectedOffsets) {
  return baseSnapshots -> {
    Map<Integer, Long> committed = lastCommittedOffsets(baseSnapshots);
    if (!expectedOffsets.equals(committed)) {
      throw new StaleOffsetsException(String.format(
          "Superseded on %s: expected %s, committed %s", id, expectedOffsets, committed));
    }
    return true;
  };
}
```

and at the call site (replacing `appendOp.commit()` at `Coordinator.java:351` and `deltaOp.commit()` at `366`):

```java
try {
  op.commit();
} catch (StaleOffsetsException e) {
  LOG.info("Coordinator {} superseded on table {}; retaining {} envelope(s) for the next "
      + "round (another coordinator committed a newer offsets floor)", taskId, id, n, e);
  return TableCommitOutcome.SUPERSEDED;
}
return TableCommitOutcome.COMMITTED;
```

#### 5.6.3 `doCommit` — the watermark/buffer discipline, on **both** triggers

```java
private enum Trigger { FULL_QUORUM, PARTIAL_TIMEOUT, DRAIN }

private void commit(Trigger trigger) {
  long start = clock.millis();
  try {
    doCommit(trigger);
    consecutiveCommitFailures = 0;
  } catch (RuntimeException e) {
    // NARROWED. The old code swallowed EVERY RuntimeException on the partial path
    // (Coordinator.java:167-176) before the classifier ran, so a fix installed only on
    // the quorum path was invisible in exactly the churn regime that produces overlap.
    metrics.commitFailure(trigger, e);
    if (e instanceof CommitStateUnknownException) {
      LOG.error("Coordinator {} commit state UNKNOWN for round {}; retaining buffer and "
          + "watermark so the next round re-reads the floor", taskId, commitId(), e);
      return;                                   // never clear, never advance, never die
    }
    if (!isRetryable(e)) {
      LOG.error("Coordinator {} non-retryable commit failure; will be re-elected", taskId, e);
      throw e;                                  // terminates THIS coordinator only
    }
    if (++consecutiveCommitFailures >= config.commitMaxConsecutiveFailures()) {
      throw e;
    }
    LOG.warn("Coordinator {} retryable commit failure {}/{}", taskId,
        consecutiveCommitFailures, config.commitMaxConsecutiveFailures(), e);
  } finally {
    commitState.endCurrentCommit();
    metrics.commitRound(trigger, clock.millis() - start);
    progress();                                 // feeds CoordinatorThread.lastProgressMs
  }
}

private void doCommit(Trigger trigger) {
  Map<TableReference, List<Envelope>> commitMap = commitState.tableCommitMap();
  OffsetDateTime validThroughTs = commitState.validThroughTs(trigger == Trigger.FULL_QUORUM);
  Map<TableReference, TableCommitOutcome> outcomes = new ConcurrentHashMap<>();

  Tasks.foreach(commitMap.entrySet())
      .executeWith(exec)
      .stopOnFailure()
      .run(entry -> outcomes.put(entry.getKey(),
          commitToTable(entry.getKey(), entry.getValue(), controlTopicOffsets(), validThroughTs)));

  boolean allSettled = outcomes.values().stream()
      .allMatch(o -> o == TableCommitOutcome.COMMITTED || o == TableCommitOutcome.NOTHING_TO_COMMIT);

  if (allSettled) {
    commitConsumerOffsets();     // max-merged; see 5.6.4
    commitState.clearResponses();
  } else {
    metrics.retainedRound(outcomes);
    LOG.warn("Coordinator {} round {} not fully settled ({}); watermark NOT advanced and "
        + "{} envelope(s) retained", taskId, commitId(), outcomes, commitState.bufferedCount());
  }
  send(new Event(config.connectGroupId(), new CommitComplete(commitId(), validThroughTs)));
}
```

**Preserved by construction (do not "fix" this):** multi-table commits are not atomic — `Tasks.foreach(...).stopOnFailure()` checks the flag only at task start (`Tasks.java:304-311`), `revertTask` is null, and `throwFailureWhenFinished` defaults true (`73-74, 393-397`). Today's torn commit self-heals *only* because the watermark advances after **all** tables and because the per-table floor is written as `max(previousFloor, GLOBAL controlTopicOffsets)` (`Coordinator.java:296-302`). The code above keeps both properties. Any later refactor to a per-table watermark must add a compensating mechanism or it converts a benign torn commit into duplicates.

#### 5.6.4 `Channel` — no membership, no coordinator transaction, monotone watermark

```java
abstract class Channel {                       // no longer implements ConsumerRebalanceListener

  private final boolean transactional;
  private Producer<String, byte[]> producer;   // created in start(), on the owner thread
  private Consumer<String, byte[]> consumer;

  void start() {
    this.producer = transactional
        ? clientFactory.createTransactionalProducer(transactionalId)   // worker only
        : clientFactory.createIdempotentProducer();                    // coordinator
    this.consumer = clientFactory.createConsumer(offsetStoreGroupId, autoOffsetReset);
    // MANUAL ASSIGNMENT. Consequences, all verified:
    //  * no group membership -> a slow catalog fan-out can no longer evict us
    //    (commitSync sends Generation.NO_GENERATION, ConsumerCoordinator.java:1288,
    //     skipping the eviction/CommitFailedException branch at 1265-1285);
    //  * every coordinator sees ALL control partitions -> overlap is duplicated work,
    //    never a split view (this is the I7 fix);
    //  * committed offsets are still the start position
    //    (ClassicKafkaConsumer.java:1182-1194 runs under manual assignment too) and are
    //    still visible to `kafka-consumer-groups --describe` for lag monitoring;
    //  * the control topic may now have >1 partition safely.
    List<TopicPartition> tps = consumer.partitionsFor(controlTopic).stream()
        .map(pi -> new TopicPartition(pi.topic(), pi.partition())).toList();
    consumer.assign(tps);
    consumeAvailable(Duration.ofSeconds(1));
  }

  protected void send(List<Event> events, Map<TopicPartition, Offset> sourceOffsets) {
    List<ProducerRecord<String, byte[]>> records = encode(events);
    if (!transactional) {
      records.forEach(producer::send);           // idempotent, acks=all
      producer.flush();
      return;
    }
    synchronized (producer) { ...unchanged worker transaction (Channel.java:118-137)... }
  }

  protected void commitConsumerOffsets() {
    // MONOTONE. Two coordinators both see all partitions; the one that started earlier
    // could otherwise commit a LOWER value and rewind the group.
    Map<TopicPartition, OffsetAndMetadata> current =
        consumer.committed(Set.copyOf(consumer.assignment()), COMMIT_FETCH_TIMEOUT);
    Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
    controlTopicOffsets().forEach((p, v) -> {
      TopicPartition tp = new TopicPartition(controlTopic, p);
      OffsetAndMetadata prev = current.get(tp);
      if (prev == null || v > prev.offset()) {
        toCommit.put(tp, new OffsetAndMetadata(v));
      }
    });
    if (!toCommit.isEmpty()) {
      consumer.commitSync(toCommit);
    }
  }

  protected void consumeAvailable(Duration pollDuration) {
    ConsumerRecords<String, byte[]> records = consumer.poll(pollDuration);
    while (!records.isEmpty() && canConsumeMore()) {      // backpressure hook (I8)
      records.forEach(this::handle);                      // unchanged merge-then-receive order
      records = consumer.poll(pollDuration);
    }
  }

  protected boolean canConsumeMore() { return true; }     // Coordinator overrides
}
```

Deleting the coordinator's transaction is safe: it carried no source offsets (`Channel.java:98-100` → `send(list, ImmutableMap.of())`), the catalog commit already precedes it (`Coordinator.java:340-366` before `375`), and **nothing consumes** `CommitToTable` or `CommitComplete` — `Coordinator.receive` handles only `DATA_WRITTEN`/`DATA_COMPLETE` (`145-159`) and `Worker.receive` only `START_COMMIT` (`70-72`). What we lose is the lazy fencing of a loser's control-topic writes; what we gain is the elimination of root cause #4 (the per-rebuild epoch steal), of `initTransactions()` from the promotion path, and of three exception types that could kill a coordinator (`ProducerFenced`, `InvalidProducerEpoch`, `UnknownProducerId`). Duplicate `StartCommit`s from an overlapping pair cost extra file rolls, which is already possible today. **The worker's transactional producer is unchanged** — it is the sole source-offset advance and carries the KIP-447 fence via `sendOffsetsToTransaction(offsets, KafkaUtils.consumerGroupMetadata(context))` (`Channel.java:124-127`).

#### 5.6.5 `CoordinatorThread` — no leaks, progress exposed

```java
class CoordinatorThread extends Thread {
  private final AtomicLong lastProgressMs = new AtomicLong(System.currentTimeMillis());

  @Override public void run() {
    try {
      coordinator.start();
      while (!terminated) {
        coordinator.process();
        lastProgressMs.set(coordinator.lastProgressMs());
      }
    } catch (Exception e) {
      LOG.error("Coordinator error, exiting thread", e);
      exception.set(e);
      terminated = true;
    } finally {
      // Fixes I9a: today a SELF-terminated coordinator never reaches exec.shutdownNow()
      // because stopCoordinator guards with !thread.isTerminated()
      // (CommitterImpl.java:235-237), leaking up to commitThreads daemon threads
      // per generation.
      try { coordinator.terminate(); } catch (RuntimeException e) { LOG.warn("terminate", e); }
      try { coordinator.stop(); }      catch (RuntimeException e) { LOG.warn("stop", e); }
    }
  }
  long lastProgressMs() { return lastProgressMs.get(); }
}
```

and in `Coordinator.terminate()` (replacing `Coordinator.java:460-474`):

```java
void terminate() {
  this.terminated = true;
  // shutdown(), NOT shutdownNow(). Fixes I9b: shutdownNow() drains queued FutureTasks
  // from the queue without running or cancelling them, so they stay NEW and
  // Tasks.waitFor busy-polls future.isDone() at 100 Hz forever (Tasks.java:474-533),
  // pinning the iceberg-coord thread and its clients for the life of the JVM.
  // Queued tasks now RUN, see `terminated`, and fail fast (Coordinator.java:329-332).
  exec.shutdown();
  try {
    if (!exec.awaitTermination(config.coordinatorStopTimeoutMs(), TimeUnit.MILLISECONDS)) {
      exec.shutdownNow();          // last resort; no exception thrown either way
      LOG.warn("Coordinator {} commit pool did not drain in {} ms", taskId, ...);
    }
  } catch (InterruptedException e) {
    Thread.currentThread().interrupt();
  }
}
```

The `ThreadPoolExecutor` (`Coordinator.java:106-116`) additionally gets `allowCoreThreadTimeOut(true)` so an idle pool does not pin `commitThreads` daemon threads.

#### 5.6.6 `CommitState` — set coverage and a promotion-aware clock

```java
class CommitState {
  private final List<Envelope> commitBuffer = Lists.newArrayList();
  private final List<DataComplete> readyBuffer = Lists.newArrayList();
  private final Set<TopicPartition> receivedAssignments = Sets.newHashSet();   // was an int
  private long lastCommitStartedMs;                                            // was startTime
  private UUID currentCommitId;

  void addReady(Envelope envelope) {
    DataComplete dc = (DataComplete) envelope.event().payload();
    readyBuffer.add(dc);
    if (Objects.equals(currentCommitId, dc.commitId())) {
      // SET, not `receivedPartitionCount += dc.assignments().size()`
      // (CommitState.java:69). Duplicate DataCompletes from a re-sending worker can no
      // longer reach quorum early, so validThroughTs can no longer be a min over a subset.
      dc.assignments().forEach(a ->
          receivedAssignments.add(new TopicPartition(a.topic(), a.partition())));
    }
  }

  boolean isCommitReady(Set<TopicPartition> expected) {
    if (!isCommitInProgress()) { return false; }
    if (receivedAssignments.containsAll(expected)) { return true; }
    Set<TopicPartition> missing = Sets.difference(expected, receivedAssignments);
    LOG.warn("Commit {} short by {} of {} partition(s); missing {}",
        currentCommitId, missing.size(), expected.size(), missing);   // I3/I4c observable
    return false;
  }

  boolean isCommitIntervalReached() {
    if (isCommitInProgress()) { return false; }
    // THE STALL FIX. Old code seeded startTime=now on the first call and then waited a
    // full interval (CommitState.java:81-88), so a coordinator rebuilt more often than
    // commitIntervalMs never emitted a StartCommit at all.
    if (lastCommitStartedMs == 0L) { return true; }
    return System.currentTimeMillis() - lastCommitStartedMs >= config.commitIntervalMs();
  }

  void startNewCommit() {
    currentCommitId = UUID.randomUUID();
    lastCommitStartedMs = System.currentTimeMillis();
    receivedAssignments.clear();
  }

  int bufferedCount() { return commitBuffer.size(); }
}
```

The expected set is refreshed each round on the coordinator thread from `consumer.partitionsFor(topic)` over the topic set the leader pushes in via `updateSourceTopics(...)`, replacing the one-shot `totalPartitionCount` constructor argument (`Coordinator.java:82, 102`; `CommitterImpl.java:101, 305-314`).

#### 5.6.7 Backpressure instead of dropping (I8)

```java
// Coordinator
@Override protected boolean canConsumeMore() {
  boolean ok = commitState.bufferedCount() < config.commitBufferMaxEvents();
  if (!ok && !bufferPaused) {
    bufferPaused = true;
    metrics.bufferPaused();
    LOG.warn("Coordinator {} control-topic consumption PAUSED: {} buffered events >= {}. "
        + "StartCommit is suppressed, so workers stop advancing source offsets and source "
        + "lag will grow. No data is dropped.", taskId, commitState.bufferedCount(),
        config.commitBufferMaxEvents());
  }
  return ok;
}

void process() {
  if (!bufferPaused && commitState.isCommitIntervalReached()) {
    commitState.startNewCommit();
    send(new Event(config.connectGroupId(), new StartCommit(commitState.currentCommitId())));
  }
  consumeAvailable(POLL_DURATION);
  if (commitState.isCommitTimedOut()) {
    commit(Trigger.PARTIAL_TIMEOUT);
  } else if (bufferPaused && !commitState.isCommitInProgress()) {
    commit(Trigger.DRAIN);                       // drain what we have; no new StartCommit
  }
  if (bufferPaused && commitState.bufferedCount() < config.commitBufferMaxEvents() / 2) {
    bufferPaused = false;                        // hysteresis
    metrics.bufferResumed();
  }
}
```

This makes I8 a *bounded, lossless, visible* property: heap is capped, nothing is discarded, and the pressure surfaces as source-topic consumer lag — the one signal every Kafka shop already alerts on. (Contrast: dropping buffered envelopes would create a brand-new I2 loss path, because their source offsets are already durable via the worker transaction, `Channel.java:118-137`.)

### 5.7 File-by-file change list

**Modified**

| File | Change |
|---|---|
`CommitterImpl.java` | `open(...)`: delete leadership logic (`89-111`) → log only. `close(...)`: delete the leader-TP branch (`134-140`) and the unconditional stop for non-empty sets; wrap everything; replace `KafkaUtils.seekToLastCommittedOffsets(consumer())` (`144`) with bounded `rewindClosedPartitions()`. `save(...)`: `reconcile()` first, then `worker.save`, then `worker.process()`. **New:** `enum Role`, `static Role decide(...)`, `reconcile()`, `rewindClosedPartitions()`, `long promotionGapMs`, `int taskIndex`, `SinkMetrics metrics`. **Deleted:** `processControlEvents()`'s throw (`164`), `isProducerFenced` (`281-291`), the fenced catch (`196-214`), `COORDINATOR_STOP_TIMEOUT_MS` (`49`), `sourcePartitionCount(Set)` (`305-314`), `AtomicReference<TopicPartition> leaderTopicPartition` (`60`) → plain field. `startCoordinator(int)` → `startCoordinator(Set<String>)`; `stopCoordinator()` → `stopCoordinator(String reason)`; `startWorker()` now called from `reconcile`, not gated on a non-empty batch (`149-152`).
`Coordinator.java` | ctor: drop `int topicPartitionCount` (`94`), take `Set<String> sourceTopics` + `SinkMetrics`; move client construction to `start()`. `process()`: backpressure + `Trigger`. `receive()`: `isCommitReady(expectedAssignments())`. **New:** `enum TableCommitOutcome`, `StaleOffsetsException`, `expectedAssignments()` (refreshed), `updateSourceTopics(Set<String>)` (volatile), `lastProgressMs()`, `progress()`. `commitToTable` returns `TableCommitOutcome`; the two `NoSuchTableException`/UUID returns (`275-289`) become `TABLE_UNAVAILABLE`. `doCommit` gates `commitConsumerOffsets()`/`clearResponses()` (`242-243`). `commit(boolean)` → `commit(Trigger)` with the narrowed catch. `terminate()` uses `shutdown()` + bounded await. **Deleted:** `onPartitionsAssigned` (`138-143`), `partialCommitFailures`/`partialCommitFailureCount()` (`86, 456-458`) → metrics.
`Channel.java` | **Deleted:** `implements ConsumerRebalanceListener` (`46`), `onPartitionsAssigned`/`onPartitionsRevoked` (`168-172`), `seekToTrackedOffsets` (`174-191`), the `Admin` field/creation/close (`55, 84, 213`). **Changed:** clients created in `start()`; `assign()` instead of `subscribe(...)` (`203`); `send` transactional only when `transactional`; `commitConsumerOffsets` max-merges; `consumeAvailable` honours `canConsumeMore()`; ctor takes `boolean transactional`. `transactionalId(...)` (`89-96`) keeps only the worker branch.
`CommitState.java` | `receivedPartitionCount:int` → `Set<TopicPartition> receivedAssignments`; `isCommitReady(int)` → `isCommitReady(Set<TopicPartition>)`; `startTime` → `lastCommitStartedMs` with promotion semantics; add `bufferedCount()`.
`CoordinatorThread.java` | `finally { coordinator.terminate(); coordinator.stop(); }`; add `lastProgressMs()`, `updateSourceTopics(...)` delegation.
`Worker.java` | ctor: group-less consumer (`51-56`), `transactional = true`. `receive()`: record `lastForeignStartCommitMs` when the `StartCommit`'s `commitId` is not in the locally-emitted set; expose `lastForeignStartCommitMs()`. Add `Set<UUID> ownCommitIds` fed by the local coordinator (bounded ring of 16).
`KafkaClientFactory.java` | Split `createProducer(String)` → `createTransactionalProducer(String)` (unchanged, `44-59`) + `createIdempotentProducer()`. `createConsumer`: overload with no `group.id`; change `AUTO_OFFSET_RESET_CONFIG` from `putIfAbsent` (`67`) to a hard `put` for the coordinator (so `iceberg.kafka.auto.offset.reset=latest` can no longer silently skip every pending `DataWritten`), with a WARN if the user set it. Delete `createAdmin()` if no caller remains.
`KafkaUtils.java` | **Delete** `seekToLastCommittedOffsets` (`43-67`). **Keep** `consumerGroupMetadata` and `kafkaConsumer` (`39-41, 69-79`) — the reflective handle is mandatory for the worker's KIP-447 fence and is the only source of `subscription()`.
`IcebergSinkTask.java` | Delete the `flush(...)` override (`93-98`) — dead, because `preCommit`'s default body is the only caller of `flush` and this class overrides `preCommit` (`100-105`). Add one-line `start()` call to environment validation.
`IcebergSinkConfig.java` | Add 3 configs, change 1 default, deprecate 1 (§5.8). Add `int taskIndex()`, `long coordinatorStopTimeoutMs()`, `int standbyPromotionIntervals()`, `int commitBufferMaxEvents()`.

**New**

| File | Contents |
|---|---|
`channel/SinkMetrics.java` + `channel/SinkMetricsMBean.java` | ~150 lines. Per-task JMX MBean, `org.apache.iceberg.connect:type=sink,connector=<name>,task=<id>`. Reinstates and extends what commit `015a274a4c` deleted. |
`channel/TableCommitOutcome.java` | 5-value enum (could be nested; separate file keeps `Coordinator` shorter). |
`connect/EnvironmentValidator.java` | ~70 lines. `start()`-time validation + one-shot WARNs (§5.8). |

**Deleted**

| File | Reason |
|---|---|
`channel/NotRunningException.java` (`21`) | Nothing throws it any more. "Coordinator death = task death" is a deleted concept. |

### 5.8 Config surface

**New**

| Key | Type | Default | Validation | Purpose |
|---|---|---|---|---|
`iceberg.control.coordinator.standby-promotion-intervals` | INT | `3` | `Range.atLeast(0)`; `0` disables the standby role | `promotionGapMs = value × commitIntervalMs`. Also the no-progress watchdog threshold (A3) and the `TABLE_UNAVAILABLE` escalation threshold. |
`iceberg.control.coordinator.stop-timeout-ms` | LONG | `2000` | `Range.between(0, 30000)` | Replaces the hardcoded 60 000 `join` (`CommitterImpl.java:49`) and the 1-minute `awaitTermination` (`Coordinator.java:467`). Sized as a small fraction of `task.shutdown.graceful.timeout.ms` = 5000 **total for all tasks** (`WorkerConfig.java:95-100`). |
`iceberg.control.commit.buffer-max-events` | INT | `500000` | `Range.atLeast(1000)` | High-water mark for control-topic backpressure (I8). |

**Changed default**

| Key | Old | New | Why |
|---|---|---|---|
`iceberg.control.commit.max-consecutive-failures` | `1` (`IcebergSinkConfig.java:110`) | `5` | At 1, `isRetryable` (`Coordinator.java:221-227`) is dead code: the **first** retryable failure terminates the coordinator. |

**Deprecated**

| Key | Note |
|---|---|
`iceberg.control.group-id-prefix` (`IcebergSinkConfig.java:83, 100`) | Unused once the worker's control-topic consumer uses `assign()` with no `group.id`. Mark `@Deprecated`, log a WARN if set, remove in 2.0.0. This also retires the doc/code drift: `docs/docs/kafka-connect.md:80` documents `cg-control` while the code default is `cg-control-`. |

**Per-table commit time budget.** Set `commit.retry.total-timeout-ms` on each Iceberg commit operation from a new derived value `min(commitIntervalMs, 300000)` instead of inheriting Iceberg's 30-minute default (`TableProperties.java:99`), so one slow table cannot pin a pool thread for 6× `max.poll.interval.ms`. *(No new user-facing knob; derived.)*

**`start()`-time validation (`EnvironmentValidator`), all WARN-not-reject unless noted:**

1. Resolve the sink consumer's effective `partition.assignment.strategy`; if no assignor supports EAGER, **WARN** that `close()` becomes a delta and that buffered records for retained partitions depend on the bounded rewind. (Reachable: `connector.client.config.override.policy` defaults to `All`, `WorkerConfig.java:149-155`, and `consumer.override.*` is applied last, `Worker.java (Connect):910`.) Log the resolved protocol unconditionally so the next investigator need not re-derive it from Kafka source.
2. If `group.protocol=consumer` is set, **WARN**: not operable against a default broker in this fork (`GroupCoordinatorConfig.java:78` defaults `group.coordinator.rebalance.protocols` to `[classic]`; the resulting `UNSUPPORTED_VERSION` maps to `handleFatalFailure`, `HeartbeatRequestManager.java:405-411`).
3. If `iceberg.kafka.auto.offset.reset=latest`, **WARN** that the coordinator now forcibly uses `earliest` and why.
4. **REJECT** (`ConfigException`) if `standby-promotion-intervals > 0` and `taskIndex` cannot be parsed — the standby role would be silently inert.
5. Log the control topic's partition count and its `retention.ms`; **WARN** if `retention.ms < standbyPromotionIntervals × commitIntervalMs × 10`. Rationale: a worker's `DataWritten` and its source offsets are committed in **one** transaction (`Channel.java:118-137`), so control-topic durability *is* data durability between write and table commit.
6. **WARN** if the catalog implementation is `HadoopCatalog` over a non-atomic FS: every invariant here rests on `TableOperations.commit` being a true CAS.

### 5.9 Metrics (one gauge per invariant that can be violated)

| Metric | Type | Detects |
|---|---|---|
`coordinator-role` (0/1/2) | gauge | I7 — sum > 1 across tasks means overlap |
`is-leader-by-key` (0/1), `leader-key` | gauge/string | election divergence |
`coordinator-absent-ms` = `now − lastForeignStartCommitMs` on every non-coordinating task | gauge | **I4 — the primary alarm.** Do *not* alarm on control-topic lag or Connect offset metrics: with zero `StartCommit`s the control topic is idle and `offset-commit-success-percentage` reads 100 % (`WorkerSinkTask.java:467-471`) |
`ms-since-last-start-commit`, `ms-since-last-successful-commit` | gauge | I4 |
`start-commit-count`, `commit-round-duration-ms` | counter/histogram | I4, I6 |
`quorum-shortfall` (+ WARN naming the missing TPs) | gauge | I3, I4c — today a permanently unattainable quorum is invisible |
`superseded-count` | counter | I1/I7 — should be ~0; non-zero means real overlap |
`table-unavailable-count`, `table-unavailable-rounds` | counter/gauge | I2 — the loss path that used to be silent |
`retained-round-count`, `commit-buffer-events`, `commit-buffer-paused-ms` | counter/gauge | I8 |
`commit-failure-count`, `commit-state-unknown-count` | counter | I5, robustness |
`coordinator-restart-count`, `standby-promotion-count`, `abandoned-coordinator-count` | counter | I4, I6, I9 |
`rewind-failure-count` | counter | I5a residual |

Plus one structured log line per leadership transition carrying `(task, from → to, reason, leaderKey, assignment size)`, and one per coordinator start/stop with a reason enum.

---

## 6. Invariant table

| # | Invariant | Today | **What upholds it in the new design** | Residual risk |
|---|---|---|---|---|
**I1** | No duplicate committed data | UPHELD by Iceberg, not by leadership | Unchanged trio — offsets CAS (`Coordinator.java:340,354`; exact equality `394-398`; re-evaluated after `refresh()` each attempt, `SnapshotProducer.java:293-300,371-382,485-496`), per-table floor (`303-311`), `distinctByKey(location)` (`318,326`) — **plus** the CAS is now *load-bearing by design*: a lost CAS is `SUPERSEDED` and the loser re-drives instead of dying, and `assign()` guarantees every coordinator sees the whole control topic so no coordinator commits from a partial view | **(a) The floor fails OPEN.** Once the snapshot carrying `kafka.connect.offsets.*` leaves the ancestry (`expire_snapshots`/rollback/REPLACE), `SnapshotUtil.ancestorsOf` truncates silently (`SnapshotUtil.java:245-260`), `lastCommittedOffsets` → `Map.of()` (`Coordinator.java:433-441`), the filter admits everything and the validator passes on `{}.equals({})`. **Closed by Phase 6** (durable table-property floor). Until then: **explicitly accepted**, with `superseded-count` and a documented rule that `expire_snapshots` must retain at least one sink-written snapshot. **(b)** Non-CAS catalogs — WARN at `start()`. **(c)** Two live instances of one task index both answering the same `StartCommit` write different file locations, so `distinctByKey` cannot dedup — **accepted**, Connect provides no sink fencing (`DistributedHerder.java:1724-1726`) |
**I2** | No data loss | PARTIALLY; three holes | (i) worker-transaction atomicity unchanged (`Channel.java:118-137` + empty `preCommit`, `IcebergSinkTask.java:100-105`); (ii) **`TABLE_UNAVAILABLE` no longer advances the watermark or clears the buffer** — this closes the only unconditional, silent, unrecoverable loss path (`Coordinator.java:275-289` then `242-243`), reachable from any unrecognised REST 404 (`ErrorHandlers.java:141-155`); (iii) memory is bounded by **backpressure, not dropping** (§5.6.7), so I8's fix adds no loss path; (iv) coordinator's `auto.offset.reset=earliest` is now a hard `put`, closing the `latest` bypass (`KafkaClientFactory.java:67`); (v) control-topic retention validated against the absence budget | **(a)** By-design worker drops (dynamic routing to a missing table → `NoOpWriter`; null route value) — out of scope, but now metered. **(b)** Control-topic retention deleting `DataWritten` during an absence longer than the validated budget — bounded by I4 + the retention WARN. **Accepted.** |
**I3** | `valid-through-ts` never overstates | NOTHING | Set coverage over `(topic,partition)` from `DataComplete.assignments()` (§5.6.6, zero Avro change); **refreshed** expected set each round (replaces the frozen `totalPartitionCount`); **every assigned task** runs a `Worker` (A5) so its partitions can be reported; `validThroughTs` still forced null on any non-full-quorum trigger (`CommitState.java:146-151`) | Expected set under `topics.regex`/dynamic routing is derived from the leader's `subscription()` pushed via `updateSourceTopics`; a stale leader view can make the set too small for one round. Bounded by the metadata refresh; metered by `quorum-shortfall`. **Accepted** |
**I4** | A coordinator exists and emits `StartCommit` at bounded intervals | NOTHING (3 structural holes) | (a) role is a **level**, reconciled every `save()` ≤ 60 s, so the fenced/terminated coordinator is *reaped and re-elected* (A2) instead of waiting for a later `open()`; (b) the clock-reset stall is gone — promotion commits immediately (§5.4); (c) every assigned task has a `Worker` (A5); (d) the **control-topic watchdog** bounds every remaining cause at `standbyPromotionIntervals × commitIntervalMs` (§5.3 table); (e) `coordinator-absent-ms` makes it observable | Both the leader-key owner **and** task 0 simultaneously gone → bounded only by Connect reassignment (`scheduled.rebalance.max.delay.ms` = 300 000, `DistributedConfig.java:187-192`). **Explicitly accepted:** covering it needs a second election substrate, which loses axis 1; it is alarmed by `coordinator-absent-ms` |
**I5** | Benign churn must never fail a task | NOTHING (6 paths) | (a) the unguarded `consumer.committed()` is gone from the callback → bounded 5 s inside a total try/catch; (b) no `contains(null)` — the level read uses `assignment.contains(leaderKey)` with an explicit null guard; (c) no `partitionsFor` in a callback; (d) no `initTransactions()` in a callback *or* on the task thread (moved to `Coordinator.start()`), and the coordinator has no transaction at all; (e) lost CAS → `SUPERSEDED`; (f) `max-consecutive-failures` default 5, `commitSync` under `assign()` cannot raise the eviction `CommitFailedException` (`ConsumerCoordinator.java:1288`); (g) `open()`/`close()` bodies are **total** — no `Throwable` escapes, so the paired-`open()`-skipped trap (`WorkerSinkTask.java:779`) is unreachable | A `WakeupException` can still abort a callback mid-body (`ConsumerRebalanceListenerInvoker.java:67-68,93-94`) — it propagates as designed, and teardown is idempotent and re-drivable from `save()` (R2) |
**I6** | Callback work bounded well below the rebalance/poll budget | NOTHING (~180 s reachable) | `open()`/`close()` are O(1) plus one 5 s bounded RPC; coordinator start is object allocation + `Thread.start()`; teardown is `stop-timeout-ms` (default 2000) and **correctness does not depend on it completing** — an abandoned coordinator is neutralised by the CAS, not by `join()`. Budget is stated against the binding constraint (5000 ms **total for all tasks**, `WorkerConfig.java:95-100`), not against `max.poll.interval.ms` | A pathological `committed()` that ignores its timeout; metered by `rewind-failure-count` |
**I7** | At most one *effective* control plane, or a provably-correct aggregator when several exist | PARTIALLY and lazily | Reframed as **"overlap degrades to duplicated work"** and enforced three ways: (a) `assign()` means every coordinator sees **all** control partitions — no split view, so readiness/watermark logic always has a complete view (this is the actual I7 fix; the old fixed `transactional.id` never provided it); (b) the CAS makes at most one coordinator's snapshot land, and the loser re-drives; (c) `commitConsumerOffsets` is monotone, so a slower coordinator cannot rewind the group | Overlapping coordinators duplicate work: extra `StartCommit`s force extra file rolls (smaller files) and the loser leaves **orphan data files** — nothing in kafka-connect ever deletes a data file. **Accepted**, with a documented `remove_orphan_files` cadence and `superseded-count` as the alarm |
**I8** | Coordinator memory bounded across repeated failures | NOTHING | High-water mark → **pause control-topic consumption and suppress `StartCommit`** (§5.6.7). Heap capped at `buffer-max-events`; nothing dropped; pressure surfaces as source-topic lag. `endCurrentCommit()` still clears `readyBuffer`/`receivedAssignments` every round | Retaining on `SUPERSEDED`/`TABLE_UNAVAILABLE` fills the buffer faster than before → the pause fires sooner. That is the intended, visible behaviour |
**I9** | No leaked threads or clients per generation | NOTHING | (a) `CoordinatorThread.run()`'s `finally` always calls `terminate()` then `stop()`, so a self-terminated coordinator can no longer skip pool shutdown (`CommitterImpl.java:235-237`); (b) `terminate()` uses `shutdown()` so queued `FutureTask`s **run** and complete, ending the 100 Hz `Tasks.waitFor` spin (`Tasks.java:474-533`); (c) `allowCoreThreadTimeOut(true)`; (d) the unused per-`Channel` `Admin` is deleted (`Channel.java:84,213` were its only references — ~4 idle clients per 4-task connector); (e) the worker's per-rebalance **consumer group** disappears (`Worker.java:51-56`); (f) generations are now rare, because a rebalance no longer creates one | An abandoned coordinator (teardown timed out) still holds its clients until it exits. Metered by `abandoned-coordinator-count`. **Accepted** — the alternative is a 60 s block on the task thread |

---

## 7. Failure-mode catalogue

| # | Scenario | Old behaviour | New behaviour | Bounded by |
|---|---|---|---|---|
1 | Unrelated task joins/leaves → sink-group rebalance | `close(FULL)`+`open(FULL)` → coordinator destroyed & rebuilt; clock reset; epoch stolen; ~180 s of work in the callback | **Non-event.** `open`/`close` log; `save()` sees the same `leaderKey` in `assignment`; no teardown, no election, no clock reset, no epoch bump | O(1) |
2 | Sink group rebalances more often than `commitIntervalMs` | **Zero commits, forever, silently.** Source lag + orphan files grow superlinearly | Coordinator survives; even if it is rebuilt, promotion commits immediately | `commitIntervalMs` |
3 | Leader worker crashes | TP0 moves → new leader → first commit one full interval later | TP0 moves → T1 on a survivor → immediate commit | sink `session.timeout.ms` 45 000 + rebalance |
4 | Leader task's new assignment is empty (`close` with no `open`) | Coordinator stopped, never restarted on that task; absence until some other rebalance | T1 false → A4; the new TP0 owner's T1 fires | ≤ 60 s tick |
5 | Coordinator thread dies (any non-fenced cause) | `NotRunningException` → **task FAILED, never auto-restarted** | A2: ERROR + metric + re-elect on the same task | ≤ 60 s tick |
6 | Coordinator hangs in a 30-min catalog commit | Nothing happens; `-coord` member evicted past `max.poll.interval.ms` → `CommitFailedException` → with `max-consecutive-failures=1` the coordinator and task die | No eviction (`assign()`); per-table budget capped; A3 restarts it; task 0 promotes as standby | `promotionGapMs`; per-table `commit.retry.total-timeout-ms` |
7 | Two coordinators overlap (zombie instance, or `topics.regex` divergence) | Split control-topic view → one sees 0 partitions yet still emits `StartCommit`; loser eventually fenced up to 5 min later; catalog commit unprotected | Both see all partitions; the CAS picks one winner; loser gets `SUPERSEDED` → retains + re-drives; watermark monotone | Iceberg CAS + one commit round |
8 | Loser of the offsets CAS | `ValidationException` → non-retryable → **task FAILED** | `SUPERSEDED` INFO + metric + retain + re-drive | one commit round |
9 | Same, on the **timeout** trigger | Swallowed into an unexported counter → silent forever-stall | Same `SUPERSEDED` handling; blanket swallow narrowed | one commit round |
10 | Table dropped, or REST returns an unrecognised 404 | `commitToTable` returns normally → watermark advances → buffer cleared → **permanent silent loss** | `TABLE_UNAVAILABLE`: retain, no advance, metric, ERROR after K rounds | K rounds, then operator |
11 | `CommitStateUnknownException` | Non-retryable → coordinator dies (quorum path) / swallowed (timeout path) | Explicit branch: retain buffer + watermark, ERROR, next round re-reads the floor and self-heals | one commit round |
12 | Catalog outage for 30 min | `commitBuffer` grows unbounded → OOM of the Connect worker, taking down all other connectors | Buffer capped; control-topic consumption pauses; `StartCommit` suppressed; source lag grows | `buffer-max-events` |
13 | Broker hiccup during `close()` | `consumer.committed()` `TimeoutException` → `rebalanceException` → **task FAILED** | Bounded 5 s, caught, metered, WARN | 5 s |
14 | Worker task paused via REST | Consumer paused but `put(empty)` still runs (`WorkerSinkTask.java:625-635`) — undefined for the coordinator | **Specified:** a paused task keeps reconciling and keeps coordinating, so uncommitted `DataWritten` is still drained (its source offsets are already durable). Documented + tested | — |
15 | `tasks.max > partitions` (idle tasks) | Idle tasks get **neither** callback and create no `Worker`; their partitions still count toward the frozen quorum → quorum permanently unattainable → every round partial, `validThroughTs` always null | Set-coverage over reported assignments; assigned tasks always have a `Worker`; task 0 always does | — |
16 | Source topic gains partitions | Frozen count too low → premature quorum → **overstated `valid-through-ts`** | Expected set refreshed each round | one round |
17 | Connector reconfigured | All tasks stop/restart (`DistributedHerder.java:786-800`); coordinator rebuilt; `txnSuffix` regenerated (`IcebergSinkConnector.java:52`) un-fencing old worker ids | Same restart, but the first commit is immediate; the coordinator has no `transactional.id` to abandon (worker ids unchanged — the reconfig duplicate-window remains, §11 Q8) | `commitIntervalMs` |
18 | Rolling restart of the whole worker fleet | One skipped commit interval **per worker**, silently | At most one skipped round per leadership move, and each promotion commits immediately | `commitIntervalMs` |
19 | Worker shutdown (5000 ms **total** budget) | ~120 s teardown → herder proceeds while a heartbeating, partition-owning zombie remains | ≤ `stop-timeout-ms` (2000) then deliberate abandonment; correctness independent of it | 2000 ms |
20 | `expire_snapshots` removes the last sink snapshot, then the control topic is replayed | Floor → `Map.of()`, validator passes on `{}.equals({})` → **duplicate rows**, possibly referencing files orphan-cleanup deleted | Unchanged until **Phase 6**; then the durable table property is the floor and `expire_snapshots` is harmless | Phase 6 |
21 | Operator sets `consumer.override.partition.assignment.strategy=CooperativeStickyAssignor` | Predicates silently stop firing; behaviour changes with no signal | Level read is unaffected; `start()` logs the resolved protocol and WARNs | — |
22 | Both the leader-key owner and task 0 are gone | Indefinite absence, invisible | Indefinite absence, **alarmed** by `coordinator-absent-ms` | Connect reassignment (300 000) |

---

## 8. Deletion list

**Concepts deleted (the primary axis)**

1. **Edge-triggered election.** No predicate over a callback argument anywhere.
2. **"Coordinator death = task death."** `NotRunningException` file removed; `processControlEvents` cannot throw.
3. **The coordinator's Kafka transaction**, its fixed `transactional.id`, `initTransactions()` on the promotion path, the per-rebuild epoch steal, and `isProducerFenced` — i.e. root cause #4, deleted rather than worked around.
4. **Consumer-group membership for the control topic** (both channels) — and with it the rebalance listener, `seekToTrackedOffsets`, the split-view hazard, the eviction-by-slow-catalog hazard, and the "control topic must have exactly 1 partition" implicit requirement.
5. **A consumer group per task per rebalance** (`Worker.java:51-56`).
6. **The frozen readiness quorum** (`totalPartitionCount` + `sourcePartitionCount` + its `partitionsFor` loop).
7. **Blocking work inside a rebalance callback** — all of it.
8. **The 60 s `join` and the 1-minute `awaitTermination`** as correctness dependencies.
9. **The unused per-`Channel` `Admin` client.**
10. **`IcebergSinkTask.flush`** (provably dead).
11. **`iceberg.control.group-id-prefix`** (deprecated → removed in 2.0.0), which also retires the `cg-control` vs `cg-control-` doc drift.

**Runtime objects deleted per connector (4 tasks, 1 leader)**

| | Before | After |
|---|---|---|
Consumer groups with membership | 2 (`connect-<c>`, `<c>-coord`) + 1 per task per rebalance | **1** (the Connect sink group). `<c>-coord` survives as an offset store with no members |
Kafka clients on the leader task | 7 (framework consumer; worker producer/consumer/admin; coordinator producer/consumer/admin) | **4** (framework consumer; worker producer/consumer; coordinator producer/consumer) — and the coordinator's producer is no longer transactional |
Idle `Admin` clients per connector | 4–5 | **0** |
`initTransactions()` calls per rebalance | 1 per task (worker) + 1 (coordinator) | 1 per task (worker) only |
Threads on the leader task | 2 + `commitThreads` | 2 + `commitThreads`, but now with `allowCoreThreadTimeOut` and no per-generation leak |

**Line accounting (honest)**

| Area | Δ lines |
|---|---|
`CommitterImpl` (−lifecycle, +reconcile/role/rewind) | ≈ −70 / +95 |
`Channel` (−listener, −seek, −admin, +assign/monotone/backpressure hook) | ≈ −60 / +40 |
`Coordinator` (−onPartitionsAssigned/−partialCommitFailures, +outcomes/expected set/progress/narrowed catch) | ≈ −35 / +95 |
`CommitState` (int → set, clock) | ≈ −20 / +35 |
`CoordinatorThread`, `Worker`, `KafkaClientFactory`, `IcebergSinkTask` | ≈ −45 / +55 |
`KafkaUtils.seekToLastCommittedOffsets`, `NotRunningException.java` | ≈ −50 / 0 |
`SinkMetrics` + MBean (**new**) | 0 / +150 |
`TableCommitOutcome`, `EnvironmentValidator` (**new**) | 0 / +85 |
**Total** | **≈ −280 / +555 ⇒ net +275** |

**So the net is NOT a code deletion, and I will not pretend otherwise.** Of the +555, **235 lines are the two new leaf classes** (metrics + validation), which exist only because the user's own axis 3 requires that "absence of a coordinator must be bounded and **observable**" and because axis 3 requires invariants held by *enforced mechanism* rather than assumption — and today the module has **zero** metrics (commit `015a274a4c` deleted the only MBean). Excluding those two leaf classes the change is ≈ −280 / +320, i.e. flat. The genuine simplification is in the **concept** count and the **runtime object** count above: one reconcile point instead of three lifecycle entry points, one group instead of three-plus, one fewer transactional identity, one fewer client per task, and six invariants moved from "held by convention" to "held by a named mechanism". If a reviewer insists on a strict LOC deletion, the metrics MBean is the only cuttable item, and cutting it makes the design unverifiable in production — I recommend against it.

---

## 9. Phased rollout

Each phase is independently shippable, default-safe, and revertible by reverting its own commit. **No phase changes a `Committer` SPI signature** (`open(Catalog, IcebergSinkConfig, SinkTaskContext, Collection)` / `close(Collection)` are the existing methods, `Committer.java:36-54`) and **no phase touches the `kafka-connect-events` Avro protocol**. RevAPI does not cover `iceberg-kafka-connect`, so the SPI/Avro contracts are policed by review only — hence the explicit statement.

| Phase | Contents | Default-safe? | Rollback | SPI / Avro / deprecation |
|---|---|---|---|---|
**P0** | `SinkMetrics` MBean + structured leadership/coordinator log lines + `EnvironmentValidator` (WARN-only). No behaviour change. | Yes — pure observability | Revert 3 files | none |
**P1** | Tri-state `TableCommitOutcome`; `StaleOffsetsException`; watermark/buffer gated on all-settled; `TABLE_UNAVAILABLE`; narrowed `commit()` catch on **both** triggers; explicit `CommitStateUnknownException` branch; `max-consecutive-failures` 1 → 5; derived per-table `commit.retry.total-timeout-ms`. | Yes — strictly fewer task failures and strictly less loss | Revert `Coordinator.java` | **Renegotiates** `TestCoordinator.testCommitFailedExceptionPropagates` (it asserts today's fail-fast). Deliberate, documented in the PR. |
**P2** | Level-triggered reconcile: `open`/`close` become O(1)+total; `decide()`; A1–A5 minus the standby; client construction moved into `Coordinator.start()`; delete the fenced catch, `isProducerFenced`, `NotRunningException`, `COORDINATOR_STOP_TIMEOUT_MS`, `sourcePartitionCount`; bounded rewind; `stop-timeout-ms` config; promotion-immediate clock; `CoordinatorThread` finally-terminate; `exec.shutdown()`; `allowCoreThreadTimeOut`; delete `IcebergSinkTask.flush`. | Yes — same election key, same semantics, fewer failures | Revert `CommitterImpl/Coordinator/CoordinatorThread/CommitState/IcebergSinkTask` | none |
**P3** | Control-topic watchdog + `STANDBY` role + `standby-promotion-intervals` (default 3; `0` disables). Worker tracks foreign `StartCommit`. | Yes, **because P1 landed** — the watchdog deliberately manufactures overlap and P1 is what makes overlap benign. Ship `0` first if a reviewer prefers opt-in. | Set `standby-promotion-intervals=0` (no redeploy), or revert | none |
**P4** | `assign()` for both control-topic channels; group-less worker consumer; delete `Channel`'s listener/`seekToTrackedOffsets`/`Admin`; monotone `commitConsumerOffsets`; coordinator producer becomes idempotent (no transaction); hard `put` of `auto.offset.reset=earliest`; deprecate `iceberg.control.group-id-prefix`. | Yes **with a migration note**: during a mixed-version window an old coordinator is still a member of `<c>-coord`, so the new coordinator's simple `commitSync` can return `UNKNOWN_MEMBER_ID`. That is now non-fatal (retain + retry) and resolves as soon as the old task stops; a connector restart makes the window a few seconds. | Revert `Channel/KafkaClientFactory/Worker` | **Deprecates one config** (WARN + `@Deprecated`, remove in 2.0.0). No Avro change. |
**P5** | Set-coverage readiness; refreshed expected set; `Worker` for every assigned task and for task 0; `quorum-shortfall` metric + missing-TP WARN; control-topic backpressure + `buffer-max-events`. | Yes — quorum becomes *harder* to reach, so `valid-through-ts` becomes conservative rather than optimistic | Revert `CommitState/Coordinator/CommitterImpl` | Uses `DataComplete.assignments()`, **no Avro change** |
**P6** | Durable offsets floor: write the merged offsets map as a **table property** in the same metadata commit as the snapshot (`table.newTransaction()` → append + `updateProperties()` → `commitTransaction()`), and use `max(tableProperty, ancestryValue)` as the floor and as the CAS `expected`. Closes I1(a). | Yes — the property is additive and the ancestry path still works if it is absent | Stop writing the property; the ancestry floor remains | New **table property** is user-visible (documented). No Avro/SPI change. **UNVERIFIED:** that `SnapshotAncestryValidator` semantics are preserved when the `SnapshotProducer` runs inside a `Transaction` — must be proven by test before merge (§11 Q3) |
**P7** | Docs: `docs/docs/kafka-connect.md` — coordinator runbook (`coordinator-absent-ms` alarm, `kafka-consumer-groups --describe --group connect-<c>` to find the TP0 owner, `remove_orphan_files` cadence, `expire_snapshots` retention rule), fix the `cg-control`/`cg-control-` drift, drop the "`--partitions 1`" requirement now that `assign()` handles N partitions. | Yes | Revert | none |

**Recommended merge order:** P0 → P1 → P2 → P5 → P3 → P4 → P6 → P7. (P5 before P3 so the standby's promotions are observable through the quorum metrics; P4 after P3 so the watchdog is validated against today's group-based channel first.)

---

## 10. Test plan

Existing classes to extend (absolute paths under `/Users/pritamkumar863/Desktop/apdp/work/personal/open_source_forks/iceberg/kafka-connect/kafka-connect/src/test/java/org/apache/iceberg/connect/`):
`channel/TestCommitterImpl.java`, `channel/TestCoordinator.java`, `channel/TestCommitState.java`, `channel/TestChannel.java`, `channel/TestWorker.java`, `channel/TestCoordinatorThread.java`, `channel/ChannelTestBase.java`, `TestIcebergSinkConfig.java`, `TestIcebergSinkTask.java`; and integration tests under `/Users/.../kafka-connect/kafka-connect-runtime/src/integration/java/org/apache/iceberg/connect/` (`IntegrationTestBase.java`, `TestIntegration.java`, `KafkaConnectUtils.java`).

| Invariant | Test | Where | What would fail today |
|---|---|---|---|
I4 / root cause | **`testRebalanceWithRetainedLeaderKeyDoesNotRebuildCoordinator`**: `open(FULL)`, capture the `CoordinatorThread` identity, `close(FULL)`, `open(FULL)`, `save()`; assert the *same* instance and that `CommitState.lastCommitStartedMs` was not reset | `TestCommitterImpl` | Today `close` nulls the field (`CommitterImpl.java:232`) and `open` builds a new one |
I4 | **`testCommitClockSurvivesPromotion`** / **`testFreshCoordinatorCommitsImmediately`**: fake clock, `commitIntervalMs=300000`, brand-new `CommitState` → `isCommitIntervalReached()` true on the first call | `TestCommitState` | Today it is false for 300 s (`CommitState.java:81-88`) |
I4 (**the outage**) | **`testCommitsContinueUnderRebalanceStorm`** (IT): `commitInterval=60s`, drive a rebalance every 20 s (start/stop a second connector on the same group), assert ≥ 2 snapshots in 3 min | `TestIntegration` | Today: zero snapshots, zero errors |
I4 | **`testStandbyPromotesWhenNoForeignStartCommit`** and **`testStandbyDemotesOnForeignStartCommit`**: fake clock + injected `lastForeignStartCommitMs` | `TestCommitterImpl` (pure `decide()` + reconcile) | new |
I4 | **`testTerminatedCoordinatorIsReElectedNotPropagated`**: replaces `testTerminatedCoordinatorFromOtherErrorFailsTask`, which currently asserts `NotRunningException` | `TestCommitterImpl:148-167` | contract change, deliberate |
I4 | **`testNoProgressRestartsCoordinator`**: stub `lastProgressMs()` in the past | `TestCommitterImpl` | new |
I1/I7 | **`testSupersededCommitRetainsBufferAndWatermark`**: validator throws `StaleOffsetsException`; assert no `commitSync`, no `clearResponses`, `superseded-count == 1`, and that the **next** round commits the non-dominated envelopes | `TestCoordinator` | Today: `ValidationException` propagates and kills the thread |
I1/I7 | **`testSupersededOnTimeoutTriggerAlsoRetains`** — the same on `commit(PARTIAL_TIMEOUT)` | `TestCoordinator` | Today the partial path swallows it (`167-176`) |
I2 | **`testNoSuchTableDoesNotAdvanceWatermark`** and **`testUuidMismatchDoesNotClearBuffer`** | `TestCoordinator` | Today both advance and clear (`275-289` → `242-243`) |
I2 | **`testCommitStateUnknownRetainsEverything`** | `TestCoordinator` | Today fatal / swallowed |
I3 | **`testDuplicateDataCompleteDoesNotReachQuorumEarly`**: two identical `DataComplete`s for the same `commitId`; assert not ready and `validThroughTs` is the min over the **full** set | `TestCommitState` | Today `receivedPartitionCount` double-counts (`CommitState.java:69`) |
I3 | **`testExpectedSetRefreshesOnPartitionGrowth`** | `TestCoordinator` | Today frozen at promotion |
I3/I4c | **`testIdleAssignedTaskParticipatesInQuorum`** (IT): `tasks.max=4`, 2 source partitions; assert `valid-through-ts` is published | `TestIntegration` | Today quorum is unattainable (`CommitterImpl.java:149-152`) |
I5 | **`testCloseSurvivesCommittedTimeout`**: `consumer.committed(...)` throws `TimeoutException`; assert `close()` returns normally and the metric increments | `TestCommitterImpl` | Today it becomes `rebalanceException` → FAILED |
I5 | **`testCloseWithoutOpenAndOpenWithoutClose`**: `close(FULL)` with no prior `open`; `open(FULL)` twice; assert no NPE and one coordinator | `TestCommitterImpl` | Today `contains(null)` NPEs |
I5 | **`testTaskWithEmptyAssignmentNeverCallbacked`**: only `save()` is called; assert task 0 still ticks and can promote | `TestCommitterImpl` | new |
I5 | **`testLostPartitionsCloseWithNoPreCommit`** (IT): force a session-timeout generation reset | `TestIntegration` | new |
I6 | **`testCloseIsBoundedAndNonBlocking`**: `Coordinator.terminate()` blocks 10 s; assert `close()` returns within `stop-timeout-ms + ε` | `TestCommitterImpl` | Today ~120 s |
I6 | **`testOpenPerformsNoKafkaRpc`**: `MockConsumer`/`MockProducer` strict-verify that `open()` issues no `partitionsFor`/`committed`/`initTransactions` | `TestCommitterImpl` | Today all three are reachable |
I7 | **`testTwoCoordinatorsBothSeeAllControlPartitions`**: two `Coordinator`s over a 3-partition mock control topic; assert both `assign()` all 3 | `TestChannel`/`TestCoordinator` | Today they share a group and split |
I7 | **`testWatermarkNeverMovesBackward`**: coordinator A commits 150, coordinator B (started earlier, at 120) commits; assert no `commitSync` below 150 | `TestChannel` | new |
I7 | **`testTwoCoordinatorsProduceOneSnapshot`** (IT): start two coordinators deliberately; assert exactly one snapshot per round, zero task failures, `superseded-count > 0` | `TestIntegration` | Today one task goes FAILED |
I8 | **`testBufferHighWaterMarkPausesConsumptionAndSuppressesStartCommit`**; assert no envelope is discarded | `TestCoordinator` | Today unbounded |
I9 | **`testSelfTerminatedCoordinatorShutsDownPool`**: assert `exec.isTerminated()` after a self-termination | `TestCoordinatorThread` | Today the `!isTerminated()` guard skips it |
I9 | **`testTerminateWithMoreTablesThanThreadsDoesNotSpin`**: `commitThreads=1`, 4 tables, terminate mid-fan-out; assert the thread exits within `stop-timeout-ms` | `TestCoordinator` | Today it spins at 100 Hz forever |
I1(a)/P6 | **`testFloorSurvivesSnapshotExpiry`** (IT): commit, `expire_snapshots`, replay the control topic; assert **no duplicate rows** | `TestIntegration` | Today duplicates commit because the CAS degenerates to `{}.equals({})` |
config | **`testRejectsUnparseableTaskIdWhenStandbyEnabled`**, **`testWarnsOnCooperativeAssignor`**, **`testCoordinatorAutoOffsetResetCannotBeLatest`** | `TestIcebergSinkConfig` | new |
misc | **`testPausedTaskKeepsCoordinating`** (IT): pause the connector, assert an in-flight round still commits | `TestIntegration` | unspecified today |
misc | **`testRollingRestartSkipsAtMostOneRound`** (IT) | `TestIntegration` | Today one skipped interval per worker |

`ChannelTestBase` needs: a `MockConsumer` seeded with `partitionsFor` for `assign()`, an injectable `Clock`, and removal of the `Admin`/`describeTopics` stubbing (`ChannelTestBase.java:107-116`) once `Channel`'s `Admin` is gone. `TestChannel`'s three `transactionalId` tests (`TestChannel.java:27-50`) shrink to the worker cases only.

---

## 11. Open questions (maintainer decisions)

**Q1. `assign()` for the `-coord` consumer — accept the mixed-version window?**
During a rolling upgrade, an old coordinator is still a *member* of `<c>-coord`, so a new coordinator's simple `commitSync` (generation `NO_GENERATION`, `ConsumerCoordinator.java:1288`) can be rejected with `UNKNOWN_MEMBER_ID` until the old task stops. **Recommendation: accept.** The failure is now non-fatal (retain + retry, P1), the window closes when the connector's tasks finish restarting, and the alternative — keeping `subscribe()` — leaves the split-view (I7) and eviction (I5f) hazards in place. Document it in the P4 PR and in `kafka-connect.md`.

**Q2. Standby = `task.id == 0`, or a dedicated election topic?**
**Recommendation: task 0.** It is deterministic, needs no new group/thread/client/config beyond the interval knob, is legible to operators (`GET /connectors/<c>/tasks/0/status`), and the primary path (TP0 ownership) already gives fast failover. An election topic would give a tunable failover SLO and cover the "leader owner **and** task 0 both gone" case, at the cost of a subsystem. Revisit only if `coordinator-absent-ms` alarms in practice for that specific reason.

**Q3. P6: is `SnapshotAncestryValidator` sound inside a `Transaction`?**
`validateWith` is on `SnapshotUpdate` (`SnapshotUpdate.java:91` → `SnapshotProducer.java:189-190`) and runs in `apply()` (`293-300`). Whether `BaseTransaction`'s deferred single metadata commit preserves the "validate against refreshed state, atomically with the commit" property is **UNVERIFIED — I did not read `BaseTransaction`.** **Recommendation:** gate P6 on a test that proves the validator still rejects a concurrently-advanced floor when the append runs inside a transaction. If it does not, fall back to a second `updateProperties()` commit and accept 2 metadata commits per table per round (and re-evaluate against catalog quota).

**Q4. Reject or warn on a purely-cooperative assignor?**
**Recommendation: warn.** The level read makes leadership correct under either protocol; only the `close()`-time rewind semantics differ, and rejecting would break existing users who set the override for unrelated reasons. Log the resolved protocol once at `start()` regardless.

**Q5. `max-consecutive-failures` 1 → 5?**
**Recommendation: yes.** At 1 the entire `isRetryable` classifier (`Coordinator.java:221-227`) is dead code, and with P2 a terminated coordinator is re-elected rather than fatal, so a higher budget can no longer strand the connector.

**Q6. Immediate commit on promotion, or derive the deadline from durable state?**
**Recommendation: immediate**, after a settle of `min(commitIntervalMs, 5000)` ms. It is 4 lines and removes the outage. The cost — a small extra snapshot per promotion — is bounded because promotions become rare (a rebalance no longer causes one). Deriving the deadline from the last snapshot timestamp would need a catalog read per table at promotion and is not worth it.

**Q7. Buffer overflow: pause (backpressure) or drop?**
**Recommendation: pause.** Dropping buffered envelopes would create a brand-new I2 loss path, because their source offsets are already durable (`Channel.java:118-137`). Pausing converts an OOM into visible source lag.

**Q8. Worker-side duplication on reconfiguration — in scope?**
`IcebergSinkConnector.taskConfigs` mints a fresh `txnSuffix` UUID on every call (`IcebergSinkConnector.java:52`), so every reconfiguration changes every **worker** `transactional.id` and un-fences in-doubt transactions from the previous generation → the same rows can be written twice at different locations, which `distinctByKey(location)` cannot dedup. **Recommendation: out of scope for this document, tracked separately;** the fix is to derive the suffix deterministically (e.g. from the connector name) rather than randomly, which is a one-line change with its own compatibility discussion.

**Q9. Deprecate `iceberg.control.group-id-prefix`?**
**Recommendation: yes** — it becomes unused, and deprecating it is the moment to fix the `cg-control` / `cg-control-` doc drift.

**Q10. Branch target.**
**Recommendation: `bugfix/split_brain_fix` (`f70ddecf52`)**, which is what the tree is on and what every citation here is pinned to. See §12.

**Q11. Should the coordinator's producer really lose its transaction?**
This is the most opinionated deletion in the document. It is safe *given* P1 + P4 (CAS safety + no split view) and given that nothing consumes `CommitToTable`/`CommitComplete`. If a maintainer wants to keep a control-plane fence, the cost is: `initTransactions()` back on the coordinator-thread startup path (not the task thread, so no I6 regression) plus the epoch-steal flap on every promotion. **Recommendation: delete it**, and revisit only if a future consumer of `CommitToTable` appears.

---

## 12. Branch caveat: what changes if the target is `parallel_control_processing`

During the investigation the working tree was switched off `bugfix/split_brain_fix`; it is currently on `parallel_control_processing` (`ca1fe06257`, a merge of `main`), which is **not** a descendant of `f70ddecf52`. Every line number in this document was read via `git show f70ddecf52:<path>` and is accurate for that commit only. If the maintainer retargets to `parallel_control_processing`:

| Difference on `parallel_control_processing` | Effect on this design |
|---|---|
`IcebergSinkConnector.taskConfigs` writes `txnSuffix + i`, so the **coordinator** `transactional.id` is per-task-unique and two coordinators cannot fence each other at all | **Neutral-to-positive.** This design deletes the coordinator transaction anyway (§5.6.4), so the divergence becomes moot. Do **not** attempt to restore a fixed coordinator `transactional.id` on that branch as a "fix". |
`Channel.controlTopicOffsets` uses `put(...)` instead of `merge(..., Long::max)` and is a `ConcurrentMap` | **Must be corrected before P4.** The monotone `commitConsumerOffsets` in §5.6.4 assumes `controlTopicOffsets` is itself monotone; with `put` a re-read can lower an entry. Restore `merge(Long::max)`. The `ConcurrentMap` is harmless and mildly preferable, since the map is handed by reference to up to `commitThreads` pool threads inside `commitToTable`. |
Admin-based election | The whole of §5.3 replaces it. The `Admin`-client deletion in §5.7 becomes larger, not smaller. |
Line numbers | **Every citation in §5.7 must be re-derived.** Do not port the change list mechanically. |

---

## 13. Summary of what this document commits to

- One recommended design, chosen on **simple > rebalance-independent > robust > upstreamable**: leadership as a level read from the current assignment on the task thread; safety from the Iceberg CAS; absence bounded by a watchdog built on the control topic the connector already consumes.
- Every judge-identified fatal flaw is either fixed in the text (the "skip vs `SUPERSEDED`" trap; the partial-path blanket swallow; the null-key `contains`; the `WakeupException` teardown ordering; the backward watermark; the split view; the frozen quorum; the two resource leaks; the loss-on-table-missing) or **explicitly accepted with a reason** (fail-open floor until P6; two-live-instances of one task index; both-leader-and-task-0-gone; orphan files from duplicated work; expected-set staleness under `topics.regex`).
- Every `NOTHING` in the digest's invariant table becomes a named mechanism or an accepted risk with a rationale (§6).
- Net **concepts** and net **runtime objects** go down; net **LOC** is flat once the two new observability/validation leaf classes are excluded, and I say so rather than claiming a deletion I cannot show.