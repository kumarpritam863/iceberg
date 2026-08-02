# Decoupling Coordinator Election from Source Topic-Partition Assignment

> Research + design exploration for the Iceberg Kafka Connect sink (`pluggable-committer` branch).
> Goal: make the commit coordinator **robust** (strong at-most-one-leader), **resilient** (fast, clean
> failover; low churn), and **error-free** (no weakening of exactly-once), shipped incrementally and
> backward-compatibly behind the existing `iceberg.committer.class` SPI.

## Executive summary

The sink elects its single commit coordinator **implicitly**: whichever sink task owns the
globally-lowest `(topic, partition)` of the **source** consumer group runs the `Coordinator`. Both the
election *and* the commit-readiness quorum are derived from a transient, point-in-time view of
source-partition assignment. This double coupling is the root cause of the documented fragility.

The problem decomposes into **two orthogonal axes**:

- **(A) Leader-election decoupling** — choose *who* coordinates independently of source-partition
  assignment, and fence a zombie leader.
- **(B) Membership-independent commit-readiness** — decide *which* partitions a commit must cover from
  authoritative cluster metadata, not a frozen membership snapshot.

**Recommendation — the "Fenced Election Committer":** a new opt-in `ElectionTopicCommitter` that
combines (A1) election by ownership of the **single partition of a configured election topic**
(`iceberg.coordinator.election-topic`, default: the control topic; validated single-partition) in a
dedicated election consumer group (Kafka gives at-most-one-owner + auto-failover for free), (A2) a **fixed
coordinator `transactional.id`** that epoch-fences a zombie's control-plane writes, and (B) a
**set-coverage readiness** check (`receivedSet.containsAll(expectedSet)` where `expectedSet` comes from
`Admin.describeTopics`, refreshed each interval). It preserves every exactly-once mechanism verbatim and
ships in four reversible, default-off phases.

---

## 1. Current mechanism (verified against source)

| Concern | Where | What it does |
|---|---|---|
| Election trigger | `IcebergSinkTask.open/close` → `CommitterImpl.open/close` | Re-evaluated by **every task** on **every rebalance** |
| Election rule | `CommitterImpl.hasLeaderPartition:78`, `containsFirstPartition:93`, `findFirstTopicPartition:122` | `Admin.describeConsumerGroups(connectGroupId)` → flat-map all members' assignments → task owning `min (topic,partition)` becomes coordinator |
| Readiness quorum | `Coordinator.java:99-100` | `totalPartitionCount = Σ members.assignment().size()` snapshotted **once** at election, never refreshed |
| Readiness check | `CommitState.addReady:69`, `isCommitReady:117` | Additive `receivedPartitionCount += dataComplete.assignments().size()`; ready when `>= totalPartitionCount` |
| Worker report | `Worker.receive:79` | `DataComplete` assignments built from live `context.assignment()` |

Note: the control-plane consumers are **not** in the source group — the coordinator uses
`connectGroupId + "-coord"` and the worker an ephemeral `controlGroupIdPrefix + UUID` group. The source
group is used purely as a **membership oracle** for election and the partition count.

### Where exactly-once safety actually lives (and what it does NOT depend on)

Safety does **not** rest on single-leadership. It rests on the **Iceberg offsets compare-and-swap**:
every append/rowDelta is `validateWith(offsetValidator(...))` (`Coordinator.java:283/297`), and
`offsetValidator.validate` requires `expectedOffsets.equals(lastCommittedOffsets)`
(`Coordinator.java:340`), running **inside** the `SnapshotProducer` retry loop after a metadata refresh.
Combined with `distinctByKey(ContentFile::location)` + `recordCount > 0` + the `offset >= committedOffsets`
payload filter (`Coordinator.java:246-270`) and the worker's single transaction binding control events +
`sendOffsetsToTransaction` (`Channel.java:95-114`):

> **Two concurrent coordinators waste work but cannot corrupt the table or lose data.** This invariant is
> the linchpin every redesign must preserve — and must not over-claim to *replace*.

---

## 2. Failure-mode catalogue (caused by coupling election + count to source partitions)

1. **Split-brain (two coordinators).** During cooperative rebalance / migration the lowest `(topic,partition)`
   can be transiently owned by two members (old + new), so two tasks each elect themselves. Safe for the
   table (OCC) but produces duplicate `StartCommit`s, double source-offset advances, and wasted commits;
   **not** fenced today (see latent bug #2).
2. **Zero-coordinator window.** If the lowest partition is momentarily unassigned mid-rebalance,
   `findFirstTopicPartition` may pick a different min or no leader is running → commits stall.
3. **Stale `totalPartitionCount`.** Scaling tasks or any membership change after election makes the frozen
   count wrong: too high → commits never reach quorum → stuck until `commitTimeoutMs` → **partial commit**;
   too low (or same-commitId double-report under churn) → **premature `commit(false)` that drops in-flight
   files = data loss**.
4. **In-flight commit loss on churn.** Any rebalance that moves partition 0 stops the old `Coordinator` and
   starts a new one, discarding in-memory `commitBuffer`/`readyBuffer`/`receivedPartitionCount`.
5. **Hot-path Admin dependency.** `describeConsumerGroups` on every `open`/`close` adds latency, needs
   `DESCRIBE` ACL, and can throw on the rebalance path.
6. **Forced co-location.** The coordinator must run on whichever task owns the lowest partition — placement
   is dictated by data layout, not load.
7. **Assignor-dependent behavior.** Eager vs cooperative-sticky changes the churn and split-brain windows.

---

## 3. Two latent bugs found during the review (independently actionable, not redesign-gated)

- **B1 — `-coord` consumer `auto.offset.reset=latest`.** `KafkaClientFactory.java:55` does
  `putIfAbsent(AUTO_OFFSET_RESET_CONFIG, "latest")`. A **fresh or expired** `-coord` group (first start,
  offsets retention expiry, group deletion) rewinds to `latest` and **skips uncommitted control events =
  silent data loss**. Replay is idempotent (offset filter + location dedup + OCC), so `earliest` is the
  safe default for the coordinator group. *(The same factory correctly sets `read_committed` and
  `enable.auto.commit=false` at lines 57–58, so the in-connector coordinator consumer is otherwise fine.)*
- **B2 — coordinator transactional IDs are not mutually fenced.** `IcebergSinkConnector.taskConfigs:52`
  builds `txnSuffix = "-txn-" + UUID.randomUUID() + "-"` and appends the task index (`+ i`); `Channel.java:67`
  forms the id as `prefix + name + suffix`. Two coordinators on different task indices — or the same index
  across reconfigs (the UUID is regenerated every `taskConfigs()`) — get **different** transactional IDs and
  are therefore **not** epoch-fenced against each other. Today's design relies entirely on OCC for split-brain
  safety.

---

## 4. Three constraints that shape every approach

1. **The producer-epoch fence is narrow.** A fixed coordinator `transactional.id` only fences operations
   routed through the transactional producer — the control-event `send()` in `Channel.send`. It does **not**
   fence the **Iceberg catalog commit** (`appendOp/deltaOp.commit()`, `Coordinator.java:294/309`, outside any
   Kafka txn; the `send(CommitToTable)` at line 318 is *after* the catalog write) nor the **`-coord`
   watermark advance** (`commitConsumerOffsets` → `consumer.commitSync`, `Channel.java:145-152`). So "strong
   fenced" must always be read as *"strong-fenced for the Kafka control plane; OCC-authoritative for the
   table."*
2. **Kafka gives single-ownership of a 1-partition topic for free.** The group coordinator assigns one
   partition to exactly one live member with automatic failover. Approaches that hand-roll this
   (catalog-lease, compacted-claim registry, `initTransactions`-as-election) reimplement a solved problem with
   more code, worse failover, and new failure surface — they are **strictly dominated**.
3. **Readiness must become set-coverage, not a count.** A per-interval `describeTopics` *count* is necessary
   but insufficient: the additive `receivedPartitionCount` can still over-count under churn. The correct
   primitive is `receivedSet.containsAll(expectedSet)` over `(topic, partition)` keys — idempotent and immune
   to double-reports.

---

## 5. Approaches evaluated (with adversarially-verified holes)

| Approach | R | Re | EOS | Ops | Mig | Blast | **Total** | Verdict |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|---|
| **In-connector election (control topic if 1-partition, else dedicated) + set-coverage + fixed txn.id + earliest** | 4 | 4 | 5 | 3 | 4 | 4 | **24** | **Recommended (primary)** |
| Set-coverage readiness *(orthogonal building block)* | 1 | 3 | 4 | 5 | 5 | 5 | **23** | Recommended (Phase 1 component) |
| Static `task.id==0` + set-coverage + fixed txn.id | 3 | 2 | 4 | 5 | 5 | 4 | **23** | Viable w/ caveats (slow failover) |
| Dedicated election **group** (general form) | 4 | 3 | 4 | 3 | 4 | 4 | **22** | Viable (== primary family) |
| Novel/hybrid (election group + fixed id + describeTopics) | 4 | 3 | 4 | 3 | 4 | 4 | **22** | Viable (folded into primary) |
| **Standalone Coordinator-as-a-Service** + per-domain pinned id | 4 | 3 | 4 | 2 | 2 | 2 | **21** | **Recommended (fallback / north-star)** |
| `transactional.id`-as-election (fencing-as-election) | 3 | 2 | 3 | 3 | 4 | 3 | **18** | Viable but inferior |
| Catalog / external-store lease | 3 | 2 | 3 | 2 | 3 | 3 | **16** | Not recommended |
| Compacted-claim registry | 3 | 2 | 3 | 2 | 3 | 3 | **16** | Not recommended |

*(R=robustness, Re=resilience, EOS=exactly-once preservation, Ops=operational simplicity, Mig=migration
ease, Blast=small blast radius; 1–5, higher better.)*

Key verified holes:

- **Election-topic family** (the primary): rebalance-listener callbacks run on a **second thread** that must
  not directly toggle `CoordinatorThread` (fix: callback sets a `volatile` desired-leader flag, task thread
  reconciles); **never** run `initTransactions()` (blocking RPC) or `Coordinator.terminate()` (up to a **60s**
  `awaitTermination`, `Coordinator.java:410`) inside the callback; `-coord` must be `earliest` (bug B1).
- **`task0-static`**: unplanned-crash failover is dominated by `scheduled.rebalance.max.delay.ms`
  (**default 5 min**) and is **zero** in standalone Connect mode → too slow for a "resilient" mandate as the
  *primary*, but a fine even-simpler opt-in tier.
- **`txn-id-as-election`**: conflates fence (safety) with election (liveness); ~5-min heartbeat window at the
  default commit interval; no thread to run the candidate loop in non-leaders; `InitProducerId` thundering
  herd; a stalled-but-heartbeating leader holds the lock forever.
- **catalog-lease**: a `~17k` metadata-commits/day/connector storm competes with real commits for catalog
  quota and shares fate with the commit target.
- **Standalone CaaS**: a Connect SinkTask coordinator consumes via the **framework** consumer, which defaults
  to `read_uncommitted` — a **critical exactly-once regression** unless `consumer.override.isolation.level=
  read_committed` is hard-validated at `start()`; must use **one producer per domain** and keep explicit
  `commitSync`-after-full-`doCommit`.

---

## 6. Recommendation — "Fenced Election Committer"

Ship as a new `ElectionTopicCommitter` behind `iceberg.committer.class` (default unset = today's
`CommitterImpl`, **zero** behavior change). It combines:

- **(A1) Election** — leadership = ownership of the **single partition of a configured election topic**
  in a **new election consumer group** (`connectGroupId + "-election"`, *not* the source group, *not* `-coord`).
  The topic is set via `iceberg.coordinator.election-topic` (default: the control topic) and validated to be
  single-partition at `start()` (see **§6.1**); static membership + cooperative-sticky are recommended consumer
  tunings to minimize churn. The broker provides at-most-one-owner + auto-failover; **data rebalances never
  re-elect**. Delete `hasLeaderPartition` / `findFirstTopicPartition` / `describeConsumerGroups` from the
  election path.
- **(A2) Fence** — a **fixed coordinator `transactional.id`** = `transactionalPrefix + "coordinator-" +
  connectorName` (no task index/suffix; worker id unchanged). The new leader's `initTransactions()`
  epoch-bumps and fences a zombie's control-topic writes — closing the duplicate-`StartCommit` / double
  source-offset-advance window that today's per-task random id leaves open (bug B2). Documented as Kafka
  control-plane fencing only; the table stays OCC-authoritative.
- **(B) Readiness** — `receivedSet.containsAll(expectedSet)` where `expectedSet` is derived from
  `Admin.describeTopics` over subscribed source topics, refreshed each interval; `receivedSet` is the
  `(topic, partition)` union from `DataComplete`. Idempotent, assignor-independent, immune to double-counts.

Mandatory fixes baked in: `-coord` `auto.offset.reset=earliest`; rebalance callbacks set only a `volatile`
flag (reconcile lifecycle on the task thread; terminate asynchronously; `initTransactions` eagerly on
promotion off the poll thread); election poll on its own daemon thread (cadence ≪ `session.timeout.ms`);
election-target partition layout validated at start and periodically (folded into the per-interval
`describeTopics`); a **coordinator progress watchdog** (relinquish the election partition after N no-progress intervals) to cover the
hung-but-heartbeating black hole; and metrics (`is-leader`, leadership-transition count, coordinator-absent
duration, `ProducerFenced` count, `describeTopics`-fallback gauge, `expectedSet` size).

### 6.1 Election substrate: one committer, a configured single-partition election topic (implemented)

There is a **single** committer, `ElectionTopicCommitter` (select via
`iceberg.committer.class=org.apache.iceberg.connect.channel.ElectionTopicCommitter`). It elects the
coordinator purely by **ownership of the single partition of a configured election topic** — there is no
runtime adaptive branching; the operator chooses the topic via one config value:

- **Config:** `iceberg.coordinator.election-topic` (default: the **control topic**). Point it at the control
  topic when that topic has a single partition, or at a dedicated single-partition topic otherwise.
- **Election group:** all tasks join `connectGroupId + "-election"` subscribed to that topic; the task
  assigned the (only) partition is the leader and runs the `Coordinator`. The election consumer
  (`ElectionMembership`) only polls on a daemon thread to hold membership and observe its assignment — it
  discards records and never commits offsets, so its isolation level / `auto.offset.reset` are irrelevant.
- **Validation:** at `start()` the committer asserts (via `Admin.describeTopics`,
  `KafkaUtils.describeTopicPartitionCount`) that the election topic has exactly **one** partition, failing
  fast with an actionable error otherwise.
- **Lifecycle:** the daemon election thread publishes a `volatile` leadership flag; the single Connect task
  thread reconciles `start/stopCoordinator` in `save()`, so the coordinator is only ever mutated from one
  thread.

This keeps the implementation to one class and makes single-leadership unconditional (a 1-partition topic has
exactly one owner). Reusing the control topic (the default) costs no new topic; a dedicated topic is only
needed when the control topic is multi-partition.

> **Scope of this increment (implemented):** election is fully decoupled from source partitions. The
> coordinator's partition **count** is still derived from a one-time `describeConsumerGroups` snapshot taken on
> promotion (unchanged from `CommitterImpl`). Making readiness membership-independent (set-coverage, Phase 1)
> and adding the fixed coordinator `transactional.id` fence (Phase 2) remain the follow-ups that complete the
> "Fenced Election Committer".

**Fallback / north-star:** the standalone Coordinator-as-a-Service connector, for deployments that need the
coordinator off the data-path JVM — reusing the same set-coverage readiness and per-domain pinned-id
fencing, but only after hard-validating `read_committed` on the framework consumer. It is the **only**
approach that removes the in-process co-location hotspot, which is why it stays on the roadmap.

---

## 7. Phased, backward-compatible roadmap

1. **Phase 1 — Membership-independent readiness (axis B), additive, no election change.** Replace
   `CommitState.receivedPartitionCount:int` with a `Set<TopicPartition> receivedSet`; change
   `isCommitReady(int)` to `isCommitReady(Set<TopicPartition> expectedSet)` via `containsAll`. Add an
   `ExpectedPartitionSetProvider` (resolve subscribed source topics → `describeTopics` once per interval at
   `startNewCommit` → expand to `(topic,partition)` → cache last-good → fall back to legacy member count only
   when `describeTopics` is unavailable). Pin the expected set for the whole interval; require `receivedSet`
   to be a superset; keep the `commitTimeoutMs` partial-commit path as the under-count backstop. One-flag
   rollback. **Kills the data-loss and stuck-commit classes for every election scheme. No wire/SPI change.**
2. **Phase 2 — Safety hardening (still legacy election).** Fixed coordinator `transactional.id` + `-coord`
   `auto.offset.reset=earliest`, behind a flag (default off). Adds the real (narrow) fence and closes the
   fresh-group event-skip data-loss hole (bug B1+B2). Safe to enable independently.
3. **Phase 3 — Election-decoupled committer (axis A), opt-in via `iceberg.committer.class`.** ✅ *Implemented*:
   `ElectionTopicCommitter` + `ElectionMembership` + `iceberg.coordinator.election-topic` (default: control
   topic) + `KafkaUtils.describeTopicPartitionCount` single-partition validation (see §6.1). Election is fully
   decoupled from source partitions; `describeConsumerGroups` is removed from the rebalance lifecycle (it is
   now called only once on promotion, purely to seed the existing partition count — superseded by Phase 1).
   **This is the recommended primary.** Migrate per-connector with a single coordinated restart (not a long
   mixed-scheme window — overlap is OCC-safe but noisy, since old per-task-id and new fixed-id coordinators are
   not mutually fenced). *Remaining to complete the "Fenced Election Committer": wire in Phase 1 set-coverage
   readiness, Phase 2 fixed-id fence + `-coord` earliest, the progress watchdog, and metrics.*
4. **Phase 4 (optional north-star) — Standalone Coordinator-as-a-Service** for full isolation, with the
   `read_committed` / per-domain-producer / `commitSync`-ordering guardrails and a **drain-then-cut-over**
   (zero-writer) migration.

Each phase is independently shippable, default-off, and reversible.

---

## 8. Open questions

- **Source-topic resolution for the expected set under `topics.regex` / dynamic routing:** derive from an
  independently-resolved regex (risk: diverges from what Connect actually subscribed → chronic premature *or*
  stuck commits) vs the union of `(topic,partition)` ever observed in `DataComplete` reconciled with
  `describeTopics` only for partition-growth detection (more robust by construction; needs validation against
  multi-table routing).
- **Election-topic partition-count growth after start** silently breaks single-ownership — confirm periodic
  re-validation is cheap enough to fold into the per-interval `describeTopics`.
- **`transactional.id.expiration.ms` vs the coordinator-absent window:** during an extended zero-coordinator
  window the fixed id can expire and un-fence a returning zombie. Emit a periodic keepalive transaction on
  empty intervals? What minimum expiration relative to `commitIntervalMs`?
- **Should `commitConsumerOffsets` move into `sendOffsetsToTransaction`** so the epoch fence also covers the
  recovery watermark, or is `earliest` + monotonic (never-lower) commit sufficient given idempotent replay?
- **Phase 4:** does `connector.client.config.override.policy=All` reliably force `read_committed` across
  target Connect versions, and should the standalone connector refuse to start if it cannot prove it?
- **Failover-latency SLO:** what default election `session.timeout.ms` balances fast crash detection against
  GC-pause false failovers, and should static membership default on?
- **Cross-table atomicity:** the offsets-CAS is per-table and `doCommit` is `Tasks.foreach` with
  `stopOnFailure`, so a fenced/crashed handover can leave per-table commit skew. Is per-table exactly-once
  sufficient, or is a cross-table `valid-through-ts` monotonicity guard needed?

---

*Method: this document is the synthesis of a multi-agent research workflow (33 agents) that deep-read the
source, extracted the exactly-once invariants, enumerated 9 decoupling strategies, and adversarially verified
each from an exactly-once/correctness lens and a resilience/ops lens. All line references were re-verified
against the working tree.*
