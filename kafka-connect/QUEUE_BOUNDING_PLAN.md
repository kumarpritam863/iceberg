# Plan: Bounding `controlEventQueue` in Worker

## Context

The `controlEventQueue` (`ConcurrentLinkedQueue<Envelope>`) in Worker buffers `START_COMMIT` events polled by the background thread for processing by the main thread. The question is whether this queue needs explicit bounding/backpressure, and if so, what strategy is correct.

## Investigation Findings

### Event Production Rate (Coordinator side)

The Coordinator is the sole producer of `START_COMMIT` events. Key constraints from `CommitState.java`:

- **Line 82**: `!isCommitInProgress() && elapsed >= commitIntervalMs` — a new commit **cannot start** while one is in progress
- **Line 86-88**: `startNewCommit()` sets `currentCommitId`, blocking further commits
- **Line 91-94**: `endCurrentCommit()` clears `currentCommitId`, allowing next commit
- **Timeout path**: If worker doesn't respond within `commitTimeoutMs` (default 30s), Coordinator does a partial commit via `commit(true)` which calls `endCurrentCommit()`, re-enabling the next commit

**Maximum production rate**: 1 event per `max(commitIntervalMs, processingTime)`. With defaults (300s interval, 30s timeout), worst case = 1 event per 30s (if every commit times out).

### Event Consumption Rate (Main thread side)

The queue is drained in `worker.process()`, called from `CommitterImpl.processControlEvents()`, triggered by:

1. **`put()` calls** — every batch from Kafka Connect framework
2. **`flush()` calls** — `IcebergSinkTask.flush()` (line 89-92) calls `committer.save(null)`, which calls `processControlEvents()` even with null records
3. **Flush interval**: Kafka Connect calls `flush()` at `offset.flush.interval.ms` (default 60s)

**Key insight**: Even with an idle source topic, the queue is drained at least every 60 seconds via `flush()`. The "events queue indefinitely" scenario does NOT happen in practice.

### Actual Worst-Case Queue Depth

Given:
- Max production: 1 event per 30s (commit timeout pathological case)
- Min consumption: 1 drain per 60s (flush interval)
- Between flushes, at most 2 events can accumulate (60s / 30s)

In the realistic case (300s commit interval), the queue holds **at most 1 event** at any time.

### When Queue Grows Beyond 1

The only scenario where the queue holds multiple events:
1. **`handleStartCommit()` blocks for very long** (catalog slow, broker slow)
2. During that blocking, the Coordinator completes timeout → commit → starts a new commit
3. Background thread picks up the new `START_COMMIT`, queues it
4. Previous `handleStartCommit()` is still running

This requires `handleStartCommit()` to block for > `commitIntervalMs + commitTimeoutMs` (330s by default). If that happens, the system has far bigger problems than queue size.

## Recommended Approach: Bounded Queue with Drop-Oldest (AtomicReference)

Rather than adding heavyweight backpressure (which would require Coordinator protocol changes), the right approach is:

**Use a bounded queue of size 1**, dropping the oldest event when a new one arrives.

### Why This Is Correct

1. **Only the latest `START_COMMIT` matters.** Each `START_COMMIT` asks the Worker to "flush whatever you have now." If two are queued, the second one renders the first redundant — the Worker should flush once with the latest commit ID.

2. **The Coordinator handles missed responses gracefully.** If a Worker doesn't respond to a `START_COMMIT` within `commitTimeoutMs`, the Coordinator does a partial commit (Coordinator.java:127-129). The system is already designed for Workers to miss commits.

3. **No protocol changes needed.** The Coordinator doesn't need to know about the bounding. It already handles the case where Workers don't respond.

4. **Zero risk of OOM.** Even in pathological cases, the queue never holds more than 1 event.

### Implementation

**File: `Worker.java`**

Replace the `ConcurrentLinkedQueue` with an `AtomicReference<Envelope>`:

```java
// Holds at most one pending START_COMMIT event. Only the latest matters — if a new
// START_COMMIT arrives before the previous one is processed, the old one is replaced.
// This is safe because the Coordinator handles missed responses via commit timeout.
private final AtomicReference<Envelope> pendingCommitEvent = new AtomicReference<>(null);
```

**`receive()`**: Use `set()` instead of `offer()`:
```java
@Override
protected boolean receive(Envelope envelope) {
    if (envelope.event().payload().type() == PayloadType.START_COMMIT) {
        Envelope previous = pendingCommitEvent.getAndSet(envelope);
        if (previous != null) {
            StartCommit skipped = (StartCommit) previous.event().payload();
            LOG.warn("Worker {} dropping superseded START_COMMIT {}, replaced by newer event",
                workerIdentifier, skipped.commitId());
        }
        return true;
    }
    return false;
}
```

**`process()`**: Use `getAndSet(null)` instead of poll loop:
```java
void process() {
    Exception ex = errorRef.getAndSet(null);
    if (ex != null) {
        throw new ConnectException(...);
    }
    Envelope envelope = pendingCommitEvent.getAndSet(null);
    if (envelope != null) {
        handleStartCommit(((StartCommit) envelope.event().payload()).commitId());
    }
}
```

**`pendingEventCount()`**: Update for tests:
```java
@VisibleForTesting
int pendingEventCount() {
    return pendingCommitEvent.get() != null ? 1 : 0;
}
```

**`terminateBackGroundPolling()`**: Replace `controlEventQueue.clear()` with `pendingCommitEvent.set(null)`.

### Why Not Other Approaches

| Approach | Problem |
|----------|---------|
| **Bounded BlockingQueue** | `offer()` on full queue silently drops, OR we need to decide drop-oldest vs drop-newest — adds complexity for a queue that's almost always size 0-1 |
| **Backpressure to Coordinator** | Requires protocol changes (new event types), breaks the clean separation between Coordinator and Worker |
| **Semaphore / rate limiter** | Over-engineered — the production rate is already bounded by commit interval + timeout |
| **ConcurrentLinkedQueue with size check** | `.size()` is O(n) on ConcurrentLinkedQueue — not suitable for a hot path |
| **Keep unbounded queue + monitoring** | Doesn't actually prevent OOM in pathological cases; "monitor and alert" isn't a fix |

### Test Changes

**File: `TestWorker.java`**

- `testBackgroundPollingBuffersEvents`: Change to verify only the latest event is kept (send 2 START_COMMITs, verify only the second one is processed)
- `testWorkerMultipleStartCommits`: Update — with AtomicReference, only the latest commit should be processed (1 DataWritten + 1 DataComplete = 2 events total, not 4)
- Add `testOlderStartCommitIsSuperseded`: Send two START_COMMITs, verify the first commitId is dropped and only the second is processed

## Files to Modify

| File | Changes |
|------|---------|
| `Worker.java` | Replace `ConcurrentLinkedQueue` → `AtomicReference<Envelope>`, update `receive()`, `process()`, `pendingEventCount()`, `terminateBackGroundPolling()` |
| `TestWorker.java` | Update multi-event tests to reflect drop-oldest behavior, add superseding test |

## Verification

1. `./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:spotlessApply`
2. `./gradlew :iceberg-kafka-connect:iceberg-kafka-connect:test`
3. Verify no `ConcurrentLinkedQueue` references remain in Worker.java