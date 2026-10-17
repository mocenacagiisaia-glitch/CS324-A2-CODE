# Live job replication and failure recovery

This update extends coordination checkpoint `11b4804`. The seven/eight-argument
worker launcher now mounts the durable JobService, RecoveryControlService,
RecoveryStateService, coordination and membership services together. No worker
RMI listener is opened in that mode. The six-argument A1 launcher remains a
separate regression harness.

## Protocol

1. The client creates a stable job ID before its first request. Retries keep
   exactly that ID and input. Reusing an ID with different input is rejected.
2. Every checkpoint mutation goes through the lowest currently listed worker.
   It serializes updates and reads all listed replicas to select the highest
   revision. Different contents at the same revision stop recovery.
3. The elected coordinator is still selected by lowest lifetime JAC, then highest
   ID. The checkpoint writer is a serialization role, not a replacement leader.
4. Admission saves the job's input, deterministic task definitions and counters.
   Every currently listed worker must persist the full checkpoint before tasks
   can run. A failed write may leave a partial copy; the next attempt completes
   replication of that copy before admitting more work or dispatching it.
5. The elected coordinator claims and computes unfinished tasks concurrently.
   Each claim has a worker incarnation, coordinator incarnation, term and attempt.
   A matching result completes the task exactly once in the checkpoint. Completed
   tasks are retained; aggregation runs only when every task has a result.
6. Healthy former coordinators finish their unchanged attempts across five-job
   rotation. A replacement coordinator reclaims an unfinished attempt when its
   previous coordinator disappears from membership. Failed computations release
   their attempts for reassignment. Late results cannot overwrite a newer attempt.
7. All workers poll for unfinished jobs. When a worker/coordinator/checkpoint
   writer fails, recovery waits for its bootstrap lease to expire, then resumes
   from the surviving copies. A restarted worker reads its local file, gets a new
   incarnation, and catches up to the newest live replica.

Snapshot installation restores lifetime JAC and current term counters. Initial
admission charges planned allocations to other workers once, as the previous
runtime did. Reassigning an attempted chunk to another worker adds a JAC
allocation to the current coordinator without consuming another client-job slot.
Resuming the same job ID never counts as another admission. A term accepts no
more than five new jobs; recovery does not refund its original allocations.

## Client reconnection

`GrpcJobClient` keeps the same ID across unavailable/deadline/remote-stream-reset
failures and rediscovers the first available directory member. It only reconnects
when the directory advertises durable job support. It does not automatically
retry legacy services after an unknown accepted outcome or retry invalid input.
Repeated progress events after reconnect are suppressed for the UI. Client wait
is bounded to ten minutes; accepted work can remain recoverable after that wait.

A caller that must resume after restarting its own process should persist its ID:

```java
String requestId = "my-saved-job-id";
BigInteger result = new GrpcJobClient("127.0.0.1", 5000)
        .submit(requestId, job, update -> System.out.println(update.getState()));
```

The ordinary GUI automatically reconnects during the same submission. Its task
rows and generated IDs are not persisted across closing/restarting the GUI.

## Failure model and practical bounds

This is a trusted-lab, fail-stop recovery protocol with one available bootstrap
and a consistent directory view. It is not a quorum consensus protocol and does
not promise safety during network partitions or simultaneous conflicting writers.
Workers fail closed when directory discovery fails. A listed unreachable replica
blocks checkpoint progress until its lease expires. Recovery after losing every
worker requires at least one preserved checkpoint file. Conflicting revisions or
corrupt files require operator investigation rather than choosing arbitrarily.

Pure computations may execute more than once after a failure. The checkpoint
counts only the matching attempt's result; this is not exactly-once execution of
external side effects. A hung computation call is bounded to ten minutes.

The existing 64 MiB full-snapshot limit remains. Completed jobs are retained for
stable-ID deduplication; this implementation does not compact their history.
It is intended for the assignment demo, not an unbounded production workload.

Keep existing startup commands. The default checkpoint path is still
`data/recovery/worker-ID.checkpoint`. Old manual/demo snapshots lacking a saved
coordinator endpoint are deliberately not promoted to live authority. If that
specific diagnostic appears, preserve the old file and supply a fresh path as
the eighth worker argument. Never copy different worker checkpoint files onto a
running process's open store.

## Polling and connection reuse

The performance follow-up reuses recovery/control and computation channels per
worker endpoint, including incarnation ID. Runtime shutdown closes those channels.
A restarted worker's new incarnation gets a separate connection.

An unchanged healthy leadership term is reused without repeating election/status
floods for every poll. Five-job exhaustion, a missing coordinator incarnation or
a newer locally announced term still invokes election handling.

Read-only polling can reuse the already fully acknowledged local snapshot for
at most 100 milliseconds. The directory is still checked on every request.
Membership changes, a different local snapshot, an exhausted term, an unknown
job ID or a failed/in-progress replica write bypass that shortcut. Each state
mutation still reconciles all replicas; each publication still requires all
listed acknowledgements before dispatch. Partially acknowledged updates clear
the polling cache. No RPC deadline or test timeout was increased.

The added regression checks actual HTTP/2 peer connection reuse across repeated
submissions, with no extra job/revision. The partial-acknowledgement test now
starts from an existing fully acknowledged job and verifies that a later write
failure cannot dispatch new work using the old polling cache.

## Verification

`mvn clean verify -Pintegration` runs the existing suite plus recovery tests.
`RecoveryRuntimeTest` covers duplicate-ID admission, conflicting input, leader
failure, checkpoint-writer failure, unavailable listed replicas, partial write
acknowledgements, full worker restart, stale attempts and healthy rotation with
in-flight tasks. Tests use actual loopback gRPC services and checkpoint files.

`RecoveryProcessIT` blocks the pure engine with a test-only file barrier, kills
both the elected coordinator and checkpoint writer with `destroyForcibly`, and
checks that the surviving JVM resumes the accepted job after real lease expiry.
It then reopens both killed workers' original files with new incarnations and
checks catch-up and stable-ID result retrieval. Existing GrpcJobsIT and
GrpcCoordinationIT still use the production worker launcher and real engine.
Logs are saved under `target/recovery-process-*`.

Implementation is AI-assisted work prepared for Isaia while he takes over the
coordination coding tasks; Maanav's separate documentation is not its authorship.

Final local verification: Java 21.0.12, Maven 3.8.7, Linux; 75 unit tests and
4 integration tests passed with zero failures/errors/skips. The container used
`-Djava.rmi.server.hostname=127.0.0.1` for the retained legacy RMI tests.

One earlier GrpcJobsIT run exited successfully but had empty client log files,
so the unchanged assertion for 12 PASS lines failed. The cause is unknown. Its
focused rerun and the final clean full run both passed without changing that
test or its assertions.
