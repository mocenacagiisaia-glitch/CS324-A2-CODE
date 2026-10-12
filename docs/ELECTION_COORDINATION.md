# Member 2: election and coordination review

## Algorithm and state

Reviewed against the supplied `A1 cs324.txt` assignment specification. ELECTION collects candidates by flooding neighbour
links. A UUID is marked before forwarding, so cyclic and duplicate messages return
an empty contribution without blocking on their original traversal. Lowest lifetime
JAC wins, breaking ties by highest worker ID. Bootstrap supplies membership only.

The lowest-ID active worker serializes election initiation with a dedicated gate.
This gate is separate from the short-lived state monitor and is never acquired by
ELECTION or COORDINATOR handlers. While holding this gate, elect reads active
workers' status and discovers the newest term by number and UUID, even when its
own local term is null. It rechecks that term's active leader and reuses the term
only if the leader still reports the same term with fewer than five admitted jobs.
COORDINATOR propagation then adopts that term on the joining worker; existing
workers ignore the duplicate, preserving their counters and remaining job slots.
An exhausted or unavailable leader instead triggers election above the newest
observed term number. Status-call failures propagate for a later retry rather than
being treated as evidence that no coordinator exists.

The assignment permits a custom algorithm. Any worker can request an election;
serialization and winner selection remain entirely on workers, not bootstrap.

Terms compare by increasing number, then UUID. Duplicate/stale announcements do
not reset counters. COORDINATOR is installed locally before forwarding; direct
announcements to all active members allow retry after partial propagation.
Term and counters are read/written under one monitor. RMI calls never hold that
monitor. Election UUIDs are retained for process lifetime, capped at 100,000;
further new elections fail explicitly rather than forgetting delayed duplicates.

## Admission and rotation

Only the term's leader admits work. Splitting is pure preflight work. Under the
state monitor, assign checks the expected term and five-job limit, then reserves
one job slot and records allocations to other workers. JAC counts remote chunks:
a three-worker job increments the coordinator's JAC by two, excluding its own
chunk. A split failure consumes neither JAC nor a job slot. Once reserved,
allocations are counted as attempts even if dispatch fails: a transport error
cannot reliably prove whether the receiving worker started work. No counters are
refunded after admission, avoiding ambiguous replay/accounting.

Five admitted jobs exhaust the term. Rotation is attempted immediately after
scheduling dispatch calls, before waiting for results; later submissions also
retry rotation if propagation failed. Already admitted work can finish across a
term boundary. Duplicate COORDINATOR messages never replenish the five slots.

Example: workers 1, 2 and 3 start with JAC 0. Worker 3 wins. Five jobs split across
all three workers raise its JAC to 10; worker 2 wins the next term. A stale
assignment to worker 3 is rejected before work starts. In a one-worker cluster,
JAC stays zero because no other worker receives an allocation; the same worker
wins a fresh term after every five jobs.

## Member 3 contract (no API signature changes)

Implement JobEngine and register it at
`src/main/resources/META-INF/services/edu.usp.cs324.api.JobEngine`.
`split(job, workerCount)` returns 1..workerCount non-null chunks. Coordination sends
chunk i to worker i concurrently and passes results to aggregate in chunk order.
Compute and the pure engine methods must support concurrent calls. Calculations,
balanced splitting, aggregation, CSV and GUI remain Member 3's work.

Clients submit to any registered worker. A RetryException is a pre-admission
rejection only; clients may refresh/retry those rejections with bounded backoff.
RMI can wrap server exceptions: do not retry merely because an arbitrary nested
cause is RetryException. Generic remote errors/timeouts mean unknown admission or
completion and must not be replayed automatically. There is no job-ID deduplication
contract. JobFailedException means locally started work was drained; computation
RMI failures are conservatively reported as ordinary RemoteException because a
transport failure cannot prove that the remote computation stopped.

The coordinator drains all dispatched calls on errors/interruption, restores the
interrupt flag, and never cancels a future as proof that remote work stopped.
Run all JVMs with the README's RMI response timeout option. No automatic replay,
crash recovery, durable JAC, neighbour repair or partition consensus is promised.
Unreachable neighbour links are skipped. Direct election requests to all active
members cover disconnected components. Missing candidate responses fail the
election rather than electing from an incomplete set. A membership change during
an election can require a later retry; eventual agreement assumes stable
reachability, not arbitrary network partitions.

## Tests and demonstration

CoordinationTest uses three actual RMI worker endpoints connected in a cycle. It
checks duplicate messages, stale announcements, simultaneous elections, 20
concurrent attempts sharing one term (exactly five admitted), lowest-JAC rotation,
stale admission rejection, submission forwarding, unchanged counters after a
duplicate coordinator, split failures without allocation, unknown remote completion, interruption draining, failed-leader replacement with stale neighbour links, and
rotation while fifth-job computation is still running. A join regression admits
one job under worker 3, registers worker 0 with a null term, and verifies that
remote elect preserves the exact term, leader and counters across every worker.
It then admits the remaining four jobs and verifies normal five-job rotation.
Membership and runtime regression assertions remain.
MembershipIT starts bootstrap plus four worker JVMs and verifies election of ID 4
and agreement on the term over real interprocess RMI.

Fixtures intentionally do not implement the calculation algorithms. Full real-job
client/CSV/GUI tests and a remote computation that outlives an actual transport
timeout still need integration verification. The failure test uses an unreachable
endpoint, not a simulated claim that timed-out work stopped.

Review WorkerNode's election(), elect(), coordinator(), assign(), and submit(),
then run `mvn clean verify -Pintegration`. Explain how the state monitor protects
the admission check plus counter increment, why deduplication occurs before RMI
forwarding, and why accepted failures cannot be retried as new admissions.

## Specification mapping

| Requirement | Implementation / verification |
|---|---|
| Any worker initiates a custom election | elect forwards initiation to a worker gate; simultaneous-election test |
| ELECTION propagation and no duplicate processing | neighbour flood, UUID set, cycle test |
| Consider all reachable active workers | flood plus active-member coverage and completeness check |
| Lowest JAC, highest ID tie break | comparator; initial and post-allocation election tests |
| COORDINATOR propagation and eventual agreement | ordered terms, neighbour flood, direct member announcements; separate-JVM test |
| Five jobs per term | atomic admission cap; 20 racing requests admit exactly five |
| JAC for assignment to another worker | remote-chunk allocation accounting, excluding self |
| Concurrent workload execution | existing worker executor and concurrent dispatch; interruption draining tests |
| Balanced splitting, algorithms, CSV and GUI | Member 3 via existing JobEngine; not implemented here |
| Bootstrap is not an election participant | bootstrap supplies active membership only |

Allocation accounting treats each remote chunk as the job assigned to that worker.
A failure after allocation retains its count. This explicit interpretation avoids
conflating the separate five-client-job term budget with per-worker allocations.
