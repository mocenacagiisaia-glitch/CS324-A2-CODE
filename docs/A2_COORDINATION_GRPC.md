# A2 coordination transport update

Base: merged main `50fbf4b`. This update implements the first coding checkpoint
for the coordination responsibilities Isaia is taking over from Maanav.
The implementation was prepared with AI assistance; it is not Maanav's authorship.

## Implemented

- `coordination.proto`: explicit Protobuf status, candidate, term, assignment,
  input and decimal integer result messages.
- `CoordinationServiceImpl`: status, ELECTION flooding, COORDINATOR announcement,
  election initiation, submission, assignment and computation RPCs.
- `CoordinationGrpcClient`: local Java adapter for the shared `WorkerRemote`
  signatures. It uses gRPC exclusively and owns its channel.
- `WorkerNode`: selectable internal transport; seven/eight-argument gRPC mode
  uses gRPC for all worker calls and does not export or bind an RMI worker.
  The six-argument A1 launcher remains for regression tests only.
- Directory endpoint caching includes incarnation IDs, registration replies,
  active membership discovery and incoming reciprocal neighbour links.
- Existing lowest-JAC/highest-ID ranking, UUID election deduplication, synchronized
  admission/JAC updates and five-job rotation stay in the existing state machine.

The original registry/export arguments are retained in the gRPC launcher for
compatibility with the existing endpoint record and startup commands. They do
not open RMI ports in that mode.

## Failure semantics

`FAILED_PRECONDITION` maps to the original explicit pre-admission rejection.
Only this outcome permits a submission retry. `ABORTED` maps to a known drained
computation failure. Deadlines, cancellations and transport errors preserve an
unknown outcome and must not trigger automatic replay of this synchronous API.
Every adapter call has a deadline (default 30 seconds). Long-running computation
may outlive its client's deadline; no cancellation/resume guarantee is added here.

The directory still removes failed workers by lease expiry. A listed but
unreachable worker can delay/reject an election until it expires. After removal,
survivors can elect another leader. This is not partition-tolerant consensus.

## Validation

Run with Java 21 and Maven:

```powershell
mvn clean verify -Pintegration
```

There are 56 unit tests (50 existing + 6 coordination gRPC tests), and 2
integration tests (the original RMI regression plus the new gRPC process test).
`CoordinationGrpcTest` deliberately starts no worker RMI registries. It checks
cyclic propagation/deduplication, simultaneous election requests, leader ranking,
five-job rotation, JAC totals, three calculation types, stale-term rejection,
leader removal, and non-retryable transport failures.
`GrpcCoordinationIT` launches the actual bootstrap and three gRPC-mode worker
JVMs, then checks 12 concurrent calculations, term agreement, rotation and JAC.
Logs are under `target/grpc-coordination-*`.

In containers that cannot route their automatically advertised RMI address, the
legacy regression tests require an explicit loopback address:

```powershell
mvn "-Djava.rmi.server.hostname=127.0.0.1" clean verify -Pintegration
```

## Still pending

This update does not implement automatic recovery of accepted jobs. The existing
snapshot store/service are mounted but are not populated from live submissions.
The next checkpoint needs stable client job IDs, serialized snapshot updates,
replication acknowledgement rules, authority after coordinator failure, task
attempt fencing, and resumption of unfinished work. Five-job rotation must carry
in-flight state forward without losing concurrent completions.

The existing GUI/ClusterClient/HeadlessClient still use the legacy bootstrap/RMI
entry point. Krish's gRPC client work should discover `WorkerEndpoint` values and
use the coordination service or adapter; it must not use `Peer.connect()` in
gRPC mode. Streaming progress is not part of this transport checkpoint.
