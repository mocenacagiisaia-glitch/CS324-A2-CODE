> This file records an earlier checkpoint. See [live recovery runtime](A2_RECOVERY_RUNTIME.md) for the integrated durable behavior and current limits.

# A2 job/client migration checkpoint

Based on main 50fbf4b. This is AI-assisted implementation requested by Isaia
for the team's calculations/client area; it is not attributed to Krishnil as
independent coding. The repository's task table still describes A1. The full
A2 specification was unavailable during this update, so exact rubric compliance
has not been established.

## Changes

- `jobs.proto`: server-streaming SubmitJob with explicit request correlation,
  integer inputs, phase states and decimal BigInteger results.
- `JobWire`: explicit operation conversion and input validation.
- `JobServiceImpl`: adapter mounted on each worker's existing gRPC port.
- `GrpcJobClient`: gRPC discovery and submission, five-second directory deadline,
  ten-minute job deadline, sequence/result validation and per-call channel cleanup.
- Swing GUI: uses gRPC bootstrap port 5000 by default; displays live phase states
  on the Swing event thread. Parsing and networking remain background work.
- `GrpcHeadlessClient`: twelve checked concurrent submissions. The old RMI client
  remains available for the existing A1 regression integration test.

The three phase states are RECEIVED, RUNNING and COMPLETED. RUNNING means that
the adapter has begun the submission call, not durable admission or a measured
percentage. No artificial chunk percentages are displayed. The service repeats
only explicit pre-admission RetryException responses during term rotation;
unknown failures and transport errors are not automatically replayed.

## Build and run on Windows

Use Java 21 and Maven, from the project root:

```powershell
mvn clean verify -Pintegration
mvn "-Dexec.mainClass=edu.usp.cs324.network.BootstrapGrpcServer" "-Dexec.args=5000" org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

Start workers in separate terminals using the membership startup syntax already
verified by the team, for example:

```powershell
mvn "-Dexec.mainClass=edu.usp.cs324.network.WorkerNode" "-Dexec.args=1 127.0.0.1 2001 3001 4001 127.0.0.1 5000" org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

Then launch either client in another terminal:

```powershell
mvn "-Dexec.mainClass=edu.usp.cs324.client.ClientGui" "-Dexec.args=127.0.0.1 5000" org.codehaus.mojo:exec-maven-plugin:3.5.0:java
mvn "-Dexec.mainClass=edu.usp.cs324.client.GrpcHeadlessClient" "-Dexec.args=127.0.0.1 5000" org.codehaus.mojo:exec-maven-plugin:3.5.0:java
```

## Integration limits

This is a client-to-worker gRPC migration, not the entire A2 migration.
The adapter invokes existing WorkerNode.submit locally, which still uses RMI
between workers. Maanav's coordination migration should retain the service
boundary or replace its injected Submitter. Snapshot storage is not connected
to this request path. Request IDs are correlation IDs, not durable deduplication
keys. A disconnect or deadline can leave work running with an unknown outcome;
submitting again can duplicate it. Resume/reconnect, per-chunk progress and
automatic worker/coordinator recovery remain unfinished team integration work.
The GUI itself requires manual testing on a desktop.

## Verification of this checkpoint

Java 21.0.12 and Maven 3.9.12: 58 unit tests and two separate-process
integration tests passed, with zero failures, errors or skips. The new gRPC
integration test checked 24 correct results from two concurrent client JVMs
using two worker JVMs. Existing membership/election/client regression also passed.
The container needed `-Djava.rmi.server.hostname=127.0.0.1` for its legacy RMI
unit tests because its advertised container interface was unreachable.
Command: `mvn -Djava.rmi.server.hostname=127.0.0.1 verify -Pintegration`.
This verifies the transitional adapter, not an all-gRPC worker network.
