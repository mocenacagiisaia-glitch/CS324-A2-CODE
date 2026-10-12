# Contribution record

The owner requested a new, independent three-person development repository.
Codex performed the initial skeleton setup with shared contracts from the local
Safe101 reference and new task documentation. The reference and original Git
history are not copied into this repository.

Area 1 is AI-assisted adaptation by Codex for person 1. BootstrapNode and
ClusterStatus were selected from the reference; WorkerNode contains adapted
membership/startup and single-chunk execution infrastructure only. Its election
and coordination methods are explicit unimplemented entry points. New focused
membership/runtime tests and a separate-process membership test use the reference
test patterns without copying the full election/job/client integration suite.
Areas 2 and 3 are reserved for the two teammates; no implementation or authorship
is attributed to them yet. Their branches must contain their own future commits.

Setup commits use the repository-local author `Codex <codex@users.noreply.local>`.
This identifies the coding agent and does not impersonate a team member or change
any person's global Git configuration.

Verification of area 1: `mvn clean verify -Pintegration` passed 8 unit/RMI tests
and 1 separate-process membership test, with no failures, errors or skips.
The initial skeleton built with no tests; that milestone did not claim feature
completion. Election, coordinator dispatch, algorithms, CSV and clients have not
been implemented or verified in this repository.

## Member 2: election and coordination

Election and coordination were implemented with Codex assistance in
feature/election-coordination, building on the supplied runtime and public API.
No completed reference implementation was read or copied for this work.
See ELECTION_COORDINATION.md for implementation decisions and integration limits.

Reviewed against the user-supplied A1 cs324.txt specification. JAC counts allocations
to other workers, separately from the five admitted client jobs per term. Verification:
`mvn clean verify -Pintegration` passed 17 unit/RMI tests and 1 separate-process
integration test, with no failures, errors or skips. Commit author: Maanav Kumar.
Member 3 calculations, CSV and GUI remain outside this contribution.

## Area 3: calculations and clients

Implemented by Codex at the user's request on feature/jobs-client after fetching
origin and fast-forwarding origin/main (d688f0c). No reference implementation was
read or copied. This is AI-assisted work, not a claim of independent student authorship.

Adds a stateless registered JobEngine, MAX/PRIMESUM/PRIMECOUNT, balanced chunks,
BigInteger aggregation, numeric UTF-8 CSV parsing, concurrent Swing submissions,
safe pre-admission retry handling and a checked headless client. Shared API and
WorkerNode production code are unchanged. Adds calculation/parser/retry tests and
extends separate-process integration to two clients with 24 checked real jobs,
leader rotation/agreement and JAC accounting.

Verification: mvn clean verify -Pintegration passed 25 unit/RMI tests and one
separate-process integration test, with no failures, errors or skips. Swing
interaction and cross-machine networking still need a manual demo. Commits use
Codex's agent identity; the user authorized the authenticated GitHub account for
pushing and opening the PR. This does not attribute the work to that account owner.
