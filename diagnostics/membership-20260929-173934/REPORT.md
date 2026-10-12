# Original-project MembershipIT investigation

Project: C:/Users/User/OneDrive/Documents/CS324_A1_Team
Commit: 7b339041558adb385a57d92566a908796d701732 (main)
Toolchain: Temurin 21.0.11+10; Maven 3.9.12 using the same JDK.
Before editing: no tracked or staged changes. Existing untracked user files were demo-numbers.csv, demo-range.csv, and git-log-appendix.txt; they were left untouched. This investigation created diagnostics/.

## Preserved failure
The original membership-integration and failsafe-reports directories were copied here before running Maven. The report records 1 test, 1 failure, 0 errors/skips at MembershipIT.java:89, the ClusterStatus 10-second exit wait. Original status.log is 0 bytes. Bootstrap logged four registrations; all four worker logs contain WORKER READY. This confirms the reported artifact, but not the blocked stage.

## Test-only changes
Only MembershipIT.java was edited. Captures each subprocess's exact launch arguments in memory. The existing assertTrue(status.waitFor(10, TimeUnit.SECONDS)) is wrapped to invoke diagnostics only after that assertion fails, then rethrows the same AssertionError.
On timeout, diagnostics record all test-child PIDs, launch commands, start instants and alive/exit state, then attempt java.home/bin/jcmd Thread.print -l (ClusterStatus first). Probe waits share an 8-second budget. Output and attach errors go to diagnostics/status-timeout-<timestamp>/ outside target. Failures are recorded and exceptions attached to the original assertion. Only jcmd helpers created by this hook are forcibly stopped if still alive; the original test cleanup retains responsibility for its own cluster subprocesses. No production code, original assertion, 10-second deadline or 90-second test timeout changed.

## Rerun
Command: mvn -Pintegration -Dit.test=MembershipIT verify
Result: BUILD SUCCESS; 26 unit/RMI tests and 1 integration test; zero failures/errors/skips. MembershipIT elapsed 28.01 seconds; total Maven time 58.276 seconds. Its total runtime is not the ClusterStatus runtime.
ClusterStatus exited within the unchanged 10-second limit, with exit code 0 and four status lines (status.log 1176 bytes). Two client subprocesses completed their checked jobs. Rerun logs/reports are in rerun/ and the console log is rerun-maven.log. javap confirmed the compiled test contains the diagnostic helper.
The timeout hook did not fire, so no new jcmd dumps were produced. The helper compiled but its failure path was not exercised in this rerun.

## Interpretation and next step
The intermittent failure did not reproduce and has NOT been fixed. There is still no evidence locating the original stall in JVM startup, registry lookup, bootstrap.active(), first worker status(), or shutdown. Empty output alone cannot distinguish the pre-first-output stages. Keep this uncommitted test-only instrumentation for the next occurrence, then inspect the ClusterStatus main-thread stack and corresponding bootstrap/worker stacks before proposing any production change or timeout change.

No commits, pushes or production modifications were made. No unrelated processes were terminated.
