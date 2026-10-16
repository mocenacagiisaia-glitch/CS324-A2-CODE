package edu.usp.cs324.recovery;

import edu.usp.cs324.proto.recovery.RecoveryJob;
import edu.usp.cs324.proto.recovery.RecoveryJobInput;
import edu.usp.cs324.proto.recovery.RecoverySnapshot;
import edu.usp.cs324.proto.recovery.RecoveryTask;
import edu.usp.cs324.proto.recovery.RecoveryWorkerCounter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class RecoveryStateStoreTest {
    @TempDir Path directory;

    @Test
    void restoresCompleteResultsAndUnfinishedTaskAssignmentsAfterReopen() throws Exception {
        Path file = directory.resolve("worker-1/state.pb");
        RecoveryJob unfinished = job().toBuilder().setJobId("job-unfinished")
                .setState(RecoveryJob.State.RUNNING).clearResult().clearTasks()
                .addTasks(task().toBuilder().setTaskId("task-running")
                        .setState(RecoveryTask.State.RUNNING).clearResult().setAttempt(3))
                .build();
        RecoveryJob zeroResult = RecoveryJob.newBuilder().setJobId("job-zero")
                .setInput(RecoveryJobInput.newBuilder().setOperation(RecoveryJobInput.Operation.PRIMECOUNT))
                .setState(RecoveryJob.State.COMPLETED).setResult("0")
                .addTasks(task().toBuilder().setTaskId("task-zero").setResult("0")
                        .setInput(RecoveryJobInput.newBuilder().setOperation(RecoveryJobInput.Operation.PRIMECOUNT)))
                .build();
        RecoverySnapshot expected = snapshot().toBuilder().addJobs(unfinished).addJobs(zeroResult).build();
        try (RecoveryStateStore store = new RecoveryStateStore(file)) {
            assertTrue(store.latest().isEmpty());
            assertTrue(store.save(expected));
        }
        try (RecoveryStateStore restored = new RecoveryStateStore(file)) {
            assertEquals(expected, restored.latest().orElseThrow());
            assertFalse(restored.latest().orElseThrow().getJobs(1).getTasks(0).hasResult());
            assertEquals("0", restored.latest().orElseThrow().getJobs(2).getResult());
        }
    }

    @Test
    void exactReplayIsIdempotentButOlderOrConflictingRevisionsAreRejected() throws Exception {
        RecoverySnapshot current = snapshot().toBuilder().setRevision(4).build();
        try (RecoveryStateStore store = new RecoveryStateStore(directory.resolve("state.pb"))) {
            assertTrue(store.save(current));
            assertFalse(store.save(current));
            assertThrows(IllegalArgumentException.class, () -> store.save(current.toBuilder().setRevision(3).build()));
            assertThrows(IllegalArgumentException.class, () -> store.save(current.toBuilder().setElectionId("conflict").build()));
            assertEquals(current, store.latest().orElseThrow());
        }
    }

    @Test
    void rejectsOlderTermsAndCoordinatorChangesWithinTheSameTerm() throws Exception {
        RecoverySnapshot current = snapshot().toBuilder().setLeadershipTerm(2).build();
        try (RecoveryStateStore store = new RecoveryStateStore(directory.resolve("state.pb"))) {
            store.save(current);
            assertThrows(IllegalArgumentException.class, () -> store.save(current.toBuilder()
                    .setRevision(2).setLeadershipTerm(1).build()));
            assertThrows(IllegalArgumentException.class, () -> store.save(current.toBuilder()
                    .setRevision(2).setCoordinatorId(2).setCoordinatorIncarnationId("inc-2").build()));
            assertThrows(IllegalArgumentException.class, () -> store.save(current.toBuilder()
                    .setRevision(2).setElectionId("different-election").build()));
            assertEquals(current, store.latest().orElseThrow());
        }
    }

    @Test
    void acceptsAFullSnapshotJumpAndANewerLeadershipTerm() throws Exception {
        RecoverySnapshot next = snapshot().toBuilder().setRevision(8).setLeadershipTerm(2)
                .setCoordinatorId(2).setCoordinatorIncarnationId("inc-2").setElectionId("election-2").build();
        Path file = directory.resolve("state.pb");
        try (RecoveryStateStore store = new RecoveryStateStore(file)) {
            store.save(snapshot());
            assertTrue(store.save(next));
        }
        try (RecoveryStateStore restored = new RecoveryStateStore(file)) {
            assertEquals(next, restored.latest().orElseThrow());
        }
    }

    @Test
    void invalidTaskResultsIdentifiersAndCountersNeverReplaceTheCheckpoint() throws Exception {
        RecoverySnapshot current = snapshot();
        try (RecoveryStateStore store = new RecoveryStateStore(directory.resolve("state.pb"))) {
            store.save(current);
            RecoverySnapshot.Builder next = current.toBuilder().setRevision(2);
            assertThrows(IllegalArgumentException.class, () -> store.save(next.clone().setJobs(0,
                    job().toBuilder().setTasks(0, task().toBuilder().clearResult())).build()));
            assertThrows(IllegalArgumentException.class, () -> store.save(next.clone().addJobs(job()).build()));
            assertThrows(IllegalArgumentException.class, () -> store.save(next.clone().addJobs(
                    job().toBuilder().setJobId("different-job")).build())); // Duplicate task ID across jobs.
            assertThrows(IllegalArgumentException.class, () -> store.save(next.clone().setWorkerCounters(0,
                    current.getWorkerCounters(0).toBuilder().setAssignedJobs(6)).build()));
            assertThrows(IllegalArgumentException.class, () -> store.save(next.clone().setJobs(0,
                    job().toBuilder().setResult("not-an-integer")).build()));
            assertEquals(current, store.latest().orElseThrow());
        }
    }

    @Test
    void corruptionAndTruncationAreReportedRatherThanLoadingAnEmptyState() throws Exception {
        Path file = directory.resolve("state.pb");
        try (RecoveryStateStore store = new RecoveryStateStore(file)) { store.save(snapshot()); }
        byte[] good = Files.readAllBytes(file);
        byte[] corrupt = good.clone();
        corrupt[corrupt.length - 1] ^= 1;
        for (byte[] invalid : new byte[][] { Arrays.copyOf(good, 12), corrupt, Arrays.copyOf(good, good.length + 1) }) {
            Files.write(file, invalid);
            assertThrows(IOException.class, () -> {
                try (RecoveryStateStore ignored = new RecoveryStateStore(file)) { fail("Corrupt state must not open"); }
            });
            assertArrayEquals(invalid, Files.readAllBytes(file));
        }
        Files.write(file, good);
        try (RecoveryStateStore restored = new RecoveryStateStore(file)) {
            assertEquals(snapshot(), restored.latest().orElseThrow());
        }
    }

    @Test
    void exclusiveFileOwnershipIsReleasedWhenTheStoreCloses() throws Exception {
        Path file = directory.resolve("state.pb");
        try (RecoveryStateStore first = new RecoveryStateStore(file)) {
            first.save(snapshot());
            assertThrows(IOException.class, () -> {
                try (RecoveryStateStore ignored = new RecoveryStateStore(file)) { fail("A second owner must not open"); }
            });
        }
        try (RecoveryStateStore second = new RecoveryStateStore(file)) {
            assertEquals(snapshot(), second.latest().orElseThrow());
        }
    }

    @Test
    void failedAtomicReplacementPreservesThePreviousFileAndMemoryState() throws Exception {
        Path file = directory.resolve("state.pb");
        RecoverySnapshot current = snapshot();
        try (RecoveryStateStore store = new RecoveryStateStore(file)) { store.save(current); }
        byte[] before = Files.readAllBytes(file);
        try (RecoveryStateStore store = new RecoveryStateStore(file, (source, destination) -> {
            throw new IOException("Injected atomic replacement failure");
        })) {
            assertThrows(IOException.class, () -> store.save(current.toBuilder().setRevision(2).build()));
            assertEquals(current, store.latest().orElseThrow());
            assertArrayEquals(before, Files.readAllBytes(file));
            try (var paths = Files.list(directory)) {
                assertFalse(paths.anyMatch(path -> path.getFileName().toString().endsWith(".tmp")));
            }
        }
        try (RecoveryStateStore restored = new RecoveryStateStore(file)) {
            assertEquals(current, restored.latest().orElseThrow());
        }
    }

    private static RecoveryJobInput input() {
        return RecoveryJobInput.newBuilder().setOperation(RecoveryJobInput.Operation.PRIMESUM)
                .setRangeStart(1).setRangeEnd(10).build();
    }

    private static RecoveryTask task() {
        return RecoveryTask.newBuilder().setTaskId("task-1").setInput(input())
                .setState(RecoveryTask.State.COMPLETED).setAssignedWorkerId(2)
                .setAssignedWorkerIncarnationId("inc-2").setAttempt(1).setResult("17").build();
    }

    private static RecoveryJob job() {
        return RecoveryJob.newBuilder().setJobId("job-1").setInput(input())
                .setState(RecoveryJob.State.COMPLETED).addTasks(task()).setResult("17").build();
    }

    private static RecoverySnapshot snapshot() {
        return RecoverySnapshot.newBuilder().setRevision(1).setLeadershipTerm(1).setElectionId("election-1")
                .setCoordinatorId(1).setCoordinatorIncarnationId("inc-1").addJobs(job())
                .addWorkerCounters(RecoveryWorkerCounter.newBuilder().setWorkerId(1).setJac(1).setAssignedJobs(1))
                .addWorkerCounters(RecoveryWorkerCounter.newBuilder().setWorkerId(2).setJac(0).setAssignedJobs(0))
                .build();
    }
}
