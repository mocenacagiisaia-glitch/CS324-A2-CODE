package edu.usp.cs324.recovery;

import edu.usp.cs324.proto.recovery.GetRecoverySnapshotReply;
import edu.usp.cs324.proto.recovery.GetRecoverySnapshotRequest;
import edu.usp.cs324.proto.recovery.RecoverySnapshot;
import edu.usp.cs324.proto.recovery.RecoveryStateServiceGrpc;
import edu.usp.cs324.proto.recovery.RecoveryWorkerCounter;
import edu.usp.cs324.proto.recovery.SaveRecoverySnapshotRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Checkpoint requests travel through real loopback gRPC servers and real local files. */
@Timeout(20)
class RecoveryStateGrpcTest {
    @TempDir Path directory;

    @Test
    void aNewReplicaReportsNoSnapshot() throws Exception {
        try (Fixture fixture = new Fixture(directory.resolve("state.checkpoint"))) {
            assertTrue(fixture.client.latest().isEmpty());
        }
    }

    @Test
    void acknowledgedSnapshotCanBeRetrievedAfterServerAndStoreRestart() throws Exception {
        Path file = directory.resolve("state.checkpoint");
        RecoverySnapshot expected = snapshot(1);
        try (Fixture first = new Fixture(file)) {
            var reply = first.client.save(expected);
            assertTrue(reply.getWritten());
            assertEquals(1, reply.getRevision());
            assertEquals(1, reply.getLeadershipTerm());
        }
        try (Fixture restarted = new Fixture(file)) {
            assertEquals(expected, restarted.client.latest().orElseThrow());
        }
    }

    @Test
    void replayIsAcknowledgedWhileStaleAndConflictingSavesAreRejected() throws Exception {
        RecoverySnapshot current = snapshot(2);
        try (Fixture fixture = new Fixture(directory.resolve("state.checkpoint"))) {
            fixture.client.save(current);
            assertFalse(fixture.client.save(current).getWritten());
            StatusRuntimeException stale = assertThrows(StatusRuntimeException.class,
                    () -> fixture.client.save(snapshot(1)));
            assertEquals(Status.Code.FAILED_PRECONDITION, stale.getStatus().getCode());
            StatusRuntimeException conflicting = assertThrows(StatusRuntimeException.class,
                    () -> fixture.client.save(current.toBuilder().setElectionId("conflict").build()));
            assertEquals(Status.Code.FAILED_PRECONDITION, conflicting.getStatus().getCode());
            assertEquals(current, fixture.client.latest().orElseThrow());
        }
    }

    @Test
    void missingAndMalformedSnapshotRequestsAreRejectedWithoutSaving() throws Exception {
        try (Fixture fixture = new Fixture(directory.resolve("state.checkpoint"))) {
            StatusRuntimeException malformed = assertThrows(StatusRuntimeException.class,
                    () -> fixture.client.save(RecoverySnapshot.getDefaultInstance()));
            assertEquals(Status.Code.INVALID_ARGUMENT, malformed.getStatus().getCode());
            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", fixture.server.getPort())
                    .usePlaintext().build();
            try {
                var stub = RecoveryStateServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
                StatusRuntimeException missing = assertThrows(StatusRuntimeException.class,
                        () -> stub.saveSnapshot(SaveRecoverySnapshotRequest.getDefaultInstance()));
                assertEquals(Status.Code.INVALID_ARGUMENT, missing.getStatus().getCode());
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
            assertTrue(fixture.client.latest().isEmpty());
        }
    }

    @Test
    void failedDiskWriteReturnsAnRpcErrorAndPreservesTheOldCheckpoint() throws Exception {
        Path file = directory.resolve("state.checkpoint");
        RecoverySnapshot before = snapshot(1);
        try (RecoveryStateStore initial = new RecoveryStateStore(file)) { initial.save(before); }
        RecoveryStateStore failing = new RecoveryStateStore(file, (source, destination) -> {
            throw new IOException("Injected checkpoint I/O failure");
        });
        try (Fixture fixture = new Fixture(failing)) {
            StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                    () -> fixture.client.save(snapshot(2)));
            assertEquals(Status.Code.INTERNAL, error.getStatus().getCode());
            assertEquals(before, fixture.client.latest().orElseThrow());
        }
        try (RecoveryStateStore reopened = new RecoveryStateStore(file)) {
            assertEquals(before, reopened.latest().orElseThrow());
        }
    }

    @Test
    void aSilentServiceCannotBlockTheClientPastItsDeadline() throws Exception {
        Server server = ServerBuilder.forPort(0).addService(new RecoveryStateServiceGrpc.RecoveryStateServiceImplBase() {
            @Override public void getSnapshot(GetRecoverySnapshotRequest request,
                                              StreamObserver<GetRecoverySnapshotReply> observer) {
                // Deliberately send no response: the client must enforce its own deadline.
            }
        }).build().start();
        try (RecoveryStateGrpcClient client = new RecoveryStateGrpcClient("127.0.0.1", server.getPort(),
                Duration.ofMillis(250))) {
            StatusRuntimeException error = assertThrows(StatusRuntimeException.class, client::latest);
            assertEquals(Status.Code.DEADLINE_EXCEEDED, error.getStatus().getCode());
        } finally {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static RecoverySnapshot snapshot(long revision) {
        return RecoverySnapshot.newBuilder().setRevision(revision).setLeadershipTerm(1)
                .setElectionId("election-1").setCoordinatorId(1).setCoordinatorIncarnationId("inc-1")
                .addWorkerCounters(RecoveryWorkerCounter.newBuilder().setWorkerId(1)
                        .setJac(0).setAssignedJobs(0)).build();
    }

    private static final class Fixture implements AutoCloseable {
        private final RecoveryStateStore store;
        private final Server server;
        private final RecoveryStateGrpcClient client;

        Fixture(Path file) throws Exception { this(new RecoveryStateStore(file)); }

        Fixture(RecoveryStateStore store) throws Exception {
            this.store = store;
            Server started = null;
            try {
                started = ServerBuilder.forPort(0)
                        .maxInboundMessageSize(RecoveryStateServiceImpl.MAX_RPC_MESSAGE_BYTES)
                        .addService(new RecoveryStateServiceImpl(store)).build().start();
                client = new RecoveryStateGrpcClient("127.0.0.1", started.getPort());
                server = started;
            } catch (Exception e) {
                if (started != null) started.shutdownNow();
                store.close();
                throw e;
            }
        }

        @Override public void close() throws Exception {
            client.close();
            try { server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); }
            finally { store.close(); }
        }
    }
}
