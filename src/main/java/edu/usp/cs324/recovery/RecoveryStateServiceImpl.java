package edu.usp.cs324.recovery;

import edu.usp.cs324.proto.recovery.GetRecoverySnapshotReply;
import edu.usp.cs324.proto.recovery.GetRecoverySnapshotRequest;
import edu.usp.cs324.proto.recovery.RecoveryStateServiceGrpc;
import edu.usp.cs324.proto.recovery.SaveRecoverySnapshotReply;
import edu.usp.cs324.proto.recovery.SaveRecoverySnapshotRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Objects;

/** gRPC access to a single local checkpoint store, not a consensus protocol. */
public final class RecoveryStateServiceImpl extends RecoveryStateServiceGrpc.RecoveryStateServiceImplBase {
    // Allow the maximum checkpoint plus the containing request/reply's Protobuf overhead.
    public static final int MAX_RPC_MESSAGE_BYTES = RecoveryStateStore.MAX_SNAPSHOT_BYTES + 1024;
    private final RecoveryStateStore store;

    public RecoveryStateServiceImpl(RecoveryStateStore store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    @Override
    public void saveSnapshot(SaveRecoverySnapshotRequest request, StreamObserver<SaveRecoverySnapshotReply> observer) {
        if (!request.hasSnapshot()) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("Recovery snapshot is required").asRuntimeException());
            return;
        }
        var snapshot = request.getSnapshot();
        try {
            RecoverySnapshotValidator.validate(snapshot);
            if (snapshot.getSerializedSize() > RecoveryStateStore.MAX_SNAPSHOT_BYTES)
                throw new IllegalArgumentException("Snapshot exceeds the 64 MiB payload limit");
        } catch (IllegalArgumentException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        boolean written;
        try {
            written = store.save(snapshot);
        } catch (IllegalArgumentException e) {
            observer.onError(Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asRuntimeException());
            return;
        } catch (IOException e) {
			e.printStackTrace(System.err);
            observer.onError(Status.INTERNAL.withDescription("Checkpoint could not be persisted").withCause(e).asRuntimeException());
            return;
        } catch (IllegalStateException e) {
            observer.onError(Status.UNAVAILABLE.withDescription("Recovery store is closed").asRuntimeException());
            return;
        }
        // Acknowledge the exact request revision, even if another save runs before this reply.
        observer.onNext(SaveRecoverySnapshotReply.newBuilder().setWritten(written)
                .setRevision(snapshot.getRevision()).setLeadershipTerm(snapshot.getLeadershipTerm()).build());
        observer.onCompleted();
    }

    @Override
    public void getSnapshot(GetRecoverySnapshotRequest request, StreamObserver<GetRecoverySnapshotReply> observer) {
        GetRecoverySnapshotReply.Builder reply = GetRecoverySnapshotReply.newBuilder();
        try {
            store.latest().ifPresent(reply::setSnapshot);
        } catch (IllegalStateException e) {
            observer.onError(Status.UNAVAILABLE.withDescription("Recovery store is closed").asRuntimeException());
            return;
        }
        observer.onNext(reply.build());
        observer.onCompleted();
    }
}
