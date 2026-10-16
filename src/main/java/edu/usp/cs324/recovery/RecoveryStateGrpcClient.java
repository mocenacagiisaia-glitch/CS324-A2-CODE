package edu.usp.cs324.recovery;

import edu.usp.cs324.proto.recovery.GetRecoverySnapshotRequest;
import edu.usp.cs324.proto.recovery.RecoverySnapshot;
import edu.usp.cs324.proto.recovery.RecoveryStateServiceGrpc;
import edu.usp.cs324.proto.recovery.SaveRecoverySnapshotReply;
import edu.usp.cs324.proto.recovery.SaveRecoverySnapshotRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Deadline-bounded access to one worker replica; callers implement quorum policy. */
public final class RecoveryStateGrpcClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final RecoveryStateServiceGrpc.RecoveryStateServiceBlockingStub stub;
    private final long timeoutNanos;
    private final AtomicBoolean closed = new AtomicBoolean();

    public RecoveryStateGrpcClient(String host, int port) {
        this(host, port, Duration.ofSeconds(3));
    }

    public RecoveryStateGrpcClient(String host, int port, Duration timeout) {
        if (host == null || host.isBlank()) throw new IllegalArgumentException("Worker host is required");
        if (port < 1 || port > 65_535) throw new IllegalArgumentException("Worker gRPC port must be between 1 and 65535");
        timeoutNanos = Objects.requireNonNull(timeout, "timeout").toNanos();
        if (timeoutNanos <= 0) throw new IllegalArgumentException("RPC timeout must be positive");
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext()
                .maxInboundMessageSize(RecoveryStateServiceImpl.MAX_RPC_MESSAGE_BYTES).build();
        stub = RecoveryStateServiceGrpc.newBlockingStub(channel);
    }

    public SaveRecoverySnapshotReply save(RecoverySnapshot snapshot) {
        ensureOpen();
        return stub.withDeadlineAfter(timeoutNanos, TimeUnit.NANOSECONDS)
                .saveSnapshot(SaveRecoverySnapshotRequest.newBuilder()
                        .setSnapshot(Objects.requireNonNull(snapshot, "snapshot")).build());
    }

    public Optional<RecoverySnapshot> latest() {
        ensureOpen();
        var reply = stub.withDeadlineAfter(timeoutNanos, TimeUnit.NANOSECONDS)
                .getSnapshot(GetRecoverySnapshotRequest.getDefaultInstance());
        return reply.hasSnapshot() ? Optional.of(reply.getSnapshot()) : Optional.empty();
    }

    private void ensureOpen() {
        if (closed.get()) throw new IllegalStateException("Recovery gRPC client is closed");
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        channel.shutdownNow();
        try { channel.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
