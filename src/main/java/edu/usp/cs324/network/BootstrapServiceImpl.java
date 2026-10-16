package edu.usp.cs324.network;

import edu.usp.cs324.proto.BootstrapServiceGrpc;
import edu.usp.cs324.proto.HeartbeatReply;
import edu.usp.cs324.proto.HeartbeatRequest;
import edu.usp.cs324.proto.ListActiveWorkersReply;
import edu.usp.cs324.proto.ListActiveWorkersRequest;
import edu.usp.cs324.proto.RegisterWorkerReply;
import edu.usp.cs324.proto.RegisterWorkerRequest;
import edu.usp.cs324.proto.WorkerEndpoint;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/** gRPC membership directory. Workers refresh their lease using Heartbeat. */
public final class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {
    public static final Duration DEFAULT_HEARTBEAT_TIMEOUT = Duration.ofSeconds(15);

    private final NavigableMap<Long, Registration> members = new TreeMap<>();
    private final long heartbeatTimeoutNanos;
    private final LongSupplier ticker;

    public BootstrapServiceImpl() {
        this(DEFAULT_HEARTBEAT_TIMEOUT, System::nanoTime);
    }

    /** The ticker overload makes lease expiration deterministic in unit tests. */
    BootstrapServiceImpl(Duration heartbeatTimeout, LongSupplier ticker) {
        Objects.requireNonNull(heartbeatTimeout, "heartbeatTimeout");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        this.heartbeatTimeoutNanos = heartbeatTimeout.toNanos();
        if (heartbeatTimeoutNanos <= 0) {
            throw new IllegalArgumentException("Heartbeat timeout must be positive");
        }
    }

    @Override
    public synchronized void registerWorker(RegisterWorkerRequest request,
                                            StreamObserver<RegisterWorkerReply> responseObserver) {
        if (request == null || !request.hasWorker()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("worker endpoint is required").asRuntimeException());
            return;
        }

        WorkerEndpoint worker = request.getWorker();
        String invalid = validate(worker);
        if (invalid != null) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(invalid).asRuntimeException());
            return;
        }

        reapExpired();
        long workerId = Integer.toUnsignedLong(worker.getWorkerId());
        Registration current = members.get(workerId);
        if (current != null && !current.endpoint().getIncarnationId().equals(worker.getIncarnationId())) {
            responseObserver.onNext(RegisterWorkerReply.newBuilder()
                    .setAccepted(false)
                    .setMessage("Worker ID is already registered by another active incarnation")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        List<WorkerEndpoint> existing = snapshotExcept(workerId);
        members.put(workerId, new Registration(worker, ticker.getAsLong()));
        responseObserver.onNext(RegisterWorkerReply.newBuilder()
                .setAccepted(true)
                .setMessage(current == null ? "Worker registered" : "Worker registration refreshed")
                .addAllExistingWorkers(existing)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public synchronized void listActiveWorkers(ListActiveWorkersRequest request,
                                               StreamObserver<ListActiveWorkersReply> responseObserver) {
        reapExpired();
        List<WorkerEndpoint> active = members.values().stream()
                .map(Registration::endpoint)
                .toList();
        responseObserver.onNext(ListActiveWorkersReply.newBuilder().addAllWorkers(active).build());
        responseObserver.onCompleted();
    }

    @Override
    public synchronized void heartbeat(HeartbeatRequest request,
                                      StreamObserver<HeartbeatReply> responseObserver) {
        if (request == null || Integer.toUnsignedLong(request.getWorkerId()) == 0
                || request.getIncarnationId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("worker ID and incarnation ID are required")
                    .asRuntimeException());
            return;
        }

        reapExpired();
        long workerId = Integer.toUnsignedLong(request.getWorkerId());
        Registration current = members.get(workerId);
        boolean accepted = current != null
                && current.endpoint().getIncarnationId().equals(request.getIncarnationId());
        if (accepted) {
            members.put(workerId, new Registration(current.endpoint(), ticker.getAsLong()));
        }
        responseObserver.onNext(HeartbeatReply.newBuilder().setAccepted(accepted).build());
        responseObserver.onCompleted();
    }

    private void reapExpired() {
        long now = ticker.getAsLong();
        members.values().removeIf(registration -> now - registration.lastHeartbeatNanos() >= heartbeatTimeoutNanos);
    }

    private List<WorkerEndpoint> snapshotExcept(long excludedId) {
        List<WorkerEndpoint> result = new ArrayList<>();
        members.forEach((id, registration) -> {
            if (id != excludedId) result.add(registration.endpoint());
        });
        return List.copyOf(result);
    }

    private static String validate(WorkerEndpoint worker) {
        if (Integer.toUnsignedLong(worker.getWorkerId()) == 0) return "worker ID must be greater than zero";
        if (worker.getHost().isBlank()) return "worker host is required";
        long port = Integer.toUnsignedLong(worker.getGrpcPort());
        if (port == 0 || port > 65_535) return "gRPC port must be between 1 and 65535";
        if (worker.getIncarnationId().isBlank()) return "incarnation ID is required";
        return null;
    }

    private record Registration(WorkerEndpoint endpoint, long lastHeartbeatNanos) { }
}
