package edu.usp.cs324.network;

import edu.usp.cs324.api.BootstrapRemote;
import edu.usp.cs324.api.Peer;
import edu.usp.cs324.proto.AddNeighbourRequest;
import edu.usp.cs324.proto.BootstrapServiceGrpc;
import edu.usp.cs324.proto.HeartbeatRequest;
import edu.usp.cs324.proto.ListActiveWorkersRequest;
import edu.usp.cs324.proto.RegisterWorkerRequest;
import edu.usp.cs324.proto.WorkerEndpoint;
import edu.usp.cs324.proto.WorkerMembershipServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.rmi.RemoteException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Local adapter: BootstrapRemote's Java signatures, with all directory calls sent over gRPC. */
public final class BootstrapGrpcClient implements BootstrapRemote, AutoCloseable {
    private static final long RPC_TIMEOUT_SECONDS = 3;
    private final Peer peer;
    private final WorkerEndpoint self;
    private final ManagedChannel channel;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub stub;
    private final ScheduledExecutorService heartbeats;
    private final long heartbeatMillis;
    private boolean heartbeatStarted;
    private boolean closed;
    private WorkerEndpoint pendingNeighbour;

    public BootstrapGrpcClient(String host, int bootstrapPort, Peer peer, int grpcPort) {
        this(host, bootstrapPort, peer, grpcPort, Duration.ofSeconds(5));
    }

    /** Short heartbeat interval is useful in tests without waiting for a real lease to expire. */
    BootstrapGrpcClient(String host, int bootstrapPort, Peer peer, int grpcPort, Duration interval) {
        if (host == null || host.isBlank()) throw new IllegalArgumentException("Bootstrap host is required");
        checkPort(bootstrapPort);
        checkPort(grpcPort);
        this.peer = Objects.requireNonNull(peer, "peer");
        if (peer.id() <= 0 || peer.host() == null || peer.host().isBlank())
            throw new IllegalArgumentException("A positive worker ID and advertised host are required");
        checkPort(peer.registryPort());
        heartbeatMillis = Objects.requireNonNull(interval, "interval").toMillis();
        if (heartbeatMillis <= 0 || heartbeatMillis > 5_000)
            throw new IllegalArgumentException("Heartbeat interval must be between 1 and 5000 milliseconds");
        self = WorkerEndpoint.newBuilder().setWorkerId(peer.id()).setHost(peer.host())
                .setGrpcPort(grpcPort).setRegistryPort(peer.registryPort())
                .setIncarnationId(UUID.randomUUID().toString()).build();
        channel = ManagedChannelBuilder.forAddress(host, bootstrapPort).usePlaintext().build();
        stub = BootstrapServiceGrpc.newBlockingStub(channel);
        heartbeats = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "worker-" + peer.id() + "-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public synchronized void register(Peer requestedPeer) throws RemoteException {
        ensureOpen();
        if (!peer.equals(requestedPeer)) throw new RemoteException("Registration endpoint does not match this worker");
        registerEndpoint();
        if (!heartbeatStarted) {
            heartbeats.scheduleWithFixedDelay(this::refreshSafely, heartbeatMillis,
                    heartbeatMillis, TimeUnit.MILLISECONDS);
            heartbeatStarted = true;
        }
    }

    private void registerEndpoint() throws RemoteException {
        try {
            var reply = stub.withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .registerWorker(RegisterWorkerRequest.newBuilder().setWorker(self).build());
            if (!reply.getAccepted()) throw new RemoteException("Registration rejected: " + reply.getMessage());
            if (!reply.getExistingWorkersList().isEmpty()) {
                pendingNeighbour = reply.getExistingWorkers(ThreadLocalRandom.current()
                        .nextInt(reply.getExistingWorkersCount()));
                repairPendingLink();
            } else {
                pendingNeighbour = null;
            }
        } catch (StatusRuntimeException e) {
            throw new RemoteException("Bootstrap registration failed: " + e.getStatus(), e);
        }
    }

    @Override
    public synchronized List<Peer> active() throws RemoteException {
        ensureOpen();
        try {
            List<WorkerEndpoint> workers = stub.withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .listActiveWorkers(ListActiveWorkersRequest.getDefaultInstance()).getWorkersList();
            return workers.stream().map(BootstrapGrpcClient::toPeer).toList();
        } catch (StatusRuntimeException | IllegalArgumentException e) {
            throw new RemoteException("Bootstrap discovery failed: " + e.getMessage(), e);
        }
    }

    /** Also used by tests to exercise lease refresh and bootstrap restart deterministically. */
    synchronized void refreshLease() throws RemoteException {
        ensureOpen();
        try {
            boolean accepted = stub.withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .heartbeat(HeartbeatRequest.newBuilder().setWorkerId(self.getWorkerId())
                            .setIncarnationId(self.getIncarnationId()).build()).getAccepted();
            if (accepted) repairPendingLink();
            else registerEndpoint(); // Directory restarted, or the lease expired during an outage.
        } catch (StatusRuntimeException e) {
            throw new RemoteException("Bootstrap heartbeat failed: " + e.getStatus(), e);
        }
    }

    private void refreshSafely() {
        try { refreshLease(); }
        catch (RemoteException e) { System.err.println(e.getMessage()); }
    }

    private void repairPendingLink() {
        if (pendingNeighbour == null) return;
        try {
            addNeighbour(pendingNeighbour, self);
            addNeighbour(self, pendingNeighbour);
            pendingNeighbour = null;
        } catch (RuntimeException e) {
            // Registration is valid. Retry the idempotent reciprocal updates on the next heartbeat.
            System.err.println("Neighbour link pending for worker " + peer.id() + ": " + e.getMessage());
        }
    }

    private static void addNeighbour(WorkerEndpoint target, WorkerEndpoint neighbour) {
        checkPort(target.getGrpcPort());
        ManagedChannel targetChannel = ManagedChannelBuilder.forAddress(target.getHost(), target.getGrpcPort())
                .usePlaintext().build();
        try {
            var reply = WorkerMembershipServiceGrpc.newBlockingStub(targetChannel)
                    .withDeadlineAfter(RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .addNeighbour(AddNeighbourRequest.newBuilder().setNeighbour(neighbour).build());
            if (!reply.getAccepted()) throw new IllegalStateException("Neighbour rejected: " + reply.getMessage());
        } finally {
            targetChannel.shutdownNow();
        }
    }

    static Peer toPeer(WorkerEndpoint endpoint) {
        if (endpoint.getWorkerId() <= 0 || endpoint.getHost().isBlank())
            throw new IllegalArgumentException("Invalid worker ID or host in membership endpoint");
        checkPort(endpoint.getRegistryPort());
        return new Peer(endpoint.getWorkerId(), endpoint.getHost(), endpoint.getRegistryPort());
    }

    private static void checkPort(int port) {
        if (port < 1 || port > 65_535) throw new IllegalArgumentException("Port must be between 1 and 65535");
    }

    private void ensureOpen() throws RemoteException {
        if (closed) throw new RemoteException("Bootstrap gRPC client is closed");
    }

    @Override
    public synchronized void close() {
        closed = true;
        heartbeats.shutdownNow();
        channel.shutdownNow();
    }
}
