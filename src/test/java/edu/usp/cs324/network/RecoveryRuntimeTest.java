package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import edu.usp.cs324.client.GrpcJobClient;
import edu.usp.cs324.jobs.*;
import edu.usp.cs324.proto.*;
import edu.usp.cs324.proto.recovery.*;
import edu.usp.cs324.recovery.*;
import io.grpc.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(60)
class RecoveryRuntimeTest {
    @TempDir Path root;
    static final Job JOB = new Job(Job.Type.PRIMESUM, List.of(), 1, 1000);
    static final class Directory extends BootstrapServiceGrpc.BootstrapServiceImplBase {
        final Map<Integer, WorkerEndpoint> workers = new ConcurrentSkipListMap<>();
        @Override public void registerWorker(RegisterWorkerRequest r, StreamObserver<RegisterWorkerReply> o) {
            var others = workers.values().stream().filter(w -> w.getWorkerId() != r.getWorker().getWorkerId()).toList();
            workers.put(r.getWorker().getWorkerId(), r.getWorker());
            o.onNext(RegisterWorkerReply.newBuilder().setAccepted(true).addAllExistingWorkers(others).build());o.onCompleted();
        }
        @Override public void listActiveWorkers(ListActiveWorkersRequest r, StreamObserver<ListActiveWorkersReply> o) {
            o.onNext(ListActiveWorkersReply.newBuilder().addAllWorkers(workers.values()).build());o.onCompleted();
        }
        @Override public void heartbeat(HeartbeatRequest r, StreamObserver<HeartbeatReply> o) {
            var w = workers.get(r.getWorkerId());
            o.onNext(HeartbeatReply.newBuilder().setAccepted(w != null && w.getIncarnationId().equals(r.getIncarnationId())).build());o.onCompleted();
        }
    }
    static int port() throws Exception { try (var s = new ServerSocket(0)) { return s.getLocalPort(); } }
    static final class Worker implements AutoCloseable {
        final Peer peer;
        final BootstrapGrpcClient directory;
        final WorkerNode node;
        final RecoveryStateStore store;
        final RecoveryRuntime runtime;
        final Server server;
        final Map<WorkerEndpoint, CoordinationGrpcClient> connections = new ConcurrentHashMap<>();
        boolean stopped;
        final Set<java.net.SocketAddress> mutationConnections = ConcurrentHashMap.newKeySet();
        final java.util.concurrent.atomic.AtomicBoolean failWrites = new java.util.concurrent.atomic.AtomicBoolean();
        volatile long failAfterRevision;
        Worker(int id, int bootstrapPort, Path checkpoint, JobEngine engine) throws Exception {
            this(id, bootstrapPort, checkpoint, engine, port());
        }
        Worker(int id, int bootstrapPort, Path checkpoint, JobEngine engine, int grpcPort) throws Exception {
            peer = new Peer(id, "127.0.0.1", 10000 + id);
            directory = new BootstrapGrpcClient("127.0.0.1", bootstrapPort, peer, grpcPort, true);
            node = new WorkerNode(peer, directory, 0, engine, false);
            store = new RecoveryStateStore(checkpoint);
            runtime = new RecoveryRuntime(node, directory, store, engine);
            node.enableRecovery(runtime);
            node.useTransport(p -> p.equals(peer) ? node : connections.computeIfAbsent(directory.endpoint(p),
                    e -> new CoordinationGrpcClient(e, directory::endpoint)));
            server = ServerBuilder.forPort(grpcPort).intercept(new ServerInterceptor() {
                @Override public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
                    if (call.getMethodDescriptor().getFullMethodName().endsWith("/Mutate"))
                        mutationConnections.add(call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR));
                    var listener = next.startCall(call, headers);
                    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<Q>(listener) {
                        boolean rejected;
                        @Override public void onMessage(Q message) {
                            if (failWrites.get() && message instanceof SaveRecoverySnapshotRequest request
                                    && request.getSnapshot().getRevision() > failAfterRevision) {
                                rejected = true;
                                call.close(Status.UNAVAILABLE.withDescription("Injected replica write failure"), new Metadata());
                            } else super.onMessage(message);
                        }
                        @Override public void onHalfClose() { if (!rejected) super.onHalfClose(); }
                    };
                }
            }).maxInboundMessageSize(RecoveryStateServiceImpl.MAX_RPC_MESSAGE_BYTES)
                    .addService(new WorkerMembershipServiceImpl(node, directory)).addService(new RecoveryStateServiceImpl(store))
                    .addService(new CoordinationServiceImpl(node, directory::endpoint)).addService(runtime.service())
                    .addService(new JobServiceImpl(runtime::submit)).build().start();
            directory.register(peer); runtime.start();
        }
        @Override public void close() throws Exception {
            if (stopped) return; stopped = true;
            runtime.close(); directory.close(); server.shutdownNow(); node.close();
            connections.values().forEach(CoordinationGrpcClient::close);
            server.awaitTermination(3, TimeUnit.SECONDS); store.close();
        }
    }
    final class Cluster implements AutoCloseable {
        final Directory directory = new Directory();
        final Server bootstrap = ServerBuilder.forPort(0).addService(directory).build().start();
        final List<Worker> workers = new ArrayList<>();
        final GrpcJobClient client = new GrpcJobClient("127.0.0.1", bootstrap.getPort());
        Cluster(JobEngine engine) throws Exception { for (int id = 1; id <= 3; id++) workers.add(new Worker(id, bootstrap.getPort(), root.resolve("w" + id), engine)); }
        void crash(int index, boolean expireLease) throws Exception {
            Worker worker = workers.get(index); worker.close();
            if (expireLease) directory.workers.remove(worker.peer.id());
        }
        RecoverySnapshot latest(int index) { return workers.get(index).store.latest().orElse(RecoverySnapshot.getDefaultInstance()); }
        @Override public void close() throws Exception {
            for (var w : workers.reversed()) w.close();
            bootstrap.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
    }
    static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(condition.getAsBoolean(), "Recovery condition timed out");
    }
    static JobEngine blocking(CountDownLatch release, AtomicInteger calls) {
        IntegerJobEngine engine = new IntegerJobEngine();
        return new JobEngine() {
            public List<Job> split(Job job, int n) { return engine.split(job,n); }
            public BigInteger aggregate(Job.Type t, List<BigInteger> r) { return engine.aggregate(t,r); }
            public BigInteger compute(Job job) {
                calls.incrementAndGet();
                try { if (!release.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("Test release timeout"); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
                return engine.compute(job);
            }
        };
    }
    @Test void duplicateIdsShareOneDurableJobAndRejectDifferentInput() throws Exception {
        try (var c = new Cluster(new IntegerJobEngine()); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = pool.submit(() -> c.client.submit("same-job", JOB, p -> {}));
            var second = pool.submit(() -> c.client.submit("same-job", JOB, p -> {}));
            assertEquals(BigInteger.valueOf(76127), first.get(30, TimeUnit.SECONDS));
            assertEquals(first.get(), second.get(30, TimeUnit.SECONDS));
            var snapshot = c.latest(0);
            assertEquals(1, snapshot.getJobsCount());
            assertEquals(2, snapshot.getWorkerCountersList().stream().mapToLong(RecoveryWorkerCounter::getJac).sum());
            assertEquals(BigInteger.valueOf(76127), c.client.submit("same-job", JOB, p -> {}));
            var error = assertThrows(StatusRuntimeException.class, () -> c.client.submit("same-job",
                    new Job(Job.Type.MAX, List.of(42), 0, 0), p -> {}));
            assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
            // The same validation must survive forwarding through a non-authority worker.
            ManagedChannel forwarded = ManagedChannelBuilder.forAddress("127.0.0.1", c.workers.get(1).server.getPort()).usePlaintext().build();
            try {
                var stream = edu.usp.cs324.proto.jobs.JobServiceGrpc.newBlockingStub(forwarded).withDeadlineAfter(5, TimeUnit.SECONDS)
                        .submitJob(edu.usp.cs324.proto.jobs.SubmitJobRequest.newBuilder().setRequestId("same-job")
                                .setInput(JobWire.encode(new Job(Job.Type.MAX, List.of(99), 0, 0))).build());
                var forwardedError = assertThrows(StatusRuntimeException.class, () -> { while (stream.hasNext()) stream.next(); });
                assertEquals(Status.Code.INVALID_ARGUMENT, forwardedError.getStatus().getCode());
            } finally { forwarded.shutdownNow(); }
        }
    }
    @Test void healthyRepeatedSubmissionsReusePeerConnectionsAndPreserveRevision() throws Exception {
        try (var c = new Cluster(new IntegerJobEngine())) {
            assertEquals(BigInteger.valueOf(76127), c.client.submit("connection-reuse", JOB, p -> {}));
            long revision = c.latest(0).getRevision();
            for (int i = 0; i < 12; i++)
                assertEquals(BigInteger.valueOf(76127), c.workers.get(1).runtime.submit("connection-reuse", JOB));
            assertEquals(revision, c.latest(0).getRevision());
            // At most one control transport from each of the two other workers,
            // even when submission polling and the recovery monitor overlap.
            assertTrue(c.workers.getFirst().mutationConnections.size() <= 2,
                    "Repeated RPCs must reuse the peer's HTTP/2 connection");
        }
    }
    @Test void coordinatorFailureRecoversAcceptedTasksAndKeepsOneResult() throws Exception {
        CountDownLatch release = new CountDownLatch(1); AtomicInteger calls = new AtomicInteger();
        try (var c = new Cluster(blocking(release, calls)); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = pool.submit(() -> c.client.submit("leader-failure", JOB, p -> {}));
            await(() -> calls.get() >= 3);
            assertEquals(3, c.latest(0).getCoordinatorId());
            c.crash(2, true); release.countDown();
            assertEquals(BigInteger.valueOf(76127), result.get(30, TimeUnit.SECONDS));
            var state = c.latest(0);
            assertEquals(2, state.getCoordinatorId()); assertEquals(1, state.getJobsCount());
            assertTrue(state.getJobs(0).getTasksList().stream().anyMatch(t -> t.getAttempt() >= 2));
        } finally { release.countDown(); }
    }
    @Test void sequencerFailureReconnectsSameClientIdAndRecoversFromSurvivingReplica() throws Exception {
        CountDownLatch release = new CountDownLatch(1); AtomicInteger calls = new AtomicInteger();
        try (var c = new Cluster(blocking(release, calls)); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = pool.submit(() -> c.client.submit("gate-failure", JOB, p -> {}));
            await(() -> calls.get() >= 3);
            c.crash(0, true); release.countDown();
            assertEquals(BigInteger.valueOf(76127), result.get(30, TimeUnit.SECONDS));
            assertEquals(1, c.latest(1).getJobsCount());
            assertEquals("76127", c.latest(1).getJobs(0).getResult());
        } finally { release.countDown(); }
    }
    @Test void unavailableListedReplicaBlocksNewDispatchUntilItsLeaseExpires() throws Exception {
        AtomicInteger calls = new AtomicInteger(); CountDownLatch release = new CountDownLatch(0);
        try (var c = new Cluster(blocking(release, calls)); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            c.crash(1, false);
            var result = pool.submit(() -> c.client.submit("wait-for-lease", JOB, p -> {}));
            Thread.sleep(300);
            assertEquals(0, calls.get()); assertFalse(result.isDone());
            c.directory.workers.remove(2);
            assertEquals(BigInteger.valueOf(76127), result.get(30, TimeUnit.SECONDS));
        }
    }
    @Test void fiveJobRotationPreservesHealthyInflightTasksWithoutReallocation() throws Exception {
        CountDownLatch release = new CountDownLatch(1); AtomicInteger calls = new AtomicInteger();
        try (var c = new Cluster(blocking(release, calls)); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var results = new ArrayList<Future<BigInteger>>();
            for (int i = 0; i < 5; i++) {
                String id = "rotation-" + i;
                results.add(pool.submit(() -> c.client.submit(id, JOB, p -> {})));
            }
            await(() -> calls.get() >= 15);
            assertEquals(2, c.latest(0).getLeadershipTerm());
            assertEquals(10, c.latest(0).getWorkerCountersList().stream().mapToLong(RecoveryWorkerCounter::getJac).sum());
            release.countDown();
            for (var result : results) assertEquals(BigInteger.valueOf(76127), result.get(30, TimeUnit.SECONDS));
            assertTrue(c.latest(0).getJobsList().stream().flatMap(j -> j.getTasksList().stream()).allMatch(t -> t.getAttempt() == 1));
        } finally { release.countDown(); }
    }
    @Test void fullWorkerRestartReopensCheckpointsAndResumesTheSameJob() throws Exception {
        CountDownLatch release = new CountDownLatch(1); AtomicInteger calls = new AtomicInteger();
        try (var c = new Cluster(blocking(release, calls)); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = pool.submit(() -> c.client.submit("restart", JOB, p -> {}));
            await(() -> calls.get() >= 3);
            long term = c.latest(0).getLeadershipTerm();
            for (var w : c.workers) w.close();
            c.directory.workers.clear(); c.workers.clear(); release.countDown();
            for (int id = 1; id <= 3; id++) c.workers.add(new Worker(id, c.bootstrap.getPort(), root.resolve("w" + id), new IntegerJobEngine()));
            assertEquals(BigInteger.valueOf(76127), result.get(30, TimeUnit.SECONDS));
            assertEquals(1, c.latest(0).getJobsCount());
            assertTrue(c.latest(0).getLeadershipTerm() > term);
            assertTrue(c.latest(0).getWorkerCountersList().stream().anyMatch(cn -> cn.getWorkerId() == 3 && cn.getJac() >= 2));
        } finally { release.countDown(); }
    }
    @Test void partiallyAcknowledgedSnapshotIsFinishedBeforeAnyDispatchAndNotAdmittedTwice() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (var c = new Cluster(blocking(new CountDownLatch(0), calls)); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            // Start from an existing fully acknowledged snapshot to exercise
            // cache invalidation as well as partial-write recovery.
            assertEquals(BigInteger.valueOf(76127), c.client.submit("already-acknowledged", JOB, p -> {}));
            int previousCalls = calls.get();
            c.workers.getLast().failAfterRevision = c.latest(0).getRevision();
            c.workers.getLast().failWrites.set(true);
            var result = pool.submit(() -> c.client.submit("partial", JOB, p -> {}));
            await(() -> c.latest(0).getJobsCount() == 2);
            assertEquals(previousCalls, calls.get());
            c.workers.getLast().failWrites.set(false);
            assertEquals(BigInteger.valueOf(76127), result.get(30, TimeUnit.SECONDS));
            assertEquals(2, c.latest(0).getJobsCount());
            assertEquals(4, c.latest(0).getWorkerCountersList().stream().mapToLong(RecoveryWorkerCounter::getJac).sum());
        }
    }
    @Test void staleAttemptsAndFormerCoordinatorCannotOverwriteACompletedTask() throws Exception {
        CountDownLatch release = new CountDownLatch(1); AtomicInteger calls = new AtomicInteger();
        try (var c = new Cluster(blocking(release, calls)); var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = pool.submit(() -> c.client.submit("fenced", JOB, p -> {}));
            await(() -> calls.get() >= 3);
            var before = c.latest(0); var task = before.getJobs(0).getTasks(0);
            var assigned = c.directory.workers.get(task.getAssignedWorkerId());
            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", c.workers.getFirst().server.getPort()).usePlaintext().build();
            try {
                var stub = RecoveryControlServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
                var stale = RecoveryMutation.newBuilder().setAction(RecoveryMutation.Action.COMPLETE).setJobId("fenced")
                        .setTaskId(task.getTaskId()).setExpectedAttempt(task.getAttempt() - 1).setWorker(assigned).setResult("999")
                        .setLeadershipTerm(before.getLeadershipTerm()).setCoordinatorIncarnationId(before.getCoordinatorIncarnationId());
                var unchanged = stub.mutate(stale.build());
                assertFalse(unchanged.getJobs(0).getTasks(0).hasResult());
                release.countDown();
                assertEquals(BigInteger.valueOf(76127), result.get(30, TimeUnit.SECONDS));
                var after = stub.mutate(stale.setExpectedAttempt(task.getAttempt())
                        .setLeadershipTerm(before.getLeadershipTerm() - 1).setCoordinatorIncarnationId("obsolete").build());
                assertEquals("76127", after.getJobs(0).getResult());
            } finally { channel.shutdownNow(); }
        } finally { release.countDown(); }
    }
}
