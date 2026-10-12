package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import org.junit.jupiter.api.*;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class CoordinationTest {
    final List<Remote> exported = new ArrayList<>();
    final List<WorkerNode> nodes = new ArrayList<>();
    final List<Peer> peers = new ArrayList<>();
    BootstrapNode bootstrap;
    static final Job JOB = new Job(Job.Type.PRIMECOUNT, List.of(2), 0, 0);
    static JobEngine fixture() {
        return new JobEngine() {
            public List<Job> split(Job job, int count) { return Collections.nCopies(count, job); }
            public BigInteger compute(Job job) { return BigInteger.ONE; }
            public BigInteger aggregate(Job.Type type, List<BigInteger> results) {
                return results.stream().reduce(BigInteger.ZERO, BigInteger::add);
            }
        };
    }
    @BeforeEach void start() throws Exception {
        bootstrap = new BootstrapNode(0);
        exported.add(bootstrap);
        for (int id = 1; id <= 3; id++) {
            int port;
            try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
            var registry = LocateRegistry.createRegistry(port);
            exported.add(registry);
            Peer peer = new Peer(id, "localhost", port);
            WorkerNode node = new WorkerNode(peer, bootstrap, 0, fixture());
            nodes.add(node); peers.add(peer); registry.rebind("worker", node); bootstrap.register(peer);
        }
        // Add a cycle independently of random bootstrap topology.
        for (Peer a : peers) for (Peer b : peers) a.connect().addNeighbour(b);
    }
    @AfterEach void stop() throws Exception {
        for (WorkerNode node : nodes) node.close();
        for (Remote remote : exported.reversed()) UnicastRemoteObject.unexportObject(remote, true);
    }
    @Test void cycleDeduplicatesElectionAndCoordinatorAndIgnoresStaleTerms() throws Exception {
        UUID id = UUID.randomUUID();
        assertEquals(3, peers.getFirst().connect().election(id).size());
        assertTrue(peers.getLast().connect().election(id).isEmpty());
        Term term = nodes.getFirst().elect();
        assertEquals(3, term.leader().id());
        for (WorkerNode node : nodes) {
            node.coordinator(term);
            node.coordinator(new Term(1, new UUID(Long.MIN_VALUE, 0), peers.getFirst()));
            assertEquals(term, node.status().term());
        }
    }
    @Test void simultaneousElectionAttemptsConvergeOnOneTerm() throws Exception {
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Term>> calls = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                Peer peer = peers.get(i % 3);
                calls.add(pool.submit(() -> { start.await(); return peer.connect().elect(); }));
            }
            start.countDown();
            Term term = calls.getFirst().get();
            for (var call : calls) assertEquals(term, call.get());
            for (WorkerNode node : nodes) assertEquals(term, node.status().term());
        }
    }
    @Test void concurrentAdmissionsCapAtFiveAndRotateByLifetimeJac() throws Exception {
        Term term = nodes.getFirst().elect();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Boolean>> calls = new ArrayList<>();
            for (int i = 0; i < 20; i++) calls.add(pool.submit(() -> {
                try { assertEquals(BigInteger.valueOf(3), nodes.getLast().assign(JOB, term, peers)); return true; }
                catch (RetryException e) { return false; }
            }));
            int admitted = 0;
            for (var call : calls) if (call.get()) admitted++;
            assertEquals(5, admitted);
        }
        assertEquals(10, nodes.getLast().status().jac());
        Term rotated = nodes.getFirst().elect();
        assertEquals(2, rotated.leader().id());
        assertEquals(2, rotated.number());
        assertThrows(RetryException.class, () -> nodes.getLast().assign(JOB, term, peers));
        assertEquals(BigInteger.valueOf(3), nodes.getFirst().submit(JOB));
        assertEquals(2, nodes.get(1).status().jac());
    }
    @Test void unreachableDispatchIsAcceptedFailureAndDoesNotRefundJac() throws Exception {
        Term term = nodes.getFirst().elect();
        Peer missing = new Peer(99, "localhost", 1);
        RemoteException error = assertThrows(RemoteException.class,
                () -> nodes.getLast().assign(JOB, term, List.of(missing)));
        assertFalse(error instanceof RetryException);
        assertFalse(error instanceof JobFailedException);
        assertEquals(1, nodes.getLast().status().jac());
        assertEquals(1, nodes.getLast().status().assignedJobs());
    }
    @Test void duplicateCoordinatorDoesNotResetAdmissionCount() throws Exception {
        Term term = nodes.getFirst().elect();
        nodes.getLast().assign(JOB, term, peers);
        for (WorkerNode node : nodes) node.coordinator(term);
        assertEquals(1, nodes.getLast().status().assignedJobs());
        assertEquals(2, nodes.getLast().status().jac());
    }

    @Test void splitFailureDoesNotAllocateOrConsumeTermBudget() throws Exception {
        JobEngine broken = new JobEngine() {
            public List<Job> split(Job job, int count) { throw new IllegalStateException("split failed"); }
            public BigInteger compute(Job job) { return BigInteger.ONE; }
            public BigInteger aggregate(Job.Type type, List<BigInteger> results) { return BigInteger.ONE; }
        };
        Peer peer = new Peer(9, "localhost", 0);
        try (WorkerNode node = new WorkerNode(peer, null, 0, broken)) {
            Term term = new Term(1, UUID.randomUUID(), peer);
            node.coordinator(term);
            assertThrows(JobFailedException.class, () -> node.assign(JOB, term, List.of(peer)));
            assertEquals(0, node.status().jac());
            assertEquals(0, node.status().assignedJobs());
        }
    }

    @Test void interruptedDispatchDrainsStartedWorkBeforeReturning() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        JobEngine blocking = new JobEngine() {
            public List<Job> split(Job job, int count) { return List.of(job); }
            public BigInteger compute(Job job) {
                entered.countDown();
                try { if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("timeout"); }
                catch (InterruptedException e) { throw new IllegalStateException(e); }
                return BigInteger.ONE;
            }
            public BigInteger aggregate(Job.Type type, List<BigInteger> results) { return BigInteger.ONE; }
        };
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var registry = LocateRegistry.createRegistry(port);
        exported.add(registry);
        Peer peer = new Peer(9, "localhost", port);
        try (WorkerNode node = new WorkerNode(peer, null, 0, blocking);
             var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            registry.rebind("worker", node);
            Term term = new Term(1, UUID.randomUUID(), peer);
            node.coordinator(term);
            var caller = new java.util.concurrent.atomic.AtomicReference<Thread>();
            Future<Boolean> result = pool.submit(() -> {
                caller.set(Thread.currentThread());
                assertThrows(JobFailedException.class, () -> node.assign(JOB, term, List.of(peer)));
                return Thread.currentThread().isInterrupted();
            });
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                caller.get().interrupt();
                assertThrows(TimeoutException.class, () -> result.get(150, TimeUnit.MILLISECONDS));
            } finally { release.countDown(); }
            assertTrue(result.get());
        }
    }

    @Test void failedLeaderAndStaleNeighboursDoNotBlockSurvivors() throws Exception {
        assertEquals(3, nodes.getFirst().elect().leader().id());
        nodes.getLast().close();
        nodes.removeLast();
        Term replacement = nodes.getFirst().elect();
        assertEquals(2, replacement.leader().id());
        for (WorkerNode node : nodes) assertEquals(replacement, node.status().term());
    }

    @Test void fifthAllocationRotatesWhileComputationIsStillRunning() throws Exception {
        CountDownLatch entered = new CountDownLatch(5), release = new CountDownLatch(1);
        JobEngine blocking = new JobEngine() {
            public List<Job> split(Job job, int count) { return Collections.nCopies(count, job); }
            public BigInteger compute(Job job) {
                entered.countDown();
                try { if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("timeout"); }
                catch (InterruptedException e) { throw new IllegalStateException(e); }
                return BigInteger.ONE;
            }
            public BigInteger aggregate(Job.Type type, List<BigInteger> results) { return BigInteger.ONE; }
        };
        nodes.getLast().close();
        WorkerNode replacement = new WorkerNode(peers.getLast(), bootstrap, 0, blocking);
        nodes.set(2, replacement);
        LocateRegistry.getRegistry("localhost", peers.getLast().registryPort()).rebind("worker", replacement);
        for (Peer peer : peers) replacement.addNeighbour(peer);
        Term original = nodes.getFirst().elect();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<BigInteger>> calls = new ArrayList<>();
            try {
                for (int i = 0; i < 5; i++)
                    calls.add(pool.submit(() -> replacement.assign(JOB, original, peers)));
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (nodes.getFirst().status().term().equals(original) && System.nanoTime() < deadline)
                    Thread.sleep(10);
                Term next = nodes.getFirst().status().term();
                assertEquals(original.number() + 1, next.number());
                assertEquals(2, next.leader().id());
                for (var call : calls) assertFalse(call.isDone());
            } finally { release.countDown(); }
            for (var call : calls) assertEquals(BigInteger.ONE, call.get());
        }
    }

    @Test void lowerIdJoinMustPreserveAnActiveUnexhaustedTerm() throws Exception {
        Term original = nodes.getFirst().elect();
        nodes.getLast().assign(JOB, original, peers);
        assertEquals(1, nodes.getLast().status().assignedJobs());
        int port;
        try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var registry = LocateRegistry.createRegistry(port);
        exported.add(registry);
        Peer joining = new Peer(0, "localhost", port);
        WorkerNode newcomer = new WorkerNode(joining, bootstrap, 0, fixture());
        nodes.add(newcomer);
        registry.rebind("worker", newcomer);
        bootstrap.register(joining);
        assertNull(newcomer.status().term());
        Term afterJoin = joining.connect().elect();
        assertEquals(original, afterJoin,
                "A healthy coordinator with only one assigned job must keep its term after a join");
        for (WorkerNode node : nodes) assertEquals(original, node.status().term());
        assertEquals(1, nodes.get(2).status().assignedJobs());
        assertEquals(2, nodes.get(2).status().jac());
        // Reuse must preserve the remaining four slots, then rotate normally.
        for (int i = 0; i < 4; i++) nodes.get(2).assign(JOB, original, peers);
        Term rotated = joining.connect().elect();
        assertEquals(original.number() + 1, rotated.number());
        assertEquals(2, rotated.leader().id());
        assertEquals(10, nodes.get(2).status().jac());
        for (WorkerNode node : nodes) assertEquals(rotated, node.status().term());
    }
}
