package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import edu.usp.cs324.jobs.IntegerJobEngine;
import edu.usp.cs324.proto.WorkerEndpoint;
import edu.usp.cs324.proto.coordination.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.*;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(30)
class CoordinationGrpcTest {
    final List<WorkerNode> nodes = new ArrayList<>();
    final List<Server> servers = new ArrayList<>();
    final Map<Peer, WorkerEndpoint> endpoints = new ConcurrentHashMap<>();
    final Map<Peer, CoordinationGrpcClient> clients = new ConcurrentHashMap<>();
    final List<Peer> members = new CopyOnWriteArrayList<>();
    static final Job JOB = new Job(Job.Type.PRIMECOUNT, List.of(2, 3, 4, 5, 7, 8), 0, 0);
    @BeforeEach void start() throws Exception {
        BootstrapRemote directory = new BootstrapRemote() {
            public void register(Peer p) { members.add(p); }
            public List<Peer> active() { return List.copyOf(members); }
        };
        for (int id = 1; id <= 3; id++) {
            // Deliberately no RMI registry at these addresses. Any transport fallback fails.
            Peer peer = new Peer(id, "127.0.0.1", 10000 + id);
            WorkerNode node = new WorkerNode(peer, directory, 0, new IntegerJobEngine(), false);
            nodes.add(node); members.add(peer);
            node.useTransport(target -> clients.get(target));
            Server server = ServerBuilder.forPort(0).addService(new CoordinationServiceImpl(node, endpoints::get)).build().start();
            servers.add(server);
            endpoints.put(peer, WorkerEndpoint.newBuilder().setWorkerId(id).setHost(peer.host())
                    .setRegistryPort(peer.registryPort()).setGrpcPort(server.getPort()).setIncarnationId("test-" + id).build());
        }
        endpoints.forEach((p, e) -> clients.put(p, new CoordinationGrpcClient(e, endpoints::get)));
        for (var n : nodes) for (Peer p : members) n.addNeighbour(p);
    }
    @AfterEach void stop() throws Exception {
        clients.values().forEach(CoordinationGrpcClient::close);
        for (Server server : servers) server.shutdownNow();
        for (Server server : servers) server.awaitTermination(5, TimeUnit.SECONDS);
        for (WorkerNode node : nodes) node.close();
    }
    @Test void cyclicElectionUsesGrpcAndDeduplicates() throws Exception {
        UUID id = UUID.randomUUID();
        assertEquals(3, clients.get(members.getFirst()).election(id).size());
        assertTrue(clients.get(members.getLast()).election(id).isEmpty());
        Term term = clients.get(members.getFirst()).elect();
        assertEquals(3, term.leader().id());
        for (var client : clients.values()) assertEquals(term, client.status().term());
    }
    @Test void simultaneousRequestsAgreeAndJobsRotateAfterFive() throws Exception {
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new ArrayList<Future<Term>>();
            for (int i = 0; i < 9; i++) {
                var client = clients.get(members.get(i % 3));
                futures.add(pool.submit(client::elect));
            }
            Term first = futures.getFirst().get();
            for (var f : futures) assertEquals(first, f.get());
            for (int i = 0; i < 5; i++) assertEquals(BigInteger.valueOf(4), clients.get(members.getFirst()).submit(JOB));
            Term next = clients.get(members.getFirst()).elect();
            assertEquals(2, next.number()); assertEquals(2, next.leader().id());
            assertEquals(10, clients.get(members.getLast()).status().jac());
        }
    }
    @Test void staleAssignmentIsExplicitlyRetryableAndDoesNotConsumeBudget() throws Exception {
        var client = clients.get(members.getLast());
        Term term = client.elect();
        Term stale = new Term(term.number() + 1, UUID.randomUUID(), term.leader());
        assertThrows(RetryException.class, () -> client.assign(JOB, stale, members));
        assertEquals(0, client.status().assignedJobs());
    }
    @Test void allOperationsUseTheRegisteredEngine() throws Exception {
        var client = clients.get(members.getFirst());
        assertEquals(BigInteger.valueOf(42), client.submit(new Job(Job.Type.MAX, List.of(-3, 42, 0), 0, 0)));
        assertEquals(BigInteger.valueOf(76127), client.submit(new Job(Job.Type.PRIMESUM, List.of(), 1, 1000)));
        assertEquals(BigInteger.valueOf(4), client.submit(JOB));
    }
    @Test void failedLeaderIsReplacedAfterDirectoryRemovesIt() throws Exception {
        var client = clients.get(members.getFirst());
        Term first = client.elect();
        assertEquals(3, first.leader().id());
        servers.getLast().shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        members.removeLast();
        Term next = client.elect();
        assertEquals(2, next.leader().id()); assertEquals(2, next.number());
        assertEquals(BigInteger.valueOf(4), client.submit(JOB));
    }
    @Test void unreachableComputeNeverBecomesARetryableRejection() throws Exception {
        var endpoint = endpoints.get(members.getFirst()).toBuilder().setGrpcPort(1).build();
        try (var client = new CoordinationGrpcClient(endpoint, endpoints::get, Duration.ofMillis(150))) {
            var error = assertThrows(RemoteException.class, () -> client.compute(JOB));
            assertFalse(error instanceof RetryException);
        }
    }
}
