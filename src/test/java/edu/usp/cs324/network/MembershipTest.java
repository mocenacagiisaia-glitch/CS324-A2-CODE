package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import org.junit.jupiter.api.*;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class MembershipTest {
    private final List<Remote> exported = new ArrayList<>();
    private final List<WorkerNode> nodes = new ArrayList<>();
    private BootstrapNode bootstrap;

    @BeforeEach void start() throws Exception {
        bootstrap = new BootstrapNode(0);
        exported.add(bootstrap);
    }

    private Peer worker(int id) throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var registry = LocateRegistry.createRegistry(port);
        exported.add(registry);
        Peer peer = new Peer(id, "localhost", port);
        WorkerNode node = new WorkerNode(peer, bootstrap, 0, null);
        nodes.add(node);
        registry.rebind("worker", node);
        return peer;
    }

    @AfterEach void stop() throws Exception {
        for (WorkerNode node : nodes) {
            try { node.close(); } catch (java.rmi.NoSuchObjectException ignored) { }
        }
        for (Remote remote : exported.reversed()) UnicastRemoteObject.unexportObject(remote, true);
    }

    @Test void joinsCreateConnectedReciprocalTreeOverRmi() throws Exception {
        List<Peer> peers = new ArrayList<>();
        for (int id = 1; id <= 4; id++) {
            Peer peer = worker(id);
            bootstrap.register(peer);
            peers.add(peer);
        }
        assertEquals(peers, bootstrap.active());
        Set<Peer> reached = new HashSet<>();
        Deque<Peer> pending = new ArrayDeque<>(List.of(peers.getFirst()));
        int edges = 0;
        while (!pending.isEmpty()) {
            Peer peer = pending.remove();
            if (!reached.add(peer)) continue;
            Status status = peer.connect().status();
            edges += status.neighbours().size();
            assertNull(status.term());
            assertEquals(0, status.jac());
            for (Peer neighbour : status.neighbours()) {
                assertTrue(neighbour.connect().status().neighbours().contains(peer));
                pending.add(neighbour);
            }
        }
        assertEquals(new HashSet<>(peers), reached);
        assertEquals(6, edges); // Four nodes, three undirected join edges.
    }

    @Test void simultaneousDuplicateIdRegistrationsAcceptExactlyOne() throws Exception {
        Peer first = worker(7), second = worker(7);
        CountDownLatch start = new CountDownLatch(1);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Boolean>> attempts = new ArrayList<>();
            for (Peer peer : List.of(first, second)) attempts.add(pool.submit(() -> {
                start.await();
                try { bootstrap.register(peer); return true; }
                catch (RemoteException e) {
                    assertTrue(e.getMessage().contains("Duplicate worker ID"));
                    return false;
                }
            }));
            start.countDown();
            int accepted = 0;
            for (var attempt : attempts) if (attempt.get()) accepted++;
            assertEquals(1, accepted);
            assertEquals(1, bootstrap.active().size());
        }
    }

    @Test void inactiveWorkersArePrunedFromMembership() throws Exception {
        Peer peer = worker(1);
        bootstrap.register(peer);
        nodes.getFirst().close();
        assertTrue(bootstrap.active().isEmpty());
    }

    @Test void neighbourUpdatesDeduplicateAndIgnoreSelf() throws Exception {
        Peer first = worker(1), second = worker(2);
        first.connect().addNeighbour(first);
        first.connect().addNeighbour(second);
        first.connect().addNeighbour(second);
        assertEquals(List.of(second), first.connect().status().neighbours());
    }

    @Test void missingEngineFailsExplicitlyInsteadOfReturningFakeResults() throws Exception {
        Peer peer = worker(1);
        var remote = peer.connect();
        Job job = new Job(Job.Type.PRIMECOUNT, List.of(2), 0, 0);
        bootstrap.register(peer);
        assertEquals(peer, remote.elect().leader());
        assertEquals(1, remote.election(UUID.randomUUID()).size());

        assertThrows(RemoteException.class, () -> remote.submit(job));
        assertThrows(RemoteException.class, () -> remote.assign(job, null, List.of(peer)));
        assertThrows(RemoteException.class, () -> remote.compute(job));
    }
}
