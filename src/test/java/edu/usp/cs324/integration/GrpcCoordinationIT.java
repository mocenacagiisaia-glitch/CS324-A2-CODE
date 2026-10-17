package edu.usp.cs324.integration;

import edu.usp.cs324.api.Job;
import edu.usp.cs324.api.Peer;
import edu.usp.cs324.network.CoordinationGrpcClient;
import edu.usp.cs324.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Actual gRPC-mode launcher, bootstrap and workers in separate JVMs. */
class GrpcCoordinationIT {
    final List<Process> children = new ArrayList<>();
    final Set<Integer> ports = new HashSet<>();
    Path logs;
    int port() throws Exception {
        for (;;) try (var socket = new ServerSocket(0)) {
            if (ports.add(socket.getLocalPort())) return socket.getLocalPort();
        }
    }
    void start(String name, String main, String marker, String... args) throws Exception {
        Path log = logs.resolve(name + ".log");
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), main));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        children.add(process);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            String output = Files.readString(log);
            if (output.contains(marker)) return;
            assertTrue(process.isAlive(), output);
            Thread.sleep(50);
        }
        fail("Readiness timeout: " + Files.readString(log));
    }
    @Test @Timeout(90) void grpcProcessesElectAndRotateWithConcurrentJobs() throws Exception {
        logs = Files.createTempDirectory(Path.of("target"), "grpc-coordination-").toAbsolutePath();
        ManagedChannel directory = null;
        var clients = new ArrayList<CoordinationGrpcClient>();
        try {
            int bootstrapPort = port();
            start("bootstrap", "edu.usp.cs324.network.BootstrapGrpcServer", "BOOTSTRAP gRPC READY", "" + bootstrapPort);
            for (int id = 1; id <= 3; id++) {
                start("worker-" + id, "edu.usp.cs324.network.WorkerNode", "WORKER READY", "" + id,
                        "127.0.0.1", "" + port(), "" + port(), "" + port(), "127.0.0.1", "" + bootstrapPort,
                        logs.resolve("worker-" + id + ".checkpoint").toString());
            }
            directory = ManagedChannelBuilder.forAddress("127.0.0.1", bootstrapPort).usePlaintext().build();
            var workers = BootstrapServiceGrpc.newBlockingStub(directory).withDeadlineAfter(3, TimeUnit.SECONDS)
                    .listActiveWorkers(ListActiveWorkersRequest.getDefaultInstance()).getWorkersList();
            assertEquals(3, workers.size());
            Map<Peer, WorkerEndpoint> endpoints = new HashMap<>();
            for (var w : workers) endpoints.put(new Peer(w.getWorkerId(), w.getHost(), w.getRegistryPort()), w);
            for (var w : workers) clients.add(new CoordinationGrpcClient(w, endpoints::get));
            var first = clients.getFirst().elect();
            assertEquals(3, first.leader().id());
            try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
                var tasks = new ArrayList<Future<BigInteger>>();
                for (int i = 0; i < 12; i++) {
                    var client = clients.get(i % clients.size());
                    tasks.add(pool.submit(() -> {
                        // Retry only explicit rejection before admission at a term boundary.
                        for (int attempt = 0; attempt < 30; attempt++) {
                            try { return client.submit(new Job(Job.Type.PRIMESUM, List.of(), 1, 1000)); }
                            catch (edu.usp.cs324.api.RetryException e) { Thread.sleep(10); }
                        }
                        throw new AssertionError("Admission retries exhausted");
                    }));
                }
                for (var task : tasks) assertEquals(BigInteger.valueOf(76127), task.get());
            }
            var last = clients.getFirst().elect();
            assertTrue(last.number() > first.number());
            long jac = 0;
            for (var client : clients) {
                var status = client.status();
                assertEquals(last, status.term());
                assertTrue(status.assignedJobs() <= 5);
                jac += status.jac();
            }
            assertEquals(24, jac);
        } finally {
            clients.forEach(CoordinationGrpcClient::close);
            if (directory != null) directory.shutdownNow();
            for (var process : children.reversed()) {
                process.destroy();
                if (!process.waitFor(3, TimeUnit.SECONDS)) {
                    process.destroyForcibly(); process.waitFor(3, TimeUnit.SECONDS);
                }
            }
        }
    }
}
