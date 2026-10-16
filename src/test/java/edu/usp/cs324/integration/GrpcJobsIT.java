package edu.usp.cs324.integration;

import java.net.ServerSocket;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

/** Real gRPC bootstrap, two worker JVMs and two concurrent gRPC client JVMs. */
class GrpcJobsIT {
    private final List<Process> children = new ArrayList<>();
    private final Set<Integer> ports = new HashSet<>();
    private final Path logs = Path.of("target", "grpc-jobs-integration").toAbsolutePath();
    private int port() throws Exception {
        for (;;) try (ServerSocket socket = new ServerSocket(0)) {
            if (ports.add(socket.getLocalPort())) return socket.getLocalPort();
        }
    }
    private Process start(String log, String main, String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dsun.rmi.transport.tcp.responseTimeout=10000", "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), main));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(logs.resolve(log).toFile()).start();
        children.add(process);
        return process;
    }
    private void ready(Process process, String log, String marker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            String output = Files.readString(logs.resolve(log));
            if (output.contains(marker)) return;
            assertTrue(process.isAlive(), output);
            Thread.sleep(100);
        }
        fail("Readiness timeout: " + Files.readString(logs.resolve(log)));
    }
    @Test @Timeout(90) void twoClientsCompleteTwentyFourJobsAcrossWorkerProcesses() throws Exception {
        Files.createDirectories(logs);
        try {
            int bootstrap = port();
            Process directory = start("bootstrap.log", "edu.usp.cs324.network.BootstrapGrpcServer", "" + bootstrap);
            ready(directory, "bootstrap.log", "BOOTSTRAP gRPC READY");
            for (int id = 1; id <= 2; id++) {
                String log = "worker" + id + ".log";
                Process worker = start(log, "edu.usp.cs324.network.WorkerNode", "" + id, "127.0.0.1",
                        "" + port(), "" + port(), "" + port(), "127.0.0.1", "" + bootstrap,
                        logs.resolve("worker" + id + ".checkpoint").toString());
                ready(worker, log, "WORKER READY");
            }
            Process first = start("client1.log", "edu.usp.cs324.client.GrpcHeadlessClient", "127.0.0.1", "" + bootstrap);
            Process second = start("client2.log", "edu.usp.cs324.client.GrpcHeadlessClient", "127.0.0.1", "" + bootstrap);
            for (int i = 1; i <= 2; i++) {
                Process client = i == 1 ? first : second;
                assertTrue(client.waitFor(40, TimeUnit.SECONDS), "Client timed out; see " + logs);
                String output = Files.readString(logs.resolve("client" + i + ".log"));
                assertEquals(0, client.exitValue(), output);
                assertEquals(12L, output.lines().filter(line -> line.startsWith("PASS ")).count(), output);
            }
        } finally {
            for (Process child : children.reversed()) if (child.isAlive()) child.destroyForcibly();
            for (Process child : children) child.waitFor(5, TimeUnit.SECONDS);
        }
    }
}
