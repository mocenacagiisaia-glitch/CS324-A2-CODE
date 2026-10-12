package edu.usp.cs324.integration;

import edu.usp.cs324.api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.net.ServerSocket;
import java.nio.file.*;
import java.rmi.registry.LocateRegistry;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Real JVMs verify membership, election and concurrent checked clients with the real provider. */
class MembershipIT {
    private final List<Process> processes = new ArrayList<>();
    private final Map<Process, List<String>> commands = new LinkedHashMap<>();
    private final Set<Integer> allocatedPorts = new HashSet<>();
    private final Path logs = Path.of("target", "membership-integration").toAbsolutePath();

    private int port() throws Exception {
        for (;;) {
            try (ServerSocket socket = new ServerSocket(0)) {
                int port = socket.getLocalPort();
                if (allocatedPorts.add(port)) return port;
            }
        }
    }

    private Process start(String log, String main, String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dsun.rmi.transport.tcp.responseTimeout=10000", "-cp",
                Path.of("target", "classes").toAbsolutePath().toString(), main));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(logs.resolve(log).toFile()).start();
        processes.add(process);
        commands.put(process, List.copyOf(command));
        return process;
    }

    /** Failure-only diagnostics: never replace the assertion that triggered them. */
    private void captureStatusTimeout(Process status, AssertionError failure) {
        Path output = Path.of("diagnostics", "status-timeout-" + System.currentTimeMillis());
        List<Process> probes = new ArrayList<>();
        StringBuilder report = new StringBuilder("Captured at " + java.time.Instant.now() + "\n");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        try {
            Files.createDirectories(output);
            String jcmd = Path.of(System.getProperty("java.home"), "bin",
                    System.getProperty("os.name").startsWith("Windows") ? "jcmd.exe" : "jcmd").toString();
            report.append("jcmd: ").append(jcmd).append('\n');
            // Snapshot every child before attaching; capture ClusterStatus first.
            List<Process> targets = new ArrayList<>(processes);
            targets.remove(status);
            targets.addFirst(status);
            for (Process child : targets) {
                report.append("pid=").append(child.pid()).append(" command=").append(commands.get(child))
                        .append(" started=").append(child.info().startInstant()).append(" state=")
                        .append(child.isAlive() ? "alive" : "exit=" + child.exitValue()).append('\n');
            }
            Files.writeString(output.resolve("processes.txt"), report);
            for (Process child : targets) {
                if (!child.isAlive()) continue;
                if (System.nanoTime() >= deadline) {
                    report.append("Diagnostic budget exhausted before pid=").append(child.pid()).append('\n');
                    continue;
                }
                try {
                    Process probe = new ProcessBuilder(jcmd, Long.toString(child.pid()), "Thread.print", "-l")
                            .redirectErrorStream(true)
                            .redirectOutput(output.resolve("threads-" + child.pid() + ".txt").toFile()).start();
                    probes.add(probe);
                    report.append("jcmd pid=").append(probe.pid()).append(" target=").append(child.pid()).append('\n');
                } catch (Exception e) {
                    report.append("Cannot start jcmd for ").append(child.pid()).append(": ").append(e).append('\n');
                    failure.addSuppressed(e);
                }
            }
            for (Process probe : probes) {
                long remaining = Math.max(0, deadline - System.nanoTime());
                boolean exited = probe.waitFor(remaining, TimeUnit.NANOSECONDS);
                report.append("jcmd pid=").append(probe.pid()).append(exited ? " exit=" + probe.exitValue() : " timed out").append('\n');
            }
        } catch (Exception e) {
            report.append("Diagnostic failure: ").append(e).append('\n');
            failure.addSuppressed(e);
        } finally {
            // Only stop diagnostic helpers created here, never unrelated JVMs.
            for (Process probe : probes) {
                try { if (probe.isAlive()) probe.destroyForcibly(); }
                catch (Exception e) { failure.addSuppressed(e); report.append(e).append('\n'); }
            }
            try { Files.writeString(output.resolve("processes.txt"), report); }
            catch (Exception e) { failure.addSuppressed(e); System.err.println("Cannot save diagnostics: " + e); }
            System.err.println("ClusterStatus timeout diagnostics: " + output.toAbsolutePath() + "\n" + report);
        }
    }

    private void ready(Process process, String log, String marker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            String output = Files.readString(logs.resolve(log));
            if (output.contains(marker)) return;
            assertTrue(process.isAlive(), output);
            Thread.sleep(100);
        }
        fail("Readiness timeout: " + log);
    }

    @Test @Timeout(90)
    void bootstrapAndFourWorkerProcessesElectCoordinator() throws Exception {
        Files.createDirectories(logs);
        try {
            int bootstrapPort = port();
            Process bootstrapProcess = start("bootstrap.log", "edu.usp.cs324.network.BootstrapNode",
                    "127.0.0.1", "" + bootstrapPort, "" + port());
            ready(bootstrapProcess, "bootstrap.log", "BOOTSTRAP READY");
            for (int id = 1; id <= 4; id++) {
                Process worker = start("worker" + id + ".log", "edu.usp.cs324.network.WorkerNode",
                        "" + id, "127.0.0.1", "" + port(), "" + port(), "127.0.0.1", "" + bootstrapPort);
                ready(worker, "worker" + id + ".log", "WORKER READY");
            }
            BootstrapRemote bootstrap = (BootstrapRemote) LocateRegistry.getRegistry("127.0.0.1", bootstrapPort).lookup("bootstrap");
            List<Peer> members = bootstrap.active();
            assertEquals(4, members.size());
            Set<Peer> reached = new HashSet<>();
            Deque<Peer> pending = new ArrayDeque<>(List.of(members.getFirst()));
            int edges = 0;
            while (!pending.isEmpty()) {
                Peer peer = pending.remove();
                if (!reached.add(peer)) continue;
                Status status = peer.connect().status();
                assertNull(status.term());
                assertEquals(0, status.jac());
                assertEquals(0, status.assignedJobs());
                edges += status.neighbours().size();
                for (Peer neighbour : status.neighbours()) {
                    assertTrue(neighbour.connect().status().neighbours().contains(peer));
                    pending.add(neighbour);
                }
            }
            assertEquals(new HashSet<>(members), reached);
            assertEquals(6, edges);
            Term elected = members.getFirst().connect().elect();
            assertEquals(4, elected.leader().id());
            for (Peer peer : members) assertEquals(elected, peer.connect().status().term());
            Process status = start("status.log", "edu.usp.cs324.network.ClusterStatus", "127.0.0.1", "" + bootstrapPort);
            try {
                assertTrue(status.waitFor(10, TimeUnit.SECONDS));
            } catch (AssertionError failure) {
                captureStatusTimeout(status, failure);
                throw failure;
            }
            assertEquals(0, status.exitValue());
            assertEquals(4, Files.readAllLines(logs.resolve("status.log")).size());
            Process clientA = start("client-a.log", "edu.usp.cs324.client.HeadlessClient", "127.0.0.1", "" + bootstrapPort);
            Process clientB = start("client-b.log", "edu.usp.cs324.client.HeadlessClient", "127.0.0.1", "" + bootstrapPort);
            for (Process client : List.of(clientA, clientB)) {
                assertTrue(client.waitFor(45, TimeUnit.SECONDS), "Client timed out; inspect integration logs");
                assertEquals(0, client.exitValue(), "Client failed; inspect integration logs");
            }
            assertEquals(12, Files.readAllLines(logs.resolve("client-a.log")).size());
            assertEquals(12, Files.readAllLines(logs.resolve("client-b.log")).size());
            Term finalTerm = members.getFirst().connect().status().term();
            assertTrue(finalTerm.number() > elected.number());
            long allocations = 0;
            for (Peer peer : members) {
                Status after = peer.connect().status();
                assertEquals(finalTerm, after.term());
                assertTrue(after.assignedJobs() <= 5);
                allocations += after.jac();
            }
            assertEquals(72, allocations, "24 jobs each allocate to three other workers");
            Files.writeString(logs.resolve("summary.txt"), "PASS: bootstrap + 4 workers + 2 concurrent client JVMs; 24 checked calculations; leader rotation, agreement and JAC accounting.\n");
        } finally {
            for (Process process : processes.reversed()) {
                process.destroy();
                if (!process.waitFor(3, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(3, TimeUnit.SECONDS);
                }
            }
        }
    }
}
