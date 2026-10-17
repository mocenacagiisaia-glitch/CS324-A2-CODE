package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import edu.usp.cs324.client.GrpcJobClient;
import edu.usp.cs324.jobs.IntegerJobEngine;
import edu.usp.cs324.proto.recovery.*;
import edu.usp.cs324.recovery.RecoveryStateGrpcClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.math.BigInteger;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Deterministic blocked engine, real gRPC/JVMs/checkpoint files and abrupt kills. */
class RecoveryProcessIT {
    final List<Process> children = new ArrayList<>();
    Path logs;
    Process start(String name, String main, String marker, String... args) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), main));
        command.addAll(List.of(args));
        Path log = logs.resolve(name + ".log");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        children.add(process);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (Files.readString(log).contains(marker)) return process;
            assertTrue(process.isAlive(), Files.readString(log)); Thread.sleep(40);
        }
        fail("Readiness timeout: " + Files.readString(log)); return process;
    }
    RecoverySnapshot awaitSnapshot(RecoveryStateGrpcClient client, java.util.function.Predicate<RecoverySnapshot> condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
        while (System.nanoTime() < deadline) {
            var s = client.latest();
            if (s.isPresent() && condition.test(s.get())) return s.get();
            Thread.sleep(50);
        }
        fail("Checkpoint condition timed out; inspect " + logs); return null;
    }
    @Test @Timeout(120) void abruptCoordinatorAndSequencerLossResumesOnSurvivorAndRejoinedWorkersCatchUp() throws Exception {
        Files.createDirectories(Path.of("target"));
        logs = Files.createTempDirectory(Path.of("target"), "recovery-process-").toAbsolutePath();
        int bootstrapPort = RecoveryRuntimeTest.port();
        Path release = logs.resolve("release-computation");
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                start("bootstrap", "edu.usp.cs324.network.BootstrapGrpcServer", "BOOTSTRAP gRPC READY", "" + bootstrapPort);
                var workers = new ArrayList<Process>();
                var grpcPorts = new ArrayList<Integer>();
                for (int id = 1; id <= 3; id++) {
                    int grpcPort = RecoveryRuntimeTest.port(); grpcPorts.add(grpcPort);
                    workers.add(start("worker-" + id, TestWorker.class.getName(), "TEST WORKER READY", "" + id,
                            "" + bootstrapPort, logs.resolve("w" + id).toString(), release.toString(), "" + grpcPort));
                }
                var client = new GrpcJobClient("127.0.0.1", bootstrapPort);
                var result = pool.submit(() -> client.submit("process-recovery", RecoveryRuntimeTest.JOB, p -> {}));
                try (var reader = new RecoveryStateGrpcClient("127.0.0.1", grpcPorts.get(1))) {
                    var accepted = awaitSnapshot(reader, s -> s.getJobsCount() == 1 && s.getJobs(0).getTasksList().stream()
                            .allMatch(t -> t.getState() == RecoveryTask.State.RUNNING));
                    assertEquals(3, accepted.getCoordinatorId());
                    workers.get(2).destroyForcibly().waitFor(5, TimeUnit.SECONDS);
                    workers.get(0).destroyForcibly().waitFor(5, TimeUnit.SECONDS);
                    Files.writeString(release, "resume");
                    assertEquals(BigInteger.valueOf(76127), result.get(55, TimeUnit.SECONDS));
                    var recovered = awaitSnapshot(reader, s -> s.getJobs(0).getState() == RecoveryJob.State.COMPLETED);
                    assertEquals(2, recovered.getCoordinatorId());
                    assertTrue(recovered.getLeadershipTerm() > accepted.getLeadershipTerm());
                    assertTrue(recovered.getJobs(0).getTasksList().stream().allMatch(t -> t.getAttempt() >= 2));
                    // Reopen the killed workers' original files, with new incarnations.
                    for (int id : List.of(3, 1)) {
                        start("rejoined-" + id, TestWorker.class.getName(), "TEST WORKER READY", "" + id,
                                "" + bootstrapPort, logs.resolve("w" + id).toString(), release.toString(), "" + grpcPorts.get(id - 1));
                        try (var joined = new RecoveryStateGrpcClient("127.0.0.1", grpcPorts.get(id - 1))) {
                            var caughtUp = awaitSnapshot(joined, s -> s.getJobsCount() == 1 && s.getJobs(0).hasResult());
                            assertEquals("76127", caughtUp.getJobs(0).getResult());
                        }
                    }
                    assertEquals(BigInteger.valueOf(76127), client.submit("process-recovery", RecoveryRuntimeTest.JOB, p -> {}));
                    assertEquals(1, reader.latest().orElseThrow().getJobsCount());
                }
            } finally {
                for (var process : children.reversed()) {
                    process.destroy();
                    if (!process.waitFor(3, TimeUnit.SECONDS)) { process.destroyForcibly(); process.waitFor(3, TimeUnit.SECONDS); }
                }
            }
        }
    }
    public static final class TestWorker {
        public static void main(String[] args) throws Exception {
            int id = Integer.parseInt(args[0]), bootstrap = Integer.parseInt(args[1]);
            Path release = Path.of(args[3]);
            IntegerJobEngine engine = new IntegerJobEngine();
            JobEngine blocked = new JobEngine() {
                public List<Job> split(Job input, int n) { return engine.split(input,n); }
                public BigInteger aggregate(Job.Type type, List<BigInteger> values) { return engine.aggregate(type,values); }
                public BigInteger compute(Job input) {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(75);
                    while (!Files.exists(release)) {
                        if (System.nanoTime() >= deadline) throw new IllegalStateException("Test release timeout");
                        try { Thread.sleep(30); }
                        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
                    }
                    return engine.compute(input);
                }
            };
            // Production composition, replacing only the pure engine with a barrier fixture.
            try (var worker = new RecoveryRuntimeTest.Worker(id, bootstrap, Path.of(args[2]), blocked, Integer.parseInt(args[4]))) {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { worker.close(); } catch (Exception ignored) {} }));
                System.out.println("TEST WORKER READY " + id);
                worker.server.awaitTermination();
            }
        }
    }
}
