package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class WorkerRuntimeTest {
    private static final Job INPUT = new Job(Job.Type.PRIMECOUNT, List.of(2), 0, 0);

    /** Runtime fixture only; deliberately supplies no splitting or calculation algorithms. */
    private static JobEngine fixture(Callable<BigInteger> task) {
        return new JobEngine() {
            public List<Job> split(Job job, int count) { throw new UnsupportedOperationException(); }
            public BigInteger aggregate(Job.Type type, List<BigInteger> results) { throw new UnsupportedOperationException(); }
            public BigInteger compute(Job job) {
                try { return task.call(); }
                catch (Exception e) { throw new IllegalStateException(e); }
            }
        };
    }

    @Test void twoTasksActuallyOverlap() throws Exception {
        CountDownLatch entered = new CountDownLatch(2), release = new CountDownLatch(1);
        JobEngine engine = fixture(() -> {
            entered.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Test task timed out");
            return BigInteger.TEN;
        });
        try (WorkerNode node = new WorkerNode(new Peer(1, "localhost", 0), null, 0, engine);
             var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<BigInteger> first = pool.submit(() -> node.compute(INPUT));
            Future<BigInteger> second = pool.submit(() -> node.compute(INPUT));
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertFalse(first.isDone());
                assertFalse(second.isDone());
            } finally { release.countDown(); }
            assertEquals(BigInteger.TEN, first.get());
            assertEquals(BigInteger.TEN, second.get());
        }
    }

    @Test void interruptedCallerWaitsForStartedWorkAndRestoresFlag() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        JobEngine engine = fixture(() -> {
            entered.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Test task timed out");
            return BigInteger.ONE;
        });
        try (WorkerNode node = new WorkerNode(new Peer(1, "localhost", 0), null, 0, engine);
             var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            AtomicReference<Thread> caller = new AtomicReference<>();
            Future<Boolean> result = pool.submit(() -> {
                caller.set(Thread.currentThread());
                assertThrows(JobFailedException.class, () -> node.compute(INPUT));
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

    @Test void failedComputationProducesAnErrorNotAValue() throws Exception {
        JobEngine engine = fixture(() -> { throw new IllegalArgumentException("Fixture failure"); });
        try (WorkerNode node = new WorkerNode(new Peer(1, "localhost", 0), null, 0, engine)) {
            assertThrows(JobFailedException.class, () -> node.compute(INPUT));
        }
    }
}
