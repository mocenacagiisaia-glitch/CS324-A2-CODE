package edu.usp.cs324.jobs;

import edu.usp.cs324.api.*;
import java.math.BigInteger;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class IntegerJobEngineTest {
    private final JobEngine engine = new IntegerJobEngine();
    @Test void calculationsAndBoundaries() {
        assertEquals(BigInteger.valueOf(-2), engine.compute(new Job(Job.Type.MAX, List.of(-9, -2), 0, 0)));
        assertEquals(BigInteger.valueOf(4), engine.compute(new Job(Job.Type.PRIMECOUNT, List.of(2, 2, 3, 5, 1, 0, -2, 9), 0, 0)));
        assertEquals(BigInteger.valueOf(17), engine.compute(new Job(Job.Type.PRIMESUM, List.of(), -10, 10)));
        assertEquals(BigInteger.valueOf(Integer.MAX_VALUE), engine.compute(new Job(Job.Type.PRIMESUM, List.of(), Integer.MAX_VALUE, Integer.MAX_VALUE)));
        assertFalse(IntegerJobEngine.isPrime(Integer.MIN_VALUE));
        assertFalse(IntegerJobEngine.isPrime(2147395600));
        assertEquals(BigInteger.ZERO, engine.compute(new Job(Job.Type.PRIMECOUNT, List.of(), 0, 0)));
    }
    @Test void balancedListsPreserveEveryOccurrenceAndResults() {
        Random random = new Random(324);
        for (int size = 1; size < 80; size++) {
            List<Integer> values = random.ints(size, -100, 1000).boxed().toList();
            for (Job.Type type : List.of(Job.Type.MAX, Job.Type.PRIMECOUNT)) {
                Job job = new Job(type, values, 0, 0);
                for (int workers : List.of(1, 3, 8, 100)) {
                    List<Job> parts = engine.split(job, workers);
                    assertEquals(Math.min(size, workers), parts.size());
                    assertEquals(values, parts.stream().flatMap(p -> p.numbers().stream()).toList());
                    IntSummaryStatistics sizes = parts.stream().mapToInt(p -> p.numbers().size()).summaryStatistics();
                    assertTrue(sizes.getMax() - sizes.getMin() <= 1);
                    assertEquals(engine.compute(job), engine.aggregate(type, parts.stream().map(engine::compute).toList()));
                }
            }
        }
    }
    @Test void rangesCoverFullIntegerDomainWithoutOverflow() {
        Job job = new Job(Job.Type.PRIMESUM, List.of(), Integer.MIN_VALUE, Integer.MAX_VALUE);
        List<Job> parts = engine.split(job, 3);
        long next = Integer.MIN_VALUE;
        long min = Long.MAX_VALUE, max = 0;
        for (Job part : parts) {
            assertEquals(next, part.start());
            long size = (long) part.end() - part.start() + 1;
            min = Math.min(min, size); max = Math.max(max, size);
            next = (long) part.end() + 1;
        }
        assertEquals((long) Integer.MAX_VALUE + 1, next);
        assertTrue(max - min <= 1);
        for (int end = -5; end < 100; end++) {
            Job small = new Job(Job.Type.PRIMESUM, List.of(), -5, end);
            assertEquals(engine.compute(small), engine.aggregate(small.type(), engine.split(small, 7).stream().map(engine::compute).toList()));
        }
    }
    @Test void emptyInputValidationAggregationAndServiceRegistration() {
        Job empty = new Job(Job.Type.PRIMECOUNT, List.of(), 0, 0);
        assertEquals(List.of(empty), engine.split(empty, 4));
        assertThrows(IllegalArgumentException.class, () -> engine.split(empty, 0));
        assertThrows(IllegalArgumentException.class, () -> engine.aggregate(Job.Type.MAX, List.of()));
        BigInteger huge = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        assertEquals(huge.multiply(BigInteger.TWO), engine.aggregate(Job.Type.PRIMESUM, List.of(huge, huge)));
        assertInstanceOf(IntegerJobEngine.class, ServiceLoader.load(JobEngine.class).findFirst().orElseThrow());
    }
}
