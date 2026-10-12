package edu.usp.cs324.jobs;

import edu.usp.cs324.api.*;
import java.math.BigInteger;
import java.util.*;

/** Stateless integer calculations; range arithmetic uses long to avoid int overflow. */
public final class IntegerJobEngine implements JobEngine {
    public List<Job> split(Job job, int workerCount) {
        Objects.requireNonNull(job);
        if (workerCount < 1) throw new IllegalArgumentException("Worker count must be positive");
        long size = job.type() == Job.Type.PRIMESUM
                ? (long) job.end() - job.start() + 1 : job.numbers().size();
        if (size == 0) return List.of(job);
        int count = (int) Math.min(size, workerCount);
        List<Job> parts = new ArrayList<>(count);
        long offset = 0;
        for (int i = 0; i < count; i++) {
            long length = size / count + (i < size % count ? 1 : 0);
            if (job.type() == Job.Type.PRIMESUM) {
                long start = (long) job.start() + offset;
                parts.add(new Job(job.type(), List.of(), (int) start, (int) (start + length - 1)));
            } else {
                parts.add(new Job(job.type(), job.numbers().subList((int) offset,
                        (int) (offset + length)), 0, 0));
            }
            offset += length;
        }
        return List.copyOf(parts);
    }

    public static boolean isPrime(int n) {
        if (n < 2) return false;
        if (n % 2 == 0) return n == 2;
        for (int divisor = 3; divisor <= n / divisor; divisor += 2)
            if (n % divisor == 0) return false;
        return true;
    }

    public BigInteger compute(Job part) {
        return switch (part.type()) {
            case MAX -> BigInteger.valueOf(Collections.max(part.numbers()));
            case PRIMECOUNT -> BigInteger.valueOf(part.numbers().stream().filter(IntegerJobEngine::isPrime).count());
            case PRIMESUM -> {
                BigInteger sum = BigInteger.ZERO;
                for (long n = Math.max(2L, part.start()); n <= part.end(); n++)
                    if (isPrime((int) n)) sum = sum.add(BigInteger.valueOf(n));
                yield sum;
            }
        };
    }

    public BigInteger aggregate(Job.Type type, List<BigInteger> results) {
        Objects.requireNonNull(type);
        List<BigInteger> values = List.copyOf(results);
        if (type == Job.Type.MAX) {
            if (values.isEmpty()) throw new IllegalArgumentException("MAX needs a result");
            return Collections.max(values);
        }
        return values.stream().reduce(BigInteger.ZERO, BigInteger::add);
    }
}
