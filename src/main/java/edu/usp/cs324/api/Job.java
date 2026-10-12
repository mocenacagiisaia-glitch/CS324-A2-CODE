package edu.usp.cs324.api;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Integer input domain; PRIMESUM uses inclusive start/end. */
public record Job(Type type, List<Integer> numbers, int start, int end) implements Serializable {
    public enum Type { MAX, PRIMESUM, PRIMECOUNT }

    public Job {
        Objects.requireNonNull(type, "Job type is required");
        numbers = List.copyOf(numbers);
        if (type == Type.PRIMESUM && start > end) {
            throw new IllegalArgumentException("Start must be <= end");
        }
        if (type == Type.MAX && numbers.isEmpty()) {
            throw new IllegalArgumentException("MAX requires at least one number");
        }
    }
}
