package edu.usp.cs324.client;

import edu.usp.cs324.api.Job;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;

/** Concurrent checked demo; a wrong result or failed submission exits unsuccessfully. */
public final class HeadlessClient {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("HeadlessClient bootstrapHost bootstrapPort");
        ClusterClient client = ClusterClient.connect(args[0], Integer.parseInt(args[1]));
        List<Job> jobs = List.of(JobInput.parse(Job.Type.MAX, "-9,42,7,-1"),
                JobInput.parse(Job.Type.PRIMESUM, "1,1000"),
                JobInput.parse(Job.Type.PRIMECOUNT, "2,2,3,4,5,-7,1"));
        List<BigInteger> expected = List.of(BigInteger.valueOf(42), BigInteger.valueOf(76127), BigInteger.valueOf(4));
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<BigInteger>> tasks = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                Job job = jobs.get(i % jobs.size());
                tasks.add(pool.submit(() -> client.submit(job)));
            }
            for (int i = 0; i < tasks.size(); i++) {
                BigInteger actual = tasks.get(i).get();
                if (!expected.get(i % jobs.size()).equals(actual)) throw new AssertionError("Unexpected result: " + actual);
                System.out.println("PASS " + jobs.get(i % jobs.size()).type() + " = " + actual);
            }
        }
    }
}
