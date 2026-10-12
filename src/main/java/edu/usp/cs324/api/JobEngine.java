package edu.usp.cs324.api;

import java.math.BigInteger;
import java.util.List;

/** Pure calculations shared through a service-provider implementation. */
public interface JobEngine {
    List<Job> split(Job job, int workerCount);
    BigInteger compute(Job part);
    BigInteger aggregate(Job.Type type, List<BigInteger> results);
}
