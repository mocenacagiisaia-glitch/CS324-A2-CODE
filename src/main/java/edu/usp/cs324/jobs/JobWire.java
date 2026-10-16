package edu.usp.cs324.jobs;

import edu.usp.cs324.api.Job;
import edu.usp.cs324.proto.jobs.JobInput;

/** Lossless conversion; generated enum ordinals are never used as domain values. */
public final class JobWire {
    private JobWire() { }
    public static JobInput encode(Job job) {
        return JobInput.newBuilder().setOperation(switch (job.type()) {
            case MAX -> JobInput.Operation.MAX;
            case PRIMESUM -> JobInput.Operation.PRIMESUM;
            case PRIMECOUNT -> JobInput.Operation.PRIMECOUNT;
        }).addAllNumbers(job.numbers()).setRangeStart(job.start()).setRangeEnd(job.end()).build();
    }
    public static Job decode(JobInput input) {
        Job.Type type = switch (input.getOperation()) {
            case MAX -> Job.Type.MAX;
            case PRIMESUM -> Job.Type.PRIMESUM;
            case PRIMECOUNT -> Job.Type.PRIMECOUNT;
            default -> throw new IllegalArgumentException("Unknown or missing operation");
        };
        if (type == Job.Type.PRIMESUM && input.getNumbersCount() != 0)
            throw new IllegalArgumentException("PRIMESUM requires a range, not a number list");
        return new Job(type, input.getNumbersList(), input.getRangeStart(), input.getRangeEnd());
    }
}
