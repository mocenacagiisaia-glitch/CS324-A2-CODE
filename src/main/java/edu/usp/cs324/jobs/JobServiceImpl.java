package edu.usp.cs324.jobs;

import edu.usp.cs324.api.Job;
import edu.usp.cs324.proto.jobs.*;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.math.BigInteger;
import java.util.Objects;

/** Transitional adapter; the injected coordinator owns admission, splitting and recovery.
 * RUNNING means the submission is being processed, not that admission is durable.
 * Disconnecting the stream does not cancel already submitted distributed work.
 */
public final class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
    @FunctionalInterface public interface Submitter { BigInteger submit(Job job) throws Exception; }
    private final Submitter submitter;
    public JobServiceImpl(Submitter submitter) { this.submitter = Objects.requireNonNull(submitter); }
    @Override public void submitJob(SubmitJobRequest request, StreamObserver<JobUpdate> output) {
        Job job;
        try {
            if (request.getRequestId().isBlank() || request.getRequestId().length() > 128)
                throw new IllegalArgumentException("Request ID must contain 1..128 characters");
            if (!request.hasInput()) throw new IllegalArgumentException("Job input is required");
            job = JobWire.decode(request.getInput());
        } catch (IllegalArgumentException e) {
            output.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        try {
            if (Context.current().isCancelled()) return;
            output.onNext(update(request, JobUpdate.State.RECEIVED).build());
            if (Context.current().isCancelled()) return;
            output.onNext(update(request, JobUpdate.State.RUNNING).build());
            BigInteger result = Objects.requireNonNull(submitWithAdmissionRetry(job), "Missing result");
            if (Context.current().isCancelled()) return;
            output.onNext(update(request, JobUpdate.State.COMPLETED).setResult(result.toString()).build());
            output.onCompleted();
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            if (!Context.current().isCancelled())
                output.onError(Status.UNKNOWN.withDescription(
                        "Submission failed; completion may be unknown. Do not automatically replay.")
                        .withCause(e).asRuntimeException());
        }
    }
    private BigInteger submitWithAdmissionRetry(Job job) throws Exception {
        for (int attempt = 0; ; attempt++) {
            try { return submitter.submit(job); }
            catch (java.rmi.RemoteException error) {
                // Only the explicit pre-admission rejection is safe to repeat.
                Throwable cause = error;
                while (cause instanceof java.rmi.ServerException && cause.getCause() != null)
                    cause = cause.getCause();
                if (!(cause instanceof edu.usp.cs324.api.RetryException) || attempt == 29) throw error;
                if (Context.current().isCancelled()) throw error;
                Thread.sleep(Math.min(200, 10L * (attempt + 1)));
            }
        }
    }
    private static JobUpdate.Builder update(SubmitJobRequest request, JobUpdate.State state) {
        return JobUpdate.newBuilder().setRequestId(request.getRequestId()).setState(state);
    }
}
