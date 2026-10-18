package edu.usp.cs324.jobs;

import edu.usp.cs324.api.Job;
import edu.usp.cs324.proto.jobs.*;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.math.BigInteger;
import java.util.Objects;

/** Streaming adapter; the injected coordinator owns admission, splitting and recovery.
 * RUNNING means the submission is being processed, not that admission is durable.
 * Disconnecting the stream does not cancel already submitted distributed work.
 */
public final class JobServiceImpl extends JobServiceGrpc.JobServiceImplBase {
    @FunctionalInterface public interface Submitter { BigInteger submit(Job job) throws Exception; }
    @FunctionalInterface public interface DurableSubmitter { BigInteger submit(String id, Job job) throws Exception; }
    @FunctionalInterface public interface ProgressSubmitter {
        BigInteger submit(String id, Job job, java.util.function.Consumer<JobUpdate> progress) throws Exception;
    }
    private ProgressSubmitter streaming;
    public JobServiceImpl(DurableSubmitter durable, ProgressSubmitter streaming) {
        this(durable); this.streaming = Objects.requireNonNull(streaming);
    }
    private final Submitter submitter;
    private DurableSubmitter durable;
    public JobServiceImpl(DurableSubmitter durable) {
        this.submitter = null;
        this.durable = Objects.requireNonNull(durable);
    }
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
            JobUpdate[] latest = {null};
            BigInteger result = Objects.requireNonNull(streaming == null
                    ? submitWithAdmissionRetry(request.getRequestId(), job)
                    : streaming.submit(request.getRequestId(), job, progress -> {
                        latest[0] = progress;
                        if (!Context.current().isCancelled()) output.onNext(progress);
                    }), "Missing result");
            if (Context.current().isCancelled()) return;
            var completed = latest[0] == null ? update(request, JobUpdate.State.COMPLETED)
                    : latest[0].toBuilder().setState(JobUpdate.State.COMPLETED);
            output.onNext(completed.setResult(result.toString()).build());
            output.onCompleted();
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            if (!Context.current().isCancelled() && (e instanceof IllegalArgumentException
                    || (e instanceof io.grpc.StatusRuntimeException rpc
                    && rpc.getStatus().getCode() == Status.Code.INVALID_ARGUMENT))) {
                output.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
                return;
            }
            if (!Context.current().isCancelled())
                output.onError((durable == null ? Status.UNKNOWN : Status.UNAVAILABLE).withDescription(
                        "Submission failed; completion may be unknown. Do not automatically replay.")
                        .withCause(e).asRuntimeException());
        }
    }
    private BigInteger submitWithAdmissionRetry(String id, Job job) throws Exception {
        if (durable != null) return durable.submit(id, job);
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
