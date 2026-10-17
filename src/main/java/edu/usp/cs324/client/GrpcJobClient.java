package edu.usp.cs324.client;

import edu.usp.cs324.api.Job;
import edu.usp.cs324.jobs.JobWire;
import edu.usp.cs324.proto.*;
import edu.usp.cs324.proto.jobs.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Stable request IDs make reconnection safe for the durable JobService. Thread-safe. */
public final class GrpcJobClient {
    private final String host;
    private final int port;
    public GrpcJobClient(String host, int port) {
        if (host == null || host.isBlank()) throw new IllegalArgumentException("Bootstrap host is required");
        if (port < 1 || port > 65535) throw new IllegalArgumentException("Port must be 1..65535");
        this.host = host.strip(); this.port = port;
    }
    public BigInteger submit(Job job) { return submit(job, update -> { }); }
    public BigInteger submit(Job job, Consumer<JobUpdate> progress) {
        return submit(UUID.randomUUID().toString(), job, progress);
    }
    /** Keep id and input to resume this job even after restarting the client process. */
    public BigInteger submit(String id, Job job, Consumer<JobUpdate> progress) {
        if (id == null || id.isBlank() || id.length() > 128) throw new IllegalArgumentException("Job ID must contain 1..128 characters");
        SubmitJobRequest request = SubmitJobRequest.newBuilder().setRequestId(id).setInput(JobWire.encode(job)).build();
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
        int delivered = 0;
        boolean mayReconnect = true;
        for (;;) {
            try {
                WorkerEndpoint endpoint = discover();
                mayReconnect = endpoint.getDurableJobs();
                ManagedChannel worker = channel(endpoint.getHost(), endpoint.getGrpcPort());
                try {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) throw new IllegalStateException("Client wait expired; resume with the same job ID: " + id);
                    Iterator<JobUpdate> stream = JobServiceGrpc.newBlockingStub(worker)
                            .withDeadlineAfter(remaining, TimeUnit.NANOSECONDS).submitJob(request);
                    BigInteger result = null;
                    int previous = 0;
                    while (stream.hasNext()) {
                        JobUpdate update = stream.next();
                        int state = update.getStateValue();
                        if (!id.equals(update.getRequestId()) || state != previous + 1 || state > 3)
                            throw new IllegalStateException("Invalid job progress sequence");
                        if ((state == 3) != update.hasResult()) throw new IllegalStateException("Result must be present only on completion");
                        if (state == 3) result = new BigInteger(update.getResult());
                        previous = state;
                        if (state > delivered) { progress.accept(update); delivered = state; }
                    }
                    if (result == null) throw new IllegalStateException("Stream ended without a result; resume job ID: " + id);
                    return result;
                } finally { worker.shutdownNow(); }
            } catch (StatusRuntimeException e) {
                var code = e.getStatus().getCode();
                if (Thread.currentThread().isInterrupted() || io.grpc.Context.current().isCancelled()) throw e;
                if (!mayReconnect || (code != Status.Code.UNAVAILABLE && code != Status.Code.DEADLINE_EXCEEDED
                        && code != Status.Code.CANCELLED)) throw e;
                if (System.nanoTime() >= deadline) throw new IllegalStateException("Client wait expired; resume job ID: " + id, e);
                try { Thread.sleep(250); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException("Interrupted; resume job ID: " + id, interrupted); }
            }
        }
    }
    private WorkerEndpoint discover() {
        ManagedChannel directory = channel(host, port);
        try {
            var workers = BootstrapServiceGrpc.newBlockingStub(directory).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .listActiveWorkers(ListActiveWorkersRequest.getDefaultInstance()).getWorkersList();
            if (workers.isEmpty()) throw Status.UNAVAILABLE.withDescription("No active workers").asRuntimeException();
            WorkerEndpoint endpoint = workers.getFirst();
            if (endpoint.getHost().isBlank() || endpoint.getGrpcPort() < 1 || endpoint.getGrpcPort() > 65535)
                throw new IllegalStateException("Invalid worker gRPC endpoint");
            return endpoint;
        } finally { directory.shutdownNow(); }
    }
    private static ManagedChannel channel(String host, int port) {
        return ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }
}
