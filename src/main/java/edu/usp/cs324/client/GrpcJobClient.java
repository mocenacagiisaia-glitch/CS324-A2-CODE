package edu.usp.cs324.client;

import edu.usp.cs324.api.Job;
import edu.usp.cs324.jobs.JobWire;
import edu.usp.cs324.proto.*;
import edu.usp.cs324.proto.jobs.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Each call owns its channels. No replay after any transport failure. Thread-safe. */
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
        SubmitJobRequest request = SubmitJobRequest.newBuilder().setRequestId(UUID.randomUUID().toString())
                .setInput(JobWire.encode(job)).build();
        ManagedChannel directory = channel(host, port);
        WorkerEndpoint endpoint;
        try {
            var workers = BootstrapServiceGrpc.newBlockingStub(directory).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .listActiveWorkers(ListActiveWorkersRequest.getDefaultInstance()).getWorkersList();
            if (workers.isEmpty()) throw new IllegalStateException("No active workers");
            endpoint = workers.getFirst();
        } finally { directory.shutdownNow(); }
        if (endpoint.getHost().isBlank() || endpoint.getGrpcPort() < 1 || endpoint.getGrpcPort() > 65535)
            throw new IllegalStateException("Invalid worker gRPC endpoint");
        ManagedChannel worker = channel(endpoint.getHost(), endpoint.getGrpcPort());
        try {
            Iterator<JobUpdate> stream = JobServiceGrpc.newBlockingStub(worker)
                    .withDeadlineAfter(10, TimeUnit.MINUTES).submitJob(request);
            BigInteger result = null;
            int previous = 0;
            while (stream.hasNext()) {
                JobUpdate update = stream.next();
                int state = update.getStateValue();
                if (!request.getRequestId().equals(update.getRequestId()) || state != previous + 1 || state > 3)
                    throw new IllegalStateException("Invalid job progress sequence");
                if ((state == 3) != update.hasResult())
                    throw new IllegalStateException("Result must be present only on completion");
                if (state == 3) result = new BigInteger(update.getResult());
                previous = state;
                progress.accept(update);
            }
            if (result == null) throw new IllegalStateException("Stream ended without a completed result; do not replay");
            return result;
        } finally { worker.shutdownNow(); }
    }
    private static ManagedChannel channel(String host, int port) {
        return ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }
}
