package edu.usp.cs324.jobs;

import edu.usp.cs324.api.Job;
import edu.usp.cs324.client.GrpcJobClient;
import edu.usp.cs324.proto.*;
import edu.usp.cs324.proto.jobs.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class GrpcJobsTest {
    private final Job max = new Job(Job.Type.MAX, List.of(-8, 42, 1), 0, 0);
    @Test void wireRoundTripsAllOperationsAndBoundaries() {
        for (Job job : List.of(max, new Job(Job.Type.PRIMESUM, List.of(), Integer.MIN_VALUE, Integer.MAX_VALUE),
                new Job(Job.Type.PRIMECOUNT, List.of(2, 2, -1), 0, 0)))
            assertEquals(job, JobWire.decode(JobWire.encode(job)));
        assertThrows(IllegalArgumentException.class, () -> JobWire.decode(JobInput.getDefaultInstance()));
        assertThrows(IllegalArgumentException.class, () -> JobWire.decode(JobInput.newBuilder()
                .setOperation(JobInput.Operation.MAX).build()));
    }
    @Test void clientReceivesOrderedProgressAndActualResults() throws Exception {
        IntegerJobEngine engine = new IntegerJobEngine();
        try (Fixture f = new Fixture(new JobServiceImpl(engine::compute))) {
            List<JobUpdate.State> states = new ArrayList<>();
            assertEquals(BigInteger.valueOf(42), f.client.submit(max, update -> states.add(update.getState())));
            assertEquals(List.of(JobUpdate.State.RECEIVED, JobUpdate.State.RUNNING, JobUpdate.State.COMPLETED), states);
            assertEquals(BigInteger.ZERO, f.client.submit(new Job(Job.Type.PRIMECOUNT, List.of(), 0, 0)));
            assertEquals(BigInteger.valueOf(17), f.client.submit(new Job(Job.Type.PRIMESUM, List.of(), 1, 10)));
        }
    }
    @Test void invalidRequestDoesNotReachCoordinator() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Fixture f = new Fixture(new JobServiceImpl(job -> { calls.incrementAndGet(); return BigInteger.ONE; }))) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", f.jobs.getPort()).usePlaintext().build();
            try {
                var stream = JobServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS)
                        .submitJob(SubmitJobRequest.getDefaultInstance());
                var error = assertThrows(StatusRuntimeException.class, stream::hasNext);
                assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
                assertEquals(0, calls.get());
            } finally { channel.shutdownNow(); }
        }
    }
    @Test void failureIsNeverAutomaticallyReplayed() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Fixture f = new Fixture(new JobServiceImpl(job -> {
            calls.incrementAndGet(); throw new IllegalStateException("after admission");
        }))) {
            assertThrows(StatusRuntimeException.class, () -> f.client.submit(max));
            assertEquals(1, calls.get());
        }
    }
    @Test void onlyExplicitPreAdmissionRejectionIsRetried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Fixture f = new Fixture(new JobServiceImpl(job -> {
            if (calls.incrementAndGet() == 1) throw new edu.usp.cs324.api.RetryException("term rotation");
            return BigInteger.valueOf(42);
        }))) {
            assertEquals(BigInteger.valueOf(42), f.client.submit(max));
            assertEquals(2, calls.get());
        }
    }
    @Test void nestedRetryFromAcceptedWorkIsNotReplayed() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Fixture f = new Fixture(new JobServiceImpl(job -> {
            calls.incrementAndGet();
            throw new edu.usp.cs324.api.JobFailedException("accepted computation failed",
                    new edu.usp.cs324.api.RetryException("nested computation error"));
        }))) {
            assertThrows(StatusRuntimeException.class, () -> f.client.submit(max));
            assertEquals(1, calls.get());
        }
    }
    @Test void incompleteStreamCannotBecomeSuccessfulResult() throws Exception {
        try (Fixture f = new Fixture(new JobServiceGrpc.JobServiceImplBase() {
            @Override public void submitJob(SubmitJobRequest request, StreamObserver<JobUpdate> output) {
                output.onNext(JobUpdate.newBuilder().setRequestId(request.getRequestId())
                        .setState(JobUpdate.State.RECEIVED).build());
                output.onCompleted();
            }
        })) { assertThrows(IllegalStateException.class, () -> f.client.submit(max)); }
    }
    @Test void concurrentSubmissionsCanOverlap() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        try (Fixture f = new Fixture(new JobServiceImpl(job -> {
            entered.countDown();
            if (!entered.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Calls did not overlap");
            return BigInteger.valueOf(42);
        })); ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<BigInteger> first = pool.submit(() -> f.client.submit(max));
            Future<BigInteger> second = pool.submit(() -> f.client.submit(max));
            assertEquals(BigInteger.valueOf(42), first.get(10, TimeUnit.SECONDS));
            assertEquals(BigInteger.valueOf(42), second.get(10, TimeUnit.SECONDS));
        }
    }
    @Test void unavailableLegacyServiceIsNeverReplayed() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Fixture f = new Fixture(new JobServiceGrpc.JobServiceImplBase() {
            @Override public void submitJob(SubmitJobRequest request, StreamObserver<JobUpdate> output) {
                calls.incrementAndGet(); output.onError(Status.UNAVAILABLE.asRuntimeException());
            }
        })) {
            assertThrows(StatusRuntimeException.class, () -> f.client.submit(max));
            assertEquals(1, calls.get());
        }
    }
    @Test void durableReconnectKeepsRequestIdAndSuppressesRepeatedProgress() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        var id = new java.util.concurrent.atomic.AtomicReference<String>();
        try (Fixture f = new Fixture(new JobServiceGrpc.JobServiceImplBase() {
            @Override public void submitJob(SubmitJobRequest request, StreamObserver<JobUpdate> output) {
                if (calls.incrementAndGet() == 1) id.set(request.getRequestId());
                assertEquals(id.get(), request.getRequestId());
                output.onNext(JobUpdate.newBuilder().setRequestId(id.get()).setState(JobUpdate.State.RECEIVED).build());
                output.onNext(JobUpdate.newBuilder().setRequestId(id.get()).setState(JobUpdate.State.RUNNING).build());
                if (calls.get() == 1) { output.onError(Status.UNAVAILABLE.asRuntimeException()); return; }
                output.onNext(JobUpdate.newBuilder().setRequestId(id.get()).setState(JobUpdate.State.COMPLETED).setResult("42").build());
                output.onCompleted();
            }
        }, true)) {
            var states = new ArrayList<JobUpdate.State>();
            assertEquals(BigInteger.valueOf(42), f.client.submit(max, u -> states.add(u.getState())));
            assertEquals(2, calls.get());
            assertEquals(List.of(JobUpdate.State.RECEIVED, JobUpdate.State.RUNNING, JobUpdate.State.COMPLETED), states);
        }
    }
    private static final class Fixture implements AutoCloseable {
        final Server jobs;
        final Server bootstrap;
        final GrpcJobClient client;
        Fixture(BindableService service) throws Exception { this(service, false); }
        Fixture(BindableService service, boolean durable) throws Exception {
            jobs = ServerBuilder.forPort(0).addService(service).build().start();
            bootstrap = ServerBuilder.forPort(0).addService(new BootstrapServiceGrpc.BootstrapServiceImplBase() {
                @Override public void listActiveWorkers(ListActiveWorkersRequest request,
                        StreamObserver<ListActiveWorkersReply> output) {
                    output.onNext(ListActiveWorkersReply.newBuilder().addWorkers(WorkerEndpoint.newBuilder()
                            .setWorkerId(1).setHost("127.0.0.1").setGrpcPort(jobs.getPort()).setDurableJobs(durable)).build());
                    output.onCompleted();
                }
            }).build().start();
            client = new GrpcJobClient("127.0.0.1", bootstrap.getPort());
        }
        @Override public void close() throws Exception {
            bootstrap.shutdownNow(); jobs.shutdownNow();
            bootstrap.awaitTermination(5, TimeUnit.SECONDS); jobs.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
