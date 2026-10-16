package edu.usp.cs324.network;

import edu.usp.cs324.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class BootstrapServiceImplTest {
    private final AtomicLong ticker = new AtomicLong();
    private final BootstrapServiceImpl service = new BootstrapServiceImpl(
            Duration.ofSeconds(10), ticker::get);

    @Test
    void registrationReturnsExistingWorkersAndListingIsSorted() {
        WorkerEndpoint second = worker(2, "inc-2");
        WorkerEndpoint first = worker(1, "inc-1");

        RegisterWorkerReply firstReply = call(RegisterWorkerReply.class, observer -> service.registerWorker(
                RegisterWorkerRequest.newBuilder().setWorker(second).build(), observer));
        RegisterWorkerReply secondReply = call(RegisterWorkerReply.class, observer -> service.registerWorker(
                RegisterWorkerRequest.newBuilder().setWorker(first).build(), observer));

        assertTrue(firstReply.getAccepted());
        assertTrue(firstReply.getExistingWorkersList().isEmpty());
        assertTrue(secondReply.getAccepted());
        assertEquals(List.of(second), secondReply.getExistingWorkersList());
        assertEquals(List.of(first, second), call(ListActiveWorkersReply.class, observer -> service.listActiveWorkers(
                ListActiveWorkersRequest.getDefaultInstance(), observer)).getWorkersList());
    }

    @Test
    void heartbeatRequiresMatchingIncarnationAndRefreshesLease() {
        WorkerEndpoint endpoint = worker(1, "inc-1");
        call(RegisterWorkerReply.class, observer -> service.registerWorker(RegisterWorkerRequest.newBuilder()
                .setWorker(endpoint).build(), observer));

        ticker.set(Duration.ofSeconds(9).toNanos());
        assertTrue(call(HeartbeatReply.class, observer -> service.heartbeat(HeartbeatRequest.newBuilder()
                .setWorkerId(1).setIncarnationId("inc-1").build(), observer)).getAccepted());

        ticker.set(Duration.ofSeconds(18).toNanos());
        assertEquals(1, call(ListActiveWorkersReply.class, observer -> service.listActiveWorkers(
                ListActiveWorkersRequest.getDefaultInstance(), observer)).getWorkersCount());
        assertFalse(call(HeartbeatReply.class, observer -> service.heartbeat(HeartbeatRequest.newBuilder()
                .setWorkerId(1).setIncarnationId("old-incarnation").build(), observer)).getAccepted());
    }

    @Test
    void expiredWorkerIsRemovedAndCanRejoinWithNewIncarnation() {
        call(RegisterWorkerReply.class, observer -> service.registerWorker(RegisterWorkerRequest.newBuilder()
                .setWorker(worker(1, "old-incarnation")).build(), observer));
        ticker.set(Duration.ofSeconds(10).toNanos());

        RegisterWorkerReply reply = call(RegisterWorkerReply.class, observer -> service.registerWorker(RegisterWorkerRequest.newBuilder()
                .setWorker(worker(1, "new-incarnation")).build(), observer));

        assertTrue(reply.getAccepted());
        assertEquals(1, call(ListActiveWorkersReply.class, observer -> service.listActiveWorkers(
                ListActiveWorkersRequest.getDefaultInstance(), observer)).getWorkersCount());
        assertEquals("new-incarnation", call(ListActiveWorkersReply.class, observer -> service.listActiveWorkers(
                ListActiveWorkersRequest.getDefaultInstance(), observer)).getWorkers(0).getIncarnationId());
    }

    @Test
    void activeDuplicateIdAndInvalidEndpointAreRejected() {
        call(RegisterWorkerReply.class, observer -> service.registerWorker(RegisterWorkerRequest.newBuilder()
                .setWorker(worker(1, "inc-1")).build(), observer));
        RegisterWorkerReply duplicate = call(RegisterWorkerReply.class, observer -> service.registerWorker(RegisterWorkerRequest.newBuilder()
                .setWorker(worker(1, "inc-2")).build(), observer));
        assertFalse(duplicate.getAccepted());

        WorkerEndpoint invalid = worker(2, "inc-2").toBuilder().setGrpcPort(0).build();
        RuntimeException error = assertThrows(RuntimeException.class, () -> call(RegisterWorkerReply.class, observer ->
                service.registerWorker(RegisterWorkerRequest.newBuilder().setWorker(invalid).build(), observer)));
        assertTrue(error.getMessage().contains("gRPC port"));
    }

    @Test
    void generatedGrpcClientCanRegisterAndListAWorker() throws Exception {
        Server server = ServerBuilder.forPort(0)
                .addService(new BootstrapServiceImpl())
                .build()
                .start();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build();
        try {
            BootstrapServiceGrpc.BootstrapServiceBlockingStub stub =
                    BootstrapServiceGrpc.newBlockingStub(channel);
            WorkerEndpoint endpoint = worker(3, "inc-3");
            assertTrue(stub.registerWorker(RegisterWorkerRequest.newBuilder()
                    .setWorker(endpoint).build()).getAccepted());
            assertEquals(List.of(endpoint), stub.listActiveWorkers(
                    ListActiveWorkersRequest.getDefaultInstance()).getWorkersList());
        } finally {
            channel.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static WorkerEndpoint worker(int id, String incarnation) {
        return WorkerEndpoint.newBuilder()
                .setWorkerId(id)
                .setHost("127.0.0.1")
                .setGrpcPort(5000 + id)
                .setIncarnationId(incarnation)
                .build();
    }

    private static <T> T call(Class<T> responseType, Consumer<StreamObserver<T>> invocation) {
        AtomicReference<T> value = new AtomicReference<>();
        AtomicReference<RuntimeException> error = new AtomicReference<>();
        boolean[] completed = new boolean[1];
        invocation.accept(new StreamObserver<>() {
            @Override public void onNext(T item) { value.set(item); }
            @Override public void onError(Throwable throwable) {
                error.set(throwable instanceof RuntimeException runtime
                        ? runtime : new RuntimeException(throwable));
            }
            @Override public void onCompleted() { completed[0] = true; }
        });
        if (error.get() != null) throw error.get();
        assertTrue(completed[0], "RPC should complete");
        assertNotNull(value.get(), "RPC should return a response");
        return responseType.cast(value.get());
    }
}
