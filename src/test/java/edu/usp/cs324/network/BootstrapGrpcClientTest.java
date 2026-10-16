package edu.usp.cs324.network;

import edu.usp.cs324.api.Peer;
import edu.usp.cs324.proto.*;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.rmi.RemoteException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Real loopback gRPC calls, including reciprocal updates to actual WorkerNode instances. */
@Timeout(20)
class BootstrapGrpcClientTest {
    @Test
    void registersDiscoversAndCreatesReciprocalNeighbours() throws Exception {
        try (GrpcServer directory = new GrpcServer(new BootstrapServiceImpl());
             Worker first = new Worker(1, directory.port());
             Worker second = new Worker(2, directory.port())) {
            first.client.register(first.peer);
            second.client.register(second.peer);

            assertEquals(List.of(first.peer, second.peer), second.client.active());
            assertEquals(List.of(second.peer), first.node.status().neighbours());
            assertEquals(List.of(first.peer), second.node.status().neighbours());
        }
    }

    @Test
    void scheduledHeartbeatsUseTheRegisteredIncarnation() throws Exception {
        CountDownLatch received = new CountDownLatch(2);
        AtomicReference<WorkerEndpoint> registered = new AtomicReference<>();
        AtomicReference<HeartbeatRequest> heartbeat = new AtomicReference<>();
        var recordingService = new BootstrapServiceGrpc.BootstrapServiceImplBase() {
            @Override
            public void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerReply> observer) {
                registered.set(request.getWorker());
                observer.onNext(RegisterWorkerReply.newBuilder().setAccepted(true).build());
                observer.onCompleted();
            }

            @Override
            public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatReply> observer) {
                heartbeat.set(request);
                observer.onNext(HeartbeatReply.newBuilder().setAccepted(true).build());
                observer.onCompleted();
                received.countDown();
            }
        };
        Peer peer = new Peer(3, "127.0.0.1", 6003);
        try (GrpcServer directory = new GrpcServer(recordingService);
             BootstrapGrpcClient client = new BootstrapGrpcClient("127.0.0.1", directory.port(),
                     peer, 5003, Duration.ofMillis(50))) {
            client.register(peer);
            assertTrue(received.await(5, TimeUnit.SECONDS), "Scheduled heartbeats should reach the bootstrap");
            assertEquals(registered.get().getWorkerId(), heartbeat.get().getWorkerId());
            assertEquals(registered.get().getIncarnationId(), heartbeat.get().getIncarnationId());
            assertFalse(heartbeat.get().getIncarnationId().isBlank());
        }
    }

    @Test
    void expiredLeaseIsRegisteredAgainWithTheSameIncarnation() throws Exception {
        AtomicLong ticker = new AtomicLong();
        BootstrapServiceImpl service = new BootstrapServiceImpl(Duration.ofSeconds(10), ticker::get);
        try (GrpcServer directory = new GrpcServer(service);
             Worker worker = new Worker(4, directory.port())) {
            worker.client.register(worker.peer);
            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", directory.port())
                    .usePlaintext().build();
            try {
                var stub = BootstrapServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
                WorkerEndpoint before = stub.listActiveWorkers(ListActiveWorkersRequest.getDefaultInstance()).getWorkers(0);
                ticker.set(Duration.ofSeconds(11).toNanos());

                worker.client.refreshLease();

                assertEquals(before, stub.listActiveWorkers(ListActiveWorkersRequest.getDefaultInstance()).getWorkers(0));
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void activeDuplicateIdCannotTakeOverTheRegistration() throws Exception {
        try (GrpcServer directory = new GrpcServer(new BootstrapServiceImpl());
             Worker original = new Worker(5, directory.port());
             BootstrapGrpcClient duplicate = new BootstrapGrpcClient("127.0.0.1", directory.port(),
                     original.peer, 5005)) {
            original.client.register(original.peer);

            RemoteException error = assertThrows(RemoteException.class, () -> duplicate.register(original.peer));

            assertTrue(error.getMessage().contains("Registration rejected"));
            original.client.refreshLease();
            assertEquals(List.of(original.peer), original.client.active());
        }
    }

    @Test
    void malformedNeighbourIsRejectedWithoutChangingWorkerState() throws Exception {
        try (GrpcServer directory = new GrpcServer(new BootstrapServiceImpl());
             Worker worker = new Worker(6, directory.port())) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", worker.server.getPort())
                    .usePlaintext().build();
            try {
                var stub = WorkerMembershipServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
                StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                        () -> stub.addNeighbour(AddNeighbourRequest.getDefaultInstance()));
                assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
                assertTrue(worker.node.status().neighbours().isEmpty());
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    private static final class GrpcServer implements AutoCloseable {
        private final Server server;

        GrpcServer(BindableService service) throws Exception {
            server = ServerBuilder.forPort(0).addService(service).build().start();
        }

        int port() { return server.getPort(); }

        @Override public void close() throws Exception {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static final class Worker implements AutoCloseable {
        private final Peer peer;
        private final WorkerNode node;
        private final Server server;
        private final BootstrapGrpcClient client;

        Worker(int id, int bootstrapPort) throws Exception {
            peer = new Peer(id, "127.0.0.1", 6000 + id);
            node = new WorkerNode(peer, null, 0, null);
            server = ServerBuilder.forPort(0).addService(new WorkerMembershipServiceImpl(node)).build().start();
            client = new BootstrapGrpcClient("127.0.0.1", bootstrapPort, peer, server.getPort());
        }

        @Override public void close() throws Exception {
            client.close();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            node.close();
        }
    }
}
