package edu.usp.cs324.network;

import edu.usp.cs324.api.Peer;
import edu.usp.cs324.proto.AddNeighbourRequest;
import edu.usp.cs324.proto.MembershipReply;
import edu.usp.cs324.proto.WorkerEndpoint;
import edu.usp.cs324.proto.WorkerMembershipServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.function.Consumer;

/** Receives membership links over gRPC and updates the local worker's neighbour set. */
public final class WorkerMembershipServiceImpl extends WorkerMembershipServiceGrpc.WorkerMembershipServiceImplBase {
    private final Consumer<Peer> addNeighbour;
    private Consumer<WorkerEndpoint> remember = endpoint -> { };

    public WorkerMembershipServiceImpl(WorkerNode worker, BootstrapGrpcClient bootstrap) {
        this(worker);
        remember = bootstrap::remember;
    }

    public WorkerMembershipServiceImpl(WorkerNode worker) {
        this(Objects.requireNonNull(worker, "worker")::addNeighbour);
    }

    WorkerMembershipServiceImpl(Consumer<Peer> addNeighbour) {
        this.addNeighbour = Objects.requireNonNull(addNeighbour, "addNeighbour");
    }

    @Override
    public void addNeighbour(AddNeighbourRequest request, StreamObserver<MembershipReply> observer) {
        if (!request.hasNeighbour()) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("Neighbour endpoint is required").asRuntimeException());
            return;
        }
        WorkerEndpoint endpoint = request.getNeighbour();
        Peer neighbour;
        try {
            neighbour = BootstrapGrpcClient.toPeer(endpoint);
            if (endpoint.getGrpcPort() < 1 || endpoint.getGrpcPort() > 65_535
                    || endpoint.getIncarnationId().isBlank())
                throw new IllegalArgumentException("Neighbour gRPC port and incarnation ID are required");
        } catch (IllegalArgumentException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        try {
            remember.accept(endpoint);
            addNeighbour.accept(neighbour);
        } catch (RuntimeException e) {
            observer.onError(Status.INTERNAL.withDescription("Could not add neighbour").withCause(e).asRuntimeException());
            return;
        }
        observer.onNext(MembershipReply.newBuilder().setAccepted(true).setMessage("Neighbour added").build());
        observer.onCompleted();
    }
}
