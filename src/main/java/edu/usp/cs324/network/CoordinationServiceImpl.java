package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import edu.usp.cs324.proto.WorkerEndpoint;
import edu.usp.cs324.proto.coordination.*;
import edu.usp.cs324.proto.recovery.RecoveryJobInput;
import io.grpc.stub.StreamObserver;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.function.Function;

/** gRPC facade around the existing, tested election/admission state machine. */
public final class CoordinationServiceImpl extends CoordinationServiceGrpc.CoordinationServiceImplBase {
    private final WorkerRemote node;
    private final Function<Peer, WorkerEndpoint> endpoints;
    public CoordinationServiceImpl(WorkerRemote node, Function<Peer, WorkerEndpoint> endpoints) {
        this.node = java.util.Objects.requireNonNull(node);
        this.endpoints = java.util.Objects.requireNonNull(endpoints);
    }
    @FunctionalInterface private interface Call<T> { T run() throws Exception; }
    private <T> void respond(StreamObserver<T> observer, Call<T> call) {
        try { T value = call.run(); observer.onNext(value); observer.onCompleted(); }
        catch (Exception e) {
            io.grpc.Status status;
            if (e instanceof RetryException) status = io.grpc.Status.FAILED_PRECONDITION;
            else if (e instanceof JobFailedException) status = io.grpc.Status.ABORTED;
            else if (e instanceof IllegalArgumentException || e instanceof NullPointerException) status = io.grpc.Status.INVALID_ARGUMENT;
            else if (e instanceof RemoteException) status = io.grpc.Status.UNAVAILABLE;
            else status = io.grpc.Status.INTERNAL;
            observer.onError(status.withDescription(e.getMessage()).withCause(e).asRuntimeException());
        }
    }
    private IntegerResult result(BigInteger value) { return IntegerResult.newBuilder().setValue(value.toString()).build(); }
    @Override public void getStatus(Empty request, StreamObserver<WorkerStatus> observer) {
        respond(observer, () -> {
            var s = node.status();
            var b = WorkerStatus.newBuilder().setWorker(endpoints.apply(s.peer())).setJac(s.jac()).setAssignedJobs(s.assignedJobs());
            if (s.term() != null) b.setTerm(CoordinationCodec.term(s.term(), endpoints));
            s.neighbours().forEach(p -> b.addNeighbours(endpoints.apply(p)));
            return b.build();
        });
    }
    @Override public void election(ElectionRequest request, StreamObserver<ElectionReply> observer) {
        respond(observer, () -> {
            var b = ElectionReply.newBuilder();
            for (var c : node.election(UUID.fromString(request.getElectionId())))
                b.addCandidates(ElectionCandidate.newBuilder().setWorker(endpoints.apply(c.peer())).setJac(c.jac()));
            return b.build();
        });
    }
    @Override public void announce(CoordinatorTerm request, StreamObserver<Empty> observer) {
        respond(observer, () -> { node.coordinator(CoordinationCodec.term(request)); return Empty.getDefaultInstance(); });
    }
    @Override public void elect(Empty request, StreamObserver<CoordinatorTerm> observer) {
        respond(observer, () -> CoordinationCodec.term(node.elect(), endpoints));
    }
    @Override public void submit(RecoveryJobInput request, StreamObserver<IntegerResult> observer) {
        respond(observer, () -> result(node.submit(CoordinationCodec.job(request))));
    }
    @Override public void assign(Assignment request, StreamObserver<IntegerResult> observer) {
        respond(observer, () -> {
            if (!request.hasInput() || !request.hasTerm()) throw new IllegalArgumentException("Input and term required");
            return result(node.assign(CoordinationCodec.job(request.getInput()), CoordinationCodec.term(request.getTerm()),
                    request.getWorkersList().stream().map(CoordinationCodec::peer).toList()));
        });
    }
    @Override public void compute(RecoveryJobInput request, StreamObserver<IntegerResult> observer) {
        respond(observer, () -> result(node.compute(CoordinationCodec.job(request))));
    }
}
