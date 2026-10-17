package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import edu.usp.cs324.proto.WorkerEndpoint;
import edu.usp.cs324.proto.coordination.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Local Java adapter; every remote call is Protobuf/gRPC, including dispatch. */
public final class CoordinationGrpcClient implements WorkerRemote, AutoCloseable {
    private final ManagedChannel channel;
    private final CoordinationServiceGrpc.CoordinationServiceBlockingStub stub;
    private final Function<Peer, WorkerEndpoint> endpoints;
    private final long timeoutNanos;
    public CoordinationGrpcClient(WorkerEndpoint target, Function<Peer, WorkerEndpoint> endpoints) {
        this(target, endpoints, Duration.ofSeconds(30));
    }
    public CoordinationGrpcClient(WorkerEndpoint target, Function<Peer, WorkerEndpoint> endpoints, Duration timeout) {
        if (target.getHost().isBlank() || target.getGrpcPort() < 1 || target.getGrpcPort() > 65535)
            throw new IllegalArgumentException("Valid gRPC endpoint required");
        timeoutNanos = timeout.toNanos();
        if (timeoutNanos <= 0) throw new IllegalArgumentException("Positive timeout required");
        this.endpoints = java.util.Objects.requireNonNull(endpoints);
        channel = ManagedChannelBuilder.forAddress(target.getHost(), target.getGrpcPort()).usePlaintext().build();
        stub = CoordinationServiceGrpc.newBlockingStub(channel);
    }
    private CoordinationServiceGrpc.CoordinationServiceBlockingStub rpc() {
        return stub.withDeadlineAfter(timeoutNanos, TimeUnit.NANOSECONDS);
    }
    @FunctionalInterface private interface Call<T> { T run(); }
    private <T> T call(Call<T> call) throws RemoteException {
        try { return call.run(); }
        catch (StatusRuntimeException e) {
            // Only the explicit pre-admission code permits replay. A deadline or
            // transport failure remains an unknown outcome, never RetryException.
            if (e.getStatus().getCode() == io.grpc.Status.Code.FAILED_PRECONDITION)
                throw new RetryException(e.getStatus().getDescription());
            if (e.getStatus().getCode() == io.grpc.Status.Code.ABORTED)
                throw new JobFailedException(e.getStatus().getDescription(), e);
            throw new RemoteException("gRPC call failed: " + e.getStatus(), e);
        }
    }
    @Override public Status status() throws RemoteException {
        return call(() -> {
            var s = rpc().getStatus(Empty.getDefaultInstance());
            return new Status(CoordinationCodec.peer(s.getWorker()), s.getJac(), s.getAssignedJobs(),
                    s.hasTerm() ? CoordinationCodec.term(s.getTerm()) : null,
                    s.getNeighboursList().stream().map(CoordinationCodec::peer).toList());
        });
    }
    @Override public List<Candidate> election(UUID id) throws RemoteException {
        return call(() -> rpc().election(ElectionRequest.newBuilder().setElectionId(id.toString()).build())
                .getCandidatesList().stream().map(c -> new Candidate(CoordinationCodec.peer(c.getWorker()), c.getJac())).toList());
    }
    @Override public void coordinator(Term term) throws RemoteException {
        call(() -> rpc().announce(CoordinationCodec.term(term, endpoints)));
    }
    @Override public Term elect() throws RemoteException {
        return call(() -> CoordinationCodec.term(rpc().elect(Empty.getDefaultInstance())));
    }
    @Override public BigInteger submit(Job job) throws RemoteException {
        return call(() -> new BigInteger(rpc().submit(CoordinationCodec.job(job)).getValue()));
    }
    @Override public BigInteger assign(Job job, Term term, List<Peer> workers) throws RemoteException {
        return call(() -> {
            var b = Assignment.newBuilder().setInput(CoordinationCodec.job(job)).setTerm(CoordinationCodec.term(term, endpoints));
            workers.forEach(p -> b.addWorkers(endpoints.apply(p)));
            return new BigInteger(rpc().assign(b.build()).getValue());
        });
    }
    @Override public BigInteger compute(Job job) throws RemoteException {
        return call(() -> new BigInteger(rpc().compute(CoordinationCodec.job(job)).getValue()));
    }
    @Override public void addNeighbour(Peer peer) throws RemoteException {
        throw new RemoteException("Use WorkerMembershipService.AddNeighbour for membership changes");
    }
    @Override public void close() { channel.shutdownNow(); }
}
