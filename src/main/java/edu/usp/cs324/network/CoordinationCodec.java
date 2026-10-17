package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import edu.usp.cs324.proto.WorkerEndpoint;
import edu.usp.cs324.proto.coordination.*;
import edu.usp.cs324.proto.recovery.RecoveryJobInput;
import java.util.UUID;
import java.util.function.Function;

/** Explicit value conversion: no Java serialization is used on the wire. */
final class CoordinationCodec {
    private CoordinationCodec() { }
    static WorkerEndpoint peer(Peer p) {
        return WorkerEndpoint.newBuilder().setWorkerId(p.id()).setHost(p.host())
                .setRegistryPort(p.registryPort()).build();
    }
    static Peer peer(WorkerEndpoint p) { return BootstrapGrpcClient.toPeer(p); }
    static CoordinatorTerm term(Term t, Function<Peer, WorkerEndpoint> endpoints) {
        return CoordinatorTerm.newBuilder().setNumber(t.number()).setElectionId(t.electionId().toString())
                .setLeader(endpoints.apply(t.leader())).build();
    }
    static Term term(CoordinatorTerm t) {
        if (t.getNumber() <= 0 || !t.hasLeader()) throw new IllegalArgumentException("Positive term and leader required");
        return new Term(t.getNumber(), UUID.fromString(t.getElectionId()), peer(t.getLeader()));
    }
    static RecoveryJobInput job(Job j) {
        var b = RecoveryJobInput.newBuilder().setOperation(RecoveryJobInput.Operation.valueOf(j.type().name()));
        if (j.type() == Job.Type.PRIMESUM) b.setRangeStart(j.start()).setRangeEnd(j.end());
        else b.addAllNumbers(j.numbers());
        return b.build();
    }
    static Job job(RecoveryJobInput j) {
        return new Job(Job.Type.valueOf(j.getOperation().name()), j.getNumbersList(), j.getRangeStart(), j.getRangeEnd());
    }
}
