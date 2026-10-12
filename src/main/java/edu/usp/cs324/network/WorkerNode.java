package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;

/** Membership, election and coordinator runtime; calculations are supplied by JobEngine. */
public final class WorkerNode extends UnicastRemoteObject implements WorkerRemote, AutoCloseable {
    private final Peer self;
    private final BootstrapRemote bootstrap;
    private final Map<Integer, Peer> neighbours = new ConcurrentSkipListMap<>();
    private final ExecutorService execution = Executors.newVirtualThreadPerTaskExecutor();
    private final JobEngine engine;
    // Never hold the state monitor across an RMI call: propagation can return via a cycle.
    private final Object electionGate = new Object();
    private final Set<UUID> seenElections = new HashSet<>();
    private Term term;
    private long jac;
    private int assignedJobs;


    public WorkerNode(Peer self, BootstrapRemote bootstrap, int exportPort, JobEngine engine)
            throws RemoteException {
        super(exportPort);
        this.self = self;
        this.bootstrap = bootstrap;
        this.engine = engine;
    }

    @Override public synchronized Status status() {
        return new Status(self, jac, assignedJobs, term, List.copyOf(neighbours.values()));
    }

    @Override public void addNeighbour(Peer peer) {
        if (peer.id() != self.id()) neighbours.put(peer.id(), peer);
    }

    @Override public List<Candidate> election(UUID id) throws RemoteException {
        Objects.requireNonNull(id, "Election ID");
        List<Candidate> candidates = new ArrayList<>();
        synchronized (this) {
            if (seenElections.contains(id)) return List.of();
            // Retain IDs for process lifetime: eviction could resurrect a delayed message.
            if (seenElections.size() >= 100_000) throw new RemoteException("Election history capacity reached");
            seenElections.add(id);
            candidates.add(new Candidate(self, jac));
        }
        for (Peer peer : neighbours.values()) {
            try { candidates.addAll(peer.connect().election(id)); }
            catch (RemoteException e) { System.err.println("Election link unavailable: " + peer.id()); }
        }
        return List.copyOf(candidates);
    }

    private static int compare(Term a, Term b) {
        int number = Long.compare(a.number(), b.number());
        return number != 0 ? number : a.electionId().compareTo(b.electionId());
    }

    @Override public void coordinator(Term incoming) throws RemoteException {
        Objects.requireNonNull(incoming, "Term");
        Objects.requireNonNull(incoming.electionId(), "Election ID");
        Objects.requireNonNull(incoming.leader(), "Leader");
        if (incoming.number() <= 0) throw new RemoteException("Term number must be positive");
        synchronized (this) {
            if (term != null && compare(incoming, term) <= 0) return;
            term = incoming;
            assignedJobs = 0;
        }
        for (Peer peer : neighbours.values()) {
            try { peer.connect().coordinator(incoming); }
            catch (RemoteException e) { System.err.println("Coordinator link unavailable: " + peer.id()); }
        }
    }

    @Override public Term elect() throws RemoteException {
        List<Peer> members = bootstrap.active();
        Peer initiator = members.stream().min(Comparator.comparingInt(Peer::id))
                .orElseThrow(() -> new RemoteException("No active workers"));
        // One worker serializes election attempts, not the bootstrap. This prevents
        // overlapping candidate snapshots from installing successive empty terms.
        if (!self.equals(initiator)) return initiator.connect().elect();
        synchronized (electionGate) {
            // A newly joined gate may have no local term. Discover the newest
            // cluster term before deciding whether another election is necessary.
            Term current = status().term();
            for (Peer peer : members) {
                Term observed = peer.connect().status().term();
                if (observed != null && (current == null || compare(observed, current) > 0))
                    current = observed;
            }
            if (current != null && members.contains(current.leader())) {
                Status leader = current.leader().connect().status();
                if (current.equals(leader.term()) && leader.assignedJobs() < 5) {
                    for (Peer peer : members) peer.connect().coordinator(current);
                    return current;
                }
            }
            long number = current == null ? 0 : current.number();
            UUID id = UUID.randomUUID();
            Map<Integer, Candidate> candidates = new HashMap<>();
            // Flood neighbours first; membership also covers disconnected live components.
            for (Peer peer : members) {
                for (Candidate candidate : peer.connect().election(id)) {
                    if (members.contains(candidate.peer())) candidates.put(candidate.peer().id(), candidate);
                }
            }
            if (candidates.size() != members.size())
                throw new RemoteException("Incomplete election response; retry with a new election ID");
            Candidate winner = candidates.values().stream().min(
                    Comparator.comparingLong(Candidate::jac)
                            .thenComparing(Comparator.comparingInt((Candidate c) -> c.peer().id()).reversed()))
                    .orElseThrow(() -> new RemoteException("No election candidates"));
            Term elected = new Term(Math.incrementExact(number), id, winner.peer());
            for (Peer peer : members) peer.connect().coordinator(elected);
            return elected;
        }
    }

    @Override public BigInteger submit(Job job) throws RemoteException {
        Objects.requireNonNull(job, "Job");
        Term elected = elect();
        // Do not retry an RMI failure: the leader may already have admitted this job.
        return elected.leader().connect().assign(job, elected, bootstrap.active());
    }

    @Override public BigInteger assign(Job job, Term expected, List<Peer> workers) throws RemoteException {
        Objects.requireNonNull(job, "Job");
        List<Peer> targets = List.copyOf(workers);
        if (targets.isEmpty() || new HashSet<>(targets).size() != targets.size())
            throw new RetryException("Worker list must be nonempty and unique");
        List<Job> parts;
        if (engine == null) throw new RetryException("JobEngine provider is not installed");
        try {
            parts = List.copyOf(engine.split(job, targets.size()));
            if (parts.isEmpty() || parts.size() > targets.size())
                throw new IllegalArgumentException("Engine must return 1..workerCount chunks");
        } catch (RuntimeException e) {
            throw new JobFailedException("Splitting failed; no work was admitted or started", e);
        }
        synchronized (this) {
            if (term == null || !term.equals(expected) || !self.equals(term.leader()) || assignedJobs >= 5)
                throw new RetryException("Stale term, wrong coordinator, or five-job limit reached");
            // Count allocations to OTHER workers, not client submissions or local chunks.
            long allocations = targets.subList(0, parts.size()).stream()
                    .filter(peer -> !peer.equals(self)).count();
            jac = Math.addExact(jac, allocations);
            assignedJobs++;
        }
        // Everything below is post-admission: no failure here may signal RetryException.
        BigInteger result;
        try {
            List<Future<BigInteger>> tasks = new ArrayList<>();
            Throwable failure = null;
            boolean interrupted = false;
            boolean uncertain = false;
            List<BigInteger> results = new ArrayList<>();
            try {
                for (int i = 0; i < parts.size(); i++) {
                    Peer peer = targets.get(i);
                    Job part = parts.get(i);
                    try { tasks.add(execution.submit(() -> peer.connect().compute(part))); }
                    catch (RuntimeException e) { failure = e; break; }
                }
                // The term ends on allocation, not on completion of a slow fifth job.
                rotateIfExhausted();
                for (Future<BigInteger> task : tasks) {
                    for (;;) {
                        try { results.add(task.get()); break; }
                        catch (InterruptedException e) { interrupted = true; failure = e; }
                        catch (ExecutionException e) {
                            failure = e.getCause();
                            // Conservatively treat remote errors as unknown completion.
                            uncertain |= failure instanceof RemoteException;
                            break;
                        }
                    }
                }
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
            if (uncertain) throw new RemoteException("Accepted job failed; remote completion unknown; do not replay", failure);
            if (failure != null) throw new JobFailedException("Accepted job failed; started calls drained", failure);
            result = Objects.requireNonNull(engine.aggregate(job.type(), List.copyOf(results)), "Aggregate result");
        } catch (RemoteException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new JobFailedException("Accepted job failed", e);
        } finally {
            rotateIfExhausted();
        }
        return result;
    }

    private void rotateIfExhausted() {
        if (status().assignedJobs() >= 5) {
            try { elect(); }
            catch (RemoteException e) { System.err.println("Rotation pending: " + e.getMessage()); }
        }
    }

    /** Executes one supplied chunk; splitting/algorithms belong to teammate 3's provider. */
    @Override public BigInteger compute(Job part) throws RemoteException {
        if (engine == null) throw new JobFailedException("Not implemented: teammate 3 JobEngine provider", null);
        Future<BigInteger> task;
        try { task = execution.submit(() -> engine.compute(part)); }
        catch (RuntimeException e) { throw new JobFailedException("Computation could not start", e); }
        InterruptedException interruption = null;
        try {
            for (;;) {
                try {
                    BigInteger result = task.get();
                    if (interruption != null) throw new JobFailedException("Interrupted; started computation has finished", interruption);
                    return result;
                } catch (InterruptedException e) {
                    interruption = e; // Drain the task before restoring the flag and reporting failure.
                } catch (ExecutionException e) {
                    throw new JobFailedException("Computation failed; task has finished", e.getCause());
                }
            }
        } finally {
            if (interruption != null) Thread.currentThread().interrupt();
        }
    }

    @Override public void close() throws java.rmi.NoSuchObjectException {
        execution.shutdown();
        UnicastRemoteObject.unexportObject(this, true);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException(
                "WorkerNode id host registryPort exportPort bootstrapHost bootstrapPort");
        System.setProperty("java.rmi.server.hostname", args[1]);
        BootstrapRemote bootstrap = (BootstrapRemote) LocateRegistry.getRegistry(args[4],
                Integer.parseInt(args[5])).lookup("bootstrap");
        Peer self = new Peer(Integer.parseInt(args[0]), args[1], Integer.parseInt(args[2]));
        JobEngine engine = ServiceLoader.load(JobEngine.class).findFirst().orElse(null);
        WorkerNode node = new WorkerNode(self, bootstrap, Integer.parseInt(args[3]), engine);
        LocateRegistry.createRegistry(self.registryPort()).rebind("worker", node);
        try { bootstrap.register(self); }
        catch (Exception e) { System.err.println("Startup failed: " + e); System.exit(1); }
        System.out.println("WORKER READY " + self.id() + "");
    }
}
