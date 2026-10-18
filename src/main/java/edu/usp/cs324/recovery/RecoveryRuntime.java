package edu.usp.cs324.recovery;

import edu.usp.cs324.api.*;
import edu.usp.cs324.network.BootstrapGrpcClient;
import edu.usp.cs324.network.CoordinationGrpcClient;
import edu.usp.cs324.network.WorkerNode;
import edu.usp.cs324.proto.WorkerEndpoint;
import edu.usp.cs324.proto.recovery.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Full-snapshot sequencer and recoverable pure task execution for a trusted,
 * fail-stop lab cluster with one available bootstrap. All listed replicas must
 * acknowledge before dispatch. This is intentionally not partition consensus.
 */
public final class RecoveryRuntime implements AutoCloseable {
    private final WorkerNode node;
    private final BootstrapGrpcClient directory;
    private final RecoveryStateStore store;
    private final JobEngine engine;
    private final Object mutations = new Object();
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService execution = Executors.newVirtualThreadPerTaskExecutor();
    private final Set<String> driving = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile String lastFailure;
    // Polls may reuse an already acknowledged snapshot for 100 ms. Mutations
    // always reconcile all replicas, and partial writes invalidate this cache.
    private RecoverySnapshot acknowledged;
    private List<WorkerEndpoint> acknowledgedMembers = List.of();
    private long acknowledgedAt;
    private final Map<WorkerEndpoint, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<WorkerEndpoint, CoordinationGrpcClient> computeClients = new ConcurrentHashMap<>();

    public RecoveryRuntime(WorkerNode node, BootstrapGrpcClient directory, RecoveryStateStore store, JobEngine engine) {
        this.node = Objects.requireNonNull(node);
        this.directory = Objects.requireNonNull(directory);
        this.store = Objects.requireNonNull(store);
        this.engine = Objects.requireNonNull(engine);
        store.latest().filter(RecoverySnapshot::hasCoordinatorEndpoint).ifPresent(node::installRecovery);
    }

    public void start() { monitor.scheduleWithFixedDelay(this::tick, 100, 300, TimeUnit.MILLISECONDS); }

    private List<WorkerEndpoint> members() throws Exception {
        var result = directory.active().stream().map(directory::endpoint)
                .sorted(Comparator.comparingInt(WorkerEndpoint::getWorkerId)).toList();
        if (!result.contains(directory.selfEndpoint()))
            throw new IllegalStateException("This worker does not own an active directory lease");
        return result;
    }

    private ManagedChannel channel(WorkerEndpoint worker) {
        synchronized (channels) {
            if (closed.get()) throw new IllegalStateException("Recovery runtime is closed");
            return channels.computeIfAbsent(worker, endpoint -> ManagedChannelBuilder
                    .forAddress(endpoint.getHost(), endpoint.getGrpcPort()).usePlaintext()
                    .maxInboundMessageSize(RecoveryStateServiceImpl.MAX_RPC_MESSAGE_BYTES).build());
        }
    }

    private CoordinationGrpcClient computeClient(WorkerEndpoint worker) {
        synchronized (channels) {
            if (closed.get()) throw new IllegalStateException("Recovery runtime is closed");
            return computeClients.computeIfAbsent(worker, endpoint ->
                    new CoordinationGrpcClient(endpoint, p -> null, java.time.Duration.ofMinutes(10)));
        }
    }

    private RecoverySnapshot remote(WorkerEndpoint gate, RecoveryMutation request) {
        ManagedChannel channel = channel(gate);
        return RecoveryControlServiceGrpc.newBlockingStub(channel).withDeadlineAfter(20, TimeUnit.SECONDS).mutate(request);
    }

    private RecoverySnapshot control(RecoveryMutation request) throws Exception {
        if (closed.get()) throw new IllegalStateException("Recovery runtime is closed");
        var gate = members().getFirst();
        return gate.equals(directory.selfEndpoint()) ? mutate(request) : remote(gate, request);
    }

    public BigInteger submit(String id, Job input) throws Exception {
        return submitWithProgress(id, input, update -> { });
    }

    public BigInteger submitWithProgress(String id, Job input,
            java.util.function.Consumer<edu.usp.cs324.proto.jobs.JobUpdate> progress) throws Exception {
        if (id == null || id.isBlank() || id.length() > 128) throw new IllegalArgumentException("Job ID must contain 1..128 characters");
        RecoverySnapshot snapshot = control(RecoveryMutation.newBuilder().setAction(RecoveryMutation.Action.REGISTER)
                .setJobId(id).setInput(encode(input)).build());
        edu.usp.cs324.proto.jobs.JobUpdate previous = null;
        for (;;) {
            RecoveryJob job = job(snapshot, id);
            var update = progress(job);
            if (!update.equals(previous)) { progress.accept(update); previous = update; }
            if (job.getState() == RecoveryJob.State.COMPLETED) return new BigInteger(job.getResult());
            if (Context.current().isCancelled()) throw new CancellationException("Stream disconnected; job remains recoverable");
            Thread.sleep(150);
            snapshot = control(RecoveryMutation.newBuilder().setAction(RecoveryMutation.Action.READ).setJobId(id).build());
        }
    }

    /** Completed work is counted only from persisted, attempt-fenced task results. */
    public static edu.usp.cs324.proto.jobs.JobUpdate progress(RecoveryJob job) {
        var update = edu.usp.cs324.proto.jobs.JobUpdate.newBuilder().setRequestId(job.getJobId())
                .setState(edu.usp.cs324.proto.jobs.JobUpdate.State.RUNNING);
        long total = 0, completed = 0;
        for (var task : job.getTasksList()) {
            var input = task.getInput();
            long units = input.getOperation() == RecoveryJobInput.Operation.PRIMESUM
                    ? Math.max(0L, (long) input.getRangeEnd() - input.getRangeStart() + 1)
                    : input.getNumbersCount();
            total += units;
            if (task.getState() == RecoveryTask.State.COMPLETED) completed += units;
            update.addTasks(edu.usp.cs324.proto.jobs.TaskProgress.newBuilder()
                    .setTaskId(task.getTaskId()).setWorkerId(task.getAssignedWorkerId())
                    .setWorkerIncarnationId(task.getAssignedWorkerIncarnationId())
                    .setState(task.getState().name()).setAttempt(task.getAttempt()).setUnits(units));
        }
        return update.setTotalUnits(total).setCompletedUnits(completed).build();
    }

    public BindableService service() {
        return new RecoveryControlServiceGrpc.RecoveryControlServiceImplBase() {
            @Override public void mutate(RecoveryMutation request, StreamObserver<RecoverySnapshot> observer) {
                try { observer.onNext(RecoveryRuntime.this.mutate(request)); observer.onCompleted(); }
                catch (Exception e) { observer.onError(error(e)); }
            }
            @Override public void installCounters(RecoverySnapshot snapshot, StreamObserver<RecoveryControlAck> observer) {
                try {
                    if (!store.latest().filter(snapshot::equals).isPresent())
                        throw new IllegalStateException("Counters must match this replica's persisted snapshot");
                    if (!snapshot.hasCoordinatorEndpoint()) throw new IllegalArgumentException("Coordinator endpoint required");
                    node.installRecovery(snapshot);
                    observer.onNext(RecoveryControlAck.getDefaultInstance()); observer.onCompleted();
                } catch (Exception e) { observer.onError(error(e)); }
            }
        };
    }

    private StatusRuntimeException error(Exception e) {
        return (e instanceof IllegalArgumentException ? io.grpc.Status.INVALID_ARGUMENT : io.grpc.Status.UNAVAILABLE)
                .withDescription(e.getMessage()).withCause(e).asRuntimeException();
    }

    /** Read every current replica, including after a partially acknowledged write. */
    private RecoverySnapshot synchronize(List<WorkerEndpoint> members) throws Exception {
        RecoverySnapshot newest = null;
        for (var worker : members) {
            RecoverySnapshot candidate;
            if (worker.equals(directory.selfEndpoint())) candidate = store.latest().orElse(null);
            else {
                ManagedChannel channel = channel(worker);
                var reply = RecoveryStateServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS)
                        .getSnapshot(GetRecoverySnapshotRequest.getDefaultInstance());
                candidate = reply.hasSnapshot() ? reply.getSnapshot() : null;
            }
            if (candidate == null) continue;
            if (!candidate.hasCoordinatorEndpoint())
                throw new IllegalStateException("Legacy/manual checkpoint lacks coordinator endpoint; use a separate checkpoint path");
            if (newest != null && candidate.getRevision() == newest.getRevision() && !candidate.equals(newest))
                throw new IllegalStateException("Conflicting checkpoint revisions; recovery stopped");
            if (newest == null || candidate.getRevision() > newest.getRevision()) newest = candidate;
        }
        if (newest != null) replicate(newest, members);
        return newest;
    }

    private void replicate(RecoverySnapshot snapshot, List<WorkerEndpoint> members) throws Exception {
        acknowledged = null;
        if (!members.equals(members())) throw new IllegalStateException("Membership changed before checkpoint write");
        // A partial failure is not rolled back. The next mutation reads all surviving
        // copies and completes replication before it permits any new dispatch.
        for (var worker : members) {
            if (worker.equals(directory.selfEndpoint())) store.save(snapshot);
            else {
                ManagedChannel channel = channel(worker);
                var ack = RecoveryStateServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS)
                        .saveSnapshot(SaveRecoverySnapshotRequest.newBuilder().setSnapshot(snapshot).build());
                if (ack.getRevision() != snapshot.getRevision()) throw new IllegalStateException("Wrong checkpoint acknowledgement");
            }
        }
        if (!members.equals(members())) throw new IllegalStateException("Membership changed during checkpoint write");
        for (var worker : members) {
            if (worker.equals(directory.selfEndpoint())) node.installRecovery(snapshot);
            else {
                ManagedChannel channel = channel(worker);
                RecoveryControlServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS).installCounters(snapshot);
            }
        }
        acknowledged = snapshot;
        acknowledgedMembers = List.copyOf(members);
        acknowledgedAt = System.nanoTime();
    }

    private Term healthyTerm(RecoverySnapshot snapshot, List<WorkerEndpoint> members) {
        if (snapshot == null || members.stream().noneMatch(w -> w.equals(snapshot.getCoordinatorEndpoint()))) return null;
        if (snapshot.getWorkerCountersList().stream().anyMatch(c ->
                c.getWorkerId() == snapshot.getCoordinatorId() && c.getAssignedJobs() >= 5)) return null;
        var endpoint = snapshot.getCoordinatorEndpoint();
        Term restored = new Term(snapshot.getLeadershipTerm(), UUID.fromString(snapshot.getElectionId()),
                new Peer(endpoint.getWorkerId(), endpoint.getHost(), endpoint.getRegistryPort()));
        return restored.equals(node.status().term()) ? restored : null;
    }

    private RecoverySnapshot mutate(RecoveryMutation request) throws Exception {
        synchronized (mutations) {
            var members = members();
            if (!members.getFirst().equals(directory.selfEndpoint()))
                throw new IllegalStateException("Checkpoint authority changed; rediscover the lowest active worker");
            var local = store.latest().orElse(null);
            if (request.getAction() == RecoveryMutation.Action.READ && local != null && local.equals(acknowledged)
                    && acknowledgedMembers.equals(members)
                    && System.nanoTime() - acknowledgedAt < TimeUnit.MILLISECONDS.toNanos(100)
                    && healthyTerm(local, members) != null
                    && (request.getJobId().isEmpty() || local.getJobsList().stream()
                    .anyMatch(j -> j.getJobId().equals(request.getJobId())))) return local;
            RecoverySnapshot before = synchronize(members);
            if (before == null && request.getAction() == RecoveryMutation.Action.READ) {
                if (!request.getJobId().isEmpty()) throw new IllegalArgumentException("Unknown job ID");
                return RecoverySnapshot.getDefaultInstance();
            }
            boolean changedIncarnation = before != null && members.stream().noneMatch(w ->
                    w.getWorkerId() == before.getCoordinatorId()
                    && w.getIncarnationId().equals(before.getCoordinatorIncarnationId()));
            // A healthy unexhausted term needs no repeated election/status flood.
            Term term = healthyTerm(before, members);
            if (term == null) term = node.electForRecovery(changedIncarnation);
            int leaderId = term.leader().id();
            WorkerEndpoint leader = members.stream().filter(w -> w.getWorkerId() == leaderId).findFirst().orElseThrow();
            var state = before == null ? RecoverySnapshot.newBuilder() : before.toBuilder();
            boolean newTerm = before == null || before.getLeadershipTerm() != term.number()
                    || !before.getElectionId().equals(term.electionId().toString());
            state.setLeadershipTerm(term.number()).setElectionId(term.electionId().toString())
                    .setCoordinatorId(leader.getWorkerId()).setCoordinatorIncarnationId(leader.getIncarnationId())
                    .setCoordinatorEndpoint(leader);
            Map<Integer, RecoveryWorkerCounter> counters = new TreeMap<>();
            if (before != null) for (var counter : before.getWorkerCountersList())
                counters.put(counter.getWorkerId(), newTerm ? counter.toBuilder().setAssignedJobs(0).build() : counter);
            for (var worker : members) counters.putIfAbsent(worker.getWorkerId(), RecoveryWorkerCounter.newBuilder()
                    .setWorkerId(worker.getWorkerId()).build());
            state.clearWorkerCounters().addAllWorkerCounters(counters.values());
            switch (request.getAction()) {
                case READ -> { if (!request.getJobId().isEmpty()) job(state.build(), request.getJobId()); }
                case REGISTER -> register(state, request, members, leader);
                case CLAIM, COMPLETE, RELEASE -> updateTask(state, request, members);
                default -> throw new IllegalArgumentException("Recovery action required");
            }
            RecoverySnapshot candidate = state.build();
            if (before == null || !candidate.equals(before)) {
                candidate = state.setRevision(Math.incrementExact(before == null ? 0 : before.getRevision())).build();
                replicate(candidate, members);
            }
            return candidate;
        }
    }

    private void register(RecoverySnapshot.Builder state, RecoveryMutation request,
                          List<WorkerEndpoint> members, WorkerEndpoint leader) {
        String id = request.getJobId();
        if (id.isBlank() || id.length() > 128 || !request.hasInput()) throw new IllegalArgumentException("Job ID and input required");
        Job input = decode(request.getInput());
        for (var existing : state.getJobsList()) if (existing.getJobId().equals(id)) {
            if (!existing.getInput().equals(request.getInput())) throw new IllegalArgumentException("Job ID already belongs to different input");
            return;
        }
        var parts = List.copyOf(engine.split(input, members.size()));
        if (parts.isEmpty() || parts.size() > members.size()) throw new IllegalArgumentException("Invalid split");
        var job = RecoveryJob.newBuilder().setJobId(id).setInput(request.getInput()).setState(RecoveryJob.State.RUNNING);
        for (int i = 0; i < parts.size(); i++) job.addTasks(RecoveryTask.newBuilder()
                .setTaskId(id + "/" + i).setInput(encode(parts.get(i))).setState(RecoveryTask.State.PENDING));
        for (int i = 0; i < state.getWorkerCountersCount(); i++) {
            var counter = state.getWorkerCounters(i);
            if (counter.getWorkerId() != leader.getWorkerId()) continue;
            if (counter.getAssignedJobs() >= 5) throw new IllegalStateException("Five-job rotation required");
            long allocations = members.subList(0, parts.size()).stream().filter(w -> w.getWorkerId() != leader.getWorkerId()).count();
            state.setWorkerCounters(i, counter.toBuilder().setJac(Math.addExact(counter.getJac(), allocations))
                    .setAssignedJobs(counter.getAssignedJobs() + 1));
        }
        state.addJobs(job);
    }

    private void updateTask(RecoverySnapshot.Builder state, RecoveryMutation request, List<WorkerEndpoint> members) {
        // Only the current leader may make new claims. A healthy former leader
        // may finish its unchanged attempt across a normal five-job rotation.
        if (request.getAction() == RecoveryMutation.Action.CLAIM
                && (request.getLeadershipTerm() != state.getLeadershipTerm()
                || !request.getCoordinatorIncarnationId().equals(state.getCoordinatorIncarnationId()))) return;
        for (int j = 0; j < state.getJobsCount(); j++) {
            var job = state.getJobs(j).toBuilder();
            if (!job.getJobId().equals(request.getJobId())) continue;
            if (job.getState() == RecoveryJob.State.COMPLETED) return;
            for (int t = 0; t < job.getTasksCount(); t++) {
                var task = job.getTasks(t).toBuilder();
                if (!task.getTaskId().equals(request.getTaskId())) continue;
                if (task.getState() == RecoveryTask.State.COMPLETED || task.getAttempt() != request.getExpectedAttempt()) return;
                if (request.getAction() == RecoveryMutation.Action.CLAIM) {
                    if (!members.contains(request.getWorker())) throw new IllegalStateException("Assignee incarnation is no longer active");
                    if (task.getState() == RecoveryTask.State.RUNNING && members.stream().anyMatch(w ->
                            w.getWorkerId() == task.getCoordinatorId()
                            && w.getIncarnationId().equals(task.getCoordinatorIncarnationId()))) return;
                    if (task.getAttempt() == Integer.MAX_VALUE) throw new IllegalStateException("Task attempt capacity reached");
                    if (task.getAttempt() > 0 && request.getWorker().getWorkerId() != state.getCoordinatorId()) {
                        for (int c = 0; c < state.getWorkerCountersCount(); c++) {
                            var counter = state.getWorkerCounters(c);
                            if (counter.getWorkerId() == state.getCoordinatorId())
                                state.setWorkerCounters(c, counter.toBuilder().setJac(Math.incrementExact(counter.getJac())));
                        }
                    }
                    task.setAttempt(task.getAttempt() + 1).setState(RecoveryTask.State.RUNNING)
                            .setAssignedWorkerId(request.getWorker().getWorkerId())
                            .setAssignedWorkerIncarnationId(request.getWorker().getIncarnationId())
                            .setLeadershipTerm(state.getLeadershipTerm()).setCoordinatorId(state.getCoordinatorId())
                            .setCoordinatorIncarnationId(state.getCoordinatorIncarnationId()).clearResult();
                } else {
                    if (task.getState() != RecoveryTask.State.RUNNING || task.getLeadershipTerm() != request.getLeadershipTerm()
                            || !task.getCoordinatorIncarnationId().equals(request.getCoordinatorIncarnationId())
                            || task.getAssignedWorkerId() != request.getWorker().getWorkerId()
                            || !task.getAssignedWorkerIncarnationId().equals(request.getWorker().getIncarnationId())) return;
                    if (request.getAction() == RecoveryMutation.Action.RELEASE) {
                        task.setState(RecoveryTask.State.PENDING).clearAssignedWorkerId().clearAssignedWorkerIncarnationId()
                                .clearLeadershipTerm().clearCoordinatorId().clearCoordinatorIncarnationId().clearResult();
                    } else {
                        String result = new BigInteger(request.getResult()).toString();
                        task.setState(RecoveryTask.State.COMPLETED).setResult(result);
                    }
                }
                job.setTasks(t, task);
                if (job.getTasksList().stream().allMatch(p -> p.getState() == RecoveryTask.State.COMPLETED)) {
                    BigInteger result = engine.aggregate(decode(job.getInput()).type(), job.getTasksList().stream()
                            .map(p -> new BigInteger(p.getResult())).toList());
                    job.setState(RecoveryJob.State.COMPLETED).setResult(result.toString());
                }
                state.setJobs(j, job); return;
            }
            throw new IllegalArgumentException("Unknown task ID");
        }
        throw new IllegalArgumentException("Unknown job ID");
    }

    private void tick() {
        if (closed.get()) return;
        try {
            var snapshot = control(RecoveryMutation.newBuilder().setAction(RecoveryMutation.Action.READ).build());
            lastFailure = null;
            if (!snapshot.getCoordinatorEndpoint().equals(directory.selfEndpoint())) return;
            for (var job : snapshot.getJobsList()) if (job.getState() != RecoveryJob.State.COMPLETED && driving.add(job.getJobId())) {
                try { execution.submit(Context.ROOT.wrap(() -> drive(job.getJobId()))); }
                catch (RejectedExecutionException e) { driving.remove(job.getJobId()); }
            }
        } catch (Exception e) {
            String message = e.getMessage();
            if (!Objects.equals(lastFailure, message) && !closed.get()) System.err.println("Recovery waiting: " + message);
            lastFailure = message;
        }
    }

    private RecoveryMutation.Builder action(RecoveryMutation.Action action, RecoverySnapshot snapshot, RecoveryJob job, RecoveryTask task) {
        return RecoveryMutation.newBuilder().setAction(action).setJobId(job.getJobId()).setTaskId(task.getTaskId())
                .setExpectedAttempt(task.getAttempt()).setLeadershipTerm(snapshot.getLeadershipTerm())
                .setCoordinatorIncarnationId(snapshot.getCoordinatorIncarnationId());
    }

    private void drive(String id) {
        try {
            var snapshot = control(RecoveryMutation.newBuilder().setAction(RecoveryMutation.Action.READ).setJobId(id).build());
            if (!snapshot.getCoordinatorEndpoint().equals(directory.selfEndpoint())) return;
            var job = job(snapshot, id);
            var workers = members();
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < job.getTasksCount(); i++) {
                var task = job.getTasks(i);
                if (task.getState() == RecoveryTask.State.COMPLETED) continue;
                if (task.getState() == RecoveryTask.State.RUNNING && workers.stream().anyMatch(w ->
                        w.getWorkerId() == task.getCoordinatorId()
                        && w.getIncarnationId().equals(task.getCoordinatorIncarnationId()))) continue;
                var worker = workers.get(Math.floorMod((long) i + task.getAttempt(), workers.size()));
                tasks.add(execution.submit(Context.ROOT.wrap(() -> {
                    try {
                        var claimed = control(action(RecoveryMutation.Action.CLAIM, snapshot, job, task).setWorker(worker).build());
                        var current = job(claimed, id).getTasksList().stream().filter(p -> p.getTaskId().equals(task.getTaskId())).findFirst().orElseThrow();
                        if (claimed.getLeadershipTerm() != snapshot.getLeadershipTerm() || current.getAttempt() != task.getAttempt() + 1
                                || current.getState() != RecoveryTask.State.RUNNING) return;
                        BigInteger result;
                        var client = computeClient(worker);
                        result = client.compute(decode(current.getInput()));
                        reliable(action(RecoveryMutation.Action.COMPLETE, snapshot, job, current).setWorker(worker).setResult(result.toString()).build());
                    } catch (Exception e) {
                        // A claim acknowledgement may have been lost. Release that exact
                        // potential attempt, never a newer owner's work.
                        var possible = task.toBuilder().setAttempt(task.getAttempt() + 1).build();
                        reliable(action(RecoveryMutation.Action.RELEASE, snapshot, job, possible).setWorker(worker).build());
                    }
                })));
            }
            for (var task : tasks) task.get();
        } catch (Exception e) { if (e instanceof InterruptedException) Thread.currentThread().interrupt(); }
        finally { driving.remove(id); }
    }

    private void reliable(RecoveryMutation request) {
        while (!closed.get()) {
            try { control(request); return; }
            catch (Exception e) {
                try { Thread.sleep(200); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
            }
        }
    }

    private static RecoveryJob job(RecoverySnapshot snapshot, String id) {
        return snapshot.getJobsList().stream().filter(j -> j.getJobId().equals(id)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown job ID"));
    }
    public static RecoveryJobInput encode(Job input) {
        var b = RecoveryJobInput.newBuilder().setOperation(RecoveryJobInput.Operation.valueOf(input.type().name()));
        if (input.type() == Job.Type.PRIMESUM) b.setRangeStart(input.start()).setRangeEnd(input.end());
        else b.addAllNumbers(input.numbers());
        return b.build();
    }
    public static Job decode(RecoveryJobInput input) {
        return new Job(Job.Type.valueOf(input.getOperation().name()), input.getNumbersList(), input.getRangeStart(), input.getRangeEnd());
    }
    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        monitor.shutdownNow(); execution.shutdownNow();
        synchronized (channels) {
            computeClients.values().forEach(CoordinationGrpcClient::close);
            channels.values().forEach(ManagedChannel::shutdownNow);
            computeClients.clear(); channels.clear();
        }
    }
}
