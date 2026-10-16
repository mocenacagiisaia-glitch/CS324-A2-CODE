package edu.usp.cs324.recovery;

import edu.usp.cs324.proto.recovery.RecoveryJob;
import edu.usp.cs324.proto.recovery.RecoveryJobInput;
import edu.usp.cs324.proto.recovery.RecoverySnapshot;
import edu.usp.cs324.proto.recovery.RecoveryTask;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Checks checkpoint structure; election authority and quorum decisions belong to coordination. */
final class RecoverySnapshotValidator {
    private RecoverySnapshotValidator() { }

    static void validate(RecoverySnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        require(snapshot.getRevision() > 0, "Revision must be positive");
        require(snapshot.getLeadershipTerm() > 0, "Leadership term must be positive");
        require(!snapshot.getElectionId().isBlank(), "Election ID is required");
        require(snapshot.getCoordinatorId() > 0, "Coordinator ID must be positive");
        require(!snapshot.getCoordinatorIncarnationId().isBlank(), "Coordinator incarnation ID is required");
        Set<Integer> workers = new HashSet<>();
        snapshot.getWorkerCountersList().forEach(counter -> {
            require(counter.getWorkerId() > 0 && workers.add(counter.getWorkerId()), "Worker counters need unique positive IDs");
            require(counter.getJac() >= 0, "JAC must be nonnegative");
            require(counter.getAssignedJobs() >= 0 && counter.getAssignedJobs() <= 5, "Assigned-job count must be between 0 and 5");
        });
        require(workers.contains(snapshot.getCoordinatorId()), "Coordinator counters must be present");

        Set<String> jobIds = new HashSet<>();
        Set<String> taskIds = new HashSet<>();
        for (RecoveryJob job : snapshot.getJobsList()) {
            require(!job.getJobId().isBlank() && jobIds.add(job.getJobId()), "Job IDs must be nonblank and unique");
            require(job.hasInput(), "Job input is required");
            validateInput(job.getInput());
            require(job.getState() != RecoveryJob.State.JOB_STATE_UNSPECIFIED
                    && job.getState() != RecoveryJob.State.UNRECOGNIZED, "Job state is required");
            for (RecoveryTask task : job.getTasksList()) {
                require(!task.getTaskId().isBlank() && taskIds.add(task.getTaskId()), "Task IDs must be globally unique and nonblank");
                require(task.hasInput(), "Task input is required");
                validateInput(task.getInput());
                require(task.getInput().getOperation() == job.getInput().getOperation(), "Task and job operations must match");
                require(task.getState() != RecoveryTask.State.TASK_STATE_UNSPECIFIED
                        && task.getState() != RecoveryTask.State.UNRECOGNIZED, "Task state is required");
                if (task.getState() == RecoveryTask.State.PENDING) {
                    require(task.getAssignedWorkerId() == 0 && task.getAssignedWorkerIncarnationId().isEmpty(),
                            "Pending tasks must not have a current assignee");
                } else {
                    require(task.getAssignedWorkerId() > 0 && !task.getAssignedWorkerIncarnationId().isBlank()
                            && Integer.toUnsignedLong(task.getAttempt()) > 0,
                            "Running/completed tasks require worker identity and a positive attempt");
                }
                require(task.hasResult() == (task.getState() == RecoveryTask.State.COMPLETED),
                        "Only completed tasks must have a result");
                if (task.hasResult()) validateResult(task.getResult());
                if (job.getState() == RecoveryJob.State.PENDING)
                    require(task.getState() == RecoveryTask.State.PENDING, "Pending jobs cannot contain started tasks");
            }
            if (job.getState() != RecoveryJob.State.PENDING)
                require(job.getTasksCount() > 0, "Started jobs require their task definitions");
            require(job.hasResult() == (job.getState() == RecoveryJob.State.COMPLETED),
                    "Only completed jobs must have a final result");
            if (job.getState() == RecoveryJob.State.COMPLETED) {
                require(job.getTasksList().stream().allMatch(task -> task.getState() == RecoveryTask.State.COMPLETED),
                        "Completed jobs cannot contain unfinished tasks");
                validateResult(job.getResult());
            }
        }
    }

    private static void validateInput(RecoveryJobInput input) {
        switch (input.getOperation()) {
            case MAX -> require(input.getNumbersCount() > 0, "MAX requires at least one number");
            case PRIMECOUNT -> { /* An empty input is a valid zero-count job. */ }
            case PRIMESUM -> require(input.getNumbersCount() == 0 && input.getRangeStart() <= input.getRangeEnd(),
                    "PRIMESUM requires an inclusive ordered range and no number list");
            default -> throw new IllegalArgumentException("Job operation is required");
        }
        if (input.getOperation() != RecoveryJobInput.Operation.PRIMESUM)
            require(input.getRangeStart() == 0 && input.getRangeEnd() == 0, "List jobs must not carry a range");
    }

    private static void validateResult(String value) {
        try {
            require(new BigInteger(value).toString().equals(value), "Results must use canonical decimal integers");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Results must be decimal integers", e);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
    }
}
