package edu.usp.cs324.recovery;

import edu.usp.cs324.proto.recovery.RecoverySnapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Optional;

/**
 * One worker's local Protobuf checkpoint. This class does not elect a leader,
 * replicate data, choose a recovery winner, or certify a majority acknowledgement.
 * Its caller must establish authority before using a checkpoint to resume jobs.
 */
public final class RecoveryStateStore implements AutoCloseable {
    private static final int MAGIC = 0x44524350; // DRCP
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_BYTES = 4 + 4 + 4 + 32;
    private static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;

    private final Path file;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final CheckpointMover mover;
    private RecoverySnapshot latest;
    private boolean closed;

    public RecoveryStateStore(Path file) throws IOException {
        this(file, (source, destination) -> Files.move(source, destination,
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING));
    }

    /** Test seam for a real write followed by an injected final-replace failure. */
    RecoveryStateStore(Path file, CheckpointMover mover) throws IOException {
        this.file = Objects.requireNonNull(file, "file").toAbsolutePath().normalize();
        this.mover = Objects.requireNonNull(mover, "mover");
        if (this.file.getFileName() == null) throw new IllegalArgumentException("A checkpoint filename is required");
        Files.createDirectories(this.file.getParent());
        Path lockFile = this.file.resolveSibling(this.file.getFileName() + ".lock");
        lockChannel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock acquired = null;
        try {
            try { acquired = lockChannel.tryLock(); }
            catch (OverlappingFileLockException e) { throw new IOException("Checkpoint is already open: " + this.file, e); }
            if (acquired == null) throw new IOException("Checkpoint is already open: " + this.file);
            latest = Files.exists(this.file) ? readCheckpoint() : null;
        } catch (IOException | RuntimeException e) {
            if (acquired != null) {
                try { acquired.release(); } catch (IOException cleanup) { e.addSuppressed(cleanup); }
            }
            try { lockChannel.close(); } catch (IOException cleanup) { e.addSuppressed(cleanup); }
            throw e;
        }
        lock = acquired;
    }

    /** Protobuf messages are immutable, so callers cannot mutate the stored snapshot. */
    public synchronized Optional<RecoverySnapshot> latest() {
        ensureOpen();
        return Optional.ofNullable(latest);
    }

    /**
     * Saves a full snapshot. Returns false for an identical replay, true after a new write.
     * Full-snapshot revisions may skip forward for a joining worker's catch-up.
     * I/O failure leaves the previous file and in-memory snapshot in place.
     */
    public synchronized boolean save(RecoverySnapshot snapshot) throws IOException {
        ensureOpen();
        RecoverySnapshotValidator.validate(snapshot);
        if (latest != null) {
            if (latest.equals(snapshot)) return false;
            if (snapshot.getRevision() <= latest.getRevision())
                throw new IllegalArgumentException("Checkpoint revision is stale or conflicting");
            if (snapshot.getLeadershipTerm() < latest.getLeadershipTerm())
                throw new IllegalArgumentException("Leadership term cannot decrease");
            if (snapshot.getLeadershipTerm() == latest.getLeadershipTerm()
                    && (snapshot.getCoordinatorId() != latest.getCoordinatorId()
                    || !snapshot.getElectionId().equals(latest.getElectionId())
                    || !snapshot.getCoordinatorIncarnationId().equals(latest.getCoordinatorIncarnationId())))
                throw new IllegalArgumentException("Coordinator identity cannot change within the same term");
        }
        if (snapshot.getSerializedSize() > MAX_PAYLOAD_BYTES)
            throw new IllegalArgumentException("Checkpoint exceeds the 64 MiB payload limit");
        byte[] payload = snapshot.toByteArray();
        ByteBuffer contents = ByteBuffer.allocate(HEADER_BYTES + payload.length)
                .putInt(MAGIC).putInt(FORMAT_VERSION).putInt(payload.length).put(digest(payload)).put(payload);
        contents.flip();
        Path temporary = Files.createTempFile(file.getParent(), file.getFileName() + ".", ".tmp");
        try {
            try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                while (contents.hasRemaining()) output.write(contents);
                output.force(true);
            }
            // Require an atomic filesystem replacement; never silently use a partial-write fallback.
            mover.replace(temporary, file);
            latest = snapshot;
            return true;
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private RecoverySnapshot readCheckpoint() throws IOException {
        long size = Files.size(file);
        if (size < HEADER_BYTES || size > (long) HEADER_BYTES + MAX_PAYLOAD_BYTES)
            throw new IOException("Invalid checkpoint length: " + file);
        byte[] bytes = Files.readAllBytes(file);
        ByteBuffer input = ByteBuffer.wrap(bytes);
        if (input.getInt() != MAGIC || input.getInt() != FORMAT_VERSION)
            throw new IOException("Unknown checkpoint format: " + file);
        int payloadLength = input.getInt();
        if (payloadLength < 0 || payloadLength > MAX_PAYLOAD_BYTES || input.remaining() != 32 + payloadLength)
            throw new IOException("Checkpoint is truncated or has an invalid payload length: " + file);
        byte[] expectedDigest = new byte[32];
        input.get(expectedDigest);
        byte[] payload = new byte[payloadLength];
        input.get(payload);
        if (!MessageDigest.isEqual(expectedDigest, digest(payload)))
            throw new IOException("Checkpoint checksum does not match: " + file);
        RecoverySnapshot snapshot = RecoverySnapshot.parseFrom(payload);
        try { RecoverySnapshotValidator.validate(snapshot); }
        catch (IllegalArgumentException e) { throw new IOException("Checkpoint state is invalid: " + file, e); }
        return snapshot;
    }

    private static byte[] digest(byte[] payload) {
        try { return MessageDigest.getInstance("SHA-256").digest(payload); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException("SHA-256 is unavailable", e); }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("Recovery state store is closed");
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        try { lock.release(); }
        finally { lockChannel.close(); }
    }

    @FunctionalInterface
    interface CheckpointMover {
        void replace(Path source, Path destination) throws IOException;
    }
}
