package edu.usp.cs324.client;

import edu.usp.cs324.api.*;
import java.math.BigInteger;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.util.List;

/** Safe bounded retries only for explicit pre-admission rejections. */
public final class ClusterClient {
    private final BootstrapRemote bootstrap;
    public ClusterClient(BootstrapRemote bootstrap) { this.bootstrap = bootstrap; }
    public static ClusterClient connect(String host, int port) throws RemoteException, NotBoundException {
        // RMI treats null/empty hosts as localhost; require an explicit destination.
        if (host == null || host.isBlank()) throw new IllegalArgumentException("Bootstrap host is required");
        return new ClusterClient((BootstrapRemote) LocateRegistry.getRegistry(host, port).lookup("bootstrap"));
    }
    public BigInteger submit(Job job) throws RemoteException, InterruptedException {
        for (int attempt = 0; attempt < 30; attempt++) {
            List<Peer> workers = bootstrap.active();
            if (workers.isEmpty()) throw new RemoteException("No active workers");
            try { return workers.getFirst().connect().submit(job); }
            catch (RemoteException e) {
                if (!preAdmission(e)) throw e;
                if (attempt == 29) throw e;
                Thread.sleep(Math.min(200, 10L * (attempt + 1)));
            }
        }
        throw new AssertionError("Unreachable");
    }
    static boolean preAdmission(RemoteException error) {
        // RMI adds ServerException envelopes. Never search arbitrary nested causes:
        // an accepted computation may itself have failed with RetryException.
        Throwable cause = error;
        while (cause instanceof ServerException && cause.getCause() != null) cause = cause.getCause();
        return cause instanceof RetryException;
    }
}
