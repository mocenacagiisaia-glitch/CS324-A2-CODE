package edu.usp.cs324.api;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public record Peer(int id, String host, int registryPort) implements Serializable {
    public WorkerRemote connect() throws RemoteException {
        try {
            return (WorkerRemote) LocateRegistry.getRegistry(host, registryPort).lookup("worker");
        } catch (java.rmi.NotBoundException e) {
            throw new RemoteException("Worker is not bound: " + id, e);
        }
    }
}
