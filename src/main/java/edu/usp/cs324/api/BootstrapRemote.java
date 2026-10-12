package edu.usp.cs324.api;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface BootstrapRemote extends Remote {
    /** Registers a unique ID and establishes a reciprocal random neighbour link. */
    void register(Peer peer) throws RemoteException;
    List<Peer> active() throws RemoteException;
}
