package edu.usp.cs324.api;

import java.math.BigInteger;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.UUID;

public interface WorkerRemote extends Remote {
    Status status() throws RemoteException;
    void addNeighbour(Peer peer) throws RemoteException;
    List<Candidate> election(UUID messageId) throws RemoteException;
    void coordinator(Term term) throws RemoteException;
    Term elect() throws RemoteException;
    BigInteger submit(Job job) throws RemoteException;
    BigInteger assign(Job job, Term expected, List<Peer> workers) throws RemoteException;
    BigInteger compute(Job part) throws RemoteException;
}
