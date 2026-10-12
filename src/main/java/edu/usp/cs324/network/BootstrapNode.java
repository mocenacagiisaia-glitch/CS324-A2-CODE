package edu.usp.cs324.network;

import edu.usp.cs324.api.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/** Membership only: no election decisions and no computational work. */
public final class BootstrapNode extends UnicastRemoteObject implements BootstrapRemote {
    private final Map<Integer, Peer> members = new TreeMap<>();
    public BootstrapNode(int exportPort) throws RemoteException { super(exportPort); }

    @Override public synchronized List<Peer> active() {
        members.values().removeIf(peer -> {
            try { peer.connect().status(); return false; }
            catch (RemoteException e) { System.err.println("Inactive worker " + peer.id()); return true; }
        });
        return List.copyOf(members.values());
    }

    @Override public synchronized void register(Peer peer) throws RemoteException {
        List<Peer> existing = active();
        if (members.containsKey(peer.id())) throw new RemoteException("Duplicate worker ID " + peer.id());
        peer.connect().status();
        if (!existing.isEmpty()) {
            Peer neighbour = existing.get(ThreadLocalRandom.current().nextInt(existing.size()));
            peer.connect().addNeighbour(neighbour);
            neighbour.connect().addNeighbour(peer);
        }
        members.put(peer.id(), peer);
        System.out.println("REGISTER " + peer);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("BootstrapNode host registryPort exportPort");
        System.setProperty("java.rmi.server.hostname", args[0]);
        LocateRegistry.createRegistry(Integer.parseInt(args[1])).rebind("bootstrap",
                new BootstrapNode(Integer.parseInt(args[2])));
        System.out.println("BOOTSTRAP READY");
    }
}
