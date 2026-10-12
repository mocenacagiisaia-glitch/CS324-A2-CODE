package edu.usp.cs324.network;

import edu.usp.cs324.api.BootstrapRemote;
import java.rmi.registry.LocateRegistry;

public final class ClusterStatus {
    private ClusterStatus() { }
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("ClusterStatus bootstrapHost bootstrapPort");
        BootstrapRemote bootstrap = (BootstrapRemote) LocateRegistry.getRegistry(args[0],
                Integer.parseInt(args[1])).lookup("bootstrap");
        for (var peer : bootstrap.active()) System.out.println(peer.connect().status());
    }
}
