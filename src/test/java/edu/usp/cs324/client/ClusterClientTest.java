package edu.usp.cs324.client;

import edu.usp.cs324.api.*;
import edu.usp.cs324.network.BootstrapNode;
import java.net.ServerSocket;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ClusterClientTest {
    @Test void missingHostCannotSilentlyConnectToLocalBootstrap() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
        var registry = LocateRegistry.createRegistry(port);
        try {
            var bootstrap = new BootstrapNode(0);
            try {
                registry.rebind("bootstrap", bootstrap);
                assertNotNull(ClusterClient.connect("127.0.0.1", port));
                for (String host : new String[]{"", null, " ", "\t\n"}) {
                    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                            () -> ClusterClient.connect(host, port));
                    assertEquals("Bootstrap host is required", error.getMessage());
                }
            } finally { UnicastRemoteObject.unexportObject(bootstrap, true); }
        } finally { UnicastRemoteObject.unexportObject(registry, true); }
    }

    @Test void onlyPreAdmissionEnvelopesAreRetryable() {
        RetryException retry = new RetryException("stale term");
        assertTrue(ClusterClient.preAdmission(retry));
        assertTrue(ClusterClient.preAdmission(new ServerException("RMI", new ServerException("RMI", retry))));
        assertFalse(ClusterClient.preAdmission(new ServerException("RMI", new JobFailedException("accepted", retry))));
        assertFalse(ClusterClient.preAdmission(new RemoteException("completion unknown", retry)));
        assertFalse(ClusterClient.preAdmission(new java.rmi.ConnectException("offline")));
    }
}
