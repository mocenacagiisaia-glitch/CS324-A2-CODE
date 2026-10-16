package edu.usp.cs324.network;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/** Starts the gRPC bootstrap membership service. */
public final class BootstrapGrpcServer {
    private BootstrapGrpcServer() { }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: BootstrapGrpcServer <grpcPort>");
        }
        int port = Integer.parseInt(args[0]);
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("gRPC port must be between 1 and 65535");
        }

        Server server = ServerBuilder.forPort(port)
                .addService(new BootstrapServiceImpl())
                .build()
                .start();
        System.out.println("BOOTSTRAP gRPC READY port=" + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping bootstrap gRPC server");
            server.shutdown();
        }));
        server.awaitTermination();
    }
}
