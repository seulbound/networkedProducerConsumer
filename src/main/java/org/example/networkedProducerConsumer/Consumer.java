package org.example.networkedProducerConsumer;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Consumer {
    private final int port;
    private final Server server;
    private final ExecutorService executor;

    public Consumer(int port, int numThreads, String destination) {
        this.port = port;
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.server = ServerBuilder.forPort(port)
                .addService(new NetworkedProducerConsumerServiceImpl(destination))
                .executor(executor)
                .build();
    }

    public void start() throws IOException {
        server.start();
        System.out.println("Consumer server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            Consumer.this.stop();
            System.err.println("*** server shut down");
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: Consumer <port> <numThreads> <destination_folder>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        int numThreads = Integer.parseInt(args[1]);
        String destination = args[2];
        final Consumer consumer = new Consumer(port, numThreads, destination);
        consumer.start();
        consumer.blockUntilShutdown();
    }

    private static class NetworkedProducerConsumerServiceImpl extends NetworkedProducerConsumerServiceGrpc.NetworkedProducerConsumerServiceImplBase {
        private final Path destination;

        public NetworkedProducerConsumerServiceImpl(String destination) {
            this.destination = Paths.get(destination);
        }

        @Override
        public StreamObserver<FileChunk> uploadFile(final StreamObserver<UploadStatus> responseObserver) {
            return new StreamObserver<FileChunk>() {
                private OutputStream outputStream;
                private String fileName;

                @Override
                public void onNext(FileChunk value) {
                    try {
                        if (fileName == null) {
                            fileName = value.getFileName();
                            if (fileName == null || fileName.isEmpty()) {
                                responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Filename is missing in the first chunk")));
                                return;
                            }
                            System.out.println("Receiving file: " + fileName);
                            Files.createDirectories(destination);
                            Path filePath = destination.resolve(fileName);
                            outputStream = Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                        }

                        if (outputStream != null && !value.getData().isEmpty()) {
                            outputStream.write(value.getData().toByteArray());
                        }
                    } catch (IOException e) {
                        responseObserver.onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Error receiving file: " + t.getMessage());
                    if (outputStream != null) {
                        try {
                            outputStream.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }

                @Override
                public void onCompleted() {
                    try {
                        if (outputStream != null) {
                            outputStream.close();
                        }
                        if (fileName != null) {
                            responseObserver.onNext(UploadStatus.newBuilder().setSuccess(true).setMessage("File uploaded successfully: " + fileName).build());
                            System.out.println("File received: " + fileName);
                        } else {
                            responseObserver.onNext(UploadStatus.newBuilder().setSuccess(false).setMessage("No file data received.").build());
                            System.out.println("Upload completed without receiving any file.");
                        }
                        responseObserver.onCompleted();
                    } catch (IOException e) {
                        responseObserver.onError(e);
                    }
                }
            };
        }
    }
}