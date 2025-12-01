package org.example.networkedProducerConsumer;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.tika.Tika;

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
            System.err.println("Usage: java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.networkedProducerConsumer.Consumer <port> <threads> <destinationFolder>");
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
        private final Tika tika;

        public NetworkedProducerConsumerServiceImpl(String destination) {
            this.destination = Paths.get(destination);
            this.tika = new Tika();
        }

        @Override
        public StreamObserver<FileChunk> uploadFile(final StreamObserver<UploadStatus> responseObserver) {
            return new StreamObserver<>() {
                private OutputStream outputStream;
                private String fileName;
                private Path filePath;

                @Override
                public void onNext(FileChunk value) {
                    try {
                        if (fileName == null) {
                            fileName = value.getFileName();
                            if (fileName.isEmpty()) {
                                responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Filename is missing in the first chunk")));
                                return;
                            }

                            byte[] firstData = value.getData().toByteArray();
                            String mimeType = tika.detect(firstData);

                            if (mimeType == null || !mimeType.startsWith("video/")) {
                                System.err.println("Rejected file " + fileName + " because it is not a video. Detected type: " + mimeType);
                                responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("File is not a video. Detected type: " + mimeType)));
                                return;
                            }

                            System.out.println("Receiving file: " + fileName + " (type: " + mimeType + ")");
                            Files.createDirectories(destination);
                            filePath = destination.resolve(fileName);
                            outputStream = Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                            outputStream.write(firstData);
                        } else {
                            if (outputStream != null && !value.getData().isEmpty()) {
                                outputStream.write(value.getData().toByteArray());
                            }
                        }
                    } catch (IOException e) {
                        responseObserver.onError(e);
                        closeStreamAndDeleteFile();
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Error receiving file: " + t.getMessage());
                    closeStreamAndDeleteFile();
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

                private void closeStreamAndDeleteFile() {
                    try {
                        if (outputStream != null) {
                            outputStream.close();
                        }
                        if (filePath != null && Files.exists(filePath)) {
                            Files.delete(filePath);
                        }
                    } catch (IOException e) {
                        System.err.println("Error during file cleanup: " + e.getMessage());
                    }
                }
            };
        }
    }
}