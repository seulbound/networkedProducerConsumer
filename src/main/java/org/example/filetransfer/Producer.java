package org.example.filetransfer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Producer {
    private final ManagedChannel channel;
    private final FileTransferServiceGrpc.FileTransferServiceStub asyncStub;
    private final ExecutorService executor;

    public Producer(String host, int port, int numThreads) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.asyncStub = FileTransferServiceGrpc.newStub(channel);
        this.executor = Executors.newFixedThreadPool(numThreads);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        executor.shutdown();
    }

    public void sendFile(String folderPath) {
        executor.submit(() -> {
            File folder = new File(folderPath);
            File[] files = folder.listFiles();
            if (files == null) {
                System.err.println("No files found in folder: " + folderPath);
                return;
            }

            for (File file : files) {
                if (file.isFile()) {
                    try {
                        Path path = Paths.get(file.getAbsolutePath());
                        StreamObserver<UploadStatus> responseObserver = new StreamObserver<UploadStatus>() {
                            @Override
                            public void onNext(UploadStatus value) {
                                System.out.println("Upload status: " + value.getMessage());
                            }

                            @Override
                            public void onError(Throwable t) {
                                System.err.println("Error uploading file: " + t.getMessage());
                            }

                            @Override
                            public void onCompleted() {
                                System.out.println("File upload completed for " + file.getName());
                            }
                        };

                        StreamObserver<FileChunk> requestObserver = asyncStub.uploadFile(responseObserver);
                        
                        try (InputStream inputStream = Files.newInputStream(path)) {
                            byte[] buffer = new byte[4096];
                            int bytesRead;
                            boolean firstChunk = true;

                            bytesRead = inputStream.read(buffer);
                            if (bytesRead > 0) {
                                requestObserver.onNext(FileChunk.newBuilder()
                                        .setFileName(file.getName())
                                        .setData(ByteString.copyFrom(buffer, 0, bytesRead))
                                        .build());
                                firstChunk = false;

                                while ((bytesRead = inputStream.read(buffer)) > 0) {
                                    requestObserver.onNext(FileChunk.newBuilder()
                                            .setData(ByteString.copyFrom(buffer, 0, bytesRead))
                                            .build());
                                }
                            }

                            if (firstChunk) {
                                requestObserver.onNext(FileChunk.newBuilder().setFileName(file.getName()).setData(ByteString.EMPTY).build());
                            }
                        }
                        requestObserver.onCompleted();
                    } catch (IOException e) {
                        System.err.println("Error reading file: " + e.getMessage());
                    }
                }
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 4) {
            System.err.println("Usage: Producer <host> <port> <numThreads> <folder1> [<folder2> ...]");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int numThreads = Integer.parseInt(args[2]);

        Producer producer = new Producer(host, port, numThreads);

        for (int i = 3; i < args.length; i++) {
            producer.sendFile(args[i]);
        }

        producer.executor.awaitTermination(1, TimeUnit.HOURS);
        producer.shutdown();
    }
}