package org.example.networkedProducerConsumer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.tika.Tika;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Producer {
    private final ManagedChannel channel;
    private final NetworkedProducerConsumerServiceGrpc.NetworkedProducerConsumerServiceStub asyncStub;
    private final ExecutorService executor;
    private final Tika tika;

    public Producer(String host, int port, int numThreads) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.asyncStub = NetworkedProducerConsumerServiceGrpc.newStub(channel);
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.tika = new Tika();
    }

    public void shutdown() throws InterruptedException {
        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
            executor.shutdownNow();
        }
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public Future<?> sendFile(String folderPath) {
        return executor.submit(() -> {
            File folder = new File(folderPath);
            File[] files = folder.listFiles();
            if (files == null) {
                System.err.println("No files found in folder: " + folderPath);
                return;
            }

            for (File file : files) {
                if (file.isFile()) {
                    try {
                        String mimeType = tika.detect(file);
                        if (mimeType == null || !mimeType.startsWith("video/")) {
                            System.out.println("Skipping non-video file: " + file.getName() + " (type: " + mimeType + ")");
                            continue;
                        }
                    } catch (IOException e) {
                        System.err.println("Error detecting file type for " + file.getName() + ", skipping: " + e.getMessage());
                        continue;
                    }

                    final CountDownLatch finishLatch = new CountDownLatch(1);
                    StreamObserver<UploadStatus> responseObserver = new StreamObserver<>() {
                        @Override
                        public void onNext(UploadStatus value) {
                            System.out.println("Upload status: " + value.getMessage());
                        }

                        @Override
                        public void onError(Throwable t) {
                            System.err.println("Error uploading file '" + file.getName() + "': " + t.getMessage());
                            finishLatch.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            System.out.println("File upload completed for " + file.getName());
                            finishLatch.countDown();
                        }
                    };

                    StreamObserver<FileChunk> requestObserver = asyncStub.uploadFile(responseObserver);

                    try (InputStream inputStream = Files.newInputStream(Paths.get(file.getAbsolutePath()))) {
                        byte[] buffer = new byte[4096];
                        int bytesRead;
                        boolean firstChunk = true;

                        while ((bytesRead = inputStream.read(buffer)) > 0) {
                            FileChunk.Builder chunkBuilder = FileChunk.newBuilder()
                                    .setData(ByteString.copyFrom(buffer, 0, bytesRead));
                            if (firstChunk) {
                                chunkBuilder.setFileName(file.getName());
                                firstChunk = false;
                            }
                            requestObserver.onNext(chunkBuilder.build());
                        }

                        if (firstChunk) {
                            requestObserver.onNext(FileChunk.newBuilder().setFileName(file.getName()).setData(ByteString.EMPTY).build());
                        }
                        
                        requestObserver.onCompleted();

                        if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                            System.err.println("File upload timed out for " + file.getName());
                            requestObserver.onError(new RuntimeException("Timeout waiting for server response"));
                        }
                    } catch (IOException e) {
                        System.err.println("Error reading file: " + e.getMessage());
                        requestObserver.onError(e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.err.println("Interrupted while waiting for file upload to complete.");
                        requestObserver.onError(e);
                    }
                }
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 4) {
            System.err.println("Usage: java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.networkedProducerConsumer.Producer <server> <port> <threads> <sourceFolder>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int numThreads = Integer.parseInt(args[2]);

        Producer producer = new Producer(host, port, numThreads);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 3; i < args.length; i++) {
            futures.add(producer.sendFile(args[i]));
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                System.err.println("Task failed: " + e.getCause());
            }
        }

        producer.shutdown();
    }
}
