package org.example.networkedProducerConsumer;

import com.google.protobuf.ByteString;
import com.videotransfer.grpc.FileChunk;
import com.videotransfer.grpc.TransferStatus;
import com.videotransfer.grpc.VideoTransferServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.tika.Tika;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer {

    private static String SERVER_HOST;
    private static final int SERVER_PORT = 50051;
    private int pThreads;
    private int queueCapacity;
    private BlockingQueue<File> transferQueue;
    private final AtomicInteger filesQueuedCount = new AtomicInteger(0);


    public Producer(int p, int q) {
        this.pThreads = p;
        this.queueCapacity = q;
        this.transferQueue = new ArrayBlockingQueue<>(q);
    }

    public void start(String[] sourceFolders) {
        Thread sender = new Thread(this::processQueue);
        sender.start();

        ExecutorService producerPool = Executors.newFixedThreadPool(pThreads);

        System.out.println(pThreads);
        for (int i = 0; i < pThreads; i++) {
            String folder = sourceFolders[i];
            producerPool.submit(() -> scanFolder(folder));
        }

        producerPool.shutdown();
        try {
            if (!producerPool.awaitTermination(60, TimeUnit.SECONDS)) {
                producerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            producerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void scanFolder(String folderPath) {
        File folder = new File(folderPath);
        File[] files = folder.listFiles();

        if (files == null) return;

        Tika tika = new Tika();

        System.out.println("\n\n");

        for (File f : files) {
            if (f.isFile()) {
                if (filesQueuedCount.get() >= queueCapacity) {
                    System.out.println("[Producer] Total capacity of " + queueCapacity + " files reached. Ignoring remaining files in this folder.");
                    break;
                }

                try {
                    String mimeType = tika.detect(f);
                    if (mimeType != null && mimeType.startsWith("video/")) {
                        int currentCount = filesQueuedCount.incrementAndGet();
                        if (currentCount <= queueCapacity) {
                            try {
                                transferQueue.put(f);
                                System.out.println("[Producer] Added to queue: " + f.getName() + " (type: " + mimeType + ")");
                            } catch (InterruptedException e) {
                                filesQueuedCount.decrementAndGet();
                                System.err.println("Producer thread interrupted, could not add file to queue: " + f.getName());
                                Thread.currentThread().interrupt();
                                break;
                            }
                        } else {
                            filesQueuedCount.decrementAndGet();
                            System.out.println("[Producer] Total capacity of " + queueCapacity + " files reached. Ignoring file: " + f.getName());
                            break;
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error detecting file type for " + f.getName() + ": " + e.getMessage());
                }
            }
        }
    }

    private void processQueue() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(SERVER_HOST, SERVER_PORT).usePlaintext().build();
        VideoTransferServiceGrpc.VideoTransferServiceStub stub = VideoTransferServiceGrpc.newStub(channel);

        while (true) {
            try {
                File fileToSend = transferQueue.take();
                sendFile(stub, fileToSend);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void sendFile(VideoTransferServiceGrpc.VideoTransferServiceStub stub, File file) {
        CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<FileChunk> requestObserver = stub.uploadVideo(new StreamObserver<TransferStatus>() {
            @Override
            public void onNext(TransferStatus status) {
                System.out.println("[Consumer Response] " + status.getMessage());
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Transfer Failed: " + t.getMessage());
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("Transfer Finished: " + file.getName());
                finishLatch.countDown();
            }
        });

        try (BufferedInputStream bInputStream = new BufferedInputStream(new FileInputStream(file))) {
            byte[] buffer = new byte[1024 * 512]; // 512KB chunks
            int bytesRead;
            while ((bytesRead = bInputStream.read(buffer)) != -1) {
                FileChunk chunk = FileChunk.newBuilder()
                        .setFileName(file.getName())
                        .setContent(ByteString.copyFrom(buffer, 0, bytesRead))
                        .setIsLastChunk(false)
                        .build();
                requestObserver.onNext(chunk);
            }
            requestObserver.onCompleted();
            finishLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
            requestObserver.onError(e);
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: Producer <SERVER_HOST> <P_Threads> <Q_Size> <Folder_Path1> <Folder_Path2>...");
            return;
        }

        SERVER_HOST = args[0];
        int p = Integer.parseInt(args[1]);
        int q = Integer.parseInt(args[2]);

        String[] folders = new String[args.length - 3];
        System.arraycopy(args, 3, folders, 0, args.length - 3);

        Producer app = new Producer(p, q);
        app.start(folders);
    }
}