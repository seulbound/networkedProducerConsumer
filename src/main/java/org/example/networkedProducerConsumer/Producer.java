package org.example.networkedProducerConsumer;

import com.google.protobuf.ByteString;
import com.videotransfer.grpc.FileChunk;
import com.videotransfer.grpc.TransferStatus;
import com.videotransfer.grpc.VideoTransferServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.*;

public class Producer {

    private static String SERVER_HOST;
    private static final int SERVER_PORT = 50051;
    private int pThreads;
    private int queueCapacity;
    private BlockingQueue<File> transferQueue;

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
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".mp4") || name.endsWith(".mkv"));

        if (files == null) return;

        for (File f : files) {
            boolean added = transferQueue.offer(f);
            if (added) {
                System.out.println("[Producer] Added to queue: " + f.getName());
            } else {
                System.out.println("[Producer] Queue full ("+queueCapacity+"). Ignored: " + f.getName());
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