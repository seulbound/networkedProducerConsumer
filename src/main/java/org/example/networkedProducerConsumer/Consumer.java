package org.example.networkedProducerConsumer;

import com.videotransfer.grpc.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.layout.TilePane;
import javafx.scene.layout.VBox;
import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import javafx.scene.media.MediaView;
import javafx.stage.Stage;
import javafx.util.Duration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.Executors;

public class Consumer extends Application {

    private static final int PORT = 50051;
    private static final String OUTPUT_DIR = "folder1";
    private TilePane tilePane;
    private static int CONSUMER_THREADS = 4;

    public static void main(String[] args) {
        if (args.length > 0) CONSUMER_THREADS = Integer.parseInt(args[0]);
        new File(OUTPUT_DIR).mkdirs();
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        tilePane = new TilePane();
        tilePane.setHgap(10);
        tilePane.setVgap(10);
        tilePane.setPrefColumns(4);

        ScrollPane scrollPane = new ScrollPane(tilePane);
        scrollPane.setFitToWidth(true);

        Scene scene = new Scene(scrollPane, 800, 600);
        primaryStage.setTitle("Video Consumer Queue");
        primaryStage.setScene(scene);
        primaryStage.show();

        loadExistingVideos();
        startGrpcServer();
        startFileWatcher();
    }

    private void startFileWatcher() {
        new Thread(() -> {
            try {
                WatchService watchService = FileSystems.getDefault().newWatchService();
                Paths.get(OUTPUT_DIR).register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);

                WatchKey key;
                while ((key = watchService.take()) != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            File newFile = Paths.get(OUTPUT_DIR, event.context().toString()).toFile();
                            addVideoToGallery(newFile);
                        } else if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
                            removeVideoFromGallery(event.context().toString());
                        }
                    }
                    key.reset();
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void removeVideoFromGallery(String fileName) {
        Platform.runLater(() -> tilePane.getChildren().removeIf(node -> {
            if (node instanceof VideoThumbnail) {
                return ((VideoThumbnail) node).getFileName().equals(fileName);
            }
            return false;
        }));
    }

    private void loadExistingVideos() {
        File outputDirectory = new File(OUTPUT_DIR);
        if (outputDirectory.exists() && outputDirectory.isDirectory()) {
            File[] videoFiles = outputDirectory.listFiles((dir, name) ->
                    name.toLowerCase().endsWith(".mp4") ||
                    name.toLowerCase().endsWith(".mov") ||
                    name.toLowerCase().endsWith(".avi") ||
                    name.toLowerCase().endsWith(".mkv")
            );
            if (videoFiles != null) {
                for (File videoFile : videoFiles) {
                    addVideoToGallery(videoFile);
                }
            }
        }
    }

    private void startGrpcServer() {
        new Thread(() -> {
            try {
                Server server = ServerBuilder.forPort(PORT)
                        .executor(Executors.newFixedThreadPool(CONSUMER_THREADS))
                        .addService(new VideoTransferServiceImpl())
                        .build()
                        .start();
                System.out.println("\n\nConsumer Server started on port " + PORT);
                server.awaitTermination();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void addVideoToGallery(File videoFile) {
        Platform.runLater(() -> {
            for (javafx.scene.Node node : tilePane.getChildren()) {
                if (node instanceof VideoThumbnail && ((VideoThumbnail) node).getFileName().equals(videoFile.getName())) {
                    return;
                }
            }
            try {
                VideoThumbnail thumbnail = new VideoThumbnail(videoFile);
                tilePane.getChildren().add(thumbnail);
            } catch (Exception e) {
                System.err.println("Error loading video UI: " + e.getMessage());
            }
        });
    }

    private class VideoTransferServiceImpl extends VideoTransferServiceGrpc.VideoTransferServiceImplBase {
        @Override
        public StreamObserver<FileChunk> uploadVideo(StreamObserver<TransferStatus> responseObserver) {
            return new StreamObserver<FileChunk>() {
                FileOutputStream fos;
                String fileName;
                File finalFile;

                @Override
                public void onNext(FileChunk chunk) {
                    try {
                        if (fos == null) {
                            fileName = chunk.getFileName();
                            finalFile = Paths.get(OUTPUT_DIR, fileName).toFile();
                            fos = new FileOutputStream(finalFile);
                        }
                        fos.write(chunk.getContent().toByteArray());
                    } catch (IOException e) {
                        onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Upload error: " + t.getMessage());
                    try { if (fos != null) fos.close(); } catch (IOException e) { /* ignored */ }
                }

                @Override
                public void onCompleted() {
                    try {
                        if (fos != null) fos.close();
                        TransferStatus status = TransferStatus.newBuilder()
                                .setSuccess(true).setMessage("Upload Complete").build();
                        responseObserver.onNext(status);
                        responseObserver.onCompleted();

                        addVideoToGallery(finalFile);

                    } catch (IOException e) {
                        onError(e);
                    }
                }
            };
        }
    }

    private class VideoThumbnail extends VBox {
        private final String fileName;
        private MediaPlayer player;
        private MediaView mediaView;

        public VideoThumbnail(File file) {
            this.fileName = file.getName();
            String mediaUrl = file.toURI().toString();
            Media media = new Media(mediaUrl);
            player = new MediaPlayer(media);
            mediaView = new MediaView(player);

            mediaView.setFitWidth(200);
            mediaView.setPreserveRatio(true);

            Label fileNameLabel = new Label(file.getName());

            this.getChildren().addAll(mediaView, fileNameLabel);
            this.setStyle("-fx-border-color: black; -fx-border-width: 2;");
            this.setAlignment(Pos.CENTER);

            this.setOnMouseEntered(e -> {
                player.setStartTime(Duration.ZERO);
                player.setStopTime(Duration.seconds(10));
                player.play();
            });

            this.setOnMouseExited(e -> {
                player.stop();
                player.seek(Duration.ZERO);
            });

            this.setOnMouseClicked(e -> playFullScreen(mediaUrl));
        }

        public String getFileName() {
            return fileName;
        }

        private void playFullScreen(String url) {
            Stage stage = new Stage();
            MediaPlayer mp = new MediaPlayer(new Media(url));
            MediaView mv = new MediaView(mp);
            mv.fitWidthProperty().bind(stage.widthProperty());
            mv.fitHeightProperty().bind(stage.heightProperty());
            mv.setPreserveRatio(true);

            VBox root = new VBox(mv);
            root.setAlignment(Pos.CENTER);
            Scene s = new Scene(root);
            stage.setScene(s);
            stage.show();

            mp.play();
            stage.setOnCloseRequest(ev -> mp.dispose());
        }
    }
}