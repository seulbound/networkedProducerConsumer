A producer-consumer program which can upload videos. The producer will send the files to the consumer, which will display them in a GUI.

To run the program:
Run the consumer with: java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.filetransfer.Consumer 8080 <threads> <destination folder>
Run the producer with: java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.filetransfer.Producer localhost 8080 <threads> <source folder>