A producer-consumer program which can upload videos. The producer will send the files to the consumer, which will display them in a GUI.

To run the program:

Build with mvn clean install

Run the consumer with: mvn javafx:run -Djavafx.run.arguments=<threads>

Run the producer with: java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.networkedProducerConsumer.Producer <server> <threads> <queue length> <source folder(s)>
