A producer-consumer program which can upload videos. The producer will send the files to the consumer, which will display them in a GUI.

To run the program:

Ensure that Java 23+ is used.

Build with mvn clean install

Run the consumer with: mvn javafx:run -Djavafx.run.arguments=<threads>

Run the producer with: java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.networkedProducerConsumer.Producer <server> <threads/folder count> <queue length> <source folder(s)>

The destination folder for the consumer is set to be "folder1" in the root directory.

https://github.com/seulbound/networkedProducerConsumer

Test Case Commands:
- java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.networkedProducerConsumer.Producer 192.168.1.6 1 2 nonVideo
- java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.networkedProducerConsumer.Producer 192.168.1.6 1 2 sameContentDifferentName
- java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.networkedProducerConsumer.Producer 192.168.1.6 2 2 sameNameDifferentContent/folder2 sameNameDifferentContent/folder3
- java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.networkedProducerConsumer.Producer 192.168.1.6 1 2 queueOverflow
- java -cp target/networkedProducerConsumer-1.0-SNAPSHOT.jar org.example.networkedProducerConsumer.Producer 192.168.1.6 1 3 lackingThreads/folder2 lackingThreads/folder3

https://drive.google.com/file/d/1iPjKrmuGsaOn1jpyLLQXbtHgWPddAfPX/view?usp=sharing
The folders in the zip file linked above were placed in root during testing.
