import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Properties;

public class SocetOut {
    public static void main(String[] args) {
        PrintWriter out = null;
        ServerSocket servers = null;
        Socket fromclient = null;
        // create server socket
        Properties props = new Properties();
        props.put("bootstrap.servers", "sandbox.hortonworks.com:6667");
        props.put("group.id", "consumer-tutorial");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        //Figure out where to start processing messages from
        KafkaConsumer kafkaConsumer;
        kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(Arrays.asList("javaworld"));


        try {
            servers = new ServerSocket(4459);
        } catch (IOException e) {
            System.err.println("trouble with port");
        }
        try {
            fromclient = servers.accept();
        } catch (IOException e) {
        }
        try {
            out = new PrintWriter(fromclient.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("start");
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("send " + record);
                out.println(record.value());
            }
        }
    }
}
