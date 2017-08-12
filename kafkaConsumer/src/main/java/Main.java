

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "consumer-tutorial");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());


        //Figure out where to start processing messages from
        KafkaConsumer kafkaConsumer;
        kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(Arrays.asList("javaworld"));
        //Start processing messages
//        while (true) {
//            Map<String, ConsumerRecords<String, String>> records = kafkaConsumer.poll(1000);
//
//            for (ConsumerRecords<String, String> record : records.values()) {
//                System.out.println(record);
//            }
//        }

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.println(record.offset() + ": " + record.value());
        }
    }
}