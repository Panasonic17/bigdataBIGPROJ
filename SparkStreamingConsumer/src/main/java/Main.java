import kafka.Kafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;


import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class Main {
     public static  void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        Map<String, Object> props = new HashMap();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "consumer-tutorial");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Collection<String> topics = Arrays.asList("javaworld");

//         JavaPairReceiverInputDStream<String, String> kafkaStream =KafkaUtils.createDirectStream()
//
////                         .createStream(streamingContext,
////                         [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume]);


         JavaInputDStream<ConsumerRecord<String, String>> stream =
                 KafkaUtils.createDirectStream(
                         jssc,
                         LocationStrategies.PreferConsistent(),
                         ConsumerStrategies.<String, String>Subscribe(topics, props)
                 );
//
//        stream.map(a -> {
//            System.out.println(a.value());
//            return a.value();
//        }).print();
//        jssc.start();
//        jssc.awaitTermination();

    }
}
