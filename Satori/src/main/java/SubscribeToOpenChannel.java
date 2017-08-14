

import com.satori.rtm.*;
import com.satori.rtm.model.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SubscribeToOpenChannel {
    static final String endpoint = "wss://open-data.api.satori.com";
    static final String appkey = "9fbd1c4BEa889C66cFf83B042B0fDCed";
    static final String channel = "wiki-rc-feed";

    public static void main(String[] args) throws InterruptedException {
        final RtmClient client = new RtmClientBuilder(endpoint, appkey)
                .setListener(new RtmClientAdapter() {
                    @Override
                    public void onEnterConnected(RtmClient client) {
                        System.out.println("Connected to Satori RTM!");
                    }
                })
                .build();

        final Producer<String, String> producer = new KafkaProd().getProduser();

        SubscriptionAdapter listener = new SubscriptionAdapter() {
            @Override
            public void onSubscriptionData(SubscriptionData data) {
                for (AnyJson json : data.getMessages()) {
                    ProducerRecord<String, String> wiki = new ProducerRecord<String, String>(
                            "javaworld", "test", json.toString());

                    producer.send(wiki);
                }
            }
        };

        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);
        System.out.println("started");
        client.start();
    }
}