package comp.github.dsajdl.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys  {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";
        // create Producer properties
        Properties properties = new Properties();

        // set properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG === "bootstrap.servers"
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG ===  "key.serializer"
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG === "value.serializer"


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create producer record
        for (int i = 0; i <= 10; i++) {

            String topic = "first_topic";
            String value = "hello in great new world_" + i;
//            String key  = "id_1"; // the same kye will guarantee order so ii will always go to partition 0 and consumer wil receive message in order
//            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            // to guarantee order you need to run it synchronously which make bad performance by using sent().get()
            // if you run is again then each message will go to the same partition as the key guarantee that message will always go to the same partition
            String key1 = "id_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key1, value);
            // id_0 is going to partition 1
            // id_1 partition 0
            // id_2 partition 2
            // id_3 partition 0
            // id_4 partition 2
            // id_5 partition 2
            // id_6 partition 0
            // id_7 partition 2
            // id_8 partition 1
            // id_9 partition 2
            // id_10 partition 2

            logger.info("Key: " + key1);
            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // this is executed every time a recored is sent
                    if (e == null) {
                        // if exeption is null then the record is successfully sent
                        logger.info("recieved new medadata." +
                                "\nTopic: " + recordMetadata.topic() +
                                "\nPartition: " + recordMetadata.partition() +
                                "\nOffset: " + recordMetadata.offset() +
                                "\nTimestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // block the .sent() to make it Synchronous - don't to this in production
        }
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
