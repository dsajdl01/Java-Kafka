package comp.github.dsajdl.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

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
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        // send data - asynchronous
        producer.send(record);
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
