package kafka.wikimedia;


// logging
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Kafka
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.StringSerializer;

// EventHandler
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

// Others
import java.util.Properties;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "localhost:9092";
        String topicName = "src-java-wikimedia-recentchange";
        String srcUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        // # -- create Producer Properties - Properties is basically the config of our producer
        Properties properties = new Properties();

        // setting the broker properties
        properties.setProperty("bootstrap.servers", bootstrapServers);

        // setting the producer specific properties - both props below serialize the keys and values using an official Kafka class
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // safe producer config
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // same as "-1"
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // # -- create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        EventHandler eventHandler = new WikimediaChangesHandler(producer, topicName);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(srcUrl));
        EventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block the program until then
        TimeUnit.MINUTES.sleep(10);
    }
}