package kafka.wikimedia;


// logging
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Kafka
import org.apache.kafka.clients.producer.KafkaProducer;
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