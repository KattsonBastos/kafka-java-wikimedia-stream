package kafka.wikimedia;


// logging
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Kafka
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

// event handler
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangesHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    // # -- creating a logger
    private final Logger log = LoggerFactory.getLogger(WikimediaChangesHandler.class.getSimpleName());

    public WikimediaChangesHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // nothing here
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception{
        log.info(messageEvent.getData());
        // async
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment){ 
        // nothing here        
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream Reading..", t);
    }
}