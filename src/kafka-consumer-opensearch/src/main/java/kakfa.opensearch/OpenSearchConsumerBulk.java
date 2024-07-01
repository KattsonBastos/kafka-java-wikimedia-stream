package kafka.opensearch;


// logging
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Kafka
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

// OpenSearch
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;

// EventHandler
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

// Others
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumerBulk {

    // rest high level client
    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-java";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);

    }

    private static String extractRecordId(String json) {
        // gson lib 
        return JsonParser.parseString(json)
         .getAsJsonObject()
         .get("meta")
         .getAsJsonObject()
         .get("id")
         .getAsString();
    }

    public static void main(String[] args) throws IOException {

        String osIndexName = "wikimedia";
        String topic = "src-java-wikimedia-recentchange";

        Logger log = LoggerFactory.getLogger(OpenSearchConsumerBulk.class.getSimpleName());
        log.info("Consumer Started");


        // # -- create opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // # -- create Kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();


        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // create the index on OpenSearch if don't exists
        try (openSearchClient; consumer) { // this try already clsoe the openSearchClient so we don't need the openSearchClient.close()

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(osIndexName), RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(osIndexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia Index has been created!");
            } else{
                log.info("The index {} already exists!", osIndexName);
            }

            // subscribing the consumer
            consumer.subscribe(Collections.singleton(topic));

            // main code
            log.info("Starting main code");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();

                log.info("Received "+ recordCount + " record(s).");

                BulkRequest bulkRequest = new BulkRequest();

                // sending records into opensearch
                for (ConsumerRecord<String, String> record : records) {

                    //String recordId = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try {
                        String recordId = extractRecordId(record.value());
                        IndexRequest indexRequest = new IndexRequest(osIndexName)
                            .source(record.value(), XContentType.JSON)
                            .id(recordId);

                        // IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        bulkRequest.add(indexRequest); // filling the bulkRequest

                        // log.info(response.getId());
                    } catch (Exception e) {

                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " records");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // committing offsets after batch is consumed
                    consumer.commitSync();
                    log.info(recordCount + " offsets committed!");
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            openSearchClient.close();
            log.info("The consumer is now gracefully shut down");
        }
    }
}