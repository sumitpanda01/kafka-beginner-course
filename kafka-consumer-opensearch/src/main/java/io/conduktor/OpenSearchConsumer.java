package io.conduktor;

import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.GetIndexRequest;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class OpenSearchConsumer {

    public static OpenSearchClient createOpenSearchClient() {

        HttpHost host = new HttpHost("http", "localhost", 9200);

        OpenSearchTransport transport =
                ApacheHttpClient5TransportBuilder
                        .builder(host)
                        .build();

        OpenSearchClient client =
                new OpenSearchClient(transport);

        return client;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){
        String groupId = "consumer-opensearch-demo";

        //creating the consumer property

        Properties properties= new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","latest");

        return new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);

        // Create an OpenSearch client
        OpenSearchClient openSearchClient = createOpenSearchClient();

        //create our kafka client
        KafkaConsumer<String,String> consumer = createKafkaConsumer();

        try {
            // Check if the "wikimedia" index already exists
            boolean indexExists = openSearchClient.indices()
                    .exists(e -> e.index("wikimedia"))
                    .value();

            if (!indexExists) {

                // Create the "wikimedia" index
                openSearchClient.indices()
                        .create(c -> c.index("wikimedia"));

                log.info("The wikimedia index has been created!");

            } else {

                log.info("The wikimedia index already exists");
            }

            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received " + recordCount +" record(s)");

                for(ConsumerRecord<String,String> record : records){
                    // send the record into opensearch

                }
            }

        } catch (Exception e) {
            log.error("Error while creating/checking the Wikimedia index", e);
        }
    }
}
