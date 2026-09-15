package io.conduktor.demos.kafka;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class WikimediaChangesProducer
{
    public static void main( String[] args ) throws StreamException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers",bootstrapServers);

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchange";

        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer,topic);

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder =
                new EventSource.Builder(
                        ConnectStrategy.http(URI.create(url))
                                .header("User-Agent", "KafkaWikimediaProducer/1.0")
                );

        EventSource eventSource = builder.build();

        eventSource.start();

        //we produce for 10 minutes and block the code until then
        TimeUnit.MINUTES.sleep(10);

    }
}
