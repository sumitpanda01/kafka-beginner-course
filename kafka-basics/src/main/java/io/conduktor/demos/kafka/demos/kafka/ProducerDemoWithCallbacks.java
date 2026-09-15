package io.conduktor.demos.kafka.demos.kafka;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {

    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);

    public static void main(String[] args) {

//        log.info("Producer started");
        //create producer properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batchsize","400");

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for(int j=0;j<10;j++){
            for(int i=0;i<10;i++){
                //create a producer record
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","hello world" + i);

                //send the data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e ==null){
                            log.info("Recoed new metedata \n" +
                                    "topic: " + recordMetadata.topic() + "\n" +
                                    "Partitions: " + recordMetadata.partition() + "\n"+
                                    "offset: " + recordMetadata.offset() + "\n" +
                                    "TimeStamp: " + recordMetadata.timestamp());
                        }else{
                            log.error("error while producing " + e);
                        }
                    }
                });
            }

            try{
                Thread.sleep(500);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }









        //flush & close the producer

        //flush mean that it tell the producer to send all data and block until done --synchronous
        producer.flush();

        //close the producer
        producer.close();

    }
}