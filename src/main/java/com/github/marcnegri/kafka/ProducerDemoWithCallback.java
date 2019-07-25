package com.github.marcnegri.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        //System.out.println("Hello World");

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        for(int i=0;i <10;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello world_" + Integer.toString(i));

            //send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //test if the record was sent successfully or not
                    if (e == null) {
                        logger.info("Received new metadata : \n" +
                                "Topic " + recordMetadata.topic() + "\n" +
                                "Partition " + recordMetadata.partition() + "\n" +
                                "Offset " + recordMetadata.offset() + "\n" +
                                "TimeStamp " + recordMetadata.timestamp());
                    } else {
                        logger.info("Error : " + e);
                    }
                }
            });
        }
        producer.flush();
        producer.close();

    }
}
