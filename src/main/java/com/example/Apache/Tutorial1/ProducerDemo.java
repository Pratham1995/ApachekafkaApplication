package com.example.Apache.Tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        //Create Producer properties
        String bootStrapServers="127.0.0.1:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG ,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create producer
        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);

        //Producer Record
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("firstTopic","Hello World");

        //Send Data -asynchronous
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
