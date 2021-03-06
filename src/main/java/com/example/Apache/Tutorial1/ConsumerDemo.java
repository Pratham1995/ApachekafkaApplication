package com.example.Apache.Tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger looger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        Properties properties=new Properties();
        String bootstrap="127.0.0.1:9092";
        String groupID="my-fourth-application";
        String topic="firstTopic";

        //Create Consumer Config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String,String> kafkaConsumer=
                new KafkaConsumer<String, String>(properties);

        //Subscribe consumer to topics
        kafkaConsumer.subscribe(Collections.singleton(topic));

        //poll new data
        while(true){
           ConsumerRecords<String,String> records=
                   kafkaConsumer.poll(Duration.ofMillis(100));
           for(ConsumerRecord<String,String> record:records){
               looger.info("Key: "+record.key() +", Value: "+record.value());
               looger.info("Partition: "+ record.partition() +" Offset: "+record.offset());
           }
        }

    }
}
