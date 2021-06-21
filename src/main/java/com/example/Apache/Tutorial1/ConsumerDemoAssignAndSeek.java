package com.example.Apache.Tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        Logger looger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());
        Properties properties=new Properties();
        String bootstrap="127.0.0.1:9092";
        String groupID="my-Seventh-application";
        String topic="firstTopic";

        //Create Consumer Config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String,String> kafkaConsumer=
                new KafkaConsumer<String, String>(properties);

        //Assign and seek are used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom=new TopicPartition(topic,0);
        Long offSetToReadFrom=15L;
        kafkaConsumer.assign(Arrays.asList(partitionToReadFrom));
        //seek
        kafkaConsumer.seek(partitionToReadFrom,offSetToReadFrom);

        int numberofMessagesToRead=5;
        boolean keepOnReading=true;
        int numberofMessagesRead=0;

        //poll new data
        while(keepOnReading){
           ConsumerRecords<String,String> records=
                   kafkaConsumer.poll(Duration.ofMillis(100));
           for(ConsumerRecord<String,String> record:records){
               numberofMessagesRead++;
               looger.info("Key: "+record.key() +", Value: "+record.value());
               looger.info("Partition: "+ record.partition() +" Offset: "+record.offset());
               if(numberofMessagesRead>= numberofMessagesToRead){
                   keepOnReading=false;
                   break;
               }
           }
        }
        looger.info("Exiting the Application");

    }
}
