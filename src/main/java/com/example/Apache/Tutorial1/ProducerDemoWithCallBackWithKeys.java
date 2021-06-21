package com.example.Apache.Tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallBackWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        //Create Producer properties
        String bootStrapServers="127.0.0.1:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create producer
        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);

        //Producer Record
        for(int i=0;i<10;i++){
            Scanner sc=new Scanner(System.in);
            String topic="firstTopic";
            System.out.println("Enter Line here");
            String value=sc.nextLine()+ Integer.toString(i);
            String Key="id_" +Integer.toString(i);
            ProducerRecord<String,String> record=new ProducerRecord<String, String>(topic,Key,value);

            logger.info("Key: "+Key);
            //Send Data -asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully sent or exception is thrown
                    if(e ==null){
                        //record is successfully sent
                        logger.info("Received new Metadata.\n"+
                                "Topic: "+recordMetadata.topic() +" \n"+
                                "Partition: "+recordMetadata.partition()+"\n"+
                                "Offsets: "+recordMetadata.offset()+"\n"+
                                "TimeStamp: "+recordMetadata.timestamp()+"\n");

                    }else{
                        logger.error("Error while producing",e);
                    }
                }
            }).get();//block the send to make it synchronous but dont do it in production
        }
        producer.flush();
        producer.close();
    }
}
