package com.example.Apache.Tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    public ConsumerDemoWithThreads() {
    }
    public void run(){
        Logger looger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootstrap="127.0.0.1:9092";
        String groupID="my-sixth-application";
        String topic="firstTopic";
        //Creating the latch
        CountDownLatch latch=new CountDownLatch(1);
        //Creating consumer Runnable
        looger.info("Creating the consumer thread");
        Runnable myConsumerThread = new ConsumerRunnable(
                bootstrap,
                groupID,
                topic,
                latch);

        Thread thread=new Thread(myConsumerThread);
        thread.start();
        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            looger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerThread).shutdown();
            try{
                latch.await();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            looger.info("Application has exited");
        }));


        try{
            latch.await();
        } catch (InterruptedException e) {
            looger.info("Application got interrupted");
        }finally {
            looger.info("Application is closing");
        }


    }

    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch; //Shutdowns application correctly
        private KafkaConsumer<String,String> kafkaConsumer;
        private Logger looger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        public ConsumerRunnable(String bootstrap,String groupID, String topic,CountDownLatch latch) {
            this.latch=latch;
            Properties properties=new Properties();
            //Create Consumer Config
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            //Create Consumer
            kafkaConsumer= new KafkaConsumer<String, String>(properties);
            //Subscribe consumer to topics
            kafkaConsumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run() {
            //poll new data
            try{
                while (true) {
                    ConsumerRecords<String, String> records =
                            kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        looger.info("Key: " + record.key() + ", Value: " + record.value());
                        looger.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }

            }catch (WakeupException e){
                looger.info("Info received shutdown signal");
            }finally {
                kafkaConsumer.close();
                latch.countDown();
            }
        }
        public void shutdown(){
            //The wakeup method is a special method to interrupt consumer.poll()
            //It will throw the exception which is wake up
            kafkaConsumer.wakeup();

        }
    }
}
