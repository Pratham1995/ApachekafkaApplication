package com.example.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient(){
        //https://ykzjtxyrdc:h54i3a36s0@apache-kafka-course-1110623004.us-east-1.bonsaisearch.net:443
        String hostname="apache-kafka-course-1110623004.us-east-1.bonsaisearch.net";
        String username="ykzjtxyrdc";
        String password="h54i3a36s0";
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        KafkaConsumer<String,String> consumer=KafkaConsumer("twitter_tweets");
        while(true){

            ConsumerRecords<String,String> records=
                    consumer.poll(Duration.ofMillis(5));
            Integer recordCount=records.count();
            logger.info("Received: "+ recordCount+" records");

            BulkRequest bulkRequest=new BulkRequest();

            for(ConsumerRecord<String,String> record:records) {
                //System.out.println(record.value());
                //2 Strategies
                //String id=record.topic()+"_"+record.partition()+"_"+record.partition();
                //twitter feed specific id
                try {
                    String id = extratValueFromTweets(record.value());
                    //Where we instert data in elasticSearch
                    logger.info(id);
                    IndexRequest indexRequest;
                    indexRequest = new IndexRequest("twitter")
                            .source(record.value(), XContentType.JSON).id(id);
                    bulkRequest.add(indexRequest);//Add to bulk request
                    //System.out.println(record.value());
                } catch (NullPointerException e) {
                    logger.warn("Skipping data: " + record.value());
                }
            }
            if(recordCount>0){
                BulkResponse bulkItemResponses=client.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("Committing offsets.......");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                Thread.sleep(1000);
            }

        }
       // client.close();
    }
    private static JsonParser jsonParser=new JsonParser();
    private static String extratValueFromTweets(String tweetJson) {
        //gson Library
       return jsonParser.parse(tweetJson).
                getAsJsonObject().
                get("id_str").
                getAsString();
    }

    public static KafkaConsumer<String, String> KafkaConsumer(String topic){
        Logger looger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        Properties properties=new Properties();
        String bootstrap="127.0.0.1:9092";
        String groupID="Kafka-demo-elasticsearch";

        //Create Consumer Config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");//Auto commit offset off
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");

        //Create Consumer
        KafkaConsumer<String,String> kafkaConsumer=
                new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        return kafkaConsumer;
    }
}
