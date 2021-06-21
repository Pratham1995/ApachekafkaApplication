package com.example.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    private static JsonParser jsonParser=new JsonParser();
    public static void main(String[] args) {
        //Create properties
        Properties properties=new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"Demo-Kafka-Streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //Create topology
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        //Input topic
        KStream<String,String> inputTopic =streamsBuilder.stream("twitter_tweets");
        KStream<String,String> filteredStream=inputTopic.filter(
                (k,jsonTweet)->
                    extractUserFollowers(jsonTweet)>100000
                //Filter tweets which has a user of over 1000 followers
        );
        filteredStream.to("important_tweets");
        //build the topology
        KafkaStreams kafkaStreams=new KafkaStreams(streamsBuilder.build(),properties);

        //start our streams application
        kafkaStreams.start();
    }

    private static int extractUserFollowers(String jsonTweet) {
        //gson Library
        try{
            return jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();

    }catch (NullPointerException e){
            return 0;
        }
    }
}
