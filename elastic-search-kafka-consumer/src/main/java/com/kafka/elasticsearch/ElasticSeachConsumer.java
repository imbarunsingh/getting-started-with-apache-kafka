package com.kafka.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.gson.JsonParser;
import com.kafka.elasticsearch.config.ElasticSearchConfig;
import com.kafka.elasticsearch.config.KafkaConfig;
import com.kafka.elasticsearch.constants.CommonConstants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticSeachConsumer {
	
	private static JsonParser jsonParser = new JsonParser();

	public static void main(String[] args) throws Exception {
		
		
		
		KafkaConsumer<String, String> consumer = KafkaConfig.initKafkaConsumer();
		//subscribing to the twitter_tweets kafka topic
		consumer.subscribe(Arrays.asList(CommonConstants.TWITTER_TWEETS_TOPIC));
		
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for (ConsumerRecord<String, String> consumerRecord : records) {
				produceToElasticSearch(consumerRecord);				
			}
		}
		

	}
	
	public static void produceToElasticSearch(ConsumerRecord<String, String> consumerRecord) throws IOException, InterruptedException {
		RestHighLevelClient restHighLevelClient = ElasticSearchConfig.createClient();
		
		//2 strategies to generate id for idempotent consumer
		
		//1. Kafka generic Id
		//String id = consumerRecord.topic() + "_" + consumerRecord.partition() + "_" + consumerRecord.offset();
		
		//2. Extract from twitter tweets to make consumer idompotent
		String id;
		try {
			id = extractIdFromTweets(consumerRecord.value());
		} catch(Exception e) {
			log.warn("Skipping bad data :: {}", consumerRecord.value());
			return;
		}
		
		IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(consumerRecord.value(), XContentType.JSON);
		IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
		String idResString = indexResponse.getId();
		log.info("ID of document inserted :: " + idResString);
		
		Thread.sleep(20000); // introducing delay
		
	}
	
	private static String extractIdFromTweets(String tweetJson) {
		return jsonParser.parse(tweetJson)
							.getAsJsonObject()
							.get("id_str")
							.getAsString();
		
	}
}
