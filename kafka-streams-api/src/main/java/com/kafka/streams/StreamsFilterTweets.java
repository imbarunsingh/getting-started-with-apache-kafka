package com.kafka.streams;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.kafka.streams.config.KafkaStreamsConfig;
import com.kafka.streams.constants.KafkaConstants;

public class StreamsFilterTweets {

	private static JsonParser jsonParser = new JsonParser();

	public static void main(String[] args) {
		// create properties
		Properties properties = KafkaStreamsConfig.streamConfigs();
		
		//create a topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		//input topic
		KStream<String, String>  inputTopicStream = streamsBuilder.stream(KafkaConstants.TWITTER_TWEETS_TOPIC);
		
		KStream<String, String>  filteredStream = inputTopicStream.filter((k, jsonTweet) -> extractUserFollowersInTweets(jsonTweet) > 1000);

		filteredStream.to(KafkaConstants.IMPORTANT_TWEETS);
		
		//build the topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
		
		//start our stream applications
		kafkaStreams.start();
		//kafkaStreams.close();
	}

	private static Integer extractUserFollowersInTweets(String tweetJson) {
		try {
			return jsonParser.parse(tweetJson)
					.getAsJsonObject()
					.get("user")
					.getAsJsonObject()
					.get("followers_count")
					.getAsInt();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return 0;
		}
	}

}
