package com.kafka.twitter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.util.StringUtils;

import com.kafka.twitter.config.KafkaConfig;
import com.kafka.twitter.constants.KafkaConstants;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TwitterProducer {
	
	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	@SuppressWarnings("deprecation")
	public void run() {
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		//create twitter client
		Client client = createTwitterClient(msgQueue);
		// Attempts to establish a connection.
		client.connect();
		
		//create a kafka producer
		KafkaProducer<String, String> kafkaProducer = KafkaConfig.initKafkaProducer();		
		
		//loop to send tweets to kafka on same thread/different thread/or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				e.printStackTrace();
				client.stop();
			}
			if (!StringUtils.isEmpty(msg)) {
				log.info(msg);
				final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(KafkaConstants.TWITTER_TWEETS_TOPIC, null, msg);
				// Send Data - Asynchronous : Lambda Expression	
				kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
				      if (exception == null) {
				    	  System.out.println("Published record with key : " + producerRecord.key() 
				    	  					+ ", and value : " + producerRecord.value() 
				    	  					+ ", to partition : " + recordMetadata.partition());
				      } else {
				          log.error("An error occurred producing to kafka due to :: ", exception.getMessage());
				      }
				});
			}
		}
		
		//Add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Stopping application...");
			System.out.println("Shutting down client from twitter...");
			client.stop();
			System.out.println("Closing producer...");
			kafkaProducer.close();
		}));
		log.info("End of Application");		
	}
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms		
		hosebirdEndpoint.trackTerms(KafkaConstants.TWITTER_TERMS);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(KafkaConstants.TWITTER_CONSUMER_KEY, KafkaConstants.TWITTER_CONSUMER_SECRET, KafkaConstants.TWITTER_ACCESS_TOKEN, KafkaConstants.TWITTER_ACCESS_SECRET);
		
		ClientBuilder builder = new ClientBuilder()
									  .name("Hosebird-Client-01")                              // optional: mainly for the logs
									  .hosts(hosebirdHosts)
									  .authentication(hosebirdAuth)
									  .endpoint(hosebirdEndpoint)
									  .processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;			
	}
}
