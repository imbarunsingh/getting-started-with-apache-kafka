package com.kafka.twitter.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.kafka.twitter.constants.KafkaConstants;

public class KafkaConfig {	
	
	// Producer configuration
	public static Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		
		//create a safe producer
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
		
		//high throughput producer config(at the expense of a bit of latency and CPU usage)
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32*1024); //32Kb
		
		
		return props;
	}
	
	// Consumer configuration
	public static Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.GROUP_ID);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // could be earliest(from beginning)/latest/none
		
		//to disable auto commit of offset
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); 
		//max record polled at once
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
		return props;
	}
	
	
	public static KafkaProducer<String, String> initKafkaProducer() {
		return new KafkaProducer<String, String>(KafkaConfig.producerConfigs());		
	}
	
	public static KafkaConsumer<String, String> initKafkaConsumer() {
		return new KafkaConsumer<String, String>(KafkaConfig.consumerConfigs());		
	}

}
