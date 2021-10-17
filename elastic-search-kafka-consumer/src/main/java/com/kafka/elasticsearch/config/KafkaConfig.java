package com.kafka.elasticsearch.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.kafka.elasticsearch.constants.CommonConstants;

public class KafkaConfig {

	// Consumer configuration
	public static Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CommonConstants.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CommonConstants.GROUP_ID);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // could be earliest(from beginning)/latest/none
		
		
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		return props;
	}

	public static KafkaConsumer<String, String> initKafkaConsumer() {
		return new KafkaConsumer<String, String>(KafkaConfig.consumerConfigs());
	}

}
