package com.spring.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class Producer {

	@Value("${kafka.topic}")
	private String topic;

	@Autowired
	private KafkaTemplate<String, String> template;

	public void sendMessage(String message) {
		template.send(topic, message);
		log.info("Sent [{}] to topic [{}]", message, topic);
	}

}
