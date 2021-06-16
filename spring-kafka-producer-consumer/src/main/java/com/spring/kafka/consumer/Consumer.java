package com.spring.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class Consumer {

	@KafkaListener(topics = "${kafka.topic}")
	public void onMessage(String message) {
		log.info(String.format("#### -> Consumed message -> %s", message));
	}

}
