package org.spring.kafka.producer;

import org.spring.kafka.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class Producer {

	@Value("${kafka.producer.topic}")
	private String topic;

	@Autowired
	private KafkaTemplate<String, User> kafkaTemplate;

	public void send(User message) {
		ListenableFuture<SendResult<String, User>> future = kafkaTemplate.send(topic, message);

		future.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {

			@Override
			public void onSuccess(SendResult<String, User> result) {
				log.info("Sent [{}] to partion [{}] with offset [{}]", message, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
			}

			@Override
			public void onFailure(Throwable ex) {
				log.info("Unable to send message=[" + message + "] due to : " + ex.getMessage());
			}
		});

	}

}
