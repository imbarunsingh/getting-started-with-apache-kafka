package org.spring.kafka.consumer;

import org.spring.kafka.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class Consumer {
	
	//https://www.baeldung.com/spring-kafka : refer for more kafka consumer config
	

	@KafkaListener(topics = "string_topic", groupId = "${kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
	public void consume(String message) {
		log.info("Consumed String messgae :: " + message);
	}

	@KafkaListener(topics = "${kafka.consumer.topic}", groupId = "${kafka.consumer.json-group-id}", containerFactory = "userKafkaListenerContainerFactory")
	public void consumeUserJSON(User user) {
		log.info("Consumed JSON messgae :: " + user);
	}

	//retrieval of one or more message headers using the @Header annotation in the listener
	@KafkaListener(topics = "topicName")
	public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
	      System.out.println("Received Message: " + message + "from partition: " + partition);
	}
	
	//Consuming Messages from a Specific Partition
	@KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = { "0", "3" }), containerFactory = "userKafkaListenerContainerFactory")
    public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        System.out.println("Received Message: " + message + " from partition: " + partition);        
    }
}
