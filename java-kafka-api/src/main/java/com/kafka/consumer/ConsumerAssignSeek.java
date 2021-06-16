package com.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.kafka.config.KafkaConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsumerAssignSeek {
	public static void main(String[] args) {

		String topic = "first_topic";

		log.info("-----------------------Kafka Consumer : Assign and Seek API-------------------");

		// create consumer
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(KafkaConfig.consumerConfigs());


		// Assign and seek are mostly used to replay data or fetch a specific message from a specific partition and offset

		TopicPartition partitonToReadFrom = new TopicPartition(topic, 2);
		
		//Assign
		kafkaConsumer.assign(Arrays.asList(partitonToReadFrom));

		// Seek
		long offsetToReadFrom = 5L;
		kafkaConsumer.seek(partitonToReadFrom, offsetToReadFrom);

		int numberOfMessagesToReadFrom = 5;
		boolean keepOnReading = true;
		int numberOfMessagesReadSoFar = 0;

		// poll for data
		while (keepOnReading) {
			ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
			for (ConsumerRecord<String, String> record : consumerRecords) {
				System.out.println("Key : " + record.key() 
									+ ", value: " + record.value() 
									+ ", partition : " + record.partition() 
									+ ", offset : " + record.offset());
				numberOfMessagesReadSoFar++;
				if(numberOfMessagesReadSoFar >= numberOfMessagesToReadFrom) {
					keepOnReading = false;
					break;
				}
			}
		}

		// close consumer
		kafkaConsumer.close();
	}

}
