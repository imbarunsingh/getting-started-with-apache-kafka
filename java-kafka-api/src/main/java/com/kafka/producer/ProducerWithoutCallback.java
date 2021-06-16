package com.kafka.producer;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.kafka.config.KafkaConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProducerWithoutCallback {
	public static void main(String[] args) throws InterruptedException, ExecutionException {

		String topic = "first_topic";

		log.info("-----------------------Kafka Producer : Without Callback : Synchronous-------------------");

		// create the producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(KafkaConfig.producerConfigs());

		// create the producer record
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, "Hello World !");

		// Send Data - synchronous
		RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
		
		if(null != metadata) {
			System.out.println("Sent record with key : " + producerRecord.key() 
								+ " and value : " + producerRecord.value() 
								+ " to partition : " + metadata.partition());
		}

		// flush data
		kafkaProducer.flush();
		//close Kafka producer
		kafkaProducer.close();
	}

}
