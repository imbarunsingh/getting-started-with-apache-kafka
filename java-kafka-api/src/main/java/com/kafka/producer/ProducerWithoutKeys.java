package com.kafka.producer;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.kafka.config.KafkaConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProducerWithoutKeys {
	public static void main(String[] args) throws InterruptedException, ExecutionException {

		String topic = "first_topic";

		log.info("-----------------------Kafka Producer : With Callback : Without Keys : Asynchronous-------------------");

		// create the producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(KafkaConfig.producerConfigs());

		for (int i = 1; i < 5; i++) {
			//record to be sent to kafka
			String value = "Hello World -> " + String.valueOf(i);
			final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, value);

			// Send Data - Asynchronous
			kafkaProducer.send(producerRecord, new Callback() {				
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO Auto-generated method stub
					System.out.println("Sent record with key : " + producerRecord.key() 
										+ " and value : " + producerRecord.value() 
										+ " to partition : " + metadata.partition());
				}
			});			
		}
		//Flush Data to consumer console
		kafkaProducer.flush();
		//close
		kafkaProducer.close();		

	}
}
