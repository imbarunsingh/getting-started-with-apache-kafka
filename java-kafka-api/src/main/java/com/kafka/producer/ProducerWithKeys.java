package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.kafka.config.KafkaConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProducerWithKeys {
	
	public static void main(String[] args) throws Exception {

		String topic = "first_topic";

		log.info("-----------------------Kafka Producer : With Callback : With Keys : Asynchronous-------------------");

		// create the producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(KafkaConfig.producerConfigs());

		for (int i = 1; i < 10; i++) {
			//record to be sent to kafka
			ProducerRecord<String, String> producerRecord;
			if (i % 2 == 0) {
				producerRecord = new ProducerRecord<String, String>(topic, "Amoeba", "Hello Amoeba -> " + String.valueOf(i));
			} else {
				producerRecord = new ProducerRecord<String, String>(topic, "Bacteria", "Hello Bacteria -> " + String.valueOf(i));
			}
			// Send Data - Asynchronous : Lambda Expression	
			kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
			      if (exception == null) {
			    	  System.out.println("Sent record with key : " + producerRecord.key() 
			    	  					+ ", and value : " + producerRecord.value() 
			    	  					+ ", to partition : " + recordMetadata.partition());
			      } else {
			          log.error("An error occurred producing to kafka due to :: ", exception.getMessage());
			      }
			});
			Thread.sleep(10000);
		}
		//flush Data
		kafkaProducer.flush();
		//close Kafka producer
		kafkaProducer.close();

	}

}
