package com.kafka.avro.consumer;

import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.stereotype.Service;

import com.kafka.schema.Employee;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class AvroConsumer {

	@StreamListener(Processor.INPUT)
	public void consumeEmployeeDetails(Employee employeeDetails) {
		log.info("Let's process employee details: {}", employeeDetails);
	}

}