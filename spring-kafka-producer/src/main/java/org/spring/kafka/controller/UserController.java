package org.spring.kafka.controller;

import org.spring.kafka.model.User;
import org.spring.kafka.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class UserController {
	
	@Autowired
	private Producer producer;
	
	@GetMapping(value="/publish/{name}")
	public String postMessage(@PathVariable("name") String name) {
		User user = new User();
		user.setName(name);
		user.setDept("Alphabet Technology - R & D");
		user.setSalary(12000L);
		
		log.info("UserController :: postMessage");
		producer.send(user);
		
		return "Published Successfully";		
	}

}
