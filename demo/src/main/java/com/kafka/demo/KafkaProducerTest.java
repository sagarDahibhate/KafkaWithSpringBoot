package com.kafka.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaProducerTest {

	@Autowired
	private KafkaTemplate kafkaTemplate;

	@GetMapping("send")
	public ResponseEntity<String> pushMessage(@RequestParam String message) {

		kafkaTemplate.send("fruits", message);

		return new ResponseEntity<String>("Your message is produced", HttpStatus.OK);

	}
}
