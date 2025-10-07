package com.kafka.demo;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerTest {

	@KafkaListener(topics = { "fruits" }, groupId = "abc")
	public void getMessage(String message) {
          System.out.println(message);
	}
}
