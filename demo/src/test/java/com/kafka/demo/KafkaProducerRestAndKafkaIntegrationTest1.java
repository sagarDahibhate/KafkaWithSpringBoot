package com.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = { "fruits" })
class KafkaProducerRestAndKafkaIntegrationTest1 {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void testRestEndpointProducesToKafka() {
        // Step 1: Call REST API using RestTemplate
        String url = "http://localhost:" + port + "/send?message=Apple";
        ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);

        // Verify REST response
        assertThat(response.getStatusCodeValue()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("Your message is produced");

        // Step 2: Setup Kafka Consumer
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                consumerProps,
                new StringDeserializer(),
                new StringDeserializer()
        );
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "fruits");

        // Step 3: Poll messages from Kafka
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        consumer.close();

        // Step 4: Verify message is received in Kafka
        assertThat(records.count()).isGreaterThan(0);
        ConsumerRecord<String, String> record = records.iterator().next();
        assertThat(record.value()).isEqualTo("Apple");
    }
}
