package com.epam.course.kafka.integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.epam.course.kafka.consumer.KafkaConsumer;
import com.epam.course.kafka.model.VehicleSignal;
import com.epam.course.kafka.producer.KafkaProducer;
import com.epam.course.kafka.repository.VehicleSignalRepository;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9091", "port=9091"})
class KafkaIntegrationTest {

    @Autowired
    private KafkaConsumer kafkaConsumer;

    @Autowired
    private KafkaProducer kafkaProducer;

    @Mock
    private VehicleSignalRepository vehicleSignalRepository;

    @Value("${spring.kafka.topic.input}")
    private String inputTopic;

    @Test
    void consumeMessageTest() throws Exception {
        VehicleSignal vehicleSignal = new VehicleSignal(1L, 10.0, 10.0, 0.0);
        kafkaProducer.sendVehicle(inputTopic, vehicleSignal);

        assertTrue(kafkaConsumer.getLatch().await(10, TimeUnit.SECONDS));
    }
}
