package com.epam.course.kafka.consumer;

import java.util.concurrent.CountDownLatch;
import com.epam.course.kafka.model.VehicleSignal;
import com.epam.course.kafka.producer.KafkaProducer;
import com.epam.course.kafka.service.VehicleService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Getter
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumer {

    private final CountDownLatch latch = new CountDownLatch(1);

    private final KafkaProducer kafkaProducer;

    private final VehicleService vehicleService;

    @Value("${spring.kafka.topic.output}")
    private String topic;

    @KafkaListener(
        topics = "${spring.kafka.topic.input}",
        containerFactory = "distanceKafkaListenerFactory",
        groupId = "${spring.kafka.group.tracker}",
        concurrency = "3")
    public void consume(VehicleSignal message) {
        log.info("Receive message: {}", message);
        kafkaProducer.sendDistance(topic, message.getId(), vehicleService.calculateDistance(message));
        latch.countDown();
    }

    @KafkaListener(
        topics = "${spring.kafka.topic.output}",
        containerFactory = "loggingKafkaListenerFactory",
        groupId = "${spring.kafka.group.logging}")
    public void consumeOutput(ConsumerRecord<Long, Double> consumerRecord) {
        log.info("Receive message: {}", consumerRecord);
        latch.countDown();
    }
}
