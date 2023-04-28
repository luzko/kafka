package com.epam.course.kafka.consumer;

import java.util.concurrent.CountDownLatch;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@Getter
@RequiredArgsConstructor
public class KafkaConsumer {

    private final CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(
        topics = "${spring.kafka.topic}",
        groupId = "group1",
        containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message) {
        log.info("Receive message: {}", message);
        latch.countDown();
    }
}
