package com.epam.course.kafka.producer;

import com.epam.course.kafka.model.VehicleSignal;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaProducer {

    private final KafkaTemplate<Long, VehicleSignal> vehicleKafkaTemplate;

    private final KafkaTemplate<Long, Double> distanceKafkaTemplate;

    public void sendVehicle(String topic, VehicleSignal message) {
        log.info("Send message: {}", message);
        vehicleKafkaTemplate.send(topic, message.getId(), message);
    }

    public void sendDistance(String topic, Long vehicleId, double distance) {
        log.info("Send distance: {}", distance);
        distanceKafkaTemplate.send(topic, vehicleId, distance);
    }
}
