package com.epam.course.kafka.service;

import com.epam.course.kafka.exception.SignalException;
import com.epam.course.kafka.model.VehicleSignal;
import com.epam.course.kafka.producer.KafkaProducer;
import com.epam.course.kafka.repository.VehicleSignalRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class VehicleService {

    private final KafkaProducer kafkaProducer;

    private final VehicleSignalRepository repository;

    @Value("${spring.kafka.topic.input}")
    private String inputTopic;

    public void processSignal(VehicleSignal vehicleSignal) {
        validate(vehicleSignal);
        kafkaProducer.sendVehicle(inputTopic, vehicleSignal);
    }

    public double calculateDistance(VehicleSignal newSignal) {
        repository.findById(newSignal.getId())
            .map(signal -> calculateDistance(signal, newSignal))
            .ifPresent(newSignal::setDistance);
        repository.save(newSignal);
        return newSignal.getDistance();
    }

    public void validate(VehicleSignal vehicleSignal) {
        if (vehicleSignal.getCoordinateX() < 0.0d || vehicleSignal.getCoordinateY() < 0.0d) {
            throw new SignalException("Incorrect coordinates");
        }
    }

    private double calculateDistance(VehicleSignal oldSignal, VehicleSignal newSignal) {
        return Math.hypot(oldSignal.getCoordinateX() - newSignal.getCoordinateX(),
            oldSignal.getCoordinateY() - newSignal.getCoordinateY());
    }
}
