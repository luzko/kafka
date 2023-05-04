package com.epam.course.kafka.repository;

import com.epam.course.kafka.model.VehicleSignal;
import java.util.Optional;
import org.springframework.data.repository.CrudRepository;

public interface VehicleSignalRepository extends CrudRepository<VehicleSignal, Integer> {

    Optional<VehicleSignal> findById(Long id);
}
