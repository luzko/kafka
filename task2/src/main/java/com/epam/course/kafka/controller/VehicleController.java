package com.epam.course.kafka.controller;

import com.epam.course.kafka.model.VehicleSignal;
import com.epam.course.kafka.service.VehicleService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("vehicle")
@RequiredArgsConstructor
public class VehicleController {

    private final VehicleService service;

    @PostMapping("/create")
    public void create(@RequestBody VehicleSignal signal) {
        service.processSignal(signal);
    }
}
