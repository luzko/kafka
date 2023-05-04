package com.epam.course.kafka.model;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Entity
@Table(name = "vehicle_signal")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class VehicleSignal {

    @Id
    private Long id;

    private double coordinateX;

    private double coordinateY;

    private double distance;
}
