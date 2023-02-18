package com.custom;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Waypoint {
    private String route_name;
    private Integer waypoint_id;
    private Double latitude;
    private Double longitude;
    private Integer cell;



    @Override
    public String toString() {
        return "Waypoint{" +
                "route_name='" + route_name + '\'' +
                ", waypoint_id=" + waypoint_id +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", cell=" + cell +
                '}';
    }
}
