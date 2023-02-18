package com.custom;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
public class Point implements Serializable {

    private String mmsi;
    private Date datetime;
    private Double distance;
    private Integer cell;
    private String route;
    private Integer waypoint_id;


    public void calculateDistance(Double x1, Double x2, Double y1 ,Double y2){
        setDistance(Math.hypot(x1-x2,y1-y2));
    }

    @Override
    public String toString() {
        return "Point{" +
                "mmsi='" + mmsi + '\'' +
                ", datetime=" + datetime +
                ", distance=" + distance +
                ", cell=" + cell +
                ", route='" + route + '\'' +
                ", waypoint_id=" + waypoint_id +
                '}';
    }
}

