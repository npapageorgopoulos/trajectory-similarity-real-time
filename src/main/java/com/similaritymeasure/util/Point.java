package com.similaritymeasure.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Point implements Serializable,Comparable<Point> {

    private Date datetime;
//    private Double distance;
    private Double latitude;
    private Double longitude;
    private Integer cellId;
    private Integer trajectoryId;

    public double distance(Point other){
        return(Math.hypot(this.longitude-other.longitude,this.latitude- other.latitude));
    }


    @Override
    public String toString() {
        return "Point{" +
                "datetime=" + datetime +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", cellId=" + cellId +
                ", trajectoryId=" + trajectoryId +
                '}';
    }

    @Override
    public int compareTo(Point p) {
        return Integer.compare(this.trajectoryId, p.trajectoryId);
    }
}

