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

//    public void calculateDistance(Double x1, Double x2, Double y1 ,Double y2){
//        setDistance(Math.hypot(x1-x2,y1-y2));
//    }


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

