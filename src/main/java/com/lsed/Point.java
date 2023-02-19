package com.lsed;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
public class Point implements Serializable {

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
                ", datetime=" + datetime +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", cellId=" + cellId +
                ", trajectoryId=" + trajectoryId +
                '}';
    }
}

