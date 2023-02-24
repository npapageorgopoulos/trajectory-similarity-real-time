package com.similaritymeasure.util;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class Trajectory implements Serializable {

    private List<Point> points;

    public Trajectory(List<Point> points) {
        this.points = points;
    }

    public double lockStepEuclideanDistance(Trajectory other) {
        double distance = 0.0;
        int minSize = Math.min(points.size(), other.getPoints().size());
        for (int i = 0; i < minSize; i++) {
            Point p1 = points.get(i);
            Point p2 = other.getPoints().get(i);
            distance += Math.sqrt(Math.pow(p1.getLongitude() - p2.getLongitude(), 2) + Math.pow(p1.getLatitude() - p2.getLatitude(), 2));
        }
        return distance / minSize;
    }

    @Override
    public String toString() {
        return "traj{" +
                "points=" + points.size() +
                '}';
    }
}