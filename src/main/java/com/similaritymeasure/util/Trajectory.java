package com.similaritymeasure.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class Trajectory implements Serializable {

    private List<Point> points;
    int cellId;


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

    public double dtwDistance(Trajectory other) {
        int n = this.points.size();
        int m = other.getPoints().size();
        double[][] dtw = new double[n+1][m+1];
        for (int i = 1; i <= n; i++) {
            dtw[i][0] = Double.POSITIVE_INFINITY;
        }
        for (int j = 1; j <= m; j++) {
            dtw[0][j] = Double.POSITIVE_INFINITY;
        }
        dtw[0][0] = 0.0;
        for (int i = 1; i <= n; i++) {
            for (int j = 1; j <= m; j++) {
                double cost = this.points.get(i-1).distance(other.getPoints().get(j-1));
                dtw[i][j] = cost + Math.min(dtw[i-1][j], Math.min(dtw[i][j-1], dtw[i-1][j-1]));
            }
        }
        return dtw[n][m];
    }

    public double lcsSimilarity(Trajectory other) {
        int n = this.points.size();
        int m = other.getPoints().size();
        int[][] lcs = new int[n+1][m+1];
        for (int i = 1; i <= n; i++) {
            for (int j = 1; j <= m; j++) {
                if (this.points.get(i-1).equals(other.getPoints().get(j-1))) {
                    lcs[i][j] = lcs[i-1][j-1] + 1;
                } else {
                    lcs[i][j] = Math.max(lcs[i-1][j], lcs[i][j-1]);
                }
            }
        }
        return (double) lcs[n][m] / Math.min(n, m);
    }

    @Override
    public String toString() {
        return "traj{" +
                "points=" + points.size() +
                '}';
    }
}