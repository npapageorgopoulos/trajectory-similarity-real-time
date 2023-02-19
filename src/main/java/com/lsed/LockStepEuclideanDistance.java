package com.lsed;

import com.config.StaticVars;
import com.custom.Waypoint;
import com.util.CustomPartition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike$class;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class LockStepEuclideanDistance {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        CustomPartition customPartitioner = new CustomPartition(StaticVars.xSeperations * StaticVars.ySeperations);

        // Create a Spark Streaming context with a batch interval of 1 second
        JavaSparkContext sc = new JavaSparkContext("local[*]", "LockStepEuclideanDistance");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000));

        // input from TCP socket on 8988
        JavaReceiverInputDStream<String> input = jssc.socketTextStream("localhost", 8988);

        JavaRDD<String> wpInput = jssc.sparkContext().textFile(StaticVars.wpReplicatedSource);
        String wpHeader = wpInput.first();
        JavaRDD<Point> waypointsRDD = wpInput
                //remove header of the input RDD
                .filter(wp -> !wp.equals(wpHeader))
                .map(row -> {
                    String[] lineString = row.split(",");
                    Point p = new Point();
                    p.setCellId(Integer.parseInt(lineString[0]));
                    p.setLongitude(Double.parseDouble(lineString[2]));
                    p.setLatitude(Double.parseDouble(lineString[1]));
                    p.setTrajectoryId(Integer.parseInt(lineString[4]));
                    return p;
                });

        JavaDStream<Point> pointsInput = input.flatMap(row -> {
            List<Point> points = new ArrayList<>();
            String[] lineString = row.split(";");
            Point p = new Point();
            p.setCellId(-1);
            p.setTrajectoryId(Integer.parseInt(lineString[0]));
            p.setDatetime(sdf.parse(lineString[3]));
            p.setLongitude(Double.parseDouble(lineString[4]));
            p.setLatitude(Double.parseDouble(lineString[5]));
            points.add(p);
            return points.iterator();
        });

        JavaDStream<Point> mergedPositions = pointsInput.transform(pointsRDD -> {

            pointsRDD.union(waypointsRDD);
           return pointsRDD;
        });

        // Merge the two DStreams by transforming the tcpPositions DStream
//        JavaDStream<Point> mergedPositions = pointsInput.transform(pointsRDD -> {
//            // Join the pointsRDD with the waypointsRDD by trajectory ID
//            JavaPairRDD<Integer, Point> pointPairRDD = pointsRDD.mapToPair(pos -> new Tuple2<>(pos.getTrajectoryId(), pos));
//            JavaPairRDD<Integer, Point> wpPairRDD = waypointsRDD.mapToPair(pos -> new Tuple2<>(pos.getTrajectoryId(), pos));
//            JavaPairRDD<Integer, Tuple2<Point, Point>> joinedPairRDD = pointPairRDD.join(wpPairRDD);
//            joinedPairRDD.foreach(row -> System.out.println(row._2()));
//
//            // Combine the matching points and return the merged positions as a new RDD
//            JavaPairRDD <Integer,Point> mergedRDD = joinedPairRDD.mapToPair(tuple -> {
//                Point inputPos = tuple._2()._1();
//                Point wpPos = tuple._2()._2();
//                int cellId = (inputPos.getCellId() != -1) ? inputPos.getCellId() : wpPos.getCellId();
//                Point p = new Point();
//                p.setTrajectoryId(inputPos.getTrajectoryId());
//                p.setCellId(cellId);
//                p.setLatitude(inputPos.getLatitude() + wpPos.getLatitude());
//                p.setLongitude(inputPos.getLongitude() + wpPos.getLatitude());
//                return new Tuple2<>(cellId,p);
//            });
//
//            // Repartition the RDD by cell ID to enable efficient parallel processing
//            JavaPairRDD<Integer, Iterable<Point>> partitionedRDD = mergedRDD.groupByKey(new HashPartitioner(StaticVars.noOfPartitions));
//            JavaRDD<Point> partitionedPointsRDD = partitionedRDD.flatMap(tuple -> tuple._2().iterator());
//
//            // Return the partitioned RDD as a new DStream
////            partitionedPointsRDD.;
//            return partitionedPointsRDD;
//        });

//        wpInput.foreach(row ->{
//            System.out.println(row);
//        });

        mergedPositions.print();

//         Start the Spark Streaming context
        jssc.start();

        // Wait for the job to end
        jssc.awaitTermination();
    }
}
