package com.similaritymeasure;

import com.config.StaticVars;
import com.similaritymeasure.util.CustomPartitioner;
import com.similaritymeasure.util.Point;
import com.similaritymeasure.util.Trajectory;
import com.util.AisReceiver;
import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class DynamicTimeWarpingDistance {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        CustomPartitioner customPartitioner = new CustomPartitioner(StaticVars.xSeperations * StaticVars.ySeperations);

        // Create a Spark Streaming context with a batch interval of 1 second
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("LockStepEuclideanDistance");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // input from custom ais receiver
        JavaDStream<String> input = jssc.receiverStream(new AisReceiver())
                .filter(row -> !row.split(";")[0].equals("mmsi"));


        JavaRDD<String> wpInput = jssc.sparkContext().textFile(StaticVars.wpReplicatedSource);
        String wpHeader = wpInput.first();
        //                .values()
        JavaPairRDD<Integer, Point> waypointsRDD = wpInput
                //remove header of the input RDD
                .filter(wp -> !wp.equals(wpHeader))
                .map(row -> {
//                .mapToPair(row -> {
                    String[] lineString = row.split(",");
                    Point p = new Point();
                    p.setCellId(Integer.parseInt(lineString[0]));
                    p.setLongitude(Double.parseDouble(lineString[2]));
                    p.setLatitude(Double.parseDouble(lineString[1]));
                    p.setTrajectoryId(Integer.parseInt(lineString[4]));
                    return p;
                })
                .keyBy(row -> row.getCellId())
                .partitionBy(customPartitioner);

        JavaDStream<Point> pointsDstream = input.flatMap(row -> {
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

//         Merge the two streams by using cartesian transformation
        JavaDStream<Point> mergedPoints = pointsDstream
//        JavaDStream<Point> mergedPositions = pointsDstream
                .transform(rdd -> {
                    // Merge the two RDDs by using cartesian transformation = cross join
                    JavaPairRDD<Integer, Tuple2<Point, Point>> joinedRDD = waypointsRDD
                            .cartesian(rdd)
                            .mapToPair(row -> new Tuple2<>(row._1._1, new Tuple2<>(row._1._2, row._2)))
                            .partitionBy(customPartitioner);

                    return joinedRDD
                            .filter(tuple -> tuple._2._1().getCellId() == tuple._2._2().getCellId() || tuple._2._1().getCellId() == -1 || tuple._2._2().getCellId() == -1)
                            // after repartition returns to the allocated cells the points of the AIS receiver
                            .flatMap( tuple->{
                                Point waypoint = tuple._2._1();
                                Point point = tuple._2._2();
                                List<Point> points =  new ArrayList<>();
                                //assign on the point the cellid of it;s tuple/waypoint
                                points.add(new Point(point.getDatetime(),point.getLatitude(),point.getLongitude(),waypoint.getCellId(),point.getTrajectoryId()));
                                points.add(waypoint);
                                return points.iterator();
                            })
                            .distinct()
                            ;
                });

        // Build trajectories from the merged positions
        JavaPairDStream<Tuple2<Integer, Integer>, Trajectory> trajectoryStream = mergedPoints
                .mapToPair(p -> new Tuple2<>(new Tuple2<>(p.getTrajectoryId(), p.getCellId()), p))
                .groupByKey(customPartitioner)
                .mapValues(pointList -> new Trajectory(IteratorUtils.toList(pointList.iterator()),pointList.iterator().next().getCellId()));

        JavaDStream<Tuple2<String, Tuple2<Trajectory, Trajectory>>> joined = trajectoryStream.transform(rdd ->
                rdd.cartesian(rdd)
                        .partitionBy(customPartitioner)
                        .filter(t -> t._1._1._1.compareTo(t._2._1._1) < 0)
                        .map(t -> {
                            Integer id1 = t._1._1._1;
                            Integer id2 = t._2._1._1;
                            Integer cell1 = t._1._1._2;
                            Integer cell2 = t._2._1._2;
                            Trajectory traj1 = t._1._2;
                            Trajectory traj2 = t._2._2;
                            // filter accordingly to calculate the distance of the positions that are on the same cell
                            if (!id1.equals(id2) && (( id1 > 100 && id2 < 100 ) || ( id1 < 100 && id2 > 100 ) ) && (cell1.equals(cell2))){
                                return new Tuple2<>(id1 + "-" + id2 +" cell:" + cell1+","+cell2, new Tuple2<>(traj1, traj2));
                            }
                            return null;
                        }).filter(row -> row != null).distinct()
        );

        joined.foreachRDD(rdd -> {
            System.out.println("=============================================");
            System.out.println("Number of comparisons: " + rdd.count());
            System.out.println("---------------------------------------------");
            rdd.foreach(pair -> {
                String idPair = pair._1();
                Trajectory traj1 = pair._2()._1();
                Trajectory traj2 = pair._2()._2();
                double distance = traj1.lcsSimilarity(traj2);
                System.out.println(idPair + ": " + distance);
            });
        });

//        joined.foreachRDD(rdd -> {
//            System.out.println("=============================================");
//            System.out.println("Number of comparisons: " + rdd.count());
//            System.out.println("---------------------------------------------");
//            rdd.foreach(pair -> {
//                String idPair = pair._1();
//                Trajectory traj1 = pair._2()._1();
//                Trajectory traj2 = pair._2()._2();
//
//                System.out.println(idPair);
//                System.out.println(traj1.getPoints().size() + ": " + traj1.getPoints().get(0).getCellId() +" id:"+ traj1.getPoints().get(0).getTrajectoryId());
//                System.out.println(traj2.getPoints().size() + ": " + traj2.getPoints().get(0).getCellId() +" id:"+ traj2.getPoints().get(0).getTrajectoryId());
//                System.out.println();
//            });
//        });
        joined.foreachRDD(rdd-> System.out.println(rdd.getNumPartitions()));

//         Start the Spark Streaming context
        jssc.start();

        // Wait for the job to end
        jssc.awaitTermination();
    }
}
