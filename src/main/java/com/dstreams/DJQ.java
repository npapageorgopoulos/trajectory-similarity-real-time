package com.dstreams;

import com.config.CustomPartition;
import com.config.StaticVars;
import com.distancejoin.Point;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DJQ {

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        CustomPartition customPartitioner = new CustomPartition(StaticVars.xSeperations * StaticVars.ySeperations);

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("real time trajectory similarity");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        //listening on a TCP socket, every 10 sec
        JavaReceiverInputDStream<String> inputData = jssc.socketTextStream("localhost", 8988);

        //pairing with a key the input in order to join
        JavaPairDStream<Integer, String> records = inputData.mapToPair(record -> new Tuple2<>(1,record));

        //Waypoints and their replicates from local drive and remove of header
        JavaRDD<String> wpInput = jssc.sparkContext().textFile(StaticVars.wpReplicatedSource);
        String wpHeader = wpInput.first();
        JavaPairRDD<Integer, String> waypoints = wpInput.filter(wp -> !wp.equals(wpHeader))
                .mapToPair(wp -> new Tuple2<>(1, wp));

        //cross join for each batch of ais data and waypoints
        JavaPairDStream<Tuple3<String, Date, Integer>, Point> joined = records.transformToPair(rdd -> rdd.join(waypoints))
                .mapToPair(val -> new Tuple2<>(Integer.parseInt(val._2._2.split(",")[0]), val._2))
                //custom partitioner: partition each batch by key -> cell
                .transformToPair(rdd -> rdd.partitionBy(customPartitioner))
                //map the row string messages into a POJO for each record
                .mapToPair(val -> {
                    Point p = new Point();
                    String[] recordString = val._2._1.split(",");
                    String[] wpString = val._2._2.split(",");

                    p.setMmsi(recordString[0]);
                    p.setDatetime(sdf.parse(recordString[3]));
                    p.calculateDistance(Double.parseDouble(recordString[4]),
                            Double.parseDouble(wpString[2]),
                            Double.parseDouble(recordString[5]),
                            Double.parseDouble(wpString[1])
                    );
                    p.setCell(Integer.parseInt(wpString[0]));
                    p.setRoute(wpString[3]);
                    p.setWaypoint_id(Integer.parseInt(wpString[4]));
                    return new Tuple2<>(new Tuple3<>(p.getMmsi(), p.getDatetime(), p.getCell()), p);
                });

        //First aggregation in order to find the closest waypoints for each point in each partition
        JavaPairDStream<Tuple3<String, Date, Integer>, Point> minDistanceReduce = joined
                .reduceByKey((point, point2) -> point.getDistance() < point2.getDistance() ? point : point2, customPartitioner);

        //Map the above result in order to have in key a tuple with the values of (MMSI, Route name, cell_id) and add counter in value
        JavaPairDStream<Tuple3<String, String, Integer>, Tuple2<Point, Long>> countMapper = minDistanceReduce.mapToPair(val -> {
            Tuple3<String, String, Integer> key = new Tuple3<>(val._2.getMmsi(), val._2.getRoute(), val._2.getCell());
            Tuple2<Point, Long> value = new Tuple2<>(val._2, 1L);

            return new Tuple2<>(key, value);
        });

        //Reduce the counters by key
        JavaPairDStream<Tuple3<String, String, Integer>, Tuple2<Point, Long>> countReduce = countMapper.reduceByKey((point1, point2) -> new Tuple2<>(point1._1, point1._2 + point2._2),customPartitioner);

        //Reduce the counters to get the max
        JavaPairDStream<Tuple3<String, String, Integer>, Tuple2<Point, Long>> result = countReduce.reduceByKey((point1, point2) -> point1._2 > point2._2 ? point1 : point2, customPartitioner);


//        result.foreachRDD( rdd -> System.out.println(rdd.getNumPartitions()));
//        result.count().print();
//        result.glom().print();
//        result.print(100);

        result.foreachRDD( rdd -> {
            rdd.foreach( print -> {
                Point p = print._2._1;
                System.out.println( String.format("The vessel with MMSI %s is close to %s",p.getMmsi(),p.getRoute()));
            });
        });


        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
    }

}
