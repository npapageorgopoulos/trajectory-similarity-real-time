import com.util.CustomPartition;
import com.util.Point;
import com.config.StaticVars;
import com.custom.Waypoint;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.text.SimpleDateFormat;

public class LSEDTester {

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

        //pairing with a key the input in order to join
        JavaDStream<Point> records = input
                //remove rows that have less columns
                .transform( rdd -> rdd.filter( row -> row.split(";").length == StaticVars.numOfRows ))
                .map(record -> {
                    String[] row = record.split(";");
                    Point p = new Point();

                    p.setMmsi(row[0]);
                    p.setDatetime(sdf.parse(row[3]));
                    p.setLongitude(Double.parseDouble(row[4]));
                    p.setLatitude(Double.parseDouble(row[5]));
                    return p;
                });

        //Waypoints and their replicates from local drive and remove of header
        JavaRDD<String> wpInput = jssc.sparkContext().textFile(StaticVars.wpReplicatedSource);
        String wpHeader = wpInput.first();
        JavaRDD<Waypoint> waypoints = wpInput
                //remove header of the input RDD
                .filter(wp -> !wp.equals(wpHeader))
                .map(wp -> {
                    String[] row = wp.split(",");

                    return new Waypoint(
                            row[3],
                            Integer.parseInt(row[4]),
                            Double.parseDouble(row[1]),
                            Double.parseDouble(row[2]),
                            Integer.parseInt(row[0])
                    );
                } );



        // Create a DStream that contains data from a CSV file
//        JavaDStream<double[][]> trajectories1 = jssc.textFileStream("path/to/file1.csv")
//                .map(line -> {
//                    String[] parts = line.split(",");
//                    int length = parts.length / 2;
//                    double[][] data = new double[length][2];
//                    for (int i = 0; i < length; i++) {
//                        data[i][0] = Double.parseDouble(parts[2 * i]);
//                        data[i][1] = Double.parseDouble(parts[2 * i + 1]);
//                    }
//                    return data;
//                });
//
//        // Create another DStream that contains data from another CSV file
//        JavaDStream<double[][]> trajectories2 = jssc.textFileStream("path/to/file2.csv")
//                .map(line -> {
//                    String[] parts = line.split(",");
//                    int length = parts.length / 2;
//                    double[][] data = new double[length][2];
//                    for (int i = 0; i < length; i++) {
//                        data[i][0] = Double.parseDouble(parts[2 * i]);
//                        data[i][1] = Double.parseDouble(parts[2 * i + 1]);
//                    }
//                    return data;
//                });
//

//        // Calculate the lock-step Euclidean distance between two trajectories
//        JavaDStream<Double> distances = trajectories1.zip(trajectories2).map(data -> {
//            double[][] t1 = data._1;
//            double[][] t2 = data._2;
//            double sum = 0.0;
//            for (int i = 0; i < t1.length; i++) {
//                sum += Math.pow(t1[i][0] - t2[i][0], 2) + Math.pow(t1[i][1] - t2[i][1], 2);
//            }
//            return Math.sqrt(sum);
//        });
//
//        // Print the distances to the console
//        distances.print();
        wpInput.foreach(row -> System.out.println(row));
//        records.print();

        // Start the Spark Streaming context
        jssc.start();

        // Wait for the job to end
        jssc.awaitTermination();
    }
}

