package routeconversion;

import config.StaticVars;
import distancejoin.Waypoint;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class WpReplication {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("waypoints").master("local[*]").getOrCreate();
//        spark.conf().set("spark.sql.shuffle.partitions","12");

        Dataset<Row> waypoints = spark.read()
                .option("inferSchema","true")
                .option("header","true")
                .csv(StaticVars.routesOutputSource.concat(".csv"));


        //read ais data in order to count limints of grid
        Dataset<Row> ais = spark.read()
                .option("inferSchema","true")
                .option("delimiter",";")
                .option("header","true")
                .csv(StaticVars.dataAIS)
                .withColumn("id",monotonically_increasing_id().plus(1))
                .select(col("id"),col("imo_nr").alias("imo"),col("date_time_utc"),col("lon"),col("lat"),col("true_heading"));


        //get limits according to ais data
        Dataset<Row> limits = ais.agg(
                max("lat").alias("maxLat"),
                min("lat").alias("minLat"),
                max("lon").alias("maxLon"),
                min("lon").alias("minLon"));
        Double maxLat = ((Double) limits.first().getAs("maxLat")) + 0.00000001;
        Double maxLon = ((Double) limits.first().getAs("maxLon")) + 0.00000001;
        Double minLat = limits.first().getAs("minLat");
        Double minLon = limits.first().getAs("minLon");

        /*
        System.out.println(maxLat);
        System.out.println(maxLon);
        System.out.println(minLat);
        System.out.println(minLon);
        */

        //calculation of length/width for each cell of grid
        double partitionHeight = (maxLat - minLat) / StaticVars.ySeperations;
        double partitionWidth = (maxLon - minLon) / StaticVars.xSeperations;
         System.out.println("cell width: "+partitionWidth);
         System.out.println("cell height: "+partitionHeight);

        //Creation of a 2D Grid according to the declared axis values
        //Assign cell ID into a 2D Array in order to replicate wp into neighbour cells
        int N=0;
        int[][] cellNums = new int[StaticVars.xSeperations][StaticVars.ySeperations];
        for (int i=0; i<StaticVars.xSeperations; i++){
            for (int j=0; j<StaticVars.ySeperations; j++){
                cellNums[i][j]=N++;
            }
        }


        //cellId to waypoints --> allocating grid cells for waypoints
        waypoints = waypoints.withColumn("cell", (((col("longitude")
                .minus(minLon)).divide(partitionWidth)).cast(DataTypes.IntegerType))
                .multiply(StaticVars.xSeperations)
                .plus(((col("latitude").minus(minLat))
                        .divide(partitionHeight)).cast(DataTypes.IntegerType))
        );


        JavaRDD<distancejoin.Waypoint> waypointsRDD = waypoints
                .javaRDD() // transform dataframe to rdd of "Waypoint" objects
                .map(row -> new distancejoin.Waypoint((String) row.get(0), (Integer) row.get(1), (Double) row.get(2), (Double) row.get(3), (Integer) row.get(4)));

//        waypointsRDD.foreach(wp -> {
//            System.out.println(wp.toString());
//        });
//        System.out.println(waypointsRDD.count());


        //Creation of new RDD that has allocated waypoints and the replicated to the neighbour cells
        JavaRDD<distancejoin.Waypoint> replicatedCellRDD = waypointsRDD.map(wp -> {
            List<distancejoin.Waypoint> waypointList = new ArrayList<>();

            //push in each list the original waypoint
            waypointList.add(new distancejoin.Waypoint(wp.getRoute_name(), wp.getWaypoint_id(), wp.getLatitude(), wp.getLongitude(), wp.getCell()));

            int i = -1, j = -1; //for each cellId get its "coords" as 2 variables i and j
            for (int k = 0; k < StaticVars.xSeperations; ++k) {
                for (int l = 0; l < StaticVars.ySeperations; ++l) {
                    if (cellNums[k][l] == wp.getCell()) {
                        i = k;
                        j = l;
                    }
                }
            }
            //having exact the position of the cell in grid, get celId of neighbour cells and add them in the list
            for (int x = Math.max(0, i - 1); x <= Math.min(i + 1, StaticVars.xSeperations); x++) {
                for (int y = Math.max(0, j - 1); y <= Math.min(j + 1, StaticVars.ySeperations); y++) {
                    if ((x < StaticVars.xSeperations && y < StaticVars.ySeperations) && (x != i || y != j)) {
                        distancejoin.Waypoint waypoint = new distancejoin.Waypoint(wp.getRoute_name(), wp.getWaypoint_id(), wp.getLatitude(), wp.getLongitude(), cellNums[x][y]);
                        waypointList.add(waypoint);
                    }
                }
            }
            return waypointList; //return a list that includes the original waypoint and the replications of it in the neighbour cells
        }).flatMap(list -> list.iterator()); //maps each list and returns each waypoint intact

        //transform the RDD into Dataframe in order to write in a file
        Dataset<Row> replicatedCellDF = spark.createDataFrame(replicatedCellRDD, Waypoint.class);

        try{
            replicatedCellDF.write().option("header",true)
                    .csv(StaticVars.wpReplicatedSource);
        }catch (Exception e){
            System.out.println("filepath already exist");
        }






//        replicatedCellDF.show();
//
//        System.out.println(replicatedCellDF.count());

//        replicatedCellRDD.saveAsTextFile(StaticVars.wpReplicatedOutputSource);

        spark.close();
    }
}
