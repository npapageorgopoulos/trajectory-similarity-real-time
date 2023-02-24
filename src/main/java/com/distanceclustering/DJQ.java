package com.distanceclustering;

import com.config.StaticVars;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class DJQ {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("djq").master("local[*]").getOrCreate();

        spark.udf().register("dist", (Double x1, Double x2, Double y1 ,Double y2) -> {
            return Math.hypot(x1-x2,y1-y2);
        }, DataTypes.DoubleType);

        Dataset<Row> waypoints = spark.read()
                .option("inferSchema","true")
                .option("header","true")
                .csv(StaticVars.wpReplicatedSource)
                .repartitionByRange(col(("cell")))
                ;

        Dataset<Row> points = spark.read()
                .option("inferSchema","true")
                .option("delimiter",";")
                .option("header","true")
                .csv(StaticVars.dataAIS)
                .withColumn("id",monotonically_increasing_id().plus(1))
//                .select(col("id"),col("imo_nr").alias("imo"),col("date_time_utc"),col("lon"),col("lat"),col("true_heading"))
                .select(col("id"),col("mmsi"),col("date_time_utc"),col("lon"),col("lat"),col("true_heading"))
                ;

//        points.groupBy("mmsi").count().show();
//        System.out.println(points.groupBy("mmsi").count().count());

        Dataset<Row> joinedDF = points.join(waypoints) //cross join of ais dataset and the ds that contains the replicated waypoints
                .na().drop()
                .repartitionByRange(col(("cell"))) //repartition by cell
                .withColumn("distance",callUDF("dist",col("lon"),col("longitude"),col("lat"),col("latitude"))) //calculating distance for each point-wp for every cell
                .filter(col("distance").leq(StaticVars.theta)) //filter the closest points to the cells
                ;

        //TODO id has to be removed
        Dataset<Row> groupedDF = joinedDF
                .groupBy("id","cell")
                .agg(
                        min("distance"),
                        first("mmsi").alias("mmsi"),
                        first("route_name").alias("route_name")
                )
                .groupBy("mmsi","route_name","cell")
                .agg(
                        count("route_name").alias("count")
                )
                .groupBy("mmsi","cell")
                .agg(
                        max("count"),
                        first("route_name").alias("route_name")
                )
                ;

//        System.out.println(groupedDF.rdd().getNumPartitions());

        groupedDF.javaRDD().foreach(
                row -> System.out.println(String.format("The vessel with MMSI %s is close to %s",row.get(0),row.get(3)))
        );

//        System.out.println(groupedDF.javaRDD().getNumPartitions());
/*  file output for testing purpose
        try{
            groupedDF
//                    .coalesce(1)
                    .write().option("header",true)
                    .mode("overwrite")
                    .csv(StaticVars.djqOutput);
        }catch (Exception e){
            System.err.println(e);
        }
*/
//        Scanner scanner = new Scanner(System.in);
//        String input = scanner.nextLine();

        spark.close();
    }
}
