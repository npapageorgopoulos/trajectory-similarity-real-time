package com.distanceclustering;

import com.config.StaticVars;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class BruteForce {
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
                .csv(StaticVars.routesOutputSource.concat(".csv"));

        Dataset<Row> points = spark.read()
                .option("inferSchema","true")
                .option("delimiter",";")
                .option("header","true")
                .csv(StaticVars.sampleAISdata)
                .withColumn("id",monotonically_increasing_id().plus(1))
                .select(col("id"),col("imo_nr").alias("imo"),col("date_time_utc"),col("lon"),col("lat"),col("true_heading"));

        System.out.println(waypoints.count());
        System.out.println(points.count());

        Dataset<Row> joinedDF = points.join(waypoints)
                        .na().drop()
                        .withColumn("distance",callUDF("dist",col("lon"),col("longitude"),col("lat"),col("latitude")));

        Dataset<Row> groupedDF = joinedDF
                .groupBy("route_name","id")
                .agg(min("distance").alias("distance"),first("route_name").alias("route"))
                .groupBy("id")
                .agg(min("distance").alias("distance to closest route"),first("route").alias("route"))
//                .orderBy("id")
                ;
        groupedDF.show();
//
//        groupedDF.show(5);
////        groupedDF.write().option("header",true)
////                .csv("/src/test/resources/output/");
//
//        System.out.println(groupedDF.count());





    }
}
