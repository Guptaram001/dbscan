package spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DataFrameAnalytics {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();

        Dataset<Row> df =spark.read().format("json")
                .option("header", false)
                .option("inferSchema", true)
                .option("multiLine", true)
                .load("src/main/resources/meteorite.json");

        df.printSchema();

        System.out.println("average mass");
        averageMass(df);

        System.out.println("fall classification");
        fallClassification(df);

        System.out.println("no coordinates");
        noCoordinates(df);

        System.out.println("fake entry");
        dataCleaning(df);

        System.out.println("distance");
        pointDistance(spark, df);

        System.out.println("add data points");
        addDataPoints(spark, df);

        spark.stop();
    }

    public static void  averageMass(Dataset<Row> df) {
        df.select(avg("mass").alias("average mass")).show();
    }


    public static void  fallClassification(Dataset<Row> df) {
        df.groupBy("fall").count().show();
    }


    public static void  noCoordinates(Dataset<Row> df) {
        df.select("*").filter(col("geolocation").isNull()).orderBy(col("year").desc()).show();
    }

    public static void dataCleaning(Dataset<Row> df) {
        df.filter(col("geolocation.coordinates").isNotNull())
                .select(
                        col("id"),
                        col("name"),
                        col("reclat"),
                        col("reclong"),
                        col("geolocation.coordinates")
                )
                .filter(
                        abs(col("reclat")
                                .minus(col("geolocation.coordinates").getItem(1))).gt(0.1)
                                .or(abs(col("reclong")
                                                .minus(col("geolocation.coordinates").getItem(0))).gt(0.1))
                )
                .show(false);
    }


    public static void  pointDistance(SparkSession sparkSession,Dataset<Row> df) {
        sparkSession.udf().register("haversine", (UDF4<Double, Double, Double, Double, Double>
                ) DataFrameAnalytics::haversineDistance, DataTypes.DoubleType);

        df.filter(col("geolocation.coordinates").isNotNull()).as("left")
                        .crossJoin(df.filter(col("geolocation.coordinates").isNotNull()).as("right"))
                                .where(col("left.id").notEqual(col("right.id")) )
                .select("left.id", "left.geolocation", "right.id", "right.geolocation")
                .withColumn(
                        "dist",
                        callUDF("haversine",
                                col("left.geolocation.coordinates").getItem(0),
                                col("left.geolocation.coordinates").getItem(1),
                                col("right.geolocation.coordinates").getItem(0),
                                col("right.geolocation.coordinates").getItem(1))
                ).orderBy("dist").show(15);
    }

    public static final double R = 6372.8;
    public static double haversineDistance(double lon1, double lat1, double lon2, double lat2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);

        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.sin(dLon/2) * Math.sin(dLon/2) * Math.cos(lat1Rad) * Math.cos(lat2Rad);
        double c = 2 * Math.asin(Math.sqrt(a));
        return R * c;
    }


    public static void addDataPoints(SparkSession sparkSession,Dataset<Row> df) {

        Encoder<GeoLocation> encoder = Encoders.bean(GeoLocation.class);

        Dataset<GeoLocation> originLocations= df.filter(col("geolocation").isNotNull())
                .select(col("geolocation.coordinates").getItem(0).alias("longitude"),
                        col("geolocation.coordinates").getItem(1).alias("latitude"),
                col("geolocation.type").alias("type")).as(encoder);

        originLocations.printSchema();

        List<GeoLocation> coordinatesList = Arrays.asList(
                new GeoLocation("Point", 1.0, 2.0),
                new GeoLocation("Point", 3.0, 4.0),
                new GeoLocation("Point", 5.0, 6.0));

        Dataset<GeoLocation> addedLocations=sparkSession.createDataset(coordinatesList, encoder);

        addedLocations.printSchema();

        addedLocations.union(originLocations).show(10);

    }
}
