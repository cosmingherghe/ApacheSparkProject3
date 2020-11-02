package dev.cosmingherghe.spark;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;


public class Application {

    public static void main(String[] args) {

        //Turn off INFO log entries
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        // Create a spark session
        SparkSession spark = SparkSession.builder()
                .appName("Combine 2 Datasets")
                .master("local")
                .getOrCreate();

        // Parse out the document durham-parks.json
        Dataset<Row> durhamDf = buildDurhamParksDataFrame(spark);
        durhamDf.printSchema();
        durhamDf.show();

        // Parse out the document philadelphia_recreations.csv
        Dataset<Row> philadelphiaDf = buildPhiladelphiaParksDataFrame(spark);
        philadelphiaDf.printSchema();
        philadelphiaDf.show();

//        combineDataFrames(durhamDf, philadelphiaDf);

    }

    private static Dataset<Row> buildDurhamParksDataFrame(SparkSession spark) {

        Dataset<Row> df = spark.read().format("json").option("multiline", true)
                .load("src/main/resources/durham-parks.json");

        // create our own fields referencing fields that have not been dropped.
        df = df.withColumn("park_id", concat(df.col("datasetid"), lit("_"),
                                                df.col("fields.objectid"), lit("_Durham")))
            .withColumn("park_name", df.col("fields.park_name"))
            .withColumn("city", lit("Durham"))
            .withColumn("has_playground", df.col("fields.playground"))
            .withColumn("address", df.col("fields.address"))
            .withColumn("zip code", df.col("fields.zip"))
            .withColumn("land_in_acres", df.col("fields.acres"))
            .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
            .withColumn("geoY", df.col("geometry.coordinates").getItem(1))

            //dropping all the old fields that we don't need anymore.
            .drop("recordid")
            .drop("fields")
            .drop("geometry")
            .drop("datasetid")
            .drop("record_timestamp");

        return df;
    }

    private static Dataset<Row> buildPhiladelphiaParksDataFrame(SparkSession spark) {

        Dataset<Row> df = spark.read().format("csv").option("multiline", true)
                .option("header", true)
                .load("src/main/resources/philadelphia_recreations.csv");

//        df = df.filter(lower(df.col("USE_")).like("%park%"));
        df = df.filter("lower(USE_) like '%park%' ");

        df = df.withColumn("park_id", concat(lit("phil_"), df.col("OBJECTID")))
            .withColumnRenamed("ASSET_NAME", "park_name")
            .withColumn("city", lit("Philadelphia"))
            .withColumnRenamed("ADDRESS", "address")
            .withColumn("has_playground", lit("UNKNOWN"))
            .withColumnRenamed("ZIPCODE",  "zipcode")
            .withColumnRenamed("land_in_acres","ACREAGE")
            .withColumn("geoX", lit("UNKNOWN"))
            .withColumn("geoY", lit("UNKNOWN"))

        //dropping all the old fields that we don't need anymore.
            .drop("SITE_NAME")
            .drop("OBJECTID")
            .drop("CHILD_OF")
            .drop("TYPE")
            .drop("USE_")
            .drop("DESCRIPTION")
            .drop("SQ_FEET")
            .drop("ALLIAS")
            .drop("CHRONOLOGY")
            .drop("NOTES")
            .drop("EDITED_BY")
            .drop("OCCUPANT")
            .drop("TENANT")
            .drop("LABEL")
        ;

        return df;
    }
}