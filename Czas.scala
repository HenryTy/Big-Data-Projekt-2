package com.example.bigdata
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, hour, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SparkSession, functions}

object Czas {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Czas")
      .enableHiveSupport()
      .getOrCreate()

    val username = System.getProperty("user.name")
    import spark.implicits._



    val mainNorthEngland = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataNorthEngland.csv");


    val mainSouthEngland = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataSouthEngland.csv");

    val mainScotland = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataScotland.csv");


    val startTimeDF = mainNorthEngland
      .union(mainSouthEngland)
      .union(mainScotland)
      .select("count_date", "hour", "year")


    val timeDF = startTimeDF
      .withColumn("godzina", col("hour"))
      .withColumn("data", functions.date_format(col("count_date"), "yyyy-MM-dd"))
        .withColumn("rok", col("year"))
        .withColumn("miesiac", functions.month(col("count_date")))
          .withColumn("kwartal", functions.quarter(col("count_date")))
          .withColumn("dzien_tygodnia", functions.date_format(col("count_date"), "E"))
          .drop("count_date")
          .distinct()

    val allTimeDF = timeDF.withColumn("Time_id", monotonically_increasing_id).select("Time_id", "rok", "miesiac", "data", "godzina", "kwartal", "dzien_tygodnia")

    allTimeDF.write.insertInto("Czas")
    println("Załadowano tabele 'Czas'")

  }
}
