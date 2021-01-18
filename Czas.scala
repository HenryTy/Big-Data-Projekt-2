package com.example.bigdata
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, hour, monotonically_increasing_id, row_number}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SparkSession, functions}

object Czas {
	def main(args: Array[String]) {
		val spark = SparkSessioon
			.builder()
			.appName("Czas")
			.enableHiveSupport()
			.getOrCreate()
			
		val username = System.getProperty("user.name")
		import spark.implicts._
		
		
		
		val mainNorthEngland = spark.read.format("org.apache.spark.csv")
		  .option("header", true)
		  .option("inferSchema", true)
		  .csv(s"/user/$username/labs/spark/externaldata/mainNorthEngland.csv");
		  
		  
		val mainSouthEngland = spark.read.format("org.apache.spark.csv")
		  .option("header", true)
		  .option("inferSchema", true)
		  .csv(s"/user/$username/labs/spark/externaldata/mainSouthEngland.csv");	

		val mainScotland = spark.read.format("org.apache.spark.csv")
		  .option("header", true)
		  .option("inferSchema", true)
		  .csv(s"/user/$username/labs/spark/externaldata/mainScotland.csv");	
		  
		  
		val startTimeDF = mainNorthEngland
			.union(mainSouthEngland)
			.union(mainScotland)
			.select("count_date")
			
			
		val timeDF = startTimeDF
			.withColumn("godzina", col("hour"))
			.withColumn("data", functions.date_format(col("count_date")))
			.withColumn("rok", col("year"))
			.withColumn("miesiac", functions.month(col("count_date")))
			.withColumn("kwartal", functions.quarter(col("count_date")))
			.withColumn("dzien_tygodnia", functions.dayofweek(col("count_date")))
			.drop("count_date")
			.distinct()
			
		val allTimeDF = timeDF.withColumn("Time_id", monotonically_increasing_id).select("Time_id", "rok", "miesiac", "data", "godzina", "kwartal", "dzien_tygodnia")
		
		val window = Window.orderBy($"Time_id")
		  
		val finalDataDF = allTimeDF.withColumn("Time_id", row_number.over(window))
		
		finalDataDF.write.insertInto("Czas")
		println("Załadowano tabele 'Czas'")


}