package com.example.bigdata
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{split, col, to_timestamp, hour}

object Facts {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Facts")
      //      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val username = System.getProperty("user.name");

    import spark.implicits._

    // LOADING MAIN DATA

    val mainScotland = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(s"/user/$username/labs/spark/uk-traffic/mainDataScotland.csv") //X: dane musza byc tutaj zaladowane

    val mainSouthEngland = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(s"/user/$username/labs/spark/uk-traffic/mainDataSouthEngland.csv")

    val mainNorthEngland = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(s"/user/$username/labs/spark/uk-traffic/mainDataNorthEngland.csv")

    val allTraffic = mainScotland
      .union(mainSouthEngland)
      .union(mainNorthEngland)


    // LOADING WEATHER
   def getWeatherConditionsFromLine(line: String): String = {
      val pattern = """^.+ of (.+) on (.+) at (.+) the following weather conditions were reported: (.+)$""".r
      line match {
        case pattern(local_authoirty_ons_code, date, time, conditions) => local_authoirty_ons_code + ";" + date + " " + time + ";" + conditions
        case _ => "None"
      }
    }

    val weatherWithTime = spark.read.textFile(s"/user/ventus_piotrek/labs/spark/uk-traffic/weather.txt")
      .map(line => getWeatherConditionsFromLine(line))
      .withColumn("splitted", split($"value", ";"))
      .select(
        $"splitted".getItem(0).as("local_authoirty_ons_code").cast("string"),
        $"splitted".getItem(1).as("timestamp1"),
        $"splitted".getItem(2).as("conditions").cast("string")
      )
      .withColumn("timestamp", to_timestamp($"timestamp1", "dd/MM/yyyy HH:mm"))
      .withColumn("Year", functions.year(col("timestamp")))
      .withColumn("Month", functions.month(col("timestamp")))
      .withColumn("Day", functions.dayofmonth(col("timestamp")))
      .withColumn("Hour", hour(col("timestamp")))
      .drop("timestamp1")
      .drop("timestamp")
      .distinct()



    // TRANSFORMATIONS

    val vehicle_types = Seq("pedal_cycles", "two_wheeled_motor_vehicles", "cars_and_taxis", "buses_and_coaches", "lgvs", "hgvs_2_rigid_axle", "hgvs_3_rigid_axle", "hgvs_4_or_more_rigid_axle", "hgvs_3_or_4_articulated_axle", "hgvs_5_articulated_axle", "hgvs_6_articulated_axle")

    val allTrafficWithTime = allTraffic.flatMap(r => vehicle_types.zipWithIndex.map(v =>
      (r.getInt(0), r.getInt(2), r.getTimestamp(3), r.getInt(4), r.getString(5), r.getInt(v._2 + 17), v._1, r.getString(6), r.getString(7))))
      .toDF("ID", "year", "timestampDate", "hour", "local_authoirty_ons_code", "vehicle_count", "vehicle_type", "road_name", "road_category")


    val allTrafficWithTimeAndWeather = allTrafficWithTime.join(weatherWithTime,
      weatherWithTime("Year") === allTrafficWithTime("year") &&
      weatherWithTime("Month") === functions.month(allTrafficWithTime("timestampDate")) &&
      weatherWithTime("Day") === functions.dayofmonth(allTrafficWithTime("timestampDate")) &&
      weatherWithTime("Hour") === allTrafficWithTime("hour") &&
        weatherWithTime("local_authoirty_ons_code") === allTrafficWithTime("local_authoirty_ons_code")
    ).select(allTrafficWithTime("ID"), $"conditions")


    val timeDF = spark.sql("SELECT * FROM czas")

    val trafficTimes = allTrafficWithTime.join(timeDF,
      to_timestamp(timeDF("data")) === allTrafficWithTime("timestampDate") &&
      timeDF("godzina") === allTrafficWithTime("hour")
    ).select(allTrafficWithTime("ID").as("id"), timeDF("id").as("id_czasu"))


    val typesDF = spark.sql("SELECT * FROM typy_pojazdow")

    val trafficTypes = allTrafficWithTime.join(typesDF,
      typesDF("typ") === allTrafficWithTime("vehicle_type")
    ).select(allTrafficWithTime("ID").as("id"), typesDF("id").as("id_pojazdu"))


    val weatherDF = spark.sql("SELECT * FROM pogoda")

    val trafficWeather = allTrafficWithTimeAndWeather.join(weatherDF,
      weatherDF("opis_pogody") === allTrafficWithTimeAndWeather("conditions")
    ).select(allTrafficWithTimeAndWeather("ID").as("id"), weatherDF("id").as("id_pogody"))


    val locationDF = spark.sql("SELECT * FROM miejsca")

    val trafficLocation = allTrafficWithTime.join(locationDF,
      locationDF("kod_ons_obszaru") === allTrafficWithTime("local_authoirty_ons_code") &&
        locationDF("nazwa_drogi") === allTrafficWithTime("road_name") &&
        locationDF("kategoria_drogi") === allTrafficWithTime("road_category")
    ).select(allTrafficWithTime("ID").as("id"), locationDF("id").as("id_miejsca"), allTrafficWithTime("vehicle_count"))


    val finalTable = trafficTimes
      .join(trafficLocation, trafficLocation("id") === trafficTimes("id"))
      .join(trafficTypes, trafficTypes("id") === trafficTimes("id"))
      .join(trafficWeather, trafficWeather("id") === trafficTimes("id"))
      .select($"id_czasu", $"id_pojazdu", $"id_miejsca", $"id_pogody", trafficLocation("vehicle_count").as("liczba_pojazdow"))


    //    finalTable.show()
    //    finalTable.printSchema()
    finalTable.write.insertInto("fakty")
    println("Załadowano tabele faktow")

  }
}
