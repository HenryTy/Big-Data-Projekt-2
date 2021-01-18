package com.example.bigdata
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

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
      .dropDuplicates(Array("ID"));


    // LOADING WEATHER
    def getWeatherConditionsFromLine(line: String): String = {
      val pattern = """^ of (.+) on (.+) at (.+) the following weather conditions were reported: (.+)$""".r
      line match {
        case pattern(local_authoirty_ons_code, date, time, conditions) => local_authoirty_ons_code + ";" + date + ";" + time.substring(0, 2) + ";" + conditions
        case _ => "None"
      }
    }

    val weatherWithTime = spark.read.textFile(s"/user/$username/labs/spark/uk-traffic/weather.txt")
      .map(line => getWeatherConditionsFromLine(line))
      .filter(!_.equals("None"))
      .withColumn("splitted", split($"value", ";"))
      .select(
        $"splited".getItem(0).as("local_authoirty_ons_code").cast("string"),
        $"splited".getItem(1).as("date").cast("string"), //X: cast date as string
        $"splited".getItem(2).as("hour").cast("int"),
        $"splited".getItem(3).as("conditions").cast("string"))
      .distinct()


    // TRANSFORMATIONS

    val vehicle_types = Seq("pedal_cycles", "two_wheeled_motor_vehicles", "cars_and_taxis", "buses_and_coaches", "lgvs", "hgvs_2_rigid_axle", "hgvs_3_rigid_axle", "hgvs_4_or_more_rigid_axle", "hgvs_3_or_4_articulated_axle", "hgvs_5_articulated_axle", "hgvs_6_articulated_axle")

    val allTrafficWithTime = allTraffic.flatMap(r => vehicle_types.zipWithIndex.map(v =>
      (r.getString(3).substring(0, 10), r.getInt(4), r.getString(5), r.getInt(v._2 + 17), v._1, r.getString(6), r.getString(7))))
      .toDF("date", "hour", "local_authoirty_ons_code", "vehicle_count", "vehicle_type", "road_name", "road_category") //X: w tym miejscu substring?


    val allTrafficWithTimeAndWeather = allTraffic.join(weatherWithTime,
      weatherWithTime("hour") === allTrafficWithTime("hour") &&
        weatherWithTime("date") === allTrafficWithTime("date") &&
        weatherWithTime("local_authoirty_ons_code") === allTrafficWithTime("local_authoirty_ons_code")
    ).select(allTrafficWithTime("ID"), $"conditions")

    val timeDF = spark.sql("SELECT * FROM czas")

    val trafficTimes = allTrafficWithTime.join(timeDF,
      timeDF("godzina") === allTrafficWithTime("hour") &&
        timeDF("data") === allTrafficWithTime("date")
    ).select(allTrafficWithTime("ID").as("id"), $"id_czasu")


    val typesDF = spark.sql("SELECT * FROM typy_pojazdow")

    val trafficTypes = allTrafficWithTime.join(typesDF,
      typesDF("typ") === allTrafficWithTime("vehicle_type")
    ).select(allTrafficWithTime("ID").as("id"), $"id_pojazdu")


    val weatherDF = spark.sql("SELECT * FROM pogoda")

    val trafficWeather = allTrafficWithTimeAndWeather.join(weatherDF,
      weatherDF("opis_pogody") === allTrafficWithTime("conditions")
    ).select(allTrafficWithTimeAndWeather("ID").as("id"), $"id_pogody")


    val locationDF = spark.sql("SELECT * FROM miejsca")

    val trafficLocation = allTrafficWithTime.join(locationDF,
      weatherDF("kod_ons_obszaru") === allTrafficWithTime("local_authoirty_ons_code") &&
        weatherDF("nazwa_drogi") === allTrafficWithTime("road_name") &&
        weatherDF("kategoria_drogi") === allTrafficWithTime("road_category")
    ).select(allTrafficWithTime("ID").as("id"), $"id_miejsca")


    val finalTable = trafficTimes
      .join(trafficLocation, trafficLocation("id") === trafficTimes("id"))
      .join(trafficTypes, trafficTypes("id") === trafficTimes("id"))
      .join(trafficWeather, trafficWeather("id") === trafficTimes("id"))
      .select($"id_czasu", $"id_pojazdu", $"id_miejsca", $"id_pogody", $"vehicle_count".as("liczba_pojazdow")) //?: vehicle_count

    //finalDataDF.printSchema()
    //finalDataDF.show()
    finalTable.write.insertInto("fakty")
    //finalTable.printSchema()
    println("Załadowano tabele faktow")

  }
}
