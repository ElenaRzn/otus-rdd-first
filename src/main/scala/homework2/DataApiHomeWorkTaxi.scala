package homework2

import org.apache.flink.table.shaded.com.ibm.icu.text.SimpleDateFormat
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{broadcast, col, count, date_format, days, desc, hour, stddev}

import java.sql.Timestamp
import java.util.Properties

object DataApiHomeWorkTaxi extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"


  val taxiFactsDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiFactsDF.printSchema()
  println(taxiFactsDF.count())
  taxiFactsDF.show(10)

  val taxiZones = spark.read.option("header",true).option("inferSchema", true).csv("src/main/resources/data/taxi_zones.csv")
  taxiZones.printSchema()
  println(taxiZones.count())

  println("-- Task 1")

  val boroughPopularity = taxiFactsDF.join(broadcast(taxiZones), col("DOLocationID") === col("LocationId"), "left")
    .groupBy(col("Borough"))
    .count()
    .orderBy(col("count").desc)

  boroughPopularity.show(10)
//  boroughPopularity.write.parquet("boroughPopularity.parquet")

  println("-- Task 2")
  case class TaxiRide(
                       VendorID: Int,
                       tpep_pickup_datetime: Timestamp,
                       tpep_dropoff_datetime: Timestamp,
                       PULocationID: Int,
                       DOLocationID: Int,
                       payment_type: Int,
                       fare_amount: Double,
                       extra: Double,
                       mta_tax: Double,
                       tip_amount: Double,
                       tolls_amount: Double,
                       improvement_surcharge: Double,
                       total_amount: Double
                     )

  import spark.implicits._
  val dateFormat: SimpleDateFormat = new SimpleDateFormat("HH")

// *************************
//  вариант 1 - неоптимальный
//  val taxiFactsRdd1 = taxiFactsDF
//    .as[TaxiRide]
//    .rdd.groupBy(fact => dateFormat.format(fact.tpep_pickup_datetime))
//    .sortBy(fact => fact._2.size, false)
//    .foreach(x => println(x._1 + "->" + x._2.size))

  // *************************
  //  вариант 2 - итоговый
  val initialCount = 0;
  val addToCounts = (n: Int, t:Iterable[TaxiRide]) => n + t.size
  val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

  println("ddddddddddddddddddddddddddddd")
  val taxiFactsRdd = taxiFactsDF
    .as[TaxiRide]
    .rdd.groupBy(fact => dateFormat.format(fact.tpep_pickup_datetime))
    .aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    .sortBy(fact => fact._2, false)
    .map(x => s"${x._1} ${x._2}")


  taxiFactsRdd.persist()
  taxiFactsRdd.foreach(println)
  //TODO - раскомментируй
//  taxiFactsRdd.saveAsTextFile("count_by_hours.txt")


  // *************************
  //  Проверка через DataFrame
//  taxiFactsDF
//    .as[TaxiRide]
//    .withColumn("hour", hour(col("tpep_pickup_datetime")))
//    .groupBy(col("hour")).count()
//    .show

  //бщее количество поездок, среднее расстояние, среднеквадратическое отклонение, минимальное и максимальное расстояние
  println("-- Task 3")

  val connectionProperties = new Properties()

  connectionProperties.put("user", user)
  connectionProperties.put("password", password)
//  val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
//  connectionProperties.setProperty("Driver", driverClass)

  val statistics = taxiFactsDF
      .as[TaxiRide]
      .withColumn("days", date_format(col("tpep_pickup_datetime"), "dd-MM-yyyy"))
      .groupBy(col("days"))
    .agg(count("trip_distance"),
      functions.avg("trip_distance"),
      functions.min("trip_distance"),
      functions.max("trip_distance"),
      stddev("trip_distance"))

  statistics.show()
//    statistics.write.jdbc(url=url, table="test_result", connectionProperties=connectionProperties)



}

