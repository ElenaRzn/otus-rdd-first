package homework2

import org.apache.flink.table.shaded.com.ibm.icu.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, desc, hour}

import java.sql.Timestamp

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

  //val dateFormat: SimpleDateFormat = new SimpleDateFormat("hh")
  //   taxiFactsDF.rdd
  //    .foreach(r => {
  //      println(dateFormat.format(r.getTimestamp(2)))
  //    }
  //    )
  //*************************
  println("ddddddddddddddddddddddddddd")
  val taxiFactsRdd1 = taxiFactsDF
    .as[TaxiRide]
    .rdd.groupBy(fact => dateFormat.format(fact.tpep_pickup_datetime))
    .sortBy(fact => fact._2.size, false)
    .foreach(x => println(x._1 + "->" + x._2.size))

  println("ddddddddddddddddddddddddddd")
  val initialCount = 0;
  val addToCounts = (n: Int, t:Iterable[TaxiRide]) => n + t.size
  val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

  val taxiFactsRdd = taxiFactsDF
    .as[TaxiRide]
    .rdd.groupBy(fact => dateFormat.format(fact.tpep_pickup_datetime))
    .aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    .sortBy(fact => fact._2, false)
    //    .map(x => x._1)
    .foreach(x => println(x._1 + "->" + x._2))

  println("dddddddddddddddddddddddddd")
  taxiFactsDF
    .as[TaxiRide]
    .withColumn("hour", hour(col("tpep_pickup_datetime")))
    .groupBy(col("hour")).count()
    .show

  println("dfffffffffffffffffffff")
  val taxiFactsRdd3 = taxiFactsDF
    .as[TaxiRide]
    .rdd.map(fact => dateFormat.format(fact.tpep_pickup_datetime)).distinct().foreach(println(_))


  //*************************


//  taxiFactsDF.select("tpep_pickup_datetime")
//    .withColumn("hour", hour(col("tpep_pickup_datetime")))
//    .drop("tpep_pickup_datetime")
//    .groupBy("hour").count()
//    .sort(col("count").desc)
//    .show()
}

