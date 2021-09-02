package homework2

import org.apache.flink.table.shaded.com.ibm.icu.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

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


  val taxiFactsDF = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")(spark)

  taxiFactsDF.printSchema()
  println(taxiFactsDF.count())
  taxiFactsDF.show(10)

  val taxiZones = readCSV("src/main/resources/data/taxi_zones.csv")(spark)
  taxiZones.printSchema()
  println(taxiZones.count())

  println("-- Task 1")

  val boroughPopularity = orderByBoroughPopularity(taxiFactsDF, taxiZones)

  boroughPopularity.persist()
  boroughPopularity.show(10)
  boroughPopularity.write.parquet("boroughPopularity.parquet")

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

  val taxiFactsRdd = countOrdersByTime(taxiFactsDF.as[TaxiRide].rdd, dateFormat)


  taxiFactsRdd.persist()
  taxiFactsRdd.foreach(println)
  taxiFactsRdd.saveAsTextFile("count_by_hours.txt")


  println("-- Task 3")

  val connectionProperties = new Properties()

  connectionProperties.put("user", user)
  connectionProperties.put("password", password)

  val statistics = calculateTaxiStatistics(taxiFactsDF.as[TaxiRide])

  statistics.persist()
  statistics.foreach(x => println(x))
  statistics.write.jdbc(url=url, table="distance_statistics_by_day", connectionProperties=connectionProperties)

  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.parquet(path)

  def readCSV(path: String)(implicit spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  def orderByBoroughPopularity(df: DataFrame, zones: DataFrame) = {
    df.join(broadcast(zones), col("DOLocationID") === col("LocationId"), "left")
      .groupBy(col("Borough"))
      .count()
      .orderBy(col("count").desc)
  }

  def countOrdersByTime(rdd: RDD[TaxiRide], dateFormat: SimpleDateFormat) = {
    val initialCount = 0;
    val addToCounts = (n: Int, t:Iterable[TaxiRide]) => n + t.size
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    rdd.groupBy(fact => dateFormat.format(fact.tpep_pickup_datetime))
      .aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
      .sortBy(fact => fact._2, false)
      .map(x => s"${x._1} ${x._2}")
  }

  def calculateTaxiStatistics(ds: Dataset[TaxiRide]) = {
    ds.withColumn("days", date_format(col("tpep_pickup_datetime"), "dd-MM-yyyy"))
      .groupBy(col("days"))
      .agg(count("trip_distance"),
        functions.avg("trip_distance"),
        functions.min("trip_distance"),
        functions.max("trip_distance"),
        stddev("trip_distance"))
  }

}

