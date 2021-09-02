package homework2

import homework2.DataApiHomeWorkTaxi.{orderByBoroughPopularity, readCSV, readParquet}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class RddTest extends AnyFlatSpec {
  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test №1 for orderByBoroughPopularity")
    .getOrCreate()

  it should "orderByBoroughPopularity data" in {
    val taxiZonesDF2 = readCSV("src/main/resources/data/taxi_zones.csv")
    val taxiDF2 = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")


    val result = orderByBoroughPopularity(taxiZonesDF2, taxiDF2)

    checkAnswer(
      result,
      Row("Manhattan", 296529) ::
        Row("Queens", 13822) ::
        Row("Brooklyn", 12673) ::
        Row("Unknown", 6714) ::
        Row("Bronx", 1590) ::
        Row("EWR", 508) ::
        Row("Staten Island", 67) :: Nil
    )
  }
}
