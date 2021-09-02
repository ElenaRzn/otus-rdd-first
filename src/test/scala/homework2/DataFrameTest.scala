package homework2

import homework2.DataApiHomeWorkTaxi.{TaxiRide, calculateTaxiStatistics, countOrdersByTime, readParquet}
import org.apache.flink.table.shaded.com.ibm.icu.text.SimpleDateFormat
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameTest extends SharedSparkSession{
  import testImplicits._

  test("countOrdersByTime") {
    val taxiZonesDF2 = readParquet("src/test/resources/data/yellow_taxi_jan_25_2018")
    val dateFormat2: SimpleDateFormat = new SimpleDateFormat("HH")

    val result = countOrdersByTime(taxiZonesDF2.as[TaxiRide].rdd, dateFormat2).toDF()

    checkAnswer(
      result,
      Row("08 22121")
        :: Row("09 21598")
        :: Row("11 20884")
        :: Row("10 20318")
        :: Row("12 19528")
        :: Row("22 18867")
        :: Row("07 18664")
        :: Row("05 17843")
        :: Row("04 17483")
        :: Row("23 16840")
        :: Row("06 16160")
        :: Row("03 16082")
        :: Row("02 16001")
        :: Row("01 15564")
        :: Row("21 15445")
        :: Row("00 15348")
        :: Row("13 14652")
        :: Row("20 8600")
        :: Row("14 7050")
        :: Row("15 3978")
        :: Row("19 3133")
        :: Row("16 2538")
        :: Row("17 1610")
        :: Row("18 1586")
        :: Nil)
  }
  test("calculateTaxiStatistics") {
    val taxiZonesDF2 = readParquet("src/test/resources/data/yellow_taxi_jan_25_2018")

    val result = calculateTaxiStatistics(taxiZonesDF2.as[TaxiRide])

    checkAnswer(
      result,
      Row("25-01-2018", 252246, 2.7295107950175224, 0.0,66.0,3.509040029581382)
        :: Row("24-01-2018",79647,2.6815007470463676,0.0,55.41,3.408158517922086)
        :: Nil)
  }
}
