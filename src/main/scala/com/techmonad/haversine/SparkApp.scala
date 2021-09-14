package com.techmonad.haversine

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Source of Citi bike trip data:
 * https://s3.amazonaws.com/tripdata/index.html
 */

object SparkApp {

  import Utils._

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .master("local[*]")
        .getOrCreate()


    val path = "data/citibike-tripdata.csv"

    val df =
      spark
        .read
        .option("header", "true")
        .csv(path)


    val updatedDF: DataFrame = df.updateColumnName


    val result = HaversineDistanceCalculator.calculate(updatedDF)


    result.show()


  }


}
