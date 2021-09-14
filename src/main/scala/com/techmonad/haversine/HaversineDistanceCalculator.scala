package com.techmonad.haversine

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}


object HaversineDistanceCalculator {


  def calculate(df: DataFrame): DataFrame = {
    df
      .withColumn("distance",
        distance(col("end_station_latitude"), col("end_station_longitude"),
          col("start_station_latitude"), col("start_station_longitude"))
      )
  }

  /**
   * Haversine formula
   */

  private def distance(endLatitude: Column, endLongitude: Column, startLatitude: Column, startLongitude: Column): Column = {
    val value = pow(sin(radians(endLatitude - startLatitude) / 2), 2) +
      cos(radians(startLatitude)) * cos(radians(endLatitude)) *
        pow(sin(radians(endLongitude - startLongitude) / 2), 2)
    val radius = 6371 // in KM
    val distance = lit(2 * radius) * atan2(sqrt(value), sqrt(lit(1) - value))
    round(distance, 2)
  }


}
