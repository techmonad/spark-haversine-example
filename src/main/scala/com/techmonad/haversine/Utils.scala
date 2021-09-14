package com.techmonad.haversine

import org.apache.spark.sql.DataFrame

object Utils {


  implicit class RichDataFrame(df: DataFrame) {
    /**
     * Replace spaces in column by _
     */
    def updateColumnName: DataFrame = {
      val columns =
        df
          .columns
          .map(column => (column, column.replaceAll("\\s+", "_")))
          .filter { case (o, n) => n != o }
      columns.foldLeft(df) { case (df, (name, newName)) => df.withColumnRenamed(name, newName) }
    }
  }

}
