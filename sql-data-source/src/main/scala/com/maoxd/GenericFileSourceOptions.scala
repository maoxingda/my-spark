package com.maoxd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object GenericFileSourceOptions {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(GenericFileSourceOptions.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val dataFrame1 = sparkSession.read
      .parquet("sql-data-source/src/main/resources/data/ppdir")

    val dataFrame2 = sparkSession.read
      .option("recursiveFileLookup", "true")
      .parquet("sql-data-source/src/main/resources/data/ppdir")

    println(dataFrame1.count())
    println(dataFrame2.count())

    val dataFrame3 = sparkSession.read
      .option("pathGlobFilter", "*.parquet") //json file should be filtered out
      .parquet("sql-data-source/src/main/resources/data/filter-json")

    println(dataFrame3.count())

    //Thread.sleep(5 * 60 * 1000)
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
}
