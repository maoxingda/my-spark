package com.maoxd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(Main.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val df = sparkSession.read
      .option("recursiveFileLookup", "true")
      .parquet("hdfs://172.27.128.124:8020/user/maoxd/ppdir")

    println(df.count())
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
}
