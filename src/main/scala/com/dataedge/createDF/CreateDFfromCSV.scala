package com.dataedge.createDF

import org.apache.spark.sql.SparkSession

object CreateDFfromCSV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("CSVDF")
      .getOrCreate()

    val csvDF = spark.read.option("header","true").csv("R:\\hadoop\\data\\csv\\sample_data.csv")
    csvDF.printSchema()
    csvDF.show()
  }

}
