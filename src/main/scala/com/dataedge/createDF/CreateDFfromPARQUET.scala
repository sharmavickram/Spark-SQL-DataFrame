package com.dataedge.createDF

import org.apache.spark.sql.SparkSession

object CreateDFfromPARQUET {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("PARQUETDF")
      .getOrCreate()


    val xmlDF = spark.read.format("parquet").load("file:///R:/hadoop/resources/zipcodes.parquet")

    xmlDF.printSchema()
    xmlDF.show()

  }

}
