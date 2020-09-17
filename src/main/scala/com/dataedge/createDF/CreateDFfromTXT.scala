package com.dataedge.createDF

import org.apache.spark.sql.SparkSession

object CreateDFfromTXT {
  def main(args: Array[String]): Unit =
  {
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("TXTDF")
      .getOrCreate()

    val txtDF = spark.read.option("header","true").text("R:\\hadoop\\data\\txt\\emp.txt")
    txtDF.printSchema()
    txtDF.show()

  }

}
