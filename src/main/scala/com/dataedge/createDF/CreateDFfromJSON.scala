package com.dataedge.createDF

import org.apache.spark.sql.SparkSession

object CreateDFfromJSON {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("JSONDF")
      .getOrCreate()

    val jsonDF = spark.read.option("header", "true").json("R:\\hadoop\\data\\json\\part-r-00000.json")
    jsonDF.printSchema()
    jsonDF.show()

    val jsonDF_multi = spark.read.option("multiLine", true).json("R:\\hadoop\\data\\emp-data\\emp_multiLine.json")
    jsonDF_multi.printSchema()
    jsonDF_multi.show()

    val jsonDF_multi2 = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("R:\\hadoop\\data\\emp-data\\emp_multiLine.json")
    jsonDF_multi2.printSchema()
    jsonDF_multi2.show()
  }
}
