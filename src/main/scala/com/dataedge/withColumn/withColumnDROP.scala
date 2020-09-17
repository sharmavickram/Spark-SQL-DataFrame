package com.dataedge.withColumn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object withColumnDROP {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[1]")
      .appName("withColumnDemo")
      .getOrCreate()

    val jsonDF = spark.read.option("multiline","true").json("R:\\hadoop\\data\\emp-data\\emp_multiLine.json")

    // To Add New Column in dataframe:
    val ingestedDate = java.time.LocalDate.now
    val jsonDfWithDate = jsonDF.withColumn("inegstedDate", lit(ingestedDate.toString()))
    jsonDfWithDate.printSchema()
    jsonDfWithDate.show()

    val jsonDfWithoutManager = jsonDfWithDate.drop("manager")
    jsonDfWithoutManager.printSchema()
    jsonDfWithoutManager.show()
  }

}



