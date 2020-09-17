package com.dataedge.createDF

import org.apache.spark.sql.SparkSession

object CreateDFfromAVRO {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("AVRODF")
      .getOrCreate()


    val avroDF = spark.read.format("avro").load("file:///R:/hadoop/resources/zipcodes.avro")

    avroDF.printSchema()
    avroDF.show()

  }

}
