package com.dataedge.problems

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}

object Problem01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("READ TAB DELIMITED DATA FROM HDFS")
      .getOrCreate()

    val tsvDF = spark.read.format("csv")
      .option("delimiter", "\t")
      .load("hdfs://localhost:9000/user/data/tsv/emp.tsv")
    tsvDF.printSchema()
    tsvDF.show(false)

    val concatedDF = tsvDF.withColumn("Concatenation",
      functions.concat(col("_c0"), col("_c1")))
    concatedDF.show(false)

    //concatedDF.write.option("compression","snappy").parquet("hdfs://localhost:9000/user/data/intellij-output/solution01.parquet")

    val solution1 = spark.read.parquet("hdfs://localhost:9000/user/data/intellij-output/solution01.parquet").show()
  }

}
