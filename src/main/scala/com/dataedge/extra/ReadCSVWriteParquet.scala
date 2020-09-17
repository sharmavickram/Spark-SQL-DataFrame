package com.dataedge.extra

import org.apache.spark.sql.SparkSession

object ReadCSVWriteParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[1]").appName("ReadCSVWriteParquet").getOrCreate()

    val csvDF = spark.read
      .format("csv")
      .option("header","true")
      .option("mode","failfast")
      .load("R:\\hadoop\\data\\csv\\sample_data.csv")

    csvDF.show()

    csvDF.write.format("parquet")
      .mode("overwrite").save("hdfs://localhost:9000/user/read-write/")
  }

}
