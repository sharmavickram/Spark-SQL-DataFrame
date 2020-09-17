package com.dataedge.extra

import org.apache.spark.sql.SparkSession

object ReadParquetWriteJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[1]").appName("ReadParquetWriteJson").getOrCreate()
    val parquetDF = spark.read
      .format("parquet")
      .option("header","true")
      .option("mode","failfast")
      .load("hdfs://localhost:9000/user/read-write/part-00000-d677bcaf-08b9-4491-aa97-12adda3abc67-c000.snappy.parquet")

    parquetDF.show()
    parquetDF.write.format("json")
      .mode("overwrite").save("hdfs://localhost:9000/user/read-write/")
  }

}
