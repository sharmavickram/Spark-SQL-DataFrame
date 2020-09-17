package com.dataedge.createDF

import org.apache.spark.sql.SparkSession

object CreateDFfromMySQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("MySQLDF")
      .getOrCreate()


    val mysqlDF = spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/dataedge")
    .option("driver", "com.mysql.jdbc.Driver")
    .option("dbtable", "country")
    .option("user", "root")
    .option("password", "root")
    .load()

    mysqlDF.printSchema()
    mysqlDF.show()


  }

}
