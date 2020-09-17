package com.dataedge.extra

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object HiveSql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]").enableHiveSupport()
      .appName("appName")
      .config("spark.sql.warehouse.dir", "R:\\HADOOP_SPACE\\Examples\\Spark-SQL-DataFrame\\spark-warehouse")
      .getOrCreate()

    val log = Logger.getLogger(getClass.getName)
    import spark.sql

    sql("create database if not exists hive")
    log.info("Creating hive database : OK")
    sql("show databases").show()
    sql("select substring ('enableHiveSupport',5)").show()
  }
}
