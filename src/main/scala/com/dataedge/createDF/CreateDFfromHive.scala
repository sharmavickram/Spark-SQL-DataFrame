package com.dataedge

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object CreateDFfromHive {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]").enableHiveSupport()
      .appName("appName")
      .config("spark.sql.warehouse.dir", "R:\\HADOOP_SPACE\\Examples\\Spark-SQL-DataFrame\\spark-warehouse")
      .getOrCreate()

    val log = Logger.getLogger(getClass.getName)
    import  spark.sql

    sql("create database if not exists hive")
    log.info("Creating hive database : OK")
    sql("show databases").show()
    sql("use hive")

    sql("LOAD DATA LOCAL INPATH 'R:/hadoop/data/csv/sample_data.csv' INTO TABLE  hive.employee_base")

    val hiveDF = sql("select *from hive.employee_base")
    hiveDF.printSchema()
    sql("select *from hive.employee_base").show()

  }

}
