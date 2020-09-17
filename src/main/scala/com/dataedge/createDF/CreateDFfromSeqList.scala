package com.dataedge.createDF

import org.apache.spark.sql.SparkSession

object CreateDFfromSeqList {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[1]")
      .appName("appName")
      .getOrCreate()

    val data = Seq(("Java", "200000"), ("Python", "100000"), ("Scala", "300000"))
    import spark.implicits._
    val dfFromData1 = data.toDF()
    dfFromData1.printSchema()

    //From Data (USING createDataFrame)
    val columns = Seq("language","users_count")
    var dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
    dfFromData2.printSchema()

  }

}
