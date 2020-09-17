package com.dataedge.createDF

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object CreateDFfromRDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("createRDD").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val spark = SparkSession.builder
      .master("local[1]")
      .appName("appName")
      .getOrCreate()

    import sqlContext.implicits._
    val columns = Seq("language","users_count")
    val rdd = sc.parallelize(Seq(("Java", "200000"), ("Python", "100000"), ("Scala", "300000")))

    val dfFromRDD1 = rdd.toDF()  // first way to create DF
    dfFromRDD1.printSchema()
    dfFromRDD1.show()

    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
    dfFromRDD2.show()


  }

}
