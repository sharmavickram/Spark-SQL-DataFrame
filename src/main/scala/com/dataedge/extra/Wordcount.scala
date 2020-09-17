package com.dataedge.extra

import org.apache.spark.{SparkConf, SparkContext}

object Wordcount {
  def main(args: Array[String]): Unit = {
    //Create conf object
    val conf = new SparkConf().setAppName("WordCount Demo").setMaster("local[*]")

    //create spark context object
    val sc = new SparkContext(conf)

    //Read file and create RDD
    val rawData = sc.textFile("hdfs://localhost:9000/user/resources/test.txt")
    //convert the lines into words using flatMap operation
    val words = rawData.flatMap(line => line.split(" "))
    //count the individual words using map and reduceByKey operation
    val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _).foreach(println)
    //Save the result
    //wordCount.saveAsTextFile("hdfs://localhost:9000/user/resources/output/")
    //stop the spark context
    sc.stop
  }

}
