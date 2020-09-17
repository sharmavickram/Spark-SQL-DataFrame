package com.dataedge.extra

import org.apache.spark.{SparkConf, SparkContext}

object GetSpecificPartitionRecords {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("GetSpecificPartitionRecords").setMaster("local[1]")
    val sc = new SparkContext(conf)

    val emp_data = sc.textFile("R:\\hadoop\\data\\txt\\emp_data.txt",2)
    val emp_hdfs_data = sc.textFile("hdfs://localhost:9000/user/data/txt/emp_data.txt",4)

    println("emp_data partitions: " +emp_data.partitions.size)

    emp_data.mapPartitionsWithIndex( (index: Int, it: Iterator[String]) =>it.toList.map(x => if (index ==0) {println(x)}).iterator).collect()
    emp_data.mapPartitionsWithIndex( (index: Int, it: Iterator[String]) =>it.toList.map(x => if (index ==1) {println(x)}).iterator).collect()

    println("emp_hdfs_data partitions: " +emp_hdfs_data.partitions.size)
    emp_hdfs_data.mapPartitionsWithIndex( (index: Int, it: Iterator[String]) =>it.toList.map(x => if (index ==2) {println(x)}).iterator).collect()
    emp_hdfs_data.mapPartitionsWithIndex( (index: Int, it: Iterator[String]) =>it.toList.map(x => if (index ==3) {println(x)}).iterator).collect()

  }

}
