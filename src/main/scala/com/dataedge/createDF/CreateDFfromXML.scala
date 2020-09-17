package com.dataedge.createDF

import com.databricks.spark.xml.XmlDataFrameReader
import org.apache.spark.sql.SparkSession

object CreateDFfromXML {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[1]")
      .appName("XMLDF")
      .getOrCreate()


    //val xmlDF = spark.read.format("xml").load("file:///R:/hadoop/resources/books.xml")
    //xmlDF.printSchema()

    val xmlDF2 = spark.read
      .option("rootTag", "persons")
      .option("rowTag", "person")
      .format("xml").load("R:/hadoop/resources/persons.xml")
    xmlDF2.printSchema()
    xmlDF2.show()

    val xmlDF3 = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "root").xml("hdfs://localhost:9000/user/data/xml/orders.xml")
    xmlDF3.printSchema()
    xmlDF3.show()

  }

}
