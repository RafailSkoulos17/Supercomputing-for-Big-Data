package main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Implementation {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("SBD_Lab1_RDD")

    val spark = new SparkContext(conf)

    val data = spark.textFile(args(0)) //Read the input file.
      .map(_.split("\t")) // Each column is separated with tabs.
      .filter(_.length > 23)  // Remove the columns that do not have the field AllNames.
      .map(k => dateAllNamesKVP(k(1), k(23)))  // Create the key value pair(Date,AllNames).
      .flatMapValues(x => x) // Flatten it to the key value pair (Date,topic).
      .filter(!_._2.contains("Type ParentCategory")) //Filter out the topic Type ParentCategory.
      .map(k => (k, 1)) // Add to each pair the integer 1 in order to do the reduce operation in the next step.
      .reduceByKey(_ + _) // Do the reduce operation with key the date,topic pair.
      .map(k => (k._1._1, (k._1._2, k._2))) // Map them to the expected output format.
      .groupByKey() // Group them by the Date.
      .map(g => (g._1, g._2.toList.sortWith(_._2 > _._2).take(10))) // Take the top 10 per date.

    data.foreach(println) //print the top ten as a result.

    spark.stop()
  }

  /*
   *Format the raw date into the required format.
   */
  def formatDate(d: String): String = {
    d.take(4) + "-" + d(4) + d(5) + "-" + d(6) + d(7)
  }

  /*
   *Create the (Date,AllNames) pair.
   */
  def dateAllNamesKVP(d: String, aN: String): (String, Array[String]) = {
    (formatDate(d), aN.replaceAll("[,0-9]", "").split(";"))
  }

}
