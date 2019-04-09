package com.endava

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object App {


  def main(args : Array[String]) {

    val config = new SparkConf()
    .setAppName("SparkTest")
    .setMaster("local[*]")

    val ctx = new SparkContext(config)

    val numbers = List(1,2,3,4,5,6,7,8,9,10)

    val numbersRDD = ctx.parallelize(numbers)

    val lessThan5RDD = numbersRDD.filter(n => n < 5)


    val results = lessThan5RDD.collect()
    results.foreach(n => {
      println(n)
    })

    lessThan5RDD.toDebugString
  }

}
