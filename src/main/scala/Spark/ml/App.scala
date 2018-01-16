package Spark.ml

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 流水号比对。
  *
 */
object App  {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark ml dushao")
    val sparkContext = new SparkContext(sparkConf)
    val cassandra = sparkContext.textFile("/home/dushao/桌面/40_2017151_10001_sale")
    val needToCompare = sparkContext.textFile("/home/dushao/桌面/serialnumber")
    val compareResult = needToCompare.subtract(cassandra)
    compareResult.foreach(println(_))
  }
}
