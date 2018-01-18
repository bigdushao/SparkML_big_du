package utli

import org.apache.spark.sql.SparkSession

/**
  * Created by dushao on 18-1-18.
  */
class SparkBase {
  val spark = SparkSession.builder().getOrCreate()
}
