package sparkSQL.UserDefineFunctionTest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by dushao on 18-1-15.
  *
  * spark-sql中udf函数的使用测试
  *
  */
object UDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = Seq(("id1", 1, 100), ("id2", 4, 300), ("id3", 5, 800)).toDF("id", "value", "cnt")
    df.printSchema()

    // 注册自定义UDF函数 ,自定义的udf函数为自定义的udf函数的输入最多为21个字段不能超过，tuple数据类型参数的个数
    val udfFunction = (v:Int, w:Int) => v * v + w * w
    spark.udf.register("udf_demo", udfFunction)
    spark.udf.register("simpleUDF", (v: Int, w: Int) => v * v + w * w)

    df.select($"id", callUDF("simpleUDF", $"value", $"cnt")).toDF("id", "value").show
  }
}
