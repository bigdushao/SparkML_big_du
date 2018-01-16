package sparkSQL.UserDefineFunctionTest

import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by dushao on 18-1-15.
  * spark-sql中udaf函数测试
  * 使用udaf函数实现计算平均值的函数
  */
object UDAFDemo {
    // 用来转化对去的数据
  case class Employee(name:String, salary:Long)
  // 自定义聚合函数中使用
  case class Average(var sum:Long, var count:Long)

  object MyAverage extends UserDefinedAggregateFunction {
    // Data types of input arguments of this aggregate function　输入数据的类型
    override def inputSchema: StructType = StructType(StructField("inputcolumn", LongType) :: Nil)

    // Data types of values in the aggregation buffer,聚合缓冲中的数据类型, sum 为输入数据聚合后类型，count输入数据数量累加结果的数据类型
    override def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }

    // the date type of the returned value　返回值的类型
    override def dataType: DataType = DoubleType

    // whether this function always returns the same output on the identical input　相同输入的返回值是否相同
    override def deterministic: Boolean = true

    // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
    // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
    // the opportunity to update its values. Note that arrays and maps inside the buffer are still
    // immutable.
    // 初始化缓冲数组，存放计算的间结果
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    // Updates the given aggregation buffer `buffer` with new input data from `input
    //　将中间计算的缓冲和新输入的数据进行进一步的聚合，得到新的聚合的结果．
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) { // 判断数据的输入是否结束
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
    // 将两个缓冲数组的值进行聚合运算，得到返回的结果
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // Calculates the final result 本例使用的是聚合计算平均值，如果计算的结果是求某一列的和.
    override def evaluate(buffer: Row): Double = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }

  // 使用自定义聚合函数
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("udaf test")
      .getOrCreate()

    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("/home/dushao/software/spark-2.2.0/examples/src/main/resources/employees.json")
    df.createOrReplaceGlobalTempView("employees")
    df.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+
    // $example off:untyped_custom_aggregation$

    spark.stop()
  }
}
