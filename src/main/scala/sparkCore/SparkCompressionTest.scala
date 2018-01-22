package sparkCore

import utli.SparkBase
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.sql.{DataFrameWriter, Dataset}
/**
  * Created by dushao on 18-1-18.
  * spark的文件压缩算法的使用，以及使用Hadoop支持的压缩算法进行文件处理
  *
  */
object SparkCompressionTest extends SparkBase{
  // 这种方式是通过自定义实现InputFormat类来进行读取Hadoop上的特殊文件，例如zip文件，zip文件spark并没有很好的支持直接读取
  // 使用hadoop的压缩方式将文件压缩成.bzip的文件后缀，可以通过spark直接读取。如果不能通过spark直接读取需要通hadoopRDD来读去
  // 需要实现InputFormat类中的方法，将压缩文件进行转化
  // spark支持的压缩方式有LZ4 LZF Snappy压缩方式
  spark.sparkContext.parallelize(Seq("a", "b", "c")).saveAsTextFile("save_path",classOf[BZip2Codec])
//  val hadoopConf = new Configuration()
//  spark.sparkContext.newAPIHadoopRDD(hadoopConf, classOf[Bzip2Compressor], classOf[Text], classOf[Text])
}
