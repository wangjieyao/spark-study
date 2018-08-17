package cn.wangjy.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * spark示例
  *
  * @author wangjieyao
  * @Date 2018/8/15 10:13
  */
object SparkTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName(getClass.getSimpleName).set("spark.testing.memory", "2147480000")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new HiveContext(sparkContext)
    val logPath="E:\\code\\scala\\spark-study\\src\\main\\resources\\ds.txt"
    val textRDD = sparkContext.textFile(logPath)

    println(s"textRDD size=${textRDD.count()}")
    val studentDF=sqlContext.read.json("E:\\code\\scala\\spark-study\\src\\main\\resources\\student.json")
    studentDF.show(false)
    studentDF.registerTempTable("tb_student")
    sqlContext.sql("select name,count(1) from tb_student group by name").show(false)




    sparkContext.stop()

  }

}
