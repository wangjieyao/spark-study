package cn.wangjy.spark.sql

import cn.wangjy.spark.SparkTest._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  *
  * TODO ADD DESCRIPTION
  *
  * @author wangjieyao
  * @Date 2018/8/16 10:08
  */
object HiveSqlTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[4]").setAppName(getClass.getSimpleName).set("spark.testing.memory", "2147480000")
    val sparkContext = new SparkContext(conf)
    val hiveCtx = new HiveContext(sparkContext)
    val input = hiveCtx.read.json("E:\\code\\scala\\spark-study\\src\\main\\resources\\reqad-2018-08-16_10.json")
    //注册输入的SchemaRDD
    input.registerTempTable("ads")
    hiveCtx.sql("select * from ads limit 5").show(false)

    sparkContext.stop()
  }

}
