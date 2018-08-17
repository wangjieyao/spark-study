package cn.wangjy.spark

import _root_.kafka.serializer.StringDecoder
import cn.wangjy.spark.utils.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}


/**
  *
  * spark streaming test
  *
  * @author wangjieyao
  * @Date 2018/8/16 14:26
  */
object SparkStreamingTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName(getClass.getSimpleName).set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(conf, Seconds(5))
    //监听本地 7777端口，使用收到的数据创建DStream
    val lines = ssc.socketTextStream("10.120.11.94",7777)
//    val lines =ssc.textFileStream("E:\\code\\scala\\spark-study\\src\\main\\resources")

    //筛选包含error的行
    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()
    //启动流式计算环境并等待完成
    ssc.start()
    //等待作业完成
    ssc.awaitTermination()

//    val accessLogDStream = lines.map(line => ApacheAccessLog.parseFromLogLine(line))
//    val ipDStream = accessLogsDStream.map(entry => (entry.getIpAddress(), 1))
//    val ipCountsDStream = ipDStream.reduceByKey((x, y) => x + y)




  }

}
