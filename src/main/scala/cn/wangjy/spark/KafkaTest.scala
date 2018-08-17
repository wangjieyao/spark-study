package cn.wangjy.spark

import cn.wangjy.spark.utils.KafkaManager
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.{KafkaUtils, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  * Nginx日志统计任务
  *
  * @author wangjieyao
  * @Date 2018/8/16 11:28
  */
object KafkaTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[4]").setAppName(getClass.getSimpleName).set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(conf, Seconds(5))
    val topics = "test"
    val brokers = "10.120.11.94:9092"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "group.id" -> "pv_staytime_728"
    )
//
    val kafkaManager : KafkaManager = new KafkaManager(kafkaParams)
//
//
//    // create direct stream
    val messages = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

//    messages.foreachRDD(rdd =>{
//      val rddStartTime = System.currentTimeMillis()
//      val a = rdd.collect()
//      println(a)
//    })





    messages.print()

    ssc.start()
    ssc.awaitTermination()


//    //var topic=Map{"test" -> 1}
//    var topic="test"
////    val topicSet = topic.split(",").toSet
//
//    val topicSet = Set("test")
//    //指定zookeeper
//    //创建消费者组
//    var group="con-consumer-group"
//    //消费者配置
//    val kafkaParam = Map(
//      "bootstrap.servers" -> "10.120.11.94:9092",//用于初始化链接到集群的地址
//      //用于标识这个消费者属于哪个消费团体
//      "group.id" -> group
//      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
//      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
//
//    );
//
//    val kafkaManager : KafkaManager = new KafkaManager(kafkaParam)
//    //
//    //
//    //    // create direct stream
//        val messages = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](
//          ssc, kafkaParam, topicSet)
//
//
//    //创建DStream，返回接收到的输入数据
////    var stream=KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent,Subscribe[String,String](topic,kafkaParam))
//    //每一个stream都是一个ConsumerRecord
//    messages.print();
//    ssc.start();
//    ssc.awaitTermination();

  }

}
