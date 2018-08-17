//package cn.wangjy.spark
//
//import cn.wangjy.spark.SparkTest._
//import scala.collection.immutable.HashSet
//import org.apache.spark.{SparkContext, SparkConf}
//
///**
//  *
//  * TODO ADD DESCRIPTION
//  *
//  * @author wangjieyao
//  * @Date 2018/8/15 15:37
//  */
//object SparkTest1 {
//
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setMaster("local[4]").setAppName(getClass.getSimpleName).set("spark.testing.memory", "2147480000")
//    val sc = new SparkContext(conf)
//
//    val a = sc.parallelize(1 to 10, 3)
//
//    //定义两个输入变换函数，它们的作用均是将rdd a中的元素值翻倍
//
//    //map的输入函数，其参数e为rdd元素值
//
//    def myfuncPerElement(e:Int):Int = {
//
//      println("e="+e)
//
//      e*2
//
//    }
//
//    //mapPartitions的输入函数。iter是分区中元素的迭代子，返回类型也要是迭代子
//
//    def myfuncPerPartition ( iter : Iterator [Int] ) : Iterator [Int] = {
//
//      println("run in partition")
//
//      var res = for (e <- iter ) yield e*2
//
//      res
//
//    }
//
//
//
//    val b = a.map(myfuncPerElement).collect
//
//    val c = a.mapPartitions(myfuncPerPartition).collect
//  }
//
//}
