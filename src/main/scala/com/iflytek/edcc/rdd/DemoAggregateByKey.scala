package com.iflytek.edcc.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 8:45
  * Description
  */

object DemoAggregateByKey {

  def main(args:Array[String]):Unit = {

    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val test1 = sc.makeRDD(Seq((1,"A"),(2,"A")))
    val test2 = sc.makeRDD(Seq((1,"B"),(2,"B")))

    test1.join(test2)
      .flatMap(x => {
        val d1 = x._1
        val d2 = x._2._1
        val d3 = x._2._2
        Array((d1,d2),(d1,d3))
      }).repartition(2)
      //aggregateByKey返回值的类型不需要和RDD中value的类型一致
      //zeroValue可以确定value类型,初始化一个元组
      .aggregateByKey(new HashSet[String])(
      //seqOp:用来在同一个partition中合并值，map阶段
      (a , b) =>{
        println("aggregateByKey-step1: : "+(a.+(b)))
        a+b
      },
      //combOp:用来在不同partiton中合并值，reduce阶段
      (a , b)  => {
        println("aggregateByKey-step2: : "+(a++b))
        a.++(b)
      })
      .foreach(println)

    sc.stop()

  }

}
