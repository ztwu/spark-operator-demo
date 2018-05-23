package com.iflytek.edcc.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 11:44
  * Description
  */

object DemoJoin {

  def main(args:Array[String]):Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(("01","ztwu1"),("02","ztwu2"),("03","ztwu3")),2)
    val rdd2 = sc.parallelize(Array(("01",25),("02",26),("04",28)),2)

    System.out.println("join")
    rdd1.join(rdd2).foreach(println)

    System.out.println("left join")
    rdd1.leftOuterJoin(rdd2).foreach(println)

    System.out.println("right join")
    rdd1.rightOuterJoin(rdd2).foreach(println)

    System.out.println("full join")
    rdd1.fullOuterJoin(rdd2).foreach(println)

    sc.stop()

  }

}
