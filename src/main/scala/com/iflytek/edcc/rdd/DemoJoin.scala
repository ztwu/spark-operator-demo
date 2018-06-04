package com.iflytek.edcc.rdd

import org.apache.spark.{Partition, SparkConf, SparkContext}

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

    val rdd1 = sc.makeRDD(List(("01","ztwu1"),("02","ztwu2"),("03","ztwu3")),3)
    val rdd2 = sc.parallelize(Array(("01",25),("02",26),("04",28)),4)

    System.out.println("join")
    rdd1.join(rdd2).foreach(println)
    println("查看join之后的分区情况")
    rdd1.join(rdd2).partitions.foreach(x=>println(x))

    System.out.println("left join")
    rdd1.leftOuterJoin(rdd2).foreach(println)
    println("查看leftOuterJoin之后的分区情况")
    rdd1.leftOuterJoin(rdd2).partitions.foreach(x=>println(x))

    System.out.println("right join")
    rdd1.rightOuterJoin(rdd2).foreach(println)
    println("查看rightOuterJoin之后的分区情况")
    rdd1.rightOuterJoin(rdd2).partitions.foreach(x=>println(x))

    System.out.println("full join")
    rdd1.fullOuterJoin(rdd2).foreach(println)
    println("查看fullOuterJoin之后的分区情况")
    rdd1.fullOuterJoin(rdd2).partitions.foreach(x=>println(x))

    sc.stop()

  }

}
