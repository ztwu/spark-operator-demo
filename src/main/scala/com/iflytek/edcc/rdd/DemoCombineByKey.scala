package com.iflytek.edcc.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 8:57
  * Description
  */

object DemoCombineByKey {

  def main(args:Array[String]):Unit = {

    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    //combineByKey
    //1.6.0版的函数名更新为combineByKeyWithClassTag
    //createCombiner: V => C ，这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)
    // 并把它返回 (这一步类似于初始化操作),createCombiner就是定义了v如何转换为c

    //    mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上
    // (这个操作在每个分区内进行)

    //    mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)
    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)
    d1.combineByKey(
      //createCombiner,初始化操作
      score => (1, score),
      //mergeValue
      (c1: MVType, newScore) => {
        println("step1: "+(c1._1 + 1, c1._2 + newScore))
        (c1._1 + 1, c1._2 + newScore)
      },
      //mergeCombiners
      (c1: MVType, c2: MVType) => {
        println("step2: "+(c1._1 + c2._1, c1._2 + c2._2))
        (c1._1 + c2._1, c1._2 + c2._2)
      }
    ).map ({
      case (name, (num, socre)) => {
        (name, socre / num)
      }
    }).foreach(println)

    sc.stop()

  }

}
