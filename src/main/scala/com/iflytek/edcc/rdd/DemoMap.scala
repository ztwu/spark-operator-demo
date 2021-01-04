package com.iflytek.edcc.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 9:05
  * Description
  */

object DemoMap {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(this.getClass.getName)
    conf.setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.makeRDD(Seq((1,2),(2,3),(3,4)))
    val data2 = sc.makeRDD(Seq(("w",2),("ww",3),("www",4)))

    data.zip(data2).foreach(x=>println(x))

    //map
    data.map(x => {
      (x._1+"#",x._2+"#")
    }).foreach(println)

    //flatMap
    data.flatMap(x => {
      Array(1 to x._1)
    }).foreach(println)

    //mapValues
    data.mapValues(x=>{
      (x+"*",x+"_")
    }).foreach(println)

    //flatMapValues
    data.flatMapValues(x=>{
      Array(x+"*",x+"_")
    }).foreach(println)

    //mapPartitions
    val test1 = sc.parallelize(List(1,2,3,4,5,6),2)
    test1.mapPartitions(partition => {
      var result = List[String]()
      var sum = 0
      partition.foreach(x=>{
        sum += x
      })
      result.::(sum).iterator
    }).foreach(println)

    //mapPartitionsWithIndex
    test1.mapPartitionsWithIndex((x, partition) => {
      var result = List[String]()
      var sum = 0
      partition.foreach(x=>{
        sum += x
      })
      result.::(x+"|"+sum).iterator
    }).foreach(println)

    sc.stop()

  }

}
