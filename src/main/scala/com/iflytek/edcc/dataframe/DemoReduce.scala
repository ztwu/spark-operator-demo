package com.iflytek.edcc.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 14:15
  * Description
  */

object DemoReduce {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //设置程序名称
    conf.setAppName(this.getClass.getName)
    //设置数据自动覆盖
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    //设置主节点，local本地线程
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = sc.makeRDD(Array(("01",1),("01",2),("02",1),("03",1)))
        .toDF("userid","num").as[Data]

    val rs = data.reduce((x, y) => Data(x.userid+y.userid, y.num+x.num))
    println(rs)
    sc.stop()

  }

}

case class Data(userid:String, num:Int)
