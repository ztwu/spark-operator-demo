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

object DemoGroupBy {

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
        .toDF("userid","num")

//    定义groupby规则
    data.groupByKey(row =>{
      row.getString(0)
    }).count().show()

    data.groupBy("userid")
      .count().show()

    sc.stop()

  }

}
