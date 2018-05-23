package com.iflytek.edcc.dataframe

import com.iflytek.edcc.{Conf, ReadUtil}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2017/10/19
  * Time: 13:52
  * Description
  *
  * reducebykey，按key分组聚合处理数据，传入lambda函数，用于处于聚合操作
  *
  */

object DemoUnion {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //设置程序名称
    conf.setAppName(this.getClass.getName)
    //设置数据自动覆盖
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    //设置主节点，local本地线程
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir(Conf.checkpointDir)

    val dataframe1 = ReadUtil.readDwSchoolOrg(sc);
    val dataframe2 = ReadUtil.readDwsLogUserActive(sc);
    val dataframe3 = ReadUtil.readDwsUcUserOrganization(sc);
    val dataframe4 = ReadUtil.readBroadcast(sc);

    val data1 = dataframe3.join(dataframe1,dataframe3("school_id")===dataframe1("school_id"),"inner")
      .join(dataframe2,dataframe3("user_id")===dataframe2("user_id"),"left")

    //这种join类似于a join b using column1的形式，需要两个DataFrame中有相同的一个列名
    //传入String类型的字段名，也可传入Column类型的对象
    val data2 = dataframe3.join(dataframe1,dataframe3("school_id")===dataframe1("school_id"),"inner")
      .join(dataframe2,dataframe3("user_id")===dataframe2("user_id"),"inner")

    //不去重
    data1.unionAll(data2).show()

    sc.stop()

  }

}
