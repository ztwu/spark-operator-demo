package com.iflytek.edcc.dataframe

import com.iflytek.edcc.ReadUtil
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
//使用窗口函数
//引用分析函数
import org.apache.spark.sql.functions._

/**
  * Created with Intellij IDEA.
  * User: ztwu2
  * Date: 2018/5/23
  * Time: 14:15
  * Description
  */

object DemoAgg {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    //设置程序名称
    conf.setAppName(this.getClass.getName)
    //设置数据自动覆盖
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    //设置主节点，local本地线程
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val dataframe1 = ReadUtil.readDwSchoolOrg(sc);
    val dataframe2 = ReadUtil.readDwsLogUserActive(sc);
    val dataframe3 = ReadUtil.readDwsUcUserOrganization(sc);
    val dataframe4 = ReadUtil.readBroadcast(sc);

    //这种join类似于a join b using column1的形式，需要两个DataFrame中有相同的一个列名
    //传入String类型的字段名，也可传入Column类型的对象
    val data = dataframe3.join(dataframe1,"school_id")
      .join(dataframe2,"user_id").withColumn("num",dataframe3("school_id")+10)

    data.show()

    data.groupBy("school_id","school_name").agg(count("user_id") as "cnt").show()

    data.groupBy("school_id","school_name").agg(countDistinct("user_id") as "cnt").show()

    data.groupBy("school_id","school_name").agg(countDistinct("user_id","event") as "cnt").show()

    data.groupBy("school_id","school_name").agg(countDistinct("user_id","event") as "cnt",sum("num") as "total").show()

    data.groupBy("school_id","school_name").agg(countDistinct("user_id","event") as "cnt",sumDistinct("num") as "total").show()

    data.groupBy("school_id","school_name").agg("user_id"->"count").show()

    data.groupBy("school_id","school_name").agg("user_id"->"count","num"->"sum").show()

    sc.stop()

  }

}
