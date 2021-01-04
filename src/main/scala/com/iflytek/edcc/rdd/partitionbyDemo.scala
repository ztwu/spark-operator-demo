package com.iflytek.edcc.rdd

import org.apache.spark.sql.SparkSession

object partitionbyDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("test")
      .getOrCreate()

    val sc = spark.sparkContext
    val data = sc.makeRDD(Array((1,2),(1,3),(2,1),(2,2)))
    data.foreach(println)
    data.partitions.foreach(println)

    val data2 = data
//      对偶元组 key-value
      .partitionBy(new MySelfPartitioner(2))

//      我们常认为coalesce不产生shuffle会比repartition 产生shuffle效率高，而实际情况往往要根据具体问题具体分析，coalesce效率不一定高，有时还有大坑，大家要慎用。 coalesce 与 repartition 他们两个都是RDD的分区进行重新划分，repartition只是coalesce接口中shuffle为true的实现（假设源RDD有N个分区，需要重新划分成M个分区）
      //       1）如果N<M。一般情况下N个分区有数据分布不均匀的状况，利用HashPartitioner函数将数据重新分区为M个，这时需要将shuffle设置为true(repartition实现,coalesce也实现不了)。
      //       2）如果N>M并且N和M相差不多，(假如N是1000，M是100)那么就可以将N个分区中的若干个分区合并成一个新的分区，最终合并为M个分区，这时可以将shuff设置为false（coalesce实现），如果M>N时，coalesce是无效的，不进行shuffle过程，父RDD和子RDD之间是窄依赖关系，无法使文件数(partiton)变多。 总之如果shuffle为false时，如果传入的参数大于现有的分区数目，RDD的分区数不变，也就是说不经过shuffle，是无法将RDD的分区数变多的
      //       3）如果N>M并且两者相差悬殊，这时你要看executor数与要生成的partition关系，如果executor数 <= 要生成partition数，coalesce效率高，反之如果用coalesce会导致(executor数-要生成partiton数)个excutor空跑从而降低效率。如果在M为1的时候，为了使coalesce之前的操作有更好的并行度，可以将shuffle设置为true。
//      .repartition(2)
        .coalesce(4, false) // shuffle
//        .coalesce(4, true)// 不shuffle

//    data2.partitions.size
    data2.mapPartitionsWithIndex((index,x)=>{
      println("分区： ",index)
      x.map(item=>{
        println(item)
      })
    }).collect()

  }

}
