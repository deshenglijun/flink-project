package com.desheng.bigdata.flink.dataset.transformation

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._

import scala.collection.mutable

/**
 * 自定义分区
 *      分组分区
 */
object _08PartitionByCustomOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val persons = env.fromCollection(List(
            Person(1, "刘国伟", 18, 0, "辽宁"),
            Person(2, "马惠", 19, 1, "辽宁"),
            Person(3, "小涛涛", 29, 0, "辽宁"),
            Person(4, "刘鑫", 20, 0, "贵州"),
            Person(5, "郑朝义", 22, 0, "贵州"),
            Person(6, "韩香彧", 20, 1, "内蒙古"),
            Person(7, "佟凯", 22, 1, "内蒙古"),
            Person(8, "刘照路", 30, 1, "辽宁")
        ))


        //提取所有的分区字段
        val provinces: Seq[String] = persons.map(person => (person.province, 1))
                    .distinct(0)
                    .collect()
                    .map(kv => kv._1)

        val numPartitions = provinces.length//分组字段，代表了分组之后的分区个数
        /*
            自定义分区，分组分区
            第二个参数，指定的是分区字段
         */
        val partitioned = persons.partitionCustom(new MyGroupedPartitioner(provinces), person => person.province)
                            .setParallelism(numPartitions)

        partitioned.mapPartition(ps => {
            val list = ps.toList
            println("partitionByRange内容：" + list.mkString("[",  ", ",  "]"))
            list
        }).print()

    }
}
class MyGroupedPartitioner(provinces: Seq[String]) extends Partitioner[String] {
    val province2Index = {//完成了一个分组字段和分区之间的映射
        val map = mutable.Map[String, Int]()
        for(index <- 0 until provinces.length) {
            map.put(provinces(index), index)
        }
        map.toMap
    }

    override def partition(key: String, numPartitions: Int): Int = {
        province2Index.getOrElse(key, 0)
    }
}
