package com.desheng.bigdata.flink.dataset.transformation

import org.apache.flink.api.scala._

/**
 * partition就是分区
 *  flink默认提供了两种的分区方式
 *      partitionByHash
 *      partitionByRange
 */
object _06PartitionByHashOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val persons = env.fromCollection(List(
            Person(1, "刘国伟", 18, 0, "河北"),
            Person(2, "马惠", 19, 1, "辽宁"),
            Person(21, "小涛涛", 29, 0, "辽宁"),
            Person(3, "刘鑫", 20, 0, "贵州"),
            Person(31, "郑朝义", 22, 0, "贵州"),
            Person(4, "韩香彧", 20, 1, "内蒙古"),
            Person(41, "佟凯", 22, 1, "内蒙古"),
            Person(106, "刘照路", 30, 1, "河南")
        ))

        //使用原生的分区方式进行分区
        val partitioned:DataSet[Person] = persons.partitionByHash(person => person.province)

        partitioned.mapPartition(ps => {
            val list = ps.toList
            println("分区内容：" + list.mkString("[",  ", ",  "]"))
            list
        }).print()

    }
}
case class Person(id: Int, name: String, age: Int, gender: Int, province: String)