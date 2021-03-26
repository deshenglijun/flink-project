package com.desheng.bigdata.flink.datastream.transformation

import org.apache.flink.streaming.api.scala._

/**
 * flink的分组操作keyBy
 *      要向调用keyBy进行数据分组，该datastream的类型必须是kv键值对
 * 聚合操作reduce
 *      和spark中的reduce略有差异，flink中反映的是一个增量式的计算，会输出每一个增量之后的结果
 * 聚合操作之sum
 *      sum类似reduce，也是一个增量计算
 */
object _04KeyByAndReduceOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val genders = env.fromCollection(List(
            Student(1, "林博", 18, 0),
            Student(2, "单松", 19, 1),
            Student(3, "张皓", 20, 0),
            Student(4, "王建", 20, 1),
            Student(106, "冯岩", 30, 1)
        ).map(stu => (stu.gender, 1)))

        val keyedStream: KeyedStream[(Int, Int), Int] = genders.keyBy(kv => kv._1)

        //聚合操作
//        keyedStream.reduce((kv1, kv2) => {
//            (kv1._1, kv1._2 + kv2._2)
//        }).print()


        keyedStream.sum(1).print()


        env.execute(s"${_04KeyByAndReduceOps.getClass.getSimpleName}")
    }
}

case class Student(id: Int, name: String, age: Int, gender: Int)
