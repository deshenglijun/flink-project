package com.desheng.bigdata.flink.datastream.transformation

import org.apache.flink.streaming.api.scala._

/**
 * 其他的集合算子
 *      以max和maxBy为例
 *          max除了比较字段以外不会进行其他数据的替换
 *          maxBy和max相反,除了对比较字段进行替换以外还需要对其他所有的额字段都会进行替换
 *      同理，min和minBy的区别和max和maxBy的区别一直
 * 计算不同性别的最大年龄
 *
 */
object _05MaxAndMaxByOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val genders = env.fromCollection(List(
            Student(1, "林博", 18, 0),
            Student(2, "单松", 19, 1),
            Student(21, "单松2", 18, 1),
            Student(22, "单松3", 18, 1),
            Student(23, "单松4", 29, 1),
            Student(3, "张皓", 20, 0),
            Student(31, "张皓2", 20, 0),
            Student(4, "王建", 20, 1),
            Student(106, "冯岩", 30, 1)
        ))

        val keyedStream = genders.keyBy(stu => stu.gender)

        //计算不同性别的最大年龄
        keyedStream.max("age").print("max:::")
        keyedStream.maxBy("age").print("maxBy:::")
        env.execute(s"${_04KeyByAndReduceOps.getClass.getSimpleName}")
    }
}

