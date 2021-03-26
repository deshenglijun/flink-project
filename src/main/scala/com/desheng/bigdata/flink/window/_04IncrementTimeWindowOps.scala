package com.desheng.bigdata.flink.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
    flink中针对window窗口的操作，主要分为了两大类
        增量窗口函数计算
            窗口来一条纪录，计算一次，典型的代表就是reduce、sum、max等等
        全量窗口函数计算
            会先将一个窗口中的所有的数据集都收集完毕之后，在进行计算，奠定的代表为ProcessWindowFunction
 */
object _04IncrementTimeWindowOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val lines = env.socketTextStream("bigdata01", 9999)
        val keyedStream = lines.flatMap(_.split("\\s+")).map((_, 1)).keyBy(_._1)
        keyedStream
            .timeWindow(Time.seconds(4), Time.seconds(2))// 每隔2s钟统计4s钟产生一个时间滚动窗口
            .reduce(new ReduceFunction[(String, Int)] {
                override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
                    (v1._1, v1._2 + v2._2)
                }
            })
            .print()

        env.execute(s"${_04IncrementTimeWindowOps.getClass.getSimpleName}")
    }
}
