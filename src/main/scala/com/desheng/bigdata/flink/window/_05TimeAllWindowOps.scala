package com.desheng.bigdata.flink.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
    flink中针对window窗口的操作，主要分为了两大类
        增量窗口函数计算
            窗口来一条纪录，计算一次，典型的代表就是reduce、sum、max等等
        全量窗口函数计算
            会先将一个窗口中的所有的数据集都收集完毕之后，在进行计算，奠定的代表为ProcessWindowFunction

     全量窗口函数
 */
object _05TimeAllWindowOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val lines = env.socketTextStream("bigdata01", 9999)
        val keyedStream = lines.flatMap(_.split("\\s+")).map((_, 1)).keyBy(_._1)
        keyedStream
            .timeWindow(Time.seconds(4), Time.seconds(2))// 每隔2s钟统计4s钟产生一个时间滚动窗口
            .process(new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
                override def process(key: String, context: Context,
                                     elements: Iterable[(String, Int)], //elements就为窗口中收集到的key所对应的所有的元素
                                     out: Collector[(String, Int)]): Unit = {
                    /*
                    var sum = 0
                    for(ele <- elements) {
                        sum += ele._2
                    }
                    */
                    out.collect((key, elements.size))
                }
            })
            .print()

        env.execute(s"${_05TimeAllWindowOps.getClass.getSimpleName}")
    }
}
