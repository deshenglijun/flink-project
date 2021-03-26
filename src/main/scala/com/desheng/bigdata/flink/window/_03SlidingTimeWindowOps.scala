package com.desheng.bigdata.flink.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * window之TimeWindow
 *     每隔多长时间进行一次计算，这个才是应用最广泛的
 *  分类：
 *      Tumbling TimeWindow :  滚动的时间窗口
 *              timeWindow(x)
 *              窗口时间固定，两个窗口之间没有缝隙，这种方式相当于len=sliding
 *      Sliding TimeWindow  :  滑动的时间窗口
 *              timeWindow(len, sliding) 相当于sparkstreaming中的Window操作
 *              每隔sliding对len时间内的数据进行统计分析
 *      Session TimeWindow  :  会话时间窗口
 *
 */
object _03SlidingTimeWindowOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val lines = env.socketTextStream("bigdata01", 9999)

        val keyedStream = lines.flatMap(_.split("\\s+")).map((_, 1)).keyBy(_._1)

        keyedStream
            .timeWindow(Time.seconds(4), Time.seconds(2))// 每隔2s钟统计4s钟产生一个时间滚动窗口
            .sum(1)
            .print()

        env.execute(s"${_03SlidingTimeWindowOps.getClass.getSimpleName}")
    }
}
