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
 *
 *  执行的时候没有指定timeCharacteristic而出现了异常信息
 * Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'
 *  在时间窗口进行计算的时候，务必要指定清楚我们是基于哪一个时间来进行数据的统计与分析的
 *      时间类型TimeCharacteristic来表示：
 *          ProcessingTime（系统默认的处理时间）
 *          IngestionTime
 *          EventTime
 */
object _02TumbilingTimeWindowOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

        val lines = env.socketTextStream("bigdata01", 9999)

        val keyedStream = lines.flatMap(_.split("\\s+")).map((_, 1)).keyBy(_._1)

        keyedStream
            //以前的处理方式
            //.window(TumblingProcessingTimeWindows.of(Time.seconds(5))) //每隔5s钟产生一个时间滚动窗口
           //现在主流的处理方式
            .timeWindow(Time.seconds(5))// 每隔5s钟产生一个时间滚动窗口
            .sum(1)
            .print()

        env.execute(s"${_02TumbilingTimeWindowOps.getClass.getSimpleName}")
    }
}
