package com.desheng.bigdata.flink.watermark

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object _01TimeCharacteristicApp {
    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //要基于eventtime进行处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val lines = env.fromCollection(List(
            UserClick(1, "张三", "http://bd.ujiuye.com", 1600239353208L),
            UserClick(2, "李四", "http://java.ujiuye.com", 1600239353608L),
            UserClick(3, "王五", "http://bd.ujiuye.com", 1600239353428L),
            UserClick(4, "赵六", "http://ui.ujiuye.com", 1600239353701L),
            UserClick(5, "周七", "http://python.ujiuye.com", 1600239352258L)
        ))

        //有界无序周期性水印
        lines.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[UserClick](Time.seconds(2)) {
                override def extractTimestamp(uc: UserClick): Long = {
                    uc.timestamp
                }
            }
        ).map(uc => {
            val url = new URL(uc.url)
            url.getHost
            (url.getHost, uc)
        }).keyBy(kv => kv._1)
            .timeWindow(Time.seconds(4))
            .process(new ProcessWindowFunction[(String, UserClick), (String, Int), String, TimeWindow] {
                override def process(key: String, context: Context,
                                     elements: Iterable[(String, UserClick)],
                                     out: Collector[(String, Int)]): Unit = {
                    elements.foreach{case (host, ucs) => {
                        println(ucs)
                    }}
                    out.collect((key, elements.size))
                }
            })
            .print()

        env.execute()
    }
}
case class UserClick(id: Int, name: String, url: String, timestamp: Long)