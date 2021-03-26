package com.desheng.bigdata.flink.watermark

import java.util
import java.util.{Arrays, List}

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object SlideTumblingEventWindowExample {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
            env.setParallelism(1)
            env.getConfig.setAutoWatermarkInterval(1000)

           val socketStream = env.socketTextStream("bigdata01", 9999)

            val outputTag = new OutputTag[(String, Long)]("late-data")

            val keyedStream: KeyedStream[(String, Long), Tuple] = socketStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(3)) {
                    override def extractTimestamp(element: String): Long = {
                        val eventTime: Long = element.split(" ")(0).toLong
                            println("当前纪录的eventTime: " + eventTime)
                            eventTime
                    }
                }
            ).map(new MapFunction[String, (String, Long)] {
                override def map(value: String): (String, Long) = {
                    (value.split(" ")(1), 1L)
                }
            }).keyBy(0)


                val resultStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sideOutputLateData(outputTag)
                .allowedLateness(Time.seconds(2))
                .reduce(new ReduceFunction[(String, Long)] {
                    override def reduce(t: (String, Long), t1: (String, Long)): (String, Long) = {
                        (t._1, t._2 +t1._2)
                    }
                })
                resultStream.print()

                resultStream.getSideOutput(outputTag).print("sideOutput:::")

                env.execute()
    }
}