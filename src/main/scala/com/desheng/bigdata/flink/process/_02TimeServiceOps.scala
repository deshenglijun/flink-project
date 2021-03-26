package com.desheng.bigdata.flink.process

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
/*
    定时服务说明
        监控温度传感器的温度值，如果温度值在一秒钟之内(processing time)连续上升，则报警。
 */
object _02TimeServiceOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val dataStream = env.socketTextStream("bigdata01",9999)

        val textKeyStream = dataStream
            .map{
                line => val words = line.split("\t")
                    (words(0).trim, words(1).trim.toLong, words(2).trim.toDouble)
            }
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Double)](Time.seconds(2)) {
                    override def extractTimestamp(t: (String, Long, Double)): Long = {
                        t._2 * 1000
                    }
                }
            )
            .keyBy(_._1)

        val result = textKeyStream.process(new PorceTempFunc)

        textKeyStream.print("textKeyStream ::: ").setParallelism(1)
        result.print("result:::").setParallelism(1)
        env.execute("_02TimeServiceOps")
    }
}
class PorceTempFunc extends KeyedProcessFunction[String,(String, Long, Double),String]{
    //上一次的温度放到状态中
    lazy val lastTemp:ValueState[Double] = getRuntimeContext.getState(
        new ValueStateDescriptor[Double]("lastTem",Types.of[Double])
    )

    // 保存注册的定时器的时间戳
    lazy val currentTimer:ValueState[Long] = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("currentTimer", Types.of[Long])
    )
    //lastTem currentTimer如果没有初始化，里面的值是0

    //processElement 每一个元素都会调用这个方法,每调用一次都要更新一次lastTem
    override def processElement(current: (String, Long, Double),
                                context: KeyedProcessFunction[String, (String, Long, Double), String]#Context,
                                collector: Collector[String]): Unit = {

        //每调用一次processElement方法都要更新一次lastTem
        val perTemp = lastTemp.value()
        lastTemp.update(current._3)
        //获取定时器的时间戳
        val curTimerTimestamp = currentTimer.value()
    /*
       第一次采集数据：perTemp == 0上一次的温度是0，取消定时器
       温度上升: 第二次采集的温度比第一次采集的温度高，注册定时器
                 这个时间范围内，第三次以及后面采集的温度比上次的温度高，这个时间范围值内以及注册了定时，则不再注册。可用通过定时器的时间戳是否==0来判断。
       温度下降：这个时间范围内，温度下降，删除定时器
    */
    if(current._3 < perTemp || perTemp == 0){//如果在规定的时间之内，温度出现下降就要删除定时器
      //如果第一次采集数据，lastTemp没有初始化，中保存的值perTemp那么它的值就是0
      context.timerService().deleteProcessingTimeTimer(currentTimer.value())
      currentTimer.clear()
    } else if(current._3>perTemp && curTimerTimestamp == 0 ){
      //如果温度上升并且没有注册过定时器 注册一个定时器  如果在这个时间间隔之内已经注册过定时器则不注册
      // 获取当前时间 并+1s 设置为2s之后触发定时器,并且将时间戳保存到一个状态理念
      val TimeTs = context.timerService().currentProcessingTime()+1000L
      //P注册一个rocessingTime定时器，时间戳应该是当前时间+1s
      context.timerService().registerProcessingTimeTimer(TimeTs)
      //更新定时器的时间戳到currentTimer状态中
      currentTimer.update(TimeTs)
    }
  }
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, (String, Long, Double), String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    //输出报警信息
    out.collect("传感器 id 为: " + ctx.getCurrentKey + "的传感器温度值已经连续 1s 上升了。")
    //定时器的时间戳要清空
    currentTimer.clear()
  }
}