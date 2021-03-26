package com.desheng.bigdata.flink.watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 水印机制
 *
 *  通过代码的执行，我们看到的窗口的开始时间为窗口第一条纪录-了延迟时间，
 *  关窗时间为开始时间+窗口长度
 *  上一个窗口的关窗时间是下一个窗口的开窗时间
 *  并且上一个窗口的关窗时间不会包含在上一个窗口中，也就是窗口的特点是[start, end)
 */
object _04WaterMarkOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //设置生成数据的频率 默认的水印生成频率200ms
        env.getConfig.setAutoWatermarkInterval(100L) //每隔100ms中生成一个水印

        //修改事件语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val lines = env.fromElements(
            WatermarkBean("zhangsan", 1, 13, 10000L),
            WatermarkBean("lisi", 1, 14, 11000L),
            WatermarkBean("tom", 1, 18, 12000L),
            WatermarkBean("wangwu", 1, 15, 13000L),
            WatermarkBean("jack", 1, 17, 18000L),
            WatermarkBean("zhaoliu", 1, 16, 12500L),
            WatermarkBean("lucy", 1, 19, 12000L),
            WatermarkBean("lily", 1, 20, 17000L),
            WatermarkBean("john", 1, 23, 19000L),
            WatermarkBean("jack", 1, 22, 20000L),
            WatermarkBean("bush", 1, 23, 23000L),
            WatermarkBean("libai", 1, 20, 15000L),
            WatermarkBean("dufu", 1, 23, 16000L),
            WatermarkBean("wangwei", 1, 22, 19000L),
            WatermarkBean("sushi", 1, 23, 19500L)
        )
        val datastrem =lines.assignTimestampsAndWatermarks(new TimeLagWatermarkGenerator())
        datastrem
            .keyBy(wmb => wmb.gender)
            .timeWindow(Time.seconds(10))// 每隔3s钟产生一个时间滚动窗口
            //计算年龄最大的用户
            .process(new ProcessWindowFunction[WatermarkBean, WatermarkBean, Int, TimeWindow] {
                override def process(key: Int, context: Context, elements: Iterable[WatermarkBean], out: Collector[WatermarkBean]): Unit = {
                    val startTime = context.window.getStart
                    val endTime = context.window.getEnd
                    var wmb: WatermarkBean = null
                    val maxTime = context.window.maxTimestamp()
                    val watermark = context.currentWatermark
                    println("窗口开始时间为：" + startTime + ", 窗口的结束时间为：" + endTime + ",窗口中的最大时间为：" + maxTime + ", 当前窗口水印：" + watermark)

                    for(ele <- elements) {
                        println("窗口为："+ startTime + "，遍历的元素为：" + ele)
                        if(wmb == null || wmb.age < ele.age) {
                            wmb = ele
                        }
                    }
                    out.collect(wmb)
                    println("窗口开始时间为：" + startTime + ", 窗口的结束时间为：" + endTime + ",窗口中的最大时间为：" + maxTime + ", 当前窗口水印：" + watermark)
                }
            }).print()

        env.execute(s"${_04WaterMarkOps.getClass.getSimpleName}")
    }
}

class TimeLagWatermarkGenerator extends AssignerWithPeriodicWatermarks[WatermarkBean] {
    private final val maxTimeLag = 3000L; // 3 seconds

    override def extractTimestamp(element: WatermarkBean, previousElementTimestamp: Long): Long = {
        element.registTime
    }

    override def getCurrentWatermark(): Watermark = {
    // return the watermark as current time minus the maximum time lag
        new Watermark(System.currentTimeMillis() - maxTimeLag)
    }
}