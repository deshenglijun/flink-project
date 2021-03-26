package com.desheng.bigdata.flink.process

import com.offcn.bigdata.flink.watermark.WatermarkBean
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
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
 *
 * zhangsan,1,13,11
 * lisi,1,14,11
 * tom,1,18,14
 * wangwu,1,15,12
 * jack,1,17,15
 * zhaoliu,1,16,13
 * lucy,1,19,18
 * lily,1,20,16
 *
 * _01中关于上一次性别最大的年龄的问题修正，通过ValueState在RuntimeContext运行时中保存数据
 */
object _03KeyProcessFunctionOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //修改事件语义为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val lines = env.socketTextStream("bigdata01", 9999)
            .map(line => {
                val fields = line.split(",")
                val name = fields(0)
                val gender = fields(1).toInt
                val age = fields(2).toInt
                val registTime = fields(3).toLong
                WatermarkBean(name, gender, age, registTime)
            })

        /**
         * 按照性别分组，共计不同性别中的最大的年龄对应的信息
         *  求性别对应的最大年龄，需要两个参数即可，一者当前进入processFunction（current）的年龄，二者当前性别对应的最大年龄(max)
         *    后续的计算就变为了
         *      max=Math.max(max, current)
         *
         */
        lines.keyBy(wmb => wmb.gender)
                    .process(new KeyedProcessFunction[Int, WatermarkBean, WatermarkBean] {
                        //一个问题：此时的max为当前本地程序所持有，并不会在不同的task之间进行共享，那么会存在统计上的问题，所以需要类似于Spark中的累加来进行解决
                         lazy val lastMaxAge = getRuntimeContext.getState(
                                new ValueStateDescriptor[WatermarkBean]("maxAge", Types.of[WatermarkBean], WatermarkBean(null, -1, -1, -1))
                            )

                        override def processElement(currentBean: WatermarkBean,
                                                    ctx: KeyedProcessFunction[Int, WatermarkBean, WatermarkBean]#Context,
                                                    out: Collector[WatermarkBean]): Unit = {
                            val maxAge = lastMaxAge.value().age

                            println("processFunction中进入的纪录为：" + currentBean + ", 上一次该性别对应的最大年龄信息：" + maxAge)
                            if(maxAge < currentBean.age) {
                                lastMaxAge.update(currentBean)
                                out.collect(currentBean)
                            }
                        }
                    }).print("max age info>>>")


        env.execute(s"${_01KeyProcessFunctionOps.getClass.getSimpleName}")
    }
}