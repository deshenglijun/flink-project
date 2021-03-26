package com.desheng.bigdata.flink.window

import org.apache.flink.streaming.api.scala._

/**
 * CountWindow
 *  每隔M条记录执行一次window计算
 *  有两种类型：
 *      Tumbling CountWindow 滚动Count
 *          countWindow(M)
 *              不是从源头输入M条记录进行一次计算，而是window中某一个key的纪录条数达到了M，便启动一次window的计算，来计算这些key的次数超过M的数据。
 *      Sliding CountWindow  滑动Count
 *          countWindow(N, M)
 *              窗口中每隔M个元素，就要进行一次计算，窗口中最多存放的元素就是N个
 */
object _01CountWindowOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val lines = env.socketTextStream("bigdata01", 9999)

        val keyedStream = lines.flatMap(_.split("\\s+")).map((_, 1)).keyBy(_._1)

        keyedStream.countWindow(4, 2)
                .sum(1)
                .print()

        env.execute(s"${_01CountWindowOps.getClass.getSimpleName}")
    }
}
