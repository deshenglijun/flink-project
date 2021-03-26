package com.desheng.bigdata.flink.state

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

/**
 * 键控状态处理之ListState
 *   某一个key对应的value是一个List
 * SparkCore 分组TopN
 */
object _04MapWithStateOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE)
        env.setStateBackend(new FsStateBackend("file:/E:/data/flink/monitor/chk"))
        val lines = env.fromElements(
            "chinese ls 91",
            "english ww 56",
            "chinese zs 90",
            "chinese zl 76",
            "english zq 88",
            "chinese wb 95",
            "chinese sj 74",
            "english ts 87",
            "english ys 67",
            "english mz 77",
            "chinese yj 98",
            "english gk 96"
        )

        val keyedStream = lines.map(line => {
            val fileds = line.split("\\s+")
            if(fileds == null || fileds.length != 3) {
                TopN(null, null, -1d)
            } else {
                val course = fileds(0)
                val name = fileds(1)
                val score = fileds(2).toDouble
                TopN(course, name, score)
            }
        }).filter(topn => topn.course != null)
         .keyBy(topn => topn.course)

        keyedStream.mapWithState[TopN, TopN]((value, maxOption) => {
            println(s"新输入的元素为：${value}, 上一次的Max为${maxOption.getOrElse(null)}")
            maxOption match {
                case None => {
                    (value, Option(value))
                }
                case Some(oldMax) => {
                     val newMax = if(value.score > oldMax.score) value else oldMax
                    (newMax, Option(newMax))
                }
            }
        }).print(">本次的Max<")

        env.execute(s"${_04MapWithStateOps.getClass.getSimpleName}")
    }
}
