package com.desheng.bigdata.flink.state

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

object _03FlatMapWithStateOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
//        env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE)
//        env.setStateBackend(new FsStateBackend("file:/E:/data/flink/monitor/chk"))
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
        //分组topN，一个key对应结果是一个集合，多条记录
        keyedStream.flatMapWithState[TopN, List[TopN]]((value, optionTopN) => {
            println("上一次的Top3：" + optionTopN + ", 本次输入的元素为：" + value)
            optionTopN match {
                case None => { //第一次出现，value就是topN中的一员
                    (List[TopN](value), Option[List[TopN]](List(value)))
                }
                case Some(oldTopN) => {//历史已经存在topN
                    val newTopN = oldTopN.+:(value).sortWith((t1, t2) => t1.score > t2.score).take(3)
                    (newTopN, Option(newTopN))
                }
            }
        }).print(">分组TopN<")

        env.execute(s"${_03FlatMapWithStateOps.getClass.getSimpleName}")
    }
}
