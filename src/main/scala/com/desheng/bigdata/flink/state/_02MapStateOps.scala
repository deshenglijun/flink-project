package com.desheng.bigdata.flink.state

import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * 键控状态处理之MapState
 * SparkCore 分组TopN
 *
 * 需求：求解不同年级不同科目的最大
 *     输出是：输出一个年级的所有科目的最高成绩
 */
object _02MapStateOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE)
        env.setStateBackend(new FsStateBackend("file:/E:/data/flink/monitor/chk"))
        val lines = env.fromElements(
            "三年二班   chinese ls 91",
            "三年二班   english ww 56",
            "三年二班   chinese zs 90",
            "三年二班   chinese zl 76",
            "三年二班   english zq 88",
            "三年二班   chinese wb 95",
            "三年二班   chinese sj 74",
            "三年二班   english ts 87",
            "三年二班   english ys 67",
            "三年二班   english mz 77",
            "三年二班   chinese yj 98",
            "三年二班   english gk 96"
        )

        val keyedStream = lines.map(line => {
            val fileds = line.split("\\s+")
            if(fileds == null || fileds.length != 4) {
                TopN2(null, null, null, -1d)
            } else {
                val grade = fileds(0)
                val course = fileds(1)
                val name = fileds(2)
                val score = fileds(3).toDouble
                TopN2(grade, course, name, score)
            }
        }).filter(topn => topn.grade != null)
         .keyBy(topn => topn.grade)
        //keyBy操作会将相同key的数据拉取同一个分区中进行处理
        keyedStream.process(new KeyedProcessFunction[String, TopN2, Map[String, TopN2]] {
            lazy val courseMaxScoreState = getRuntimeContext.getMapState[String, TopN2](
                new MapStateDescriptor[String, TopN2]("maxScore", Types.of[String], Types.of[TopN2])
            )
            override def processElement(value: TopN2,
                                        ctx: KeyedProcessFunction[String, TopN2, Map[String, TopN2]]#Context,
                                        out: Collector[Map[String, TopN2]]): Unit = {
                var maxScore = courseMaxScoreState.get(value.course)
                if(maxScore == null) {
                    maxScore =  TopN2(value.grade, value.course, null, -1d)
                }
                print(s"年级为：${value.grade}上一次各科最高成绩为：${maxScore}, 本次输入的信息为：${value}, ")
                if(maxScore.score < value.score) {
                    maxScore = value
                }
                courseMaxScoreState.put(maxScore.course, maxScore)

                val resultList = ArrayBuffer[(String, TopN2)]()
                for(kv <- courseMaxScoreState.iterator()) {
                    val course = kv.getKey
                    val maxTopN2 = kv.getValue
                    resultList.append((course, maxTopN2))
                }

                println(s"本次各科最高成绩为: ${resultList}")
                out.collect(resultList.toMap)
            }
        }).print(">>course max<<")

        env.execute(s"${_02MapStateOps.getClass.getSimpleName}")
    }
}

case class TopN2(grade: String, course: String, name: String, score: Double)