package com.desheng.bigdata.flink.state

import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

/**
 * 键控状态处理之ListState
 *   某一个key对应的value是一个List
 * SparkCore 分组TopN
 */
object _01ListStateOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
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
        //keyBy操作会将相同key的数据拉取同一个分区中进行处理
        keyedStream.process(new KeyedProcessFunction[String, TopN, List[TopN]] {
            lazy val top3State = getRuntimeContext.getListState[TopN](
                new ListStateDescriptor[TopN]("topN", classOf[TopN])
            )
            override def processElement(value: TopN,
                                        ctx: KeyedProcessFunction[String, TopN, List[TopN]]#Context,
                                        out: Collector[List[TopN]]): Unit = {
                var top3 = top3State.get()
                if(top3 == null) {
                    top3 = List[TopN]()
                }
                val top3Values = top3.toList.+:(value).sortWith((t1, t2) => t1.score > t2.score).take(3)
                println("上一次的Top3：" + top3 + ", 本次输入的元素为：" + value + ", 本次计算之后的Top3：" + top3Values)
                top3State.update(top3Values)
                out.collect(top3Values)
            }
        }).print(">>top3<<")

        env.execute(s"${_01ListStateOps.getClass.getSimpleName}")
    }
}

case class TopN(course: String, name: String, score: Double)