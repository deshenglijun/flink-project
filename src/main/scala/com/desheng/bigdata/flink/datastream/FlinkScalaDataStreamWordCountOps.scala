package com.desheng.bigdata.flink.datastream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object FlinkScalaDataStreamWordCountOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setParallelism(9)
        val lines = env.socketTextStream("bigdata01", 9999)

        val ret = lines.flatMap(line => line.split("\\s+"))
                .map(word => WordCount(word, 1))
                .keyBy("word")
                .sum("count")

        ret.print("wordcount:::").setParallelism(1)


        env.execute(s"${FlinkScalaDataStreamWordCountOps.getClass.getSimpleName}")
    }
}
case class WordCount(word: String, count: Int)
