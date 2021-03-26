package com.desheng.bigdata.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * 在scala的编程中一定要导入隐式转换，org.apache.flink.api.scala._
 * 这样加载数据集才可以成功
 */
object FlinkScalaDataSetWordCountOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        val lines = env.fromElements("love is blind",
            "where shall i go to kill some time",
            "tale is cheap, show me the code")

        val ret = lines.flatMap(line => line.split("\\s+"))
                .map(word => (word, 1))
                .groupBy(0)
                .sum(1)

        ret.print()

    }
}
