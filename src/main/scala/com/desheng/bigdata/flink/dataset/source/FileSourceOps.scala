package com.desheng.bigdata.flink.dataset.source

import org.apache.flink.api.scala._

/**
 * Flink数据源之基于文件
 */
object FileSourceOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        //以指定的字符编码集加载文件
        val lines = env.readTextFile("file:/E:/data/flink/hello-utf8.txt", "utf-8")

        val ret = lines.flatMap(line => line.split("\\s+"))
            .map(word => (word, 1))
            .groupBy(0)
            .sum(1)

        ret.print()
    }
}
