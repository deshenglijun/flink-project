package com.desheng.bigdata.flink.datastream.sink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
/**
 * flink的sink算子操作
 *  sink，下沉操作，连接数据计算结果到外部存储介质中
 */
object _01FileSinkOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        val lines = env.fromElements(
            "implicit you",
            "implicit you",
            "implicit me"
        )

        val ret = lines.flatMap(_.split("\\s+")).map((_, 1)).groupBy(0).sum(1)

        //拉取到本地集合
        val info = ret.collect()
        println(s"info: ${info.mkString("[", ", ", "]")}")

        /*
            写入文件系统中
            NO_OVERWRITE: 目录存在则报错
            OVERWRITE： 覆盖已有数据
            添加WriteMode和不佳WriteMode的区别，
                不加，filePath最后一级为目录，
                添加，filePath最后一级为文件
         */
        ret.setParallelism(1)//设置当前算子的并行度为1
//            .writeAsText("file:/E:/data/output/flink/text", WriteMode.OVERWRITE)
                .writeAsCsv(//csv格式的文件
                    filePath = "file:/E:/data/output/flink/csv",
                    rowDelimiter = ";",
                    fieldDelimiter = ",",
                    writeMode = WriteMode.OVERWRITE
                )
        env.execute(s"${_01FileSinkOps.getClass.getSimpleName}")
    }
}
