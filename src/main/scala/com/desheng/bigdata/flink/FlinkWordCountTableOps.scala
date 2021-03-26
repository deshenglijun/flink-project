package com.desheng.bigdata.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
/**
 * 学习Flink UDF的操作
 * 自定义的步骤：
 *   1、创建一个类或者一个函数
 *   2、实现业务逻辑
 *   3、注册使用
 * 需求：
 *   使用sql来进行wordcount的统计
 * 执行过程：
     select
       tmp.word,
       count(1) as counts
     from (
       select
         explode(split(line, ' ')) word
       from wc
     ) tmp
     group by tmp.word
 * 错误：
 *  ValidationException: SQL validation failed. From line 7, column 14 to line 7, column 29: No match found for functio
 * 原因：
 *   目前版本的flinksql并不支持split和explode函数，所以，以上错误
 * 解决：
 *   自定义udf和udtf解决
 */
object FlinkWordCountTableOps {
    def main(args: Array[String]): Unit = {
        //加载table api的入口
        val env = ExecutionEnvironment.getExecutionEnvironment
        val bTable = BatchTableEnvironment.create(env)
        //加载外部数据
        val dataset = env.fromElements(
            "hello you",
            "hello me",
            "hello you"
        )
        val table = bTable.fromDataSet(dataset)//给当前的table指定field字段名称
            .as("line")
        //注册table为一张临时表
        bTable.registerTable("wc", table)
        // 注册
        bTable.registerFunction("myExplode", new MyExplodeFunction)


        val sql =
            s"""
              | select
              |   word,
              |   count(1) as counts
              | from wc,
              | lateral table(myExplode(line, ' ')) as t(word)
              | group by word
              |""".stripMargin

        val ret = bTable.sqlQuery(sql)
        bTable.toDataSet[Row](ret).print()
    }
}
//1、创建一个类或者一个函数
class MyExplodeFunction extends TableFunction[String] {
  
}