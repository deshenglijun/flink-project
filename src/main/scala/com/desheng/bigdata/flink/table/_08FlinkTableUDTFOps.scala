package com.desheng.bigdata.flink.table

import java.text.SimpleDateFormat

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.types.Row
import org.json.JSONObject

/**
 *  flink table的自定义udtf
 *  hive中的udtf的调用方式
 *      如果直接调用explode，在其前面不加任何字段，在select后面直接使用explode就可以了
 *      但是在explode前面如果要添加其他字段，不可以直接调用，需要用到laterview
 *
 *      select
 *          id,
 *          explode(xxx)
 *      from
 *  同样，在flink中不管explode前面加不加字段，都需要将该表函数作为一个新生成表或者视图和原先的表并列进行关联查询
 *   语法结构：
 *      select
 *          field
 *      from wc, later table(udtf) as viewName(field)
 */
object _08FlinkTableUDTFOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val bTEnv = BatchTableEnvironment.create(env)


        val ds = env.fromElements(
            "hello you",
            "hello me",
            "hello you"
           )
        //注册udtf
        bTEnv.registerFunction("myExplode", new MyExplodeFunction())

        val table = bTEnv.fromDataSet(ds).as("line")

        //注册成为一张表
        bTEnv.registerTable("wc", table)

        val sql =
            s"""
              |select
              |  tmp.word,
              |  count(1) counts
              |from (
              |   select
              |     word
              |   from wc,
              |   lateral table(myExplode(line, '\\s+')) as t(word)
              |) tmp
              |group by tmp.word
              |""".stripMargin
        val ret = bTEnv.sqlQuery(sql)

        bTEnv.toDataSet[Row](ret).print
    }
}
/*
    udtf是1对多的操作，所以在计算完毕进行输出的时候，不能结束一次性全部输出，得需要一次次的输出
 */
class MyExplodeFunction extends TableFunction[String] {

    def eval(str: String, regex: String) = {
        val array = str.split(regex)
        for(word <- array) {
            collector.collect(word)
        }
    }
    def eval(array: Array[String]) = {
        for(word <- array) {
            collector.collect(word.toUpperCase)//这里toUpperCase进行区别
        }
    }
}