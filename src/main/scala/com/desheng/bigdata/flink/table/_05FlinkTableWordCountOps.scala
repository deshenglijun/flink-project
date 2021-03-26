package com.desheng.bigdata.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
/*
    flink sql 案例之wordcount
 */
object _05FlinkTableWordCountOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val bTEnv = BatchTableEnvironment.create(env)

        val dataset = env.fromElements(
            "hello you",
            "hello me",
            "hello you"
        )
        val table = bTEnv.fromDataSet(dataset).as("line")
        bTEnv.registerFunction("split", new SplitScalarFunction())
        bTEnv.registerTable("wc", table)
        //wordcount
        //No match found for function signature split(<CHARACTER>, <CHARACTER>) 没有这样的函数
        val sql =
            """
              |select
              |  tmp.word,
              |  count(1) counts
              |from (
              |   select
              |     word
              |   from wc,
              |   lateral table(explode(split(line, '\\s+'))) as t(word)
              |) tmp
              |group by tmp.word
              |""".stripMargin

        val ret = bTEnv.sqlQuery(sql)

        bTEnv.toDataSet[Row](ret).print
    }
}