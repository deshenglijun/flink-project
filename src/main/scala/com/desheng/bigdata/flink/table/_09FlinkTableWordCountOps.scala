package com.desheng.bigdata.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/*
    flink sql 案例之wordcount
    对_05FlinkTableWordCountOps的修正
 */
object _09FlinkTableWordCountOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val bTEnv = BatchTableEnvironment.create(env)

        val dataset = env.fromElements(
            "hello you",
            "hello me",
            "hello you"
        )

        bTEnv.registerFunction("split", new SplitScalarFunction())
        bTEnv.registerFunction("myExplode", new MyExplodeFunction())

        val table = bTEnv.fromDataSet(dataset).as("line")

        bTEnv.registerTable("wc", table)
        //wordcount
        val sql =
            """
              |select
              |  tmp.word,
              |  count(tmp.word) counts
              |from (
              |  select
              |    word
              |  from wc,
              |  lateral table(myExplode(split(line, ' '))) as t(word)
              |  ) tmp
              |group by tmp.word
              |""".stripMargin

        val ret = bTEnv.sqlQuery(sql)

        bTEnv.toDataSet[Row](ret).print
    }
}
//一个字符串，转为一个集合
class SplitScalarFunction extends ScalarFunction {
    def eval(line: String, regex: String): Array[String] = {
        line.split(regex)
    }
}
