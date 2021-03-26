package com.desheng.bigdata.flink.table

import com.offcn.bigdata.flink.datastream.transformation.Goods
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
 * flink sql的操作
 *  是基于flink table之上的高阶api操作
 *
 *  我们在使用基于stream的聚合统计的时候出现了 不支持语法的异常，
 *      原因在于聚合统计，需要的是基于多条记录的统计，stream是一条一条的数据尽量进行统计，和前面所述的多条统计相矛盾，所以其不支持直接基于流进行统计
 *      要想在流的上面进行聚合统计，就得需要涉及窗口。
 */
object _03FlinkSQLOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment

        val sTEnv = BatchTableEnvironment.create(env)
        val dataStream: DataSet[Goods] = env.fromElements(
            "001|mi|mobile",
            "002|mi|mobile",
            "003|mi|mobile",
            "004|mi|mobile",
            "005|huawei|mobile",
            "006|huawei|mobile",
            "007|huawei|mobile",
            "008|Oppo|mobile",
            "009|Oppo|mobile",
            "010|uniqlo|clothing",
            "011|uniqlo|clothing",
            "012|uniqlo|clothing",
            "013|uniqlo|clothing",
            "014|uniqlo|clothing",
            "015|selected|clothing",
            "016|selected|clothing",
            "017|selected|clothing",
            "018|Armani|clothing",
            "019|lining|sports",
            "020|nike|sports",
            "021|adidas|sports",
            "022|nike|sports",
            "023|anta|sports",
            "024|lining|sports"
        ).map(line => {
            val fields = line.split("\\|")

            Goods(fields(0), fields(1), fields(2))
        })
        //load data from external system
        var table = sTEnv.fromDataSet(dataStream)
        sTEnv.registerTable("goods", table)
        //sql操作
        var sql =
            """
              |select
              |   id,
              |   brand,
              |   category
              |from goods
              |""".stripMargin

        sql =
            """
              |select
              |   category,
              |   count(1) counts
              |from goods
              |group by category
              |order by counts desc
              |""".stripMargin
        table = sTEnv.sqlQuery(sql)

        sTEnv.toDataSet[Row](table).print()
    }
}
