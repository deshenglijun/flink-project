package com.desheng.bigdata.flink.table

import java.text.SimpleDateFormat

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction}
import org.apache.flink.types.Row
import org.json.JSONObject

/**
 *  flink table的自定义udaf
 *
 *  需求：统计不同userid对应的sum price
 */
object _07FlinkTableUDAFOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val bTEnv = BatchTableEnvironment.create(env)


        val ds = env.fromElements(
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:00\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:02\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:10\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:12\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 2, \"eventTime\": \"2020-10-01 10:02:06\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:15\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}",
            "{\"userID\": 1, \"eventTime\": \"2020-10-01 10:02:16\", \"eventType\": \"browse\", \"productID\": \"product_5\", \"productPrice\": 20.99}"
        ).map(line => {
            val jsonObj = new JSONObject(line)
            val userID = jsonObj.getInt("userID")
            val eventTime = jsonObj.getString("eventTime")
            val eventType = jsonObj.getString("eventType")
            val productID = jsonObj.getString("productID")
            val productPrice = jsonObj.getDouble("productPrice")
            UserBrowseLog(userID, eventTime, eventType, productID, productPrice)
        })
        //注册udaf
        bTEnv.registerFunction("mySum", new MySumUDAFFunction)

        //自定义udf
        val table = bTEnv.fromDataSet(ds)

        //注册成为一张表
        bTEnv.registerTable("user_browse", table)

        val sql =
            s"""
              |select
              |  userID,
              |  sum(productPrice) sum_price,
              |  mySum(productPrice) my_sum_price
              |from user_browse
              |group by userID
              |""".stripMargin
        val ret = bTEnv.sqlQuery(sql)

        bTEnv.toDataSet[Row](ret).print
    }
}
/*
    自定义udaf
        需要制定聚合时候的累加器
 */
class MySumUDAFFunction extends AggregateFunction[Double, MySumAccumulator] {
    //getValue便是最后的聚合结果返回值
    override def getValue(accu: MySumAccumulator): Double = accu.sum

    def accumulate(accu: MySumAccumulator, price: Double): Unit = {
        accu.sum += price
    }

    def resetAccumulator(accu: MySumAccumulator): Unit = {
        accu.sum = 0
    }
    /**
     * 构造累加器
     * @return
     */
    override def createAccumulator(): MySumAccumulator = new MySumAccumulator
}

class MySumAccumulator {
    var sum = 0.0d
}