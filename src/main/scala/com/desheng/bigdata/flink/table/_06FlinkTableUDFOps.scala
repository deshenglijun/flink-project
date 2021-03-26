package com.desheng.bigdata.flink.table

import java.text.SimpleDateFormat

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.json.JSONObject

/**
 *  flink table的自定义udf
 *
 *  需求：将格式化时间转化为时间戳
 *      2020-10-01 10:02:02
 */
object _06FlinkTableUDFOps {
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
        //自定义udf
        bTEnv.registerFunction("to_time", new TimeScalarFunction())
        bTEnv.registerFunction("myLen", new LenScalarFunction())
        val table = bTEnv.fromDataSet(ds)
        val sql =
            s"""
              |select
              |  userID,
              |  eventTime,
              |  myLen(eventTime) my_len_et,
              |  to_time(eventTime) timestamps
              |from ${table}
              |""".stripMargin
        val ret = bTEnv.sqlQuery(sql)

        bTEnv.toDataSet[Row](ret).print
    }
}
case class UserBrowseLog(
    userID: Int,
    eventTime: String,
    eventType: String,
    productID: String,
    productPrice: Double
)

/*
    自定义类去扩展ScalarFunction 复写其中的方法：eval
    at least one method named 'eval' which is public, not
 */
class TimeScalarFunction extends ScalarFunction {
    //2020-10-01 10:02:16
    private val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    def eval(eventTime: String): Long = {
        df.parse(eventTime).getTime
    }
}

class LenScalarFunction extends ScalarFunction {
    //2020-10-01 10:02:16
    def eval(str: String): Int = {
        str.length
    }
}