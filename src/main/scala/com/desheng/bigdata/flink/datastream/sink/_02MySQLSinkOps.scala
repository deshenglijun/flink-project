package com.desheng.bigdata.flink.datastream.sink

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * flink的sink操作，没有类似于spark中的foreach的操作
 * 自定义的类，核心的有两个：
 *      SinkFunction
 *          invoke
 *      RichSinkFunction
 *          open
 *          invoke
 *          close
 */
object _02MySQLSinkOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val lines = env.fromElements(
            "implicit you",
            "implicit you",
            "implicit me"
        )

        val ret = lines.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0).sum(1)

        ret.print()

        ret.addSink(new MySQLSinkFunction())

        env.execute(s"${_02MySQLSinkOps.getClass.getSimpleName}")
    }
}
class MySQLSinkFunction extends RichSinkFunction[(String, Int)] {
    private var connection: Connection = null
    private var ps: PreparedStatement = null
    override def open(parameters: Configuration): Unit = {
        Class.forName("com.mysql.jdbc.Driver")
        val url = "jdbc:mysql://localhost:3306/test"
        val user = "mark"
        val password = "sorry"
        connection = DriverManager.getConnection(url, user, password)
        val sql =
            """
              |insert into wordcounts(word, `count`) values(?, ?)
              |""".stripMargin
        ps = connection.prepareStatement(sql)
    }
    override def invoke(kv: (String, Int), context: SinkFunction.Context[_]): Unit = {
        ps.setString(1, kv._1)
        ps.setInt(2, kv._2)
        ps.execute()
    }

    override def close(): Unit = {
        try {
            if (ps != null) {
                ps.close()
            }
        }catch {
            case e: SQLException => {
                e.printStackTrace()
            }
        } finally {
            if(connection != null) {
                connection.close()
            }
        }
    }
}
