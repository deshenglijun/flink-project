package com.desheng.bigdata.flink.datastream.source

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 * 自定义Source之RichParallelSourceFunction
 *
 * 模拟从数据库读取数据
 */
object _04UserDefineRichParallelSourceOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val result = env.addSource(new MySQLSourceFunction()).setParallelism(2)
        result.print()
        env.execute(s"${_04UserDefineRichParallelSourceOps.getClass.getSimpleName}")
    }

    class MySQLSourceFunction extends RichParallelSourceFunction[Person] {
        private var connection: Connection = null
        private var ps: PreparedStatement = null
        private var rs: ResultSet = null
        override def open(parameters: Configuration): Unit = {
            Class.forName("com.mysql.jdbc.Driver")
            val url = "jdbc:mysql://localhost:3306/test"
            val user = "mark"
            val password = "sorry"
            connection = DriverManager.getConnection(url, user, password)
            val sql =
                """
                  |select
                  |  id,
                  |  name,
                  |  age,
                  |  height
                  |from person
                  |""".stripMargin
            ps = connection.prepareStatement(sql)
        }

        override def run(ctx: SourceFunction.SourceContext[Person]): Unit = {
            rs = ps.executeQuery()
            while(rs.next()) {
                val id = rs.getInt("id")
                val name = rs.getString("name")
                val age = rs.getInt("age")
                val height = rs.getDouble("height")
                ctx.collect(Person(id, name, age, height))
            }
        }

        override def cancel(): Unit = {

        }

        override def close(): Unit = {
            try {
                if (rs != null) {
                    rs.close()
                }
            } catch {
                case e: SQLException => {
                    e.printStackTrace()
                }
            }finally {
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
    }
}
