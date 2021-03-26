package com.desheng.bigdata.flink.table

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * 基于eventTime进行窗口统计
 *      每隔2s中统计不同platform下的用户的登录人数
 */
object _03FlinkTableWindowOps {
    def main(args: Array[String]): Unit = {
        //1、获取流式执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        // 2、获取table执行环境
        val tblEnv = StreamTableEnvironment.create(env)
        //3、获取数据源
        //输入数据：
        val ds = env.socketTextStream("bigdata01", 9999)
            .map(line => {
                val fields = line.split(",")
                UserLogin(fields(0), fields(1), fields(2), fields(3).toInt, fields(4))
            })
//            .assignTimestampsAndWatermarks(
//                new BoundedOutOfOrdernessTimestampExtractor[UserLogin](Time.seconds(2)) {
//                    override def extractTimestamp(userLogin: UserLogin): Long = {
//                        userLogin.dataUnix * 1000
//                    }
//                }
//            )
//            .timeWindowAll() //tumble(ts,interval '2' second)
        //4、将DataStream转换成table
        //引入隐式
        //某天每隔2秒的输入记录条数：
        import org.apache.flink.table.api.scala._
        val table: Table = tblEnv.fromDataStream[UserLogin](ds , 'platform, 'server, 'status, 'ts.proctime)

        tblEnv.sqlQuery(
            s"""
               |select
               |  platform,
               |  count(1) counts
               |from ${table}
               |where status = 'LOGIN'
               |group by platform, tumble(ts,interval '2' second)
               |""".stripMargin)
            .toAppendStream[Row]
            .print("每隔2秒不同平台登录用户->")

        env.execute(s"${_03FlinkTableWindowOps.getClass.getSimpleName}")
    }
}
/** 用户登录
 *
 * @param platform 所在平台 id（e.g. H5/IOS/ADR/IOS_YY）
 * @param server   所在游戏服 id
 * @param uid      用户唯一 id
 * @param dataUnix 事件时间/s 时间戳
 * @param status   登录动作（LOGIN/LOGOUT）
 */
case class UserLogin(platform: String, server: String,
                     uid: String,
                     dataUnix: Int,
                     status: String)