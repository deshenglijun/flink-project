package com.desheng.bigdata.flink.cep

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * flink cep复杂事件处理
 *  首先需要定义流
 *  其次要定义进行匹配的规则
 *        连续两次登录失败
 *  再其次，基于规则去匹配流
 *  最后获取匹配上的数据，并生成相关的操作，比如警告
 */
object FlinkCep2LoginFailEventOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        //事件流
        val loginStream  = env.fromCollection(List(
            LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
            LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
            LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
            LoginEvent("2", "192.168.10.10", "success", "1558430845"))
        ).assignAscendingTimestamps(_.eventTime.toLong)


        //定义规则 10s内，连续两次登录失败
        val pattern = Pattern.begin[LoginEvent]("begin")
                .where(loginEvent => loginEvent.eventType == "fail")
                .next("next")
                .where(loginEvent => loginEvent.eventType == "fail")
                .within(Time.seconds(10))

        //使用规则去检验数据
        val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginStream, pattern)

        //获取匹配上的数据，并生成相关的警告，此时就需要使用select来对流进行选择
        patternStream.select(new MySelectPatternFunction).print()


        env.execute(s"${FlinkCep2LoginFailEventOps.getClass.getSimpleName}")
    }
    class MySelectPatternFunction extends PatternSelectFunction[LoginEvent, Warning] {
        override def select(pp: util.Map[String, util.List[LoginEvent]]): Warning = {
            val firstEvent = pp.getOrDefault("begin", null).get(0)
            val secondEvent = pp.getOrDefault("next", null).get(0)

            val userId = firstEvent.userId

            val firstEventTime = firstEvent.eventTime
            val secondEventTime = secondEvent.eventTime

            Warning(userId, firstEventTime, secondEventTime, msg = "连续两次登录失败，怀疑你有邪恶的行为，关进小黑屋~")
        }
    }
}
case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)
//最后生成的警告信息
case class Warning(userId: String, firstEventTime: String, secondEventTime: String, msg: String)