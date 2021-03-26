package com.desheng.bigdata.flink.datastream.transformation

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 我们看到前面_01中的split api已经过时，最新的api是使用getSideOutput侧输出流来进行替代
 * 需要将split函数替换成process，在该函数中完成每一条数据的标签化
 *
 * 之后再通过getSideOutput基于标签来选择不同的流
 */
object _02SideOutputStreamOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //025|lining|sports
        val lines = env.socketTextStream("bigdata01", 9999)
        val goods = lines.map(line => {
            val fields = line.split("\\|")
            val id = fields(0)
            val brand = fields(1)
            val category = fields(2)
            Goods(id, brand, category)
        })
        val sportsTag = OutputTag[Goods]("sports")
        val clothingTag = OutputTag[Goods]("clothing")
        val mobileTag = OutputTag[Goods]("mobile")
        //使用process函数打标签
        val ret = goods.process(new ProcessFunction[Goods, Goods] {
            override def processElement(value: Goods, ctx: ProcessFunction[Goods, Goods]#Context,
                                        out: Collector[Goods]): Unit = {
                value.category match {
                    //为每一条流打标签
                    case "sports" => {
                        ctx.output(sportsTag, value)
                    }
                    case "clothing" => {
                        ctx.output(clothingTag, value)
                    }
                    case "mobile" => {
                        ctx.output(mobileTag, value)
                    }
                    case _ => {
                        out.collect(value)
                    }
                }
            }
        })
        //流的选择---侧输出
        ret.getSideOutput(sportsTag).print("sports:::")
        ret.getSideOutput(mobileTag).print("mobile:::")

        env.execute(s"${_02SideOutputStreamOps.getClass.getSimpleName}")
    }
}
