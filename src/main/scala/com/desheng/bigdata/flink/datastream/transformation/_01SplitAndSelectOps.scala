package com.desheng.bigdata.flink.datastream.transformation

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala._

/**
 * 流的拆分split和选择select
 *  datastream使用split操作，将一个流拆分成多个splitstream
 *  我们要向获取其中的某一个流，就是用select来进行选择
 *  基于此我们就可以针对特定的流进行分析处理
 */
object _01SplitAndSelectOps {
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
        //进行流的拆分，按照商品的类别category进行分类
        /*
        goods.split(new OutputSelector[Goods] {
            override def select(goods: Goods): lang.Iterable[String] = {
                goods.category match {
                    case "sports" => {
                        util.Arrays.asList("sports")
                    }
                    case "clothing" => {
                        util.Arrays.asList("clothing")
                    }
                    case "mobile" => {
                        util.Arrays.asList("mobile")
                    }
                }
            }
        })
        */
        val splitStream = goods.split(goods => Seq(goods.category))//使用流中的category字段对数据进行了拆分

        //想在要向获取clothing的数据
        splitStream.select("clothing").print("clothing:::")
        //想在要向获取sports的数据
        splitStream.select("sports").print("sports:::")

        env.execute(s"${_01SplitAndSelectOps.getClass.getSimpleName}")
    }

}
case class Goods(id: String, brand: String, category: String)

