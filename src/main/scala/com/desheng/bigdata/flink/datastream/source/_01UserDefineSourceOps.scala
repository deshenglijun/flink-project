package com.desheng.bigdata.flink.datastream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 * 用户自定义Source操作
 *     主要复写四个接口，代表四种不同的source
 *       SourceFunction         ---->这是一个普通的数据源，特别需要说明一点的是，该数据源不支持并行度，也就是说parallelism=1
 *                 覆盖其中的run（核心）方法
 *       RichSourceFunction     ----> 比SourceFunction多了两个方法，open()和close()
 *                 是一个生命周期的方法，也就是说在执行run方法之前，要调用open进行初始化，调用完毕run方法之后进行调用close方法
 *                 进行资源释放
 *       ParallelSourceFunction
 *       RichParallelSourceFunction
 *          相比较于SourceFunction和RichSourceFunction，这两个操作，就多了一个支持并行度
 *
 *  通过举例说明这几个SourceFunction
 *      上游向下游不间断的发送递增的数字
 */
object _01UserDefineSourceOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val ret = env.addSource(new MySourceFunction())
            //.setParallelism(2)//该sourceFunction不支持并行度

        ret.print().setParallelism(2)

        env.execute(s"${_01UserDefineSourceOps.getClass.getSimpleName}")
    }
    class MySourceFunction extends SourceFunction[Int] {

        override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
            var num = 1
            while(true) {
                println("上游sourceFunction生产的数据：" + num)
                ctx.collect(num)
                num += 1
                Thread.sleep(500)
            }
        }

        override def cancel(): Unit = {

        }
    }
}
