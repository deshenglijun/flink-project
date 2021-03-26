package com.desheng.bigdata.flink.datastream.transformation

import org.apache.flink.streaming.api.scala._

/**
 * 流的合并union和流的链接connect
 *      union:流的合并，就是前面学习过的sql中的union all
 *      connect：流的连接,仅仅是把两个流连接成一个具有两个互相独立部分的流的集合
 */
object _03UnionAndConnectOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val left = env.fromCollection(1 to 5)

        val right = env.fromCollection(4 until 8)

        val unioned = left.union(right)

        unioned.print("union:::")
        val connected:ConnectedStreams[Int, Int] = left.connect(right)

        connected.map((leftNum: Int) => "left>>>" + (leftNum * 2), (rightNum: Int) => "right>>>" + (rightNum * 3))
                .print("connect:::")

        env.execute(s"${_03UnionAndConnectOps.getClass.getSimpleName}")
    }
}
