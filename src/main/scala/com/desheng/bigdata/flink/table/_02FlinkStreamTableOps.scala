package com.desheng.bigdata.flink.table

import com.offcn.bigdata.flink.datastream.transformation.Goods
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object _02FlinkStreamTableOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val sTEnv = StreamTableEnvironment.create(env)
        val dataStream: DataStream[Goods] = env.fromElements(
            "001|mi|mobile",
            "002|mi|mobile",
            "003|mi|mobile",
            "004|mi|mobile",
            "005|huawei|mobile",
            "006|huawei|mobile",
            "007|huawei|mobile",
            "008|Oppo|mobile",
            "009|Oppo|mobile",
            "010|uniqlo|clothing",
            "011|uniqlo|clothing",
            "012|uniqlo|clothing",
            "013|uniqlo|clothing",
            "014|uniqlo|clothing",
            "015|selected|clothing",
            "016|selected|clothing",
            "017|selected|clothing",
            "018|Armani|clothing",
            "019|lining|sports",
            "020|nike|sports",
            "021|adidas|sports",
            "022|nike|sports",
            "023|anta|sports",
            "024|lining|sports"
        ).map(line => {
            val fields = line.split("\\|")

            Goods(fields(0), fields(1), fields(2))
        })
        //load data from external system
        var table = sTEnv.fromDataStream(dataStream)

        // stream table api
        table.printSchema()
        // 高阶api的操作
        table = table.select("category").distinct()
        /*
                将一个table转化为一个DataStream的时候，有两种选择
                    toAppendStream  ：在没有聚合操作的时候使用
                    toRetractStream(缩放的含义) :在进行聚合操作之后使用
         */
        sTEnv.toRetractStream[Row](table).print()

        env.execute("FlinkStreamTableOps")
    }
}
