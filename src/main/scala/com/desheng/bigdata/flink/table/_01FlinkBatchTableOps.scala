package com.desheng.bigdata.flink.table

import com.offcn.bigdata.flink.dataset.source.Student
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/*
    Flink Table API的入口称之为TableEnvironment
           是一个接口，主要有两个实现类：
           StreamTableEnvironment
           BatchTableEnvironment
    TableEnvironment目前的版本没有提供sink操作，也就是无法触发程序的执行，所以要想将程序执行，需要将Table切换到DataSet/DataStream来进行执行。
 */
object _01FlinkBatchTableOps {
    def main(args: Array[String]): Unit = {
        //构建batch的executionEnvironment
        val env = ExecutionEnvironment.getExecutionEnvironment
        val bTEnv = BatchTableEnvironment.create(env)

        //加载外部数据
        val dataSet = env.readCsvFile[Student](filePath = "file:/E:/data/flink/student.csv",
            ignoreFirstLine = true,//是否忽略文件的第一行数据（主要考虑表头数据）
            fieldDelimiter = "|")
        //table 就相当于sparksql中的dataset
        var table = bTEnv.fromDataSet(dataSet)

        //进行table的api操作
        table.printSchema()
        //查看表中的内容
        table = table.select("name, age, course, score")

        //条件查询
        table = table.select("name, age, course, score").where("course = 'english'")
        //结果输出 -->
        bTEnv.toDataSet[Row](table).print()
//        bTEnv.execute("FlinkBatchTable")
    }
}
