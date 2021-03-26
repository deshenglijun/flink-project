package com.desheng.bigdata.flink.dataset.source

import org.apache.flink.api.scala._

/**
 * Flink数据源之基于文件
 */
object CsvFileSourceOps {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
//        val lines = env.readCsvFile[(Int, String, Int, String, String, Double)](
//            filePath = "file:/E:/data/flink/student.csv",
//            ignoreFirstLine = true,//是否忽略文件的第一行数据（主要考虑表头数据）
//            fieldDelimiter = "|", //列的分隔符
//            pojoFields = Array("id", "name", "age", "gender", "course", "score")
//        )

        val lines = env.readCsvFile[Student](
            filePath = "file:/E:/data/flink/student.csv",
            ignoreFirstLine = true,//是否忽略文件的第一行数据（主要考虑表头数据）
            fieldDelimiter = "|" //列的分隔符
        )

        val ret: DataSet[Result] = lines.
            map(stu => Result(stu.id, stu.name, stu.score)).groupBy(result => result.name)
                .maxBy(2)

        ret.print()
    }

}
case class Result(id: Int, name: String, score: Double)
case class Student(id: Int, name: String, age: Int, gender: String, course: String, score: Double)