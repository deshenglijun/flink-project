package com.desheng.bigdata.flink.dataset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink java的dataset的api操作
 *
 *  Flink中的程序的入口类是什么？
 *      在Dataset中的入口叫做ExecutionEnvironment
 *      在DataStream中的入口叫做StreamExecutionEnvironment
 *      在Table&SQL的入口叫做TableExecutionEnvironment
 *                      基于批的Table：BatchExecutionEnvironment
 *                      基于流的Table：StreamXxxx
 *  注意：在java的版本中导入的依赖，主要说的就是数据类型Tuple：
 *      org.apache.flink.api.java.tuple.TupleX
 *      千万不能导入scala.TupleX
 */
public class FlinkJavaDataSetWordCountOps {
    public static void main(String[] args) throws Exception {
        //获得程序的入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //加载外部数据集
        DataSource<String> lines = env.fromElements(
                "love is blind",
                "where shall i go to kill some time",
                "tale is cheap, show me the code"
        );

        AggregateOperator<Tuple2<String, Integer>> ret = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));//讲当前的数据收集起来 进行下一步的操作，类似于mr中的context.write()
                }
            }
        })
        .groupBy(0)//0代表了上游数据集中的第几列的内容，索引从0开始
        .sum(1);

        ret.print();//在该方法内已经触发过程作业的执行了，所以不需要再执行execute方法
        //启动的启动需要ExecutionEnvironment来进行处罚

        //env.execute("FlinkJavaDataSetWordCountOps");
    }
}
class WordCount {
    private String word;
    private int count;

    public WordCount() {
    }

    public WordCount(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
