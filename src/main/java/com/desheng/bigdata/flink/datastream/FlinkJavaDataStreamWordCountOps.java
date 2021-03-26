package com.desheng.bigdata.flink.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkJavaDataStreamWordCountOps {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8080);
        conf.setString("rest.address", "localhost");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //加载外部数据
        DataStreamSource<String> lines = env.socketTextStream("bigdata01", 9999);

        DataStream<Tuple2<String, Integer>> ret = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    out.collect(word);//讲当前的数据收集起来 进行下一步的操作，类似于mr中的context.write()
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(String word) throws Exception {
                    return new Tuple2<>(word, 1);
                }
            })
            .keyBy(0)
            .reduce((kv1, kv2) -> new Tuple2<String, Integer>(kv1.f0, kv1.f1 + kv2.f1));

        ret.print();

        env.execute(FlinkJavaDataStreamWordCountOps.class.getSimpleName());
    }
}
