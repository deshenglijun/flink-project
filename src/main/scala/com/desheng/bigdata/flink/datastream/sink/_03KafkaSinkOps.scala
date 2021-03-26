package com.desheng.bigdata.flink.datastream.sink

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * flink的sink操作，没有类似于spark中的foreach的操作
 * 自定义的类，核心的有两个：
 *      SinkFunction
 *          invoke
 *      RichSinkFunction
 *          open
 *          invoke
 *          close
 */
object _03KafkaSinkOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val lines = env.fromElements(
            "implicit you",
            "implicit you",
            "implicit me"
        )

        val ret = lines.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0).sum(1)

        ret.print()
        val topic = "hadoop"
        ret.addSink(createKafkaProducer(topic))

        env.execute(s"${_03KafkaSinkOps.getClass.getSimpleName}")
    }
    def createKafkaProducer(topic: String): FlinkKafkaProducer[(String, Int)] = {
        val serializationSchema = new MyKafkaSerializationSchema(topic)
        val props = new Properties()
        props.put("bootstrap.servers", "bigdata01:9092")
        props.put("acks", "1")
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        val producer = new FlinkKafkaProducer[(String, Int)](topic,
                            serializationSchema,
                            props,
                            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
                        )
        producer
    }

}

class MyKafkaSerializationSchema(topic: String) extends KafkaSerializationSchema[(String, Int)] {
    override def serialize(kv: (String, Int),
                           timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        val line = kv._1 + "," + kv._2
        val bytes = line.getBytes()
        new ProducerRecord[Array[Byte], Array[Byte]](topic, null, bytes)
    }
}

