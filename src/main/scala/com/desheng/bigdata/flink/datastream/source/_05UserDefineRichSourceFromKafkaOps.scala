package com.desheng.bigdata.flink.datastream.source

import java.util
import java.util.Properties

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConversions._
/**
 * 自定义source function从kafka中读取数据
 *  此时fink程序是kafka的消费者
 */
object _05UserDefineRichSourceFromKafkaOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val result: DataStream[KafkaStudent] = env.addSource(new MyKafkaConsumerSourceFunction())

        result.print()

        env.execute(s"${_05UserDefineRichSourceFromKafkaOps.getClass.getSimpleName}")
    }
    class MyKafkaConsumerSourceFunction extends RichSourceFunction[KafkaStudent] {

        private var consumer: KafkaConsumer[String, String] = null

        override def open(parameters: Configuration): Unit = {
            val properties = new Properties()
            properties.put("bootstrap.servers", "bigdata01:9092")
            properties.put("group.id", "heheheh")
            properties.put("auto.offset.reset", "earliest")
            properties.put("enable.auto.commit", "false")
            properties.put("auto.commit.interval.ms", "1000")
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

            consumer = new KafkaConsumer[String, String](properties)
            consumer.subscribe(util.Arrays.asList("hadoop"))
        }

        override def run(ctx: SourceFunction.SourceContext[KafkaStudent]): Unit = {
            while(true) {
                val consumerRecords: ConsumerRecords[String, String] = consumer.poll(1000L)

                for(consumerRecord <- consumerRecords) {
                    val line = consumerRecord.value()
                    val fields = line.split(",")
                    if(fields != null && fields.length == 6) {
                        try {
                            val id = fields(0).toInt
                            val name = fields(1)
                            val age = fields(2).toInt
                            val gender = fields(3)
                            val course = fields(4)
                            val score = fields(5).toDouble
                            ctx.collect(KafkaStudent(id, name, age, gender, course, score))
                        } catch {
                            case e: Exception => {
                                println(line)
                            }
                        }
                    }
                }
            }
        }

        override def cancel(): Unit = ???

        override def close(): Unit = {
            if(consumer != null) {
                consumer.close()
            }
        }
    }
}
case class KafkaStudent(id: Int, name: String, age:Int, gender: String, course: String, score: Double)
