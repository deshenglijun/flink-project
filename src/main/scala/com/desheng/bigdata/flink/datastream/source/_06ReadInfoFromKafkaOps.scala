package com.desheng.bigdata.flink.datastream.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
/**
 * 自定义source function从kafka中读取数据
 *  此时fink程序是kafka的消费者
 */
object _06ReadInfoFromKafkaOps {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
//        val kafkaConsumer = createKafkaConsumer()
        val kafkaConsumer = createKafkaConsumer2()
        val result: DataStream[KafkaStudent2] = env.addSource(kafkaConsumer)
        result.print()
        env.execute(s"${_06ReadInfoFromKafkaOps.getClass.getSimpleName}")
    }

    def createKafkaConsumer2(): FlinkKafkaConsumer[KafkaStudent2] = {
        val topic = "hadoop"
        val stuSchema = new StudentSchema()
        val properties = createProperties
        val consumer = new FlinkKafkaConsumer[KafkaStudent2](topic, stuSchema, properties)
        consumer
    }
    def createKafkaConsumer(): FlinkKafkaConsumer[String] = {
        val topic = "hadoop"
        val schema = new SimpleStringSchema()
        val properties = createProperties
        val consumer = new FlinkKafkaConsumer[String](topic, schema, properties)
        consumer
    }

    private def createProperties(): Properties = {
        val properties = new Properties()
        properties.put("bootstrap.servers", "bigdata01:9092")
        properties.put("group.id", "xixi")
        properties.put("auto.offset.reset", "earliest")
        properties.put("enable.auto.commit", "false")
        properties.put("auto.commit.interval.ms", "1000")
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties
    }
}
case class KafkaStudent2(id: String, name: String, age:Int, gender: String, course: String, score: Double)

/**
 * 对从kafka中读取到的数据进行反序列化
 */
class StudentSchema extends KafkaDeserializationSchema[KafkaStudent2] {
    override def isEndOfStream(nextElement: KafkaStudent2): Boolean = false

    /**
     * 进行反序列化的过程
     * @param record
     * @return
     */
    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): KafkaStudent2 = {
        val line = new String(record.value())
        val fields = line.split(",")
        if(fields != null && fields.length == 6) {
            val id = fields(0)
            val name = fields(1)
            val age = fields(2).toInt
            val gender = fields(3)
            val course = fields(4)
            val score = fields(5).toDouble
            KafkaStudent2(id, name, age, gender, course, score)
        } else {
            KafkaStudent2(null, null, -1, null, null, -1)
        }
    }
    //对反序列化之后的数据类型进行声明
    override def getProducedType: TypeInformation[KafkaStudent2] = {
        TypeInformation.of(classOf[KafkaStudent2])
    }
}
