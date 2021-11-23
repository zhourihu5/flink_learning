package cn.itcast.twophase

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

//测试flink+kafka 一致性语义效果
// 使用flink消费kafka中数据
object KafkaExactlyoneceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var topic = "test"
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
    //设置消费者的隔离级别，默认是读取未提交数据 read_uncommitted
    prop.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
    val kafkaDs: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      prop
    ))
    //打印
    kafkaDs.print("测试kafka 一致性结果数据>>")
    //启动
    env.execute()

  }
}
