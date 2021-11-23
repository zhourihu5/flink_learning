package cn.itcast.stream.source.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/*
验证flinkkafkaconsumer如何消费kafka中的数据
 */
object TestFlinkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 添加自定义的数据源 ,泛型限定了从kafka读取数据的类型
    //2.1 构建properties对象
    val prop = new Properties()
    //kafka 集群地址
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
    //消费者组
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink")
    //动态分区检测
    prop.setProperty("flink.partition-discovery.interval-millis", "5000")
    //设置kv的反序列化使用的类
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //设置默认消费的便宜量起始值
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") //从最新处消费
    //定义topic
    val topic = "test"

    //获得了kafkaconsumer对象
    val flinkKafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)
    val kafkaDs: DataStream[String] = env.addSource(flinkKafkaConsumer)
    //3 打印数据
    kafkaDs.print()
    //4 启动
    env.execute()
  }
}
