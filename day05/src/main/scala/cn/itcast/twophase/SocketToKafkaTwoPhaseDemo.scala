//package cn.itcast.twophase
//
//import java.util.Properties
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.runtime.state.filesystem.FsStateBackend
//import org.apache.flink.streaming.api.CheckpointingMode
//import org.apache.flink.streaming.api.environment.CheckpointConfig
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
//
//import org.apache.kafka.clients.producer.ProducerConfig
//
////使用socket发送数据，flink接收消息之后发送到kafka中，使用flinkkafkaproducer011（两阶段提交协议）。
//object SocketToKafkaTwoPhaseDemo {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    // 2 设置ck属性
//    // 2.1开启ck  //指定使用本地fsstatebackend
//    env.setStateBackend(new FsStateBackend("file:///e://data//checkpoint-demo//"))
//    //2.2 设置checkpoint的周期间隔  默认是没有开启ck，需要通过指定间隔来开启ck
//    env.enableCheckpointing(10000)
//    //2.3 设置ck的执行语义，最多一次，至少一次，精确一次
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    //2.4 设置两次ck之间的最小时间间隔，两次ck之间时间最少差500ms
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
//    // 2.5 设置ck的超时时间，如果超时则认为本次ck失败，继续一下一次ck即可,超时60s
//    env.getCheckpointConfig.setCheckpointTimeout(60000)
//    //2.6 设置ck出现问题，是否让程序报错还是继续任务进行下一次ck,true:让程序报错，false：不报错进行下次ck
//    //如果是false就是ck出现问题我们允许程序继续执行，如果下次ck成功则没有问题，但是如果程序下次ck也没有成功，
//    //此时程序挂掉需要从ck中恢复数据时可能导致程序计算错误，或者是重复计算数据。
//    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
//    // 2.7设置任务取消时是否保留检查点  retain：则保存检查点数据，delete:删除ck作业数据
//    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    //2.8 设置程序中同时允许几个ck任务同时进行
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
//    // 3 读取socket数据
//    val socketDs = env.socketTextStream("node1", 9999)
//    // 4 打印
//    socketDs.print()
//    // 5 输出到kafka
//    var topic = "test"
//    val prop = new Properties()
//    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
//    //设置连接kafka事务的超时时间，flinkproducer默认事务超时时间是1h，kafka中默认事务超时时间是15分钟
//    prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000 * 15 + "") //设置客户端事务超时时间与kafka保持一致
//    socketDs.addSink(new FlinkKafkaProducer011[String](
//      topic,
//      new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),
//      prop,
//      //设置producer的语义，默认是at-least-once
//      FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
//    ))
//
//    // 6 启动
//    env.execute()
//  }
//}
