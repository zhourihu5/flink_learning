package cn.itcast.sp

import java.sql.{Connection, DriverManager}
import java.util.Properties
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeutils.base.VoidSerializer
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
object KafkaToMySQLExactlyOnce1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val props = new Properties()
    props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id", "test")
    props.setProperty("auto.offset.reset", "latest")
    //kafka 的消费者不自动提交偏移量
    props.setProperty("enable.auto.commit", "false")//开启 Checkpoint
    env.enableCheckpointing(5000L)
    //一旦开启 checkpoint，flink 会在 checkpoint 同时，将偏移量更新
    //new FsStateBackend 要指定存储系统的协议： scheme (hdfs://, file://, etc)
    env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink/ck_demo"))
    //如果程序被 cancle，保留以前做的 checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //指定以后存储多个 checkpoint 目录
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //指定重启策略,默认的重启策略是不停的重启
    //程序出现异常是会重启，重启五次，每次延迟 5 秒，如果超过了 5 次，程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(20, 5000))
    val kafkaConsumer = new FlinkKafkaConsumer011[String](
      "test",
      new SimpleStringSchema,
      props
    )
    //在 checkpoint 成功的时候提交偏移量
    //可以保证 checkpoint 是成功的、通过偏移量提交成功
    //kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val lines: DataStream[String] = env.addSource(kafkaConsumer)
    val words = lines.flatMap(_.split(" "))
    val wordAndOne = words.map((_, 1))
    val reduced = wordAndOne.keyBy(0).sum(1)
    reduced.addSink(new MySqlExactlyOnceSink)
    env.execute()
  }
  class MySqlExactlyOnceSink extends TwoPhaseCommitSinkFunction[(String, Int), MySqlConnectionState, Void](new
      KryoSerializer( classOf[MySqlConnectionState], new ExecutionConfig), VoidSerializer.INSTANCE) {

    override def beginTransaction(): MySqlConnectionState = {
      val connection = DriverManager.getConnection("jdbc:mysql://node1:3306/test?characterEncoding=UTF-8", "root",
        "123456")
      connection.setAutoCommit(false)
      new MySqlConnectionState(connection)
    }
    override def invoke(transaction: MySqlConnectionState, value: (String, Int), context: SinkFunction.Context[_]): Unit
    = {
      val connection = transaction.connection
      println("=====> invoke... " + connection)
      val pstm = connection.prepareStatement("INSERT INTO t_wordcount (word, count) VALUES (?, ?) ON DUPLICATE KEY UPDATE         count = ?")
      pstm.setString(1, value._1)
      pstm.setInt(2, value._2)
      pstm.setInt(3, value._2)
      pstm.executeUpdate()
      pstm.close()
    }
    override def preCommit(transaction: MySqlConnectionState): Unit = {
      //transaction.connection.
    }
    override def commit(transaction: MySqlConnectionState): Unit = {
      transaction.connection.commit()
      transaction.connection.close()
    }
    override def abort(transaction: MySqlConnectionState): Unit = {
      transaction.connection.rollback()
      transaction.connection.close()
    }
  }
  class MySqlConnectionState(@transient val connection: Connection) {
  }
}
