package cn.itcast.twophase

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeutils.base.VoidSerializer
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.{SinkFunction, TwoPhaseCommitSinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

//测试flink+mysql 一致性语义效果 ,两阶段要结合ck才能实现,业务：单词计数，然后写入mysql指定表中
// 使用flink消费kafka中数据
object KafkaToMsqlDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 设置ck属性
    // 2.1开启ck  //指定使用本地fsstatebackend
    env.setStateBackend(new FsStateBackend("file:///e://data//checkpoint-demo2//"))
    //2.2 设置checkpoint的周期间隔  默认是没有开启ck，需要通过指定间隔来开启ck
    env.enableCheckpointing(10000)
    //2.3 设置ck的执行语义，最多一次，至少一次，精确一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //2.4 设置两次ck之间的最小时间间隔，两次ck之间时间最少差500ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 2.5 设置ck的超时时间，如果超时则认为本次ck失败，继续一下一次ck即可,超时60s
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //2.6 设置ck出现问题，是否让程序报错还是继续任务进行下一次ck,true:让程序报错，false：不报错进行下次ck
    //如果是false就是ck出现问题我们允许程序继续执行，如果下次ck成功则没有问题，但是如果程序下次ck也没有成功，
    //此时程序挂掉需要从ck中恢复数据时可能导致程序计算错误，或者是重复计算数据。
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 2.7设置任务取消时是否保留检查点  retain：则保存检查点数据，delete:删除ck作业数据
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //2.8 设置程序中同时允许几个ck任务同时进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    var topic = "test"
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
    //设置消费者的隔离级别，默认是读取未提交数据 read_uncommitted
    prop.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    //设置程序消费kafka的便宜量提交策略
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val kafkaConsumer = new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      prop
    )
    //设置kafka消费者偏移量是基于ck成功时提交
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val kafkaDs: DataStream[String] = env.addSource(kafkaConsumer)
    //打印
    kafkaDs.print("读取到的kafka中的数据>>")
    //实现单词统计逻辑
    val wcDs = kafkaDs.flatMap(_.split(" ")).map(_ -> 1).keyBy(0).sum(1)
    //写入mysql，要求使用两阶段协议保证一致性语义为exactly-once
    wcDs.addSink(new MysqlTwoPhaseCommit)
    //启动
    env.execute()

  }
}

//自定义 sinkfunciton实现TwoPhaseCommitSinkFunction in:(单词，数量） txn:transaction对象（自定义）， context:void
class MysqlTwoPhaseCommit extends TwoPhaseCommitSinkFunction[(String, Int), MysqlConnectionSate, Void](
  //自定义对象的序列化类型
  new KryoSerializer[MysqlConnectionSate](classOf[MysqlConnectionSate], new ExecutionConfig),
  //上下文件对象序列化类型
  VoidSerializer.INSTANCE
) {

  //开启事务 ，pre-commit开始阶段， 每次开始ck
  override def beginTransaction(): MysqlConnectionSate = {
    // 获取一个连接
    val connection = DriverManager.getConnection("jdbc:mysql://node1:3306/test", "root", "123456")
    //执行 ,关闭mysql的事务自动提交动作
    connection.setAutoCommit(false) //mysql中默认提交事务是自动提交，改为手动提交，ck成功时flink自动提交
    new MysqlConnectionSate(connection)
  }

  //执行你的动作
  override def invoke(transaction: MysqlConnectionSate, value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    //读取数据并写入mysql中
    val connection = transaction.connection
    //sql语句  word是主键，可以根据主键更新，duplicate key update:主键存在则更新指定字段的值，否则插入新数据
    var sql = "insert into t_wordcount(word,count) values(?,?) on duplicate key update count= ? "
    //获取preparestatement
    val ps = connection.prepareStatement(sql)
    //赋值
    ps.setString(1, value._1)
    ps.setInt(2, value._2)
    ps.setInt(3, value._2)

    ps.executeUpdate() //不是预提交，
    // 关闭
    ps.close()
  }
  //预提交
  override def preCommit(transaction: MysqlConnectionSate): Unit = {
    //invoke中完成
  }

  //真正提交
  override def commit(transaction: MysqlConnectionSate): Unit = {
    transaction.connection.commit()
    transaction.connection.close()
  }

  //回滚
  override def abort(transaction: MysqlConnectionSate): Unit = {
    transaction.connection.rollback()
    transaction.connection.close()
  }


}

//要求 该connect无需被序列化，加上transient关键字可以保证该属性不会随着对象序列化
class MysqlConnectionSate(@transient val connection: Connection) {
}