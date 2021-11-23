package cn.itcast.stream.sink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/*
flink程序计算结果保存到redis，使用flink提供的redissink
 */

/*
从socket接收数据然后计算出单词的次数，最终使用redissink写数据到redis中
 */

object SinkToRedisDemo {
  def main(args: Array[String]): Unit = {

    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载socket数据，
    val socketDs: DataStream[String] = env.socketTextStream("node1",9999)
    /*
    单词计数的逻辑
     */
    // 3 转换
    val resDs: DataStream[(String, Int)] = socketDs.flatMap(_.split(" ")).map(_ ->1).keyBy(0).sum(1)
    // 4 sink 操作 使用redissink
    // 4.1 redissink的构造：1 需要redis配置文件（连接信息），2 redismapper对象
    //4.1.1 jedisconfig
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("node2").setPort(6379).build()

    resDs.addSink(new RedisSink[(String, Int)](config,new MyRedisMapper))

    // 5 执行
    env.execute()
  }
}
//4.1.2 redismapper的对象,泛型就是写入redis的数据类型
class MyRedisMapper extends RedisMapper[(String, Int)]{
  //获取命令描述器，确定数据结构,我们使用hash结构
  override def getCommandDescription: RedisCommandDescription = {
    //指定使用hset命令，并提供hash结构的第一个key
    new RedisCommandDescription(RedisCommand.HSET,"REDISSINK")
  }

  //指定你的key
  override def getKeyFromData(data: (String, Int)): String = {
    data._1
  }
//指定你的value
  override def getValueFromData(data: (String, Int)): String = {
    data._2.toString
  }
}
