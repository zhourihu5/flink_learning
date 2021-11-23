package cn.itcast.flink.async

import java.util.concurrent.TimeUnit

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.async.RichAsyncFunction
import org.apache.flink.streaming.api.scala.async.ResultFuture
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import org.apache.flink.api.scala._

import scala.concurrent.{ExecutionContext, Future}

object AsyncDemo {
  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 选择设置事件事件和处理事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    val textDs = env.fromCollection(List("1","2","3","4","5","6","7","8","9"))

    val resDs: DataStream[String] = AsyncDataStream.orderedWait(
      textDs,
      new RedisAsyncFunction,
      1000,
      TimeUnit.MICROSECONDS,
      1
    )
    resDs.print()
    env.execute()
  }


}

class RedisAsyncFunction extends RichAsyncFunction[String, String] {

  var jedis: Jedis = null

  override def open(parameters: Configuration): Unit = {
    val config = new JedisPoolConfig()
    //是否启用后进先出, 默认true
    //config.setLifo(true)
    //最大空闲连接数, 默认8个
    config.setMaxIdle(8)
    //最大连接数, 默认8个
    config.setMaxTotal(1000)
    //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
    config.setMaxWaitMillis(-1)
    //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
    config.setMinEvictableIdleTimeMillis(1800000)
    //最小空闲连接数, 默认0
    config.setMinIdle(0)
    //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
    config.setNumTestsPerEvictionRun(3)
    //对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
    config.setSoftMinEvictableIdleTimeMillis(1800000)
    //在获取连接的时候检查有效性, 默认false
    config.setTestOnBorrow(false)
    //在空闲时检查有效性, 默认false
    config.setTestWhileIdle(false)

    //初始化redis连接池对象
    var jedisPool: JedisPool = new JedisPool(config, "node2", 6379)
    jedis = jedisPool.getResource
  }

  override def close(): Unit = {
    jedis.close()
  }


  //定义Future回调的执行上下文对象
  implicit lazy val executor = ExecutionContext.fromExecutor(Executors.directExecutor())


  override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = super.timeout(input, resultFuture)

  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    //发送异步请求。获取请求结果Future,调用Future代码
    Future {

      val result = jedis.hget("AsyncReadRedis", input)
      //println(s"订单明细异步IO拉取的数据：${orderGoodsWideBean}")
      //异步请求回调
      resultFuture.complete(Array(result+";"+input))
    }
  }
}