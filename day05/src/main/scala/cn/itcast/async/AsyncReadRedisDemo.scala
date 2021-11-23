//package cn.itcast.async
//
//
//import java.util.concurrent.TimeUnit
//
//import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.runtime.concurrent.Executors
//import org.apache.flink.streaming.api.scala.async.ResultFuture
//import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
//
//import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
//
////读取集合中的城市id使用异步IO去redis中读取城市名称
//object AsyncReadRedisDemo {
//  def main(args: Array[String]): Unit = {
//    /*
//    1 创建运行环境
//    2 加载集合中的城市id数据
//    3 使用异步io读取redis
//    4 打印数据
//    5 启动
//     */
//    //    1 创建运行环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //    2 加载集合中的城市id数据
//    val cityIdDs = env.fromCollection(List("1", "2", "3", "4", "5", "6", "7", "8", "9"))
//    // 3 使用异步io
//    val resDs: DataStream[String] = AsyncDataStream.unorderedWait(
//      cityIdDs,
//      new MyAsyncFunciton,
//      1000,
//      TimeUnit.MILLISECONDS
//    )
//    // 4 打印结果
//    resDs.print()
//    // 5启动
//    env.execute()
//
//  }
//}
//
////使用 异步IO的步骤
///*
//1 准备一个类实现asyncrichfunction
// 1.1 open 创建或者打开数据连接，一般使用连接池获取连接
// 1.2  重写timeout方法，处理异步请求超时的问题
// 1.3 发送异步请求的方法 asyncinvoke
//2 使用AsyncDataStream工具类，调用orderedwait等方法实现异步操作流
// */
//
////@param <IN> The type of the input elements.
////*@ param <OUT>
////The type of the returned elements.  修改flink版本，改为flink.1.8即可
//class MyAsyncFunciton extends RichAsyncFunction[String, String] {
//  var jedis: Jedis = null
//
//  //创建redis连接池
//  override def open(parameters: Configuration): Unit = {
//    val config = new JedisPoolConfig
//    config.setLifo(true)
//    config.setMaxTotal(10)
//    config.setMaxIdle(10)
//    //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
//    config.setMaxWaitMillis(-1)
//    //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
//    config.setMinEvictableIdleTimeMillis(1800000)
//    //最小空闲连接数, 默认0
//    config.setMinIdle(0)
//    //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
//    config.setNumTestsPerEvictionRun(3)
//    //对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
//    config.setSoftMinEvictableIdleTimeMillis(1800000)
//    //在获取连接的时候检查有效性, 默认false
//    config.setTestOnBorrow(false)
//    //在空闲时检查有效性, 默认false
//    config.setTestWhileIdle(false)
//    //初始化连接池对象
//    val pool = new JedisPool(config, "node2", 6379)
//    jedis = pool.getResource
//  }
//
//  //关闭连接
//  override def close(): Unit = {
//    jedis.close()
//  }
//
//  //处理异步请求超时的方法,否则默认超时会报出异常
//  override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
//    println("input >>" + input + ",,,请求超时。。。")
//  }
//
//  //定义一个异步回调的上下文件
//  implicit lazy val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.directExecutor())
//
//  //异步请求方法：input：输入数据， resultfutrue:异步请求结果对象
//  def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
//    //执行异步请求，请求结果封装在resultfuture中
//    Future {
//      val result = jedis.hget("AsyncReadRedis", input)
//      //异步回调返回
//      resultFuture.complete(Array(input+">>>"+result))
//    }
//  }
//}