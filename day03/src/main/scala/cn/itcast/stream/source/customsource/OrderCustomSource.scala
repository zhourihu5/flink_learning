package cn.itcast.stream.source.customsource

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

/*
自定义数据源，练习 生成订单数据
 */

//订单信息(订单ID、用户ID、订单金额、时间戳)
case class Order(id: String, userId: Int, money: Long, createTime: Long)

object OrderCustomSource {
  def main(args: Array[String]): Unit = {
    /*
    1. 创建订单样例类
    2. 获取流处理环境
    3. 创建自定义数据源
       - 循环1000次
       - 随机构建订单信息
       - 上下文收集数据
       - 每隔一秒执行一次循环
    4. 打印数据
    5. 执行任务
     */
    //1  获取流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载自定义的order数据源,RichParallelSourceFunction泛型是生产的数据类型，order
    val orderDs: DataStream[Order] = env.addSource(new RichParallelSourceFunction[Order] {
      var isRunning = true

      //2.1生成订单数据方法
      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        //2.1.1 生成订单 业务逻辑
        while (isRunning) {
          //orderid
          val orderId = UUID.randomUUID().toString
          //userid
          val userId = Random.nextInt(3)
          //money
          val money = Random.nextInt(101)
          //createTime
          val createTime = System.currentTimeMillis()
          ctx.collect(Order(orderId, userId, money, createTime))
          //每隔一秒中执行一次
          TimeUnit.SECONDS.sleep(1)
        }
      }

      //2.2 取消数据的生成方法
      override def cancel(): Unit = {
        isRunning = false
      }
    }).setParallelism(1)
    //3 打印数据
    orderDs.print()
    // 4 启动
    env.execute()


  }
}
