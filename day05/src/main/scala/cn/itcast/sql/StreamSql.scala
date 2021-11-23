package cn.itcast.sql


import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

import scala.util.Random

//    4)创建一个订单样例类 Order ，包含四个字段（订单ID、用户ID、订单金额、时间戳）
case class Order(orderId: String, userId: Int, money: Long, createTime: Long)
//使用Flink SQL来统计5秒内 用户的 订单总数、订单的最大金额、订单的最小金额。
object StreamSql {
  def main(args: Array[String]): Unit = {
    /*
    1)获取流处理运行环境
    2)获取Table运行环境
    3)设置处理时间为 EventTime
    4)创建一个订单样例类 Order ，包含四个字段（订单ID、用户ID、订单金额、时间戳）
    5)创建一个自定义数据源
    a.使用for循环生成1000个订单
    b.随机生成订单ID（UUID）
    c.随机生成用户ID（0-2）
    d.随机生成订单金额（0-100）
    e.时间戳为当前系统时间
    f.每隔1秒生成一个订单
    6)添加水印，允许延迟2秒
    7)导入 import org.apache.flink.table.api.scala._ 隐式参数
    8)使用 registerDataStream 注册表，并分别指定字段，还要指定rowtime字段
    9)编写SQL语句统计用户订单总数、最大金额、最小金额
    分组时要使用 tumble(时间列, interval '窗口时间' second) 来创建窗口
    10)使用 tableEnv.sqlQuery 执行sql语句
    11)将SQL的执行结果转换成DataStream再打印出来
    12)启动流处理程序
     */
    //    1)获取流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    2)获取Table运行环境
    val tenv = TableEnvironment.getTableEnvironment(env)
    //    3)设置处理时间为 EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    5)创建一个自定义数据源
    val orderDs: DataStream[Order] = env.addSource(new RichSourceFunction[Order] {
      var flag = true

      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        while(flag){
          ctx.collect(Order(UUID.randomUUID().toString, Random.nextInt(3),
            Random.nextInt(101), System.currentTimeMillis()))
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        flag = false
      }
    })
    //    6)添加水印，允许延迟2秒
    val wmDs: DataStream[Order] = orderDs.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(2)) {
        override def extractTimestamp(element: Order): Long = {
          element.createTime
        }
      }
    )
    //7)导入 import org.apache.flink.table.api.scala._ 隐式参数
    //    8)使用 registerDataStream 注册表，并分别指定字段，还要指定rowtime字段
    import org.apache.flink.table.api.scala._
    tenv.registerDataStream(
      "t_order",
      wmDs,
      'orderId, 'userId, 'money, 'createTime.rowtime
    )
    //    9)编写SQL语句统计用户订单总数、最大金额、最小金额
    val sql =
      """
        |select
        | userId,
        | count(1) totalCount,
        |max(money) maxMoney,
        |min(money) minMoney
        |from t_order
        |group by
        |tumble(createTime, interval '5' second),
        |userId
        |
      """.stripMargin

    //    10)使用 tableEnv.sqlQuery 执行sql语句
    val table = tenv.sqlQuery(sql)

    // 11 转为datastream打印
    tenv.toRetractStream[Row](table).print()

    // 12 启动
    env.execute()

  }
}
