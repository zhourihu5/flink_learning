package cn.itcast.stream.window

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/*
演示flink中的window操作
路灯编号,通过的数量
9,3
9,2
9,7
4,9
2,6
1,5
2,3
5,7
5,4

3.需求
每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量--滚动窗口
每5秒钟统计一次，在这过去的10秒钟内，各个路口通过红绿灯汽车的数量--滑动窗口
//会话窗口(需要事件时间支持):在30秒内无数据接入则触发窗口计算
 */
//定义case class 来封装接收到的数据
case class CarWc(num:Int,count:Int)
object WindowDemo {
  def main(args: Array[String]): Unit = {
    /*
     1 创建一个流处理的运行环境
     2 构建socket source数据源
     3 接收到的数据转为（路灯编号，汽车数量）
     4 对元组使用keyby分组（类似于批处理中的groupby），按照路灯编号进行分组
     5 使用窗口进行5s的计算：每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量--滚动窗口
     6 sum出汽车数量
     7 打印输出
     8 执行
     */
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 构建socket source数据源 ;socket参数：ip,port ,返回值类型是datastream
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    // 3 接收到的数据转为（路灯编号，汽车数量）
    val carWcDs: DataStream[CarWc] = socketDs.map(line => {
      val arr = line.split(",")
      CarWc(arr(0).trim.toInt, arr(1).trim().toInt)
    })
    // 4 按照路灯编号进行分组
   //flink中对于数据是否分组会有不同的api支持
    // 未分组数据nonkeyedstream
//    carWcDs.timeWindowAll(Time.seconds(10),Time.seconds(5))

    val windowResDs: DataStream[CarWc] = carWcDs.keyBy("num") //keyedStream
      //进行window的设置；每5秒钟统计一次，在这过去的5秒钟内，各个路口通过红绿灯汽车的数量--滚动窗口
      //        .timeWindow(Time.seconds(5),Time.seconds(5))
//      .timeWindow(Time.seconds(5)) //滚动窗口：滑动时长默认是等于窗口长度
      //进行window 设置；每5秒钟统计一次，在这过去的10秒钟内，各个路口通过红绿灯汽车的数量--滑动窗口
//      .timeWindow(Time.seconds(10),Time.seconds(5))
      //通过创建一个windowAssigner的实例来指定window的类型
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      //聚合计算
      .sum("count")
    // 5打印结果
    windowResDs.print()

    // 6 执行
    env.execute()

  }
}
