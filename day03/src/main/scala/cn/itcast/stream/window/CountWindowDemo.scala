package cn.itcast.stream.window

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
演示flink中的countwindow操作
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
对每个路口分别统计,统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现5次进行统计(对应的key出现的次数达到5次作为一个窗口,即相同的key出现5次才做一次sum聚合)
对每个路口分别统计,统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现3次进行统计
 */
//定义case class 来封装接收到的数据
case class CarWc(num:Int,count:Int)
object CountWindowDemo {
  def main(args: Array[String]): Unit = {
    /*
     1 创建一个流处理的运行环境
     2 构建socket source数据源
     3 接收到的数据转为（路灯编号，汽车数量）
     4 对元组使用keyby分组（类似于批处理中的groupby），按照路灯编号进行分组
     5 使用窗口进行5s的计算：
     对每个路口分别统计,统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现5次进行统计(对应的key出现的次数达到5次作为一个窗口,即相同的key出现5次才做一次sum聚合)
对每个路口分别统计,统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现3次进行统计
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


    val windowResDs: DataStream[CarWc] = carWcDs.keyBy("num") //keyedStream
      //基于数量的滚动窗口
      // 对每个路口分别统计,统计在最近5条消息中,各自路口通过的汽车数量,
      // 相同的key每出现5次进行统计(对应的key出现的次数达到5次作为一个窗口,即相同的key出现5次才做一次sum聚合)
//      .countWindow(5)
      // 基于数量的滑动窗口
      //对每个路口分别统计,统计在最近5条消息中,各自路口通过的汽车数量,相同的key每出现3次进行统计
//      .countWindow(5,3)
//      .window()
      //聚合计算
      .sum("count")
    // 5打印结果
    windowResDs.print()

    // 6 执行
    env.execute()

  }
}
