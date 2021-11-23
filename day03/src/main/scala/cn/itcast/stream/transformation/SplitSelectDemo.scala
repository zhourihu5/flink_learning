package cn.itcast.stream.transformation

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/*
示例
加载本地集合(1,2,3,4,5,6), 使用split进行数据分流,分为奇数和偶数. 并打印奇数结果
 */
object SplitSelectDemo {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2 加载source
    val numDs: DataStream[Int] = env.fromCollection(List(1,2,3,4,5,6))
    // 3 转换 使用split把数据流中的数据分为奇数和偶数
    val splitStream: SplitStream[Int] = numDs.split(
      item => {
        //模以2
        var res = item % 2
        //模式匹配的方式
        res match {
          case 0 => List("even") //偶数  even与odd只是名称，代表数据流的名称，但是必须放在list集合
          case 1 => List("odd") //奇数
        }
      }
    )
    // 4 从splitStream中获取奇数流和偶数流
    val evenDs: DataStream[Int] = splitStream.select("even")
    val oddDs = splitStream.select("odd")
    val allDs: DataStream[Int] = splitStream.select("even","odd")
    // 5打印结果
//    evenDs.print()

//    oddDs.print()
        allDs.print()

    // 5启动程序
    env.execute()
  }
}
