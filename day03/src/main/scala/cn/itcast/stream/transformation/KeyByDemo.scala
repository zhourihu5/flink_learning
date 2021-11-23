package cn.itcast.stream.transformation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
演示flink中keyby的用法
实现单词统计
 */
object KeyByDemo {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载socketstream
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    //3 对接收到的数据切分压平转成单词，1的元组
    val wordAndOneDs: DataStream[(String, Int)] = socketDs.flatMap(_.split(" ")).map(_ -> 1)
    // 4 按照单词分组
//    wordAndOneDs.keyBy(_._1).sum(1).print()
    wordAndOneDs.keyBy(0).sum(1).print()
    //5 启动
    env.execute()
  }
}
