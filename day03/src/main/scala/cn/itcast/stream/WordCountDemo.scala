package cn.itcast.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/*
流式计算的wordcount
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    /*
     1 创建一个流处理的运行环境
     2 构建socket source数据源
     3 接收到的数据转为（单词，1）
     4 对元组使用keyby分组（类似于批处理中的groupby）
     5 使用窗口进行5s的计算
     6 sum出单词数量
     7 打印输出
     8 执行
     */
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 构建socket source数据源 ;socket参数：ip,port ,返回值类型是datastream
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    // 3 接收到的数据转为（单词，1）
    val tupleDs: DataStream[(String, Int)] = socketDs.flatMap(_.split(" ")).map((_, 1))
    // 4 对元组使用keyby分组（类似于批处理中的groupby）
    val keyedStream: KeyedStream[(String, Int), Tuple] = tupleDs.keyBy(0)

    //    5 使用窗口进行5s的计算,没5s计算一次
    val windowStream: WindowedStream[(String, Int), Tuple, TimeWindow] = keyedStream.timeWindow(Time.seconds(5))
    //    6 sum出单词数量
    val resDs: DataStream[(String, Int)] = windowStream.sum(1)
    //    7 打印输出
    resDs.print()
    //      8 执行
    env.execute()

  }
}
