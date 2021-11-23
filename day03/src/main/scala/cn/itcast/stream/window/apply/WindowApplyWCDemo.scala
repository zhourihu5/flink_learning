package cn.itcast.stream.window.apply

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.{RichWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
流式计算的wordcount
 */
object WindowApplyWCDemo {
  def main(args: Array[String]): Unit = {
    /*
     1 创建一个流处理的运行环境
     2 构建socket source数据源
     3 接收到的数据转为（单词，1）
     4 对元组使用keyby分组（类似于批处理中的groupby）
     5 使用窗口进行5s的计算
     6 使用windowapply方式对窗口进行计算
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
    //不同的分组字段指定方式，带来了keyedstream中的keyed类型不一致
    val keyedStream2: KeyedStream[(String, Int), String] = tupleDs.keyBy(_._1)

    //    5 使用窗口进行5s的计算,每5s计算一次
    val windowStream: WindowedStream[(String, Int), Tuple, TimeWindow] = keyedStream.timeWindow(Time.seconds(5))
    //    6 使用windowapply方式对窗口进行计算,传入参数：windowfunction ,
    // 1 输入数据类型：(String, Int)，2 输出的数据类型：(String, Int)，3 输入的数据key的类型：tuple/String 4 window的类型：timewindow
    val resDs: DataStream[(String, Int)] = windowStream.apply(new WindowFunction[(String, Int), (String, Int), Tuple, TimeWindow] {
      // 6.1 apply给了我们进行对窗口中的数据进行计算的机会
      // 1 key:就是指定输入的key的类型，2 timewindow, 3 input:当前这个窗口中所有数据，放在迭代器中，4 out:计算完成的结果数据通过out发送出去
      override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        // 6.1.1单词总数计算
        val wcTuple: (String, Int) = input.reduce((t1, t2) => (t1._1, t1._2 + t2._2) //reduce方法统计单词数量
        )
        // 6.1.2 发送结果数据
        out.collect(wcTuple)
      }
    })
    //keyby使用函数指定key的方式得到的stream
    val windowStream2 = keyedStream2.timeWindow(Time.seconds(5))
    //可以通过richwindowfunction实现统计逻辑
    var resDs2 = windowStream2.apply(new RichWindowFunction[(String, Int), (String, Int), String, TimeWindow] {

      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        val wcTuple: (String, Int) = input.reduce((t1, t2) => (t1._1, t1._2 + t2._2) //reduce方法统计单词数量
        )
        // 发送结果数据
        out.collect(wcTuple)
      }
    })
    //    7 打印输出
    resDs.print()

    resDs2.printToErr("resDs2:>>>")
    //      8 执行
    env.execute()

  }
}
