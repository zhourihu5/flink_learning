//package cn.itcast.flink.watermark
//
//
//import org.apache.flink.api.java.tuple.Tuple
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala.function.WindowFunction
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
///*
//演示使用周期性方式生成水印
// */
///*
//需求：
//编写代码, 计算5秒内（滚动时间窗口），每个信号灯汽车数量
//信号灯数据(信号ID(String)、通过汽车数量、时间戳(事件时间))，要求添加水印来解决网络延迟问题。
// */
////3. 定义CarWc 样例类
//case class CarWc(id: String, num: Int, ts: Long)
//
//object WatermarkDemo {
//  /*
//  1. 创建流处理运行环境
//2. 设置处理时间为EventTime ，设置水印的周期间隔,定期生成水印的时间
//3. 定义CarWc 样例类
//4. 使用socketstream发送数据
//5. 添加水印
//   - 允许延迟2秒
//   - 在获取水印方法中，打印水印时间、事件时间和当前系统时间
//6. 按照用户进行分流
//7. 设置5秒的时间窗口
//8. 进行聚合计算
//9. 打印结果数据
//10. 启动执行流处理
//   */
//  def main(args: Array[String]): Unit = {
//    //1 创建流处理运行环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //2 设置处理时间为事件时间，
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    //3 生成水印的周期 默认200ms
//    env.getConfig.setAutoWatermarkInterval(200)
//
//    // 默认程序并行度是机器的核数，8个并行度，注意在flink程序中如果是多并行度，水印时间是每个并行度比较最小的值作为当前流的watermark
//    env.setParallelism(1)
//
//    //4 添加socketsource
//    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
//    // 5 数据处理之后添加水印
//    val carWcDs: DataStream[CarWc] = socketDs.map(
//      line => {
//        //按照逗号切分数据组成carwc
//        val arr = line.split(",")
//        CarWc(arr(0), arr(1).trim.toInt, arr(2).trim.toLong)
//      }
//    )
//    // 添加水印 周期性  AssignerWithPeriodicWatermarks 使用其子类 ,构造参数：水印允许的延迟时间,泛型是stream中的数据类型
//    val watermarkDs: DataStream[CarWc] = carWcDs.assignTimestampsAndWatermarks(
//      new BoundedOutOfOrdernessTimestampExtractor[CarWc](Time.seconds(2)) {
//        // 水印机制是在eventtime基础之上减去一段时间，就是flink允许数据延迟的范围；eventtime是来自数据，flink是不知道eventtime是多少，以及是哪个字段
//        //这个方法就是告诉flink你的数据哪个字段是eventime
//        override def extractTimestamp(element: CarWc): Long = {
//          element.ts
//        }
//      })
//    // 6 设置窗口 5s的滚动窗口
//    val windowStream: WindowedStream[CarWc, Tuple, TimeWindow] = watermarkDs.keyBy(0).
//      window(TumblingEventTimeWindows.of(Time.seconds(5)))
//    // 7 使用apply方法对窗口进行计算
//    val windowDs: DataStream[CarWc] = windowStream.apply(
//      //泛型：1 carwc,2 carwc,3 tuple,4 timewindow
//      new WindowFunction[CarWc, CarWc, Tuple, TimeWindow] {
//        //key:tuple,window:当前触发计算的window对象，input:当前窗口的数据，out:计算结果收集器
//        override def apply(key: Tuple, window: TimeWindow, input: Iterable[CarWc], out: Collector[CarWc]): Unit = {
//
//          val wc: CarWc = input.reduce(
//            (c1, c2) => {
//              CarWc(c1.id, c1.num + c2.num, c2.ts) //累加出通过的汽车数量，关于时间在这里我们不关心
//            }
//          )
//          //发送计算结果
//          out.collect(wc)
//          //获取到窗口开始和结束时间
//          println("窗口开始时间》》" + window.getStart + "=====;窗口结束时间》》" + window.getEnd + ";窗口中的数据》》" +
//            input.iterator.mkString(","))
//        }
//      }
//    )
//    // 打印结果
//    windowDs.print()
//    // 启动
//    env.execute()
//
//
//  }
//}
