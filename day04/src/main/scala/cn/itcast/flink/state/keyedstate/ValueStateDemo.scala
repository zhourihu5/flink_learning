package cn.itcast.flink.state.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/*
 使用ValueState保存中间结果对下面数据求出最大值
 */

object ValueStateDemo {
  def main(args: Array[String]): Unit = {
    /*
     1.获取流处理执行环境
      2.加载数据源 socke数据：k,v
      3.数据分组
      4.数据转换，定义ValueState,保存中间结果
      5.数据打印
      6.触发执行
     */
    //1 创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 默认程序并行度是机器的核数
    env.setParallelism(1)
    // 2 使用socket source
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    // 3 keyed stream 使用keyby进行分组
    val tupleDs: DataStream[(String, Int)] = socketDs.map(line => {
      val arr: Array[String] = line.split(",")
      //转为tuple类型
      (arr(0), arr(1).trim.toInt)
    })
    // 3.1 分组
    val keyStream: KeyedStream[(String, Int), Tuple] = tupleDs.keyBy(0)
    //    keyStream.maxBy(1)
    // 使用valuestate来存储两两比较之后的最大值，新数据到来之后如果比原来的最大值还大则把该值更新为状态值，保证状态中一直存储的是最大值
    //需要通过上下文件来获取keyedstate valuestate
    val maxDs: DataStream[(String, Int)] = keyStream.map(
      // 3.2 使用richfunction操作，需要通过上下文
      new RichMapFunction[(String, Int), (String, Int)] {
        // 3.2.1 声明一个valuestate （不是创建） ,value state无需关心key是谁以及kv之间的映射，flink维护
        var maxValueState: ValueState[Int] = _
        // 3.3 通过上下文才能获取真正的state,上下文件这种操作在执行一次的方法中使用并获取真正的状态对象
        override def open(parameters: Configuration): Unit = {
          // 3.3.1 定义一个state描述器  参数：state的名称，数据类型的字节码文件
          val maxValueDesc: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int]("maxValue", classOf[Int])
          // 3.3.2 根据上下文基于描述器获取state
          maxValueState = getRuntimeContext.getState(maxValueDesc)
        }
        // 3.4 业务逻辑,可以获取到state数据
        override def map(value: (String, Int)): (String, Int) = {
          //value是一条新数据，需要与原来最大值（valuestate）进行比较判断
          // 3.4.1 获取valuestate中的数据
          val maxNumInState: Int = maxValueState.value()
          // 3.4.2 新数据进行比较
          if (value._2 > maxNumInState) { //新数据比之前存储的数据大
            //3.4.3 更新状态中的值
            maxValueState.update(value._2)
          }
          // 3.4.4 返回最大值
          (value._1, maxValueState.value())
        }
      }
    )
    // 4 打印数据
    maxDs.print()
    // 5 启动
    env.execute()
  }
}
