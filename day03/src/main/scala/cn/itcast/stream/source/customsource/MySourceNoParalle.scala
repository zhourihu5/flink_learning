package cn.itcast.stream.source.customsource

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/*
演示自定义非并行数据源实现
 */
object MySourceNoParalle {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 添加自定义的数据源
    val myDs: DataStream[Long] = env.addSource(new MyNoParalleSourceFunction).setParallelism(3)
    //3 打印数据
    myDs.print()
    //启动
    env.execute()
  }
}

//SourceFunction泛型是我们自定义source的返回数据类型
class MyNoParalleSourceFunction extends ParallelSourceFunction[Long] {
  var ele: Long = 0
  var isRunning = true
  //发送数据，生产数据的方法
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ele += 1
      //通过上下文对象发送数据
      ctx.collect(ele)
      //降低发送速度
      Thread.sleep(1000)
    }
  }

  // 取消方法，取消是通过控制一个变量来影响run方法中的while循环
  override def cancel(): Unit = {
    isRunning = false //取消发送数据
  }
}