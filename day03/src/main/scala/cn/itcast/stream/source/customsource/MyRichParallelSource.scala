package cn.itcast.stream.source.customsource

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
演示自定义非并行数据源实现
 */
object MyRichParallelSource {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 添加自定义的数据源
    val myDs: DataStream[Long] = env.addSource(new MyRichParalleSourceFunction).setParallelism(3)
    //3 打印数据
    myDs.print()
    //启动
    env.execute()
  }
}

//SourceFunction泛型是我们自定义source的返回数据类型
class MyRichParalleSourceFunction extends RichParallelSourceFunction[Long] {
//todo 初始化方法比如打开数据库连接等昂贵操作
  override def open(parameters: Configuration): Unit = super.open(parameters)
//todo 关闭连接
  override def close(): Unit = super.close()

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