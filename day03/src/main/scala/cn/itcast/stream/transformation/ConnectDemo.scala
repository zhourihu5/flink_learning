package cn.itcast.stream.transformation

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/*
演示flink中connect的用法，把两个数据流连接到一起
需求：
创建两个流，一个产生数值，一个产生字符串数据
使用connect连接两个流，结果如何
 */
object ConnectDemo {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载source
    val numDs: DataStream[Long] = env.addSource(new MyNumberSource)
    val strDs = env.addSource(new MyStrSource)
    // 3 使用connect进行两个连接操作
     val connectedDs: ConnectedStreams[Long, String] = numDs.connect(strDs)
    //传递两个函数，分别处理数据
    val resDs: DataStream[String] = connectedDs.map(l=>"long"+l, s=>"string"+s)
    //connect意义在哪里呢？只是把两个合并为一个，但是处理业务逻辑都是按照自己的方法处理？connect之后两条流可以共享状态数据
    resDs.print()
    //5 启动
    env.execute()
  }
}

//自定义产生递增的数字 第一个数据源
class MyNumberSource extends SourceFunction[Long]{
  var flag=true
  var num=1L
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(flag){
      num +=1
      ctx.collect(num)
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = {
    flag=false
  }
}

// 自定义产生从1开始递增字符串
class MyStrSource extends SourceFunction[String]{
  var flag=true
  var num=1L
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while(flag){
      num +=1
      ctx.collect("str"+num)
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = {
    flag=false
  }
}