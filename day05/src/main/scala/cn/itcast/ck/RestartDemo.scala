package cn.itcast.ck

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
演示flink重启策略
 */

object RestartDemo {
  def main(args: Array[String]): Unit = {
    /*
     * 1.获取执行环境

      2.设置检查点机制：路径，重启策略

      3.自定义数据源

        （1）需要继承SourceFunction

        （2）让程序报错，抛出异常

       4.数据打印

       5.触发执行

     */
    //1 创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //方便观察数据，设置并行度为1
    env.setParallelism(1)
    // 2 设置检查点相关属性，重启策略
    env.enableCheckpointing(1000) //开启ck,每秒执行一次
    //设置检查点存储数据路径
//    env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink/ck"))
    env.setStateBackend(new FsStateBackend("file:///./data/flink/ck"))
    //任务取消时，保存检查点数据（后续可以从检查点中恢复数据）
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置可以同时进行几个ck任务
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //固定延迟重启策略: 程序出现异常的时候，重启3次，每次延迟5秒钟重启，超过3次，程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000))
    // 2 自定义source
    val sourceDs: DataStream[Long] = env.addSource(new MySource())

    // 3 打印数据
    sourceDs.print()
    // 4 启动
    env.execute()
  }
}

// 2 自定义source 实现sourcefunction,以及checkpointedfunction
class MySource extends SourceFunction[Long] {
  var flag = true
  //定义发送数据的初始值
  var offset = 0L
  //定义Liststate
  var offsetListState: ListState[Long] = null

  //2.1 生成数据的方法
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (flag) {
      //发送数据，实现发送一个数值，每次增加1 ，来表是所谓消费者的偏移量数据,offset应该从offsetListstate中获取，获取不到再从0开始

      offset += 1
      ctx.collect(offset)
      println("发送数据 offset>>" + offset)
      TimeUnit.SECONDS.sleep(1)
      //设置故障，程序遇到异常
      if (offset % 5 == 0) {
        println("程序遇到异常。。。，将要重启。。")
        throw new RuntimeException("程序遇到异常。。。，将要重启。。")
      }
    }
  }

  override def cancel(): Unit = {
    flag = false
  }

}