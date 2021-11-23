package cn.itcast.ck

import java.util.concurrent.TimeUnit

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/*
演示ck相关配置
 */
object CheckPonitDemo {

  def main(args: Array[String]): Unit = {
    /*
    1 获取运行环境
    2 设置ck相关属性
    3 自定义数据源source,发送字符串数据
    4 打印
    5 启动
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 设置ck属性
    // 2.1开启ck  //指定使用hdfsstatebackend
//    env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink/ck_demo"))
    env.setStateBackend(new FsStateBackend("file:///./data/ck_demo"))
    //2.2 设置checkpoint的周期间隔  默认是没有开启ck，需要通过指定间隔来开启ck
    env.enableCheckpointing(1000)
    //2.3 设置ck的执行语义，最多一次，至少一次，精确一次
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //2.4 设置两次ck之间的最小时间间隔，两次ck之间时间最少差500ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 2.5 设置ck的超时时间，如果超时则认为本次ck失败，继续一下一次ck即可,超时60s
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    //2.6 设置ck出现问题，是否让程序报错还是继续任务进行下一次ck,true:让程序报错，false：不报错进行下次ck
    //如果是false就是ck出现问题我们允许程序继续执行，如果下次ck成功则没有问题，但是如果程序下次ck也没有成功，
    //此时程序挂掉需要从ck中恢复数据时可能导致程序计算错误，或者是重复计算数据。
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 2.7设置任务取消时是否保留检查点  retain：则保存检查点数据，delete:删除ck作业数据
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //2.8 设置程序中同时允许几个ck任务同时进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 3自定义source
    val strDs: DataStream[String] = env.addSource(
      new RichSourceFunction[String] {
        var flag = true

        override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
          while (flag) {
            ctx.collect("hello flink")
            TimeUnit.SECONDS.sleep(1)
          }
        }

        override def cancel(): Unit = {
          flag = false
        }
      }
    )
    //4 打印
    strDs.print()
    // 4启动
    env.execute()

  }
}
