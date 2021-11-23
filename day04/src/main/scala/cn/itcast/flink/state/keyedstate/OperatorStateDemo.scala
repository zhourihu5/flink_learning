package cn.itcast.flink.state.keyedstate

import java.util

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 使用OperatorState实现类似于offset的管理
  */
object OperatorStateDemoOffset {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //并行度为1方便测试
    env.setParallelism(3)
    //2.设置环境参数(后面学)
    env.enableCheckpointing(1000)
    env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink/ck"))
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //固定延迟重启策略: 程序出现异常的时候，重启3次，每次延迟5秒钟重启，超过3次，程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000))
    import org.apache.flink.api.scala._
    //3.获取数据--从自定义数据源
    val streamSource = env.addSource(new MyRichSource())
    //4.输出结果
    streamSource.print()
    env.execute()
  }

  /**
    * 自定义数据源,支持使用State记录offset
    */
  class MyRichSource extends SourceFunction[String] with CheckpointedFunction{
    var isRunning:Boolean = true
    //声明一个state用来记录offset
    var offsetState:ListState[Long] = _
    //声明offset
    var offset: Long = 0L

    //当请求检查点的快照时调用此方法
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      offsetState.clear()
      offsetState.add(offset)
    }

    //初始化历史状态方法
    override def initializeState(context: FunctionInitializationContext): Unit = {
      //定义ListState属性描述符
      val descriptor = new ListStateDescriptor[Long]("offsetState",classOf[Long])

      //获取State
      offsetState = context.getOperatorStateStore.getListState(descriptor)
    }

    //生成数据的方法
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val iter: util.Iterator[Long] = offsetState.get().iterator()
      if (iter.hasNext){//如果State中保存有offset则取出来
        offset = iter.next()
      }

      while(isRunning){
        ctx.collect(s"offset:${offset+=1;offset}")
        Thread.sleep(1000)
        if(offset % 5 == 0){
          println("程序遇到异常重启....")
          throw new RuntimeException("bug...")
        }
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }

}