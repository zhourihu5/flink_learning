package cn.itcast.batch.transformation

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/*
演示flink rebalance的operator
 */
/*
需求：
演示rebalance分区均衡数据
 */
object RebalanceDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val sourceDs: DataSet[Long] = env.generateSequence(0, 100)
    //过滤掉部分数据保证倾斜现象明显
    val filterDs: DataSet[Long] = sourceDs.filter(_ > 8)


    // 3 转换操作 使用filter过滤单词长度大于4的单词
    // 3.1 如何观测到每个分区的数据量？使用richmapfunciton来观察数据
    val subTaskDs: DataSet[(Long, Long)] = filterDs.map(
      new RichMapFunction[Long, (Long, Long)] { //rich为代表的富函数可以获取到运行的上下文对象
        //自己定义map逻辑，
        override def map(value: Long): (Long, Long) = {
          //获取到当前任务编号信息（以此来代表分区）
          val subtask: Long = getRuntimeContext.getIndexOfThisSubtask
          (subtask, value)
        }
      }
    )
    // 3.2使用rebalance解决数据倾斜问题
    val rebalanceDs: DataSet[Long] = filterDs.rebalance()

    val rebDs: DataSet[(Long, Long)] = rebalanceDs.map(
      new RichMapFunction[Long, (Long, Long)] { //rich为代表的富函数可以获取到运行的上下文对象
        //自己定义map逻辑，
        override def map(value: Long): (Long, Long) = {
          //获取到当前任务编号信息（以此来代表分区）
          val subtask: Long = getRuntimeContext.getIndexOfThisSubtask
          (subtask, value)
        }
      }
    )
    // 4 输出 (如果直接打印无需进行启动，执行execute)
    subTaskDs.print()
    println("================================")
    rebDs.print()
    // 5 执行
  }
}
