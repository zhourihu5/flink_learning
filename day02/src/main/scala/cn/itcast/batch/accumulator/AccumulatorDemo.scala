package cn.itcast.batch.accumulator

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

/*
flink中累加器使用演示
需求：

遍历下列数据, 打印出数据总行数
   "aaaa","bbbb","cccc","dddd"
 */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {

    //1 运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2 source 加载本地集合
    val wordsDs: DataSet[String] = env.fromCollection(List("aaaa", "bbbb", "cccc", "dddd"))
    //3 转换 不能使用普通方法，要是richfunction,因为需要使用上下午注册累加器
    val accDs: DataSet[String] = wordsDs.map(
      // 3.1 in: String out: String ;不处理数据只是统计数量而已
      new RichMapFunction[String, String] {
        //3.2 创建累加器
        private var numLines = new IntCounter()
        // 对比：定义一个int类型变量
        var num = 0
        override def open(parameters: Configuration): Unit = {
          //3.3 注册累加器
          getRuntimeContext.addAccumulator("numLines", numLines)
        }
        //3.4使用累加器统计数据条数
        override def map(value: String): String = {
          numLines.add(1) //累加器中增加1
          num += 1
          println(num)
          value
        }
      }
    ).setParallelism(2)
    // 4 输出 保存到文件中，因为需要执行env.execute动作，不能直接打印
    accDs.writeAsText("e:/data/acc", FileSystem.WriteMode.OVERWRITE)
    // 5 启动执行
    val result: JobExecutionResult = env.execute()
    // 6 获取累加器的值
    val accRes = result.getAccumulatorResult[Int]("numLines")
    println("acc结果值：" + accRes)
  }
}
