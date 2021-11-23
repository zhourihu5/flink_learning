package cn.itcast.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

/*
使用flink批处理进行单词计数
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    /*
    1.获得一个execution environment，
    2.加载/创建初始数据，
    3.指定这些数据的转换，
    4.指定将计算结果放在哪里，
    5.触发程序执行
     */
    //  1.获得一个execution environment， 批处理程序入口对象
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度为1，
    env.setParallelism(1)
    // 2.加载/创建初始数据
    val sourceDs: DataSet[String] = env.fromElements("Apache Flink is an open source platform for " +
      "distributed stream and batch data processing",
      "Flink’s core is a streaming dataflow engine that provides data distribution")
    // 大致思路：对每行语句按照空格进行切分，切分之后组成（单词，1）tuple,按照单词分组最后进行聚合计算
    // 3.指定这些数据的转换， transformation
    val wordsDs: DataSet[String] = sourceDs.flatMap(_.split(" "))
    //(单词，1)
    val wordAndOneDs: DataSet[(String, Int)] = wordsDs.map((_, 1))
    val groupDs: GroupedDataSet[(String, Int)] = wordAndOneDs.groupBy(0)
    //聚合
    val aggDs: AggregateDataSet[(String, Int)] = groupDs.sum(1)
    // 4.指定将计算结果放在哪里，
    aggDs.writeAsText("hdfs://node1:8020/wc/out2", FileSystem.WriteMode.OVERWRITE)
    //关于默认的并行度：默认获取的是当前机器的cpu核数是8，所以有8个结果文件，
    // 5 触发程序执行
    env.execute()
  }
}
