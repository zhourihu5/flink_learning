package cn.itcast.batch.sink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

/*
演示flink dataset 中的sink operator
 */
object SinkDemo {
  def main(args: Array[String]): Unit = {
    //1 创建入口对象，获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source
    val wordsDs: DataSet[String] = env.fromElements("spark", "hadoop", "flink")

    // 3 转换

    // 4 sink （如果是print无需执行下面的execute）

    // 4.1  基于集合
    //标准输出
//    wordsDs.print()
//    println("==========================")
//    //错误输出
//    wordsDs.printToErr() //高亮显示
//    //collect 到本地
//    println("==========================")
//
//    println(wordsDs.collect())

    // 4.2 基于文件，保存结果到文件, 可以直接调整operator的并行度
    wordsDs.setParallelism(2).writeAsText("e:/data/sink/words/",FileSystem.WriteMode.OVERWRITE)
    // 5 执行
    env.execute()
  }
}
