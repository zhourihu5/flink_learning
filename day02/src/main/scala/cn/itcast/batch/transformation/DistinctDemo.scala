package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink distinct的operator
 */
/*
需求：

示例
请将以下元组数据，对数据进行去重操作
("java" , 1) , ("java", 1) ,("scala" , 1)
转换为
("java", 2), ("scala", 1)
 */
object DistinctDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val wordsDs = env.fromCollection(List(("java", 1), ("java", 2), ("scala", 1)))

    // 3 转换 使用distinct实现去重
//    wordsDs.distinct().print() //是对整个元组进行去重
    wordsDs.distinct(0).print()  //指定按照某个字段进行去重操作

    // 4 输出 (如果直接打印无需进行启动，执行execute)
//    resultDs.print()
    // 5 执行
  }
}
