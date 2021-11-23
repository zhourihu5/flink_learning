package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink union的operator
 */
/*
需求：
示例
将以下数据进行取并集操作
数据集1
"hadoop", "hive", "flume"
数据集2
"hadoop", "hive", "spark"
 */

object UnionDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    val ds1: DataSet[String] = env.fromCollection(List("hadoop",
      "hive",
      "flume"))
    val ds2: DataSet[String] = env.fromCollection(List("hadoop",
      "hive",
      "azkaban"))
    val ds3: DataSet[String] = env.fromCollection(List("hadoop",
      "hive",
      "azkaban"))
//val ds2: DataSet[Int] = env.fromCollection(List(1,
//  2,
//  3))
    // union算子要求两个ds中的数据类型必须一致！！
    // 3 转换 使用union获取两个ds的并集
    val unionDs: DataSet[String] = ds1.union(ds2).union(ds3)

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    unionDs.print()
    // 5 执行
  }
}
