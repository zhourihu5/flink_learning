package cn.itcast.batch.transformation

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable
import scala.util.Random
import org.apache.flink.api.scala._

/*
演示flink minby,maxby操作
 */
/*
计算每个学科下最大和最小成绩
 */

object MinMaxDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 演示效果明显，设置并行度为2，设置全局并行度为2
    val scores = new mutable.MutableList[(Int, String, Double)]
    scores.+=((1, "yuwen", 90.0))
    scores.+=((2, "shuxue", 20.0))
    scores.+=((3, "yingyu", 30.0))
    scores.+=((4, "wuli", 40.0))
    scores.+=((5, "yuwen", 50.0))
    scores.+=((6, "wuli", 60.0))
    scores.+=((7, "yuwen", 70.0))
    val scoreDs = env.fromCollection(Random.shuffle(scores))

    // 3 转换  计算每个学科下最大和最小成绩
    val groupDs: GroupedDataSet[(Int, String, Double)] = scoreDs.groupBy(1)
    //调用min
    val aggMinDs: AggregateDataSet[(Int, String, Double)] = groupDs.min(2)
//调用minby
    val minByDs: DataSet[(Int, String, Double)] = groupDs.minBy(2)
    // 4 输出 (如果直接打印无需进行启动，执行execute)
    aggMinDs.print()
    println("====================")
    minByDs.print()
    // 5 执行
//    env.execute()
  }
}
