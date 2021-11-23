package cn.itcast.batch.env

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/*
演示下不同env创建
 */
object FlinkEnvDemo {
  def main(args: Array[String]): Unit = {

    // 1 localenviroment 开销昂贵
//    val env = ExecutionEnvironment.createLocalEnvironment()
    //2 collectionenviroment 局限性太大，
//    val env = ExecutionEnvironment.createCollectionsEnvironment
//    // 3 executionenviroment
    val env = ExecutionEnvironment.getExecutionEnvironment //推荐使用这种方式创建
    val ds1 = env.fromElements(List(1,2,3))
    ds1.print()
  }
}
