package cn.itcast.batch.source

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/*
演示flink中dataset的常见datasource
 */
object DataSourceDemo {
  def main(args: Array[String]): Unit = {
    /*
    dataset api中datasource主要有两类
    1.基于集合
    2.基于文件
     */
    //1 获取executionenviroment
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2 source操作
    // 2.1     1.基于集合
    /*
    1.使用env.fromElements()支持Tuple，自定义对象等复合形式。
    2.使用env.fromCollection()支持多种Collection的具体类型
    3.使用env.generateSequence()支持创建基于Sequence的DataSet
     */
    // 使用env.fromElements()
    val eleDs: DataSet[String] = env.fromElements("spark", "hadoop", "flink")

    // 使用env.fromCollection()
    val collDs: DataSet[String] = env.fromCollection(Array("spark", "hadoop", "flink"))
    //使用env.generateSequence()
    val seqDs: DataSet[Long] = env.generateSequence(1, 9)

    // 3 转换 可以没有转换
    //4 sink 输出
    eleDs.print()
    collDs.print()
    seqDs.print()

    // 5 启动  在批处理中： 如果sink操作是'count()', 'collect()', or 'print()',最后不需要执行execute操作，否则会报错
    //    env.execute()
  }

}
