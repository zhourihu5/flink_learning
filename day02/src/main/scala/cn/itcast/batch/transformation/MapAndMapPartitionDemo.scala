package cn.itcast.batch.transformation

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/*
演示flink map与mappartition的operator
 */
/*
需求：
示例
使用map操作，将以下数据转换为一个scala的样例类。
"1,张三", "2,李四", "3,王五", "4,赵六"
 */
object MapAndMapPartitionDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val sourceDs: DataSet[String] = env.fromCollection(List("1,张三", "2,李四", "3,王五", "4,赵六"))

    // 3 转换操作
    // 3.1 定义case class
    case class Student(id: Int, name: String)
    // 3.2 map操作
    val stuDs: DataSet[Student] = sourceDs.map(
      line => {
        val arr: Array[String] = line.split(",")
        Student(arr(0).toInt, arr(1))
      }
    )
    // 3.3 mappartition操作
    val stuDs2: DataSet[Student] = sourceDs.mapPartition(

      iter => { //迭代器
        //todo 做一些昂贵的动作，比如开启连接
        //遍历迭代器数据转为case class类型然后返回
        iter.map(
          it => {
            val arr: Array[String] = it.split(",")
            Student(arr(0).toInt, arr(1))
          }
        )
        //todo 做一些昂贵的动作，关闭连接
      }
    )

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    stuDs.print()
    println("====================")
    stuDs2.print()
    // 5 执行
  }
}
