package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink filter的operator
 */
/*
需求：

示例
请将以下元组数据，下按照单词使用groupBy进行分组，再使用reduce操作聚合成一个最终结果
("java" , 1) , ("java", 1) ,("scala" , 1)
转换为
("java", 2), ("scala", 1)
 */
object ReduceGroupDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    //    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("java", 1)))
    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("scala", 1)))

    // 3 转换 使用reducegroup实现单词计数
    val groupDs: GroupedDataSet[(String, Int)] = wordsDs.groupBy(_._1)
    val resultDs: DataSet[(String, Int)] = groupDs.reduceGroup(
      iter => {
        //参数是一个迭代器
        iter.reduce(//再对迭代器进行reduce聚合操作
          (w1, w2) => (w1._1, w1._2 + w2._2)
        )
      }
    )

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resultDs.print()
    // 5 执行
  }
}
