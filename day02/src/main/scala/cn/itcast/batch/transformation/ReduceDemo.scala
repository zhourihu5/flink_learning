package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink filter的operator
 */
/*
需求：
示例1
请将以下元组数据，使用reduce操作聚合成一个最终结果
 ("java" , 1) , ("java", 1) ,("java" , 1)
将上传元素数据转换为("java",3)
示例2
请将以下元组数据，下按照单词使用groupBy进行分组，再使用reduce操作聚合成一个最终结果
("java" , 1) , ("java", 1) ,("scala" , 1)
转换为
("java", 2), ("scala", 1)
 */
object ReduceDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    //    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("java", 1)))
    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("scala", 1)))
    //    // 3 转换操作 使用reduce计算单词次数
    //    val resultDs: DataSet[(String, Int)] = wordsDs.reduce(
    //      (w1, w2) => { // w1是一个初始值，第一次的时候是：（单词，0），之后都是之前的累加结果
    //        //3.1 进行次数累加
    //        (w1._1, w1._2 + w2._2)
    //      }
    //    )

    //    // 3 转换操作，需要对单词进行分组，分组之后再进行次数的累加
    val groupDs: GroupedDataSet[(String, Int)] = wordsDs.groupBy(_._1)
    // 3.2 进行次数累加
    val resultDs: DataSet[(String, Int)] = groupDs.reduce(
      (w1, w2) => { // w1是一个初始值，第一次的时候是：（单词，0），之后都是之前的累加结果
        //3.1 进行次数累加
        (w1._1, w1._2 + w2._2)
      }
    )
    // 3 转换操作，按照索引分组，使用sum操作
    //    wordsDs.groupBy(0).reduce(
    //      (w1, w2) => { // w1是一个初始值，第一次的时候是：（单词，0），之后都是之前的累加结果
    //                //3.1 进行次数累加
    //                (w1._1, w1._2 + w2._2)
    //              }
    //    ).print()
    // 这种方式不允许，会报错：ava.lang.UnsupportedOperationException: Aggregate does not support grouping with KeySelector functions, yet.
    //    wordsDs.groupBy(_._1).sum(1).print()
    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resultDs.print()
    // 5 执行
  }
}
