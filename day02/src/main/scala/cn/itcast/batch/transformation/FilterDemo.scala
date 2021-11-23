package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink filter的operator
 */
/*
需求：
示例：
过滤出来以下以长度>4的单词。
"hadoop", "hive", "spark", "flink"
 */
object FilterDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val wordsDs: DataSet[String] = env.fromCollection(List("hadoop", "hive", "spark", "flink"))

    // 3 转换操作 使用filter过滤单词长度大于4的单词
    val moreThan4Words: DataSet[String] = wordsDs.filter(_.length > 4)

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    moreThan4Words.print()
    // 5 执行
  }
}
