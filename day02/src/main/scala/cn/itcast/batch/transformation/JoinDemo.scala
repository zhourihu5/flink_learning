package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink join的operator
 */
/*
需求：
示例
有两个csv文件，有一个为score.csv，一个为subject.csv，分别保存了成绩数据以及学科数据。
需要将这两个数据连接到一起，然后打印出来。
 */
//定义case class
// 成绩  学生id,学生的姓名，学科id,分数
case class Score(id: Int, name: String, subjectId: Int, score: Double)

//学科 学科id,学科名称
case class Subject(id: Int, name: String)

object JoinDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    val scoreDs: DataSet[Score] = env.readCsvFile[Score]("E:\\data\\score.csv")
    val sbujectDs: DataSet[Subject] = env.readCsvFile[Subject]("E:\\data\\subject.csv")
    // 3 转换 使用join实现两个dataset关联
    val joinDs: JoinDataSet[Score, Subject] = scoreDs.join(sbujectDs).where(2).equalTo(0)
    // 4 输出 (如果直接打印无需进行启动，执行execute)
    joinDs.print()
    // 5 执行
  }
}
