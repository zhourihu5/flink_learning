package cn.itcast.batch.broadcast

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/*
使用广播变量实现该需求
需求
创建一个成绩数据集(学生ID,学科,成绩): List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86))
再创建一个学生数据集(学生ID,姓名): List((1, "张三"), (2, "李四"), (3, "王五")),并将该数据，发布到广播
通过广播获取到学生姓名并将数据转换为(学生姓名,学科,成绩): List( ("张三", "语文", 50),("李四", "数学", 70), ("王五", "英文", 86))
 */
object BroadCastDemo {
  def main(args: Array[String]): Unit = {
    //1 运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment //推荐使用这种方式创建

    //2 source 2. 分别创建两个数据集
    import org.apache.flink.api.scala._
    // 2.1学生数据集，广播该数据集
    val studentDataSet: DataSet[(Int, String)] = env.fromCollection(List((1, "张三"), (2, "李四"), (3, "王五")))
    //2.2 成绩数据集
    val scoreDataSet: DataSet[(Int, String, Int)] = env.fromCollection(List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86)))

    // 3 转换
    // 3.1 使用RichMapFunction对成绩数据集进行map转换
    // 泛型：in,out: in:map处理的数据类型：(Int, String, Int)，out：map处理之后输出的数据集:("张三", "语文", 50)
    var resDs = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {
      // 3.3.4定义成员变量来接收获取的广播数据
      var stuMap: Map[Int, String] = null
      // 3.3  获取广播变量
      // open方法是rich函数中另外一个比较重要和常用的方法，初始化方法，执行一次，获取广播变量操作再此处执行
      override def open(parameters: Configuration): Unit = {
        // 3.3.1 根据名称获取广播变量,需要限定获取的广播变量数据类型：
        val stuList: util.List[(Int, String)] = getRuntimeContext.getBroadcastVariable[(Int, String)]("student")
        // 3.3.2 list转为map数据
        import scala.collection.JavaConverters._
        // 3.3.3 把获取到的广播数据转为map,key:学生id，value：学生姓名
        stuMap = stuList.asScala.toMap
      }
      // 3.4  根据stumap查询到学生姓名然后返回数据；map处理业务逻辑：value:接收到的一个数据
      override def map(score: (Int, String, Int)): (String, String, Int) = { //每条数据执行一次map方法，获取广播变量的方法不适合在此处执行
        // 3.4.1 获取成绩中的学生id
        val stuId = score._1
        //3.4.2 根据学生id去广播变量map中获取到学生姓名
        val stuName: String = stuMap.getOrElse(stuId, "null")
        // 3.4.3 组装数据并返回
        (stuName, score._2, score._3)
      }

      // 3.2 广播学生数据
    }) withBroadcastSet(studentDataSet, "student")

    // 4 sink
    resDs.print()
    //5 excute 执行
  }
}
