package cn.itcast.batch.distributecache

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.io.Source

/*
使用分布式缓存技术实现该需求
需求
创建一个成绩数据集(学生ID,学科,成绩): List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86))
注册一个学生数据集(学生ID,姓名): List((1, "张三"), (2, "李四"), (3, "王五"))为分布式缓存文件
通过分布式缓存文件获取到学生姓名并将数据转换为(学生姓名,学科,成绩): List( ("张三", "语文", 50),("李四", "数学", 70), ("王五", "英文", 86))
 */
object DistributeCacheDemo {
  def main(args: Array[String]): Unit = {
    //1 运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment //推荐使用这种方式创建

    //2 source  分别创建成绩数据集
    import org.apache.flink.api.scala._

    //2.1成绩数据集
    val scoreDataSet: DataSet[(Int, String, Int)] = env.fromCollection(List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86)))
    //2.2 把学生数据注册为分布式缓存文件,文件路径，缓存文件名称（别名）
    env.registerCachedFile("hdfs://node1:8020/distribute_cache_student", "cache_student")
    // 3 转换
    // 3.1 使用RichMapFunction对成绩数据集进行map转换
    // 泛型：in,out: in:map处理的数据类型：(Int, String, Int)，out：map处理之后输出的数据集:("张三", "语文", 50)
    val resDs: DataSet[(String, String, Int)] = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {
      //3.3 定义一个map成员变量
      var stuMap: Map[Int, String] = null
      //3.2通过上下文件获取缓存文件
      override def open(parameters: Configuration): Unit = {
        val file: File = getRuntimeContext.getDistributedCache.getFile("cache_student")
        //3.2.1对文件进行读取和解析，方便根据学生id查找数据
        val tuples: Iterator[(Int, String)] = Source.fromFile(file).getLines().map(
          line => {
            val arr = line.split(",")
            (arr(0).toInt, arr(1)) //学生id,学生姓名
          }
        )
        //得到一个id作为key，姓名是value的map
        stuMap = tuples.toMap
      }
      //3.4 根据map解析出学生的姓名然后返回结果
      override def map(score: (Int, String, Int)): (String, String, Int) = {
        // 3.4.1 获取成绩中的学生id
        val stuId = score._1
        //3.4.2 根据学生id去广播变量map中获取到学生姓名
        val stuName: String = stuMap.getOrElse(stuId, "null")
        // 3.4.3 组装数据并返回
        (stuName, score._2, score._3)
      }
    })


    // 4 sink
    resDs.print()
    //5 excute 执行
  }
}
