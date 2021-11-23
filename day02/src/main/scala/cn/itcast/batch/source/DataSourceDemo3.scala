package cn.itcast.batch.source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/*
演示flink 批处理基于文件创建dataset
 */
object DataSourceDemo3 {
  def main(args: Array[String]): Unit = {
    //1 获取env
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2 基于文件来创建dataset
    //2.1读取本地文本文件
    val wordsDs: DataSet[String] = env.readTextFile("E:/data/words.txt")
    // 2.2读取hdfs文件
    val hdfsDs: DataSet[String] = env.readTextFile("hdfs://node1:8020/wordcount/input/words.txt")
    // 2.3读取csv文件
    //读取csv文件需要准备一个case class
    case class Subject(id: Int, name: String)
    val subjectDs: DataSet[Subject] = env.readCsvFile[Subject]("E:/data/subject.csv")
    // 2.4 读取压缩文件
    val compressDs: DataSet[String] = env.readTextFile("E:/data/wordcount.txt.gz")
    // 2.5 遍历读取文件夹数据
    val conf = new Configuration()
    conf.setBoolean("recursive.file.enumeration",true)
    val folderDs: DataSet[String] = env.readTextFile("E:/data/wc/").withParameters(conf)
    //打印输出结果
//    wordsDs.print()
//    print("------------------------------------------")
//    hdfsDs.print()
//    println("=====================")
//    subjectDs.print()
//    println("=====================")
//    compressDs.print()
//    println("===============")
    folderDs.print()
  }
}
