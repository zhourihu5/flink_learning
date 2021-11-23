package cn.itcast.batch.env

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/*
演示下通过idea提交远程集群
 */
object FlinRemoteEnvDemo {
  def main(args: Array[String]): Unit = {

    //创建远程集群运行环境，需要能连接到集群，连接到jobmanger的地址，ip:jobmanager地址，port:jbomanger端口，jar:当前程序所打成的jar路径
    val env: ExecutionEnvironment = ExecutionEnvironment.createRemoteEnvironment("node1",
      8081,"E:\\flink\\flink_video\\day01\\code\\flink_learning\\day02\\target\\original-day02-1.0-SNAPSHOT.jar"
      )
//读取hdfs上的文件
    val resDs: DataSet[String] = env.readTextFile("hdfs://node1:8020/wordcount/input/words.txt")

    resDs.print()
  }
}
