package cn.itcast.stream.source.customsource

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.concurrent.TimeUnit

import cn.itcast.stream.sink.Student
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
演示自定义并行数据源读取mysql
 */

//定义student 样例类
case class Student1(id: Int, name: String, age: Int)

object MysqlRichParallelSource {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 添加自定义的数据源
    val stuDs: DataStream[Student] = env.addSource(new MysqlRichParalleleSource).setParallelism(1)
    //3 打印数据
    stuDs.print()
    //4 启动
    env.execute()
  }
}

// 2自定义mysql并行数据源
class MysqlRichParalleleSource extends RichParallelSourceFunction[Student] {
  var ps: PreparedStatement = null
  var connection: Connection = null

  //2.1 开启mysql连接
  override def open(parameters: Configuration): Unit = {
    //驱动方式
    connection = DriverManager.getConnection("jdbc:mysql://node1:3306/test", "root", "123456")
    //准备sql语句查询表中全部数据
    var sql = "select id ,name,age from t_student";
    //准备执行语句对象
    ps = connection.prepareStatement(sql)
  }

  //2.3 释放资源，关闭连接
  override def close(): Unit = {
    if (connection != null) {
      connection.close()
    }
    if (ps != null) ps.close()
  }


  var isRunning = true

  // 2.2 读取mysql数据
  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    while (isRunning) {
      //读取mysql中的数据
      val result: ResultSet = ps.executeQuery()
      while (result.next()) {
        val userId = result.getInt("id")
        val name = result.getString("name")
        val age = result.getInt("age")
        //收集并发送
        ctx.collect(Student(userId, name, age))

      }

      //休眠5s,执行一次
      TimeUnit.SECONDS.sleep(5)
    }
  }

  //取消方法
  override def cancel(): Unit = {
    isRunning = false
  }
}