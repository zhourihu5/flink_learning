//package cn.itcast.stream.sink
//
//import java.sql.{Connection, DriverManager, PreparedStatement}
//
//
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//
///*
//flink程序计算结果保存到mysql中
// */
////定义student case class
//case class Student(id: Int, name: String, age: Int)
//
//object SinkToMysqlDemo {
//  def main(args: Array[String]): Unit = {
//    /*
//    读取数据然后直接写入mysql,需要自己实现mysql sinkfunction
//    自定义class实现RichSinkFunction重写open,invoke,close方法
//     */
//    //1 创建一个流处理的运行环境
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    // 2 加载source
//    val stuDs: DataStream[Student] = env.fromElements(Student(0, "tony", 18))
//    // 3 直接写出到mysql
//    stuDs.addSink(new MySqlSinkFunction)
//    // 4 执行
//    env.execute()
//  }
//}
//
////准备自定义mysql sinkfunciton
//class MySqlSinkFunction extends RichSinkFunction[Student] {
//  var ps: PreparedStatement = null
//  var connection: Connection = null
//  // 3.1 打开连接
//  override def open(parameters: Configuration): Unit = {
//    // 3.1.1驱动方式
//    connection = DriverManager.getConnection("jdbc:mysql://node1:3306/test", "root", "123456")
//    //3.1.2准备sql语句插入数据到mysql表中
//    var sql = "insert into t_student(name,age) values(?,?)";
//    //3.1.3准备执行语句对象
//    ps = connection.prepareStatement(sql)
//  }
//  //关闭连接
//  override def close(): Unit = {
//    if (connection != null) {
//      connection.close()
//    }
//    if (ps != null) ps.close()
//  }
//
//  // 3.2 这个方法负责写入数据到mysql中,value就是上游datastream传入需要写入mysql的数据
//  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
//    // 3.2.1设置参数
//    ps.setString(1, value.name)
//    ps.setInt(2, value.age)
//    //3.2.2执行插入动作
//    ps.executeUpdate()
//  }
//
//}