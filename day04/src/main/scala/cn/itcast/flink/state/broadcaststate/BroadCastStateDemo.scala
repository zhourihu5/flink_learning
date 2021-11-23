package cn.itcast.flink.state.broadcaststate

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig


/*
假设有这样的一个需求，需要实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息。
 */
object BroadCastStateDemo {
  def main(args: Array[String]): Unit = {
    /*
    步骤：
    1.获取流处理执行环境

    2.设置kafka配置

    3.kafka数据转换：processfunction

    4.自定义source读取获取mysql数据源: (String, (String, Int))

    5.定义mapState广播变量，获取mysql广播流

    6.事件流和广播流连接

    7.业务处理BroadcastProcessFunction，补全数据

     */
    //    1.获取流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 配置消费kafka中事件流的数据source
    //设置并行度为1，方便观察数据
    env.setParallelism(1)
    val topic = "test"
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
    val kafkaDs = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop))
    // 3 使用processfunction来处理事件流中的数据 json格式数据转为tuple格式
    //3.1 processfunction
    val tupleDs: DataStream[(String, String, String, Int)] = kafkaDs.process(
      // 泛型：in：输入数据类型 String  out：输出数据类型 ,tuple(userid,eventime,type,productid)
      new ProcessFunction[String, (String, String, String, Int)] {
        // 3.2自定义业务处理逻辑 ，value:一条数据， out:数据收集器，数据处理之后可以使用收集器发送出去
        override def processElement(value: String, ctx: ProcessFunction[String, (String, String, String, Int)]#Context,
                                    out: Collector[(String, String, String, Int)]): Unit = {
          // 3.3 把kafka中的json数据转为tuple类型
          val jSONObject = JSON.parseObject(value)
          val userid: String = jSONObject.getString("userID")
          val eventTime: String = jSONObject.getString("eventTime")
          val eventType: String = jSONObject.getString("eventType")
          val productID: Int = jSONObject.getIntValue("productID")
          //3.4 发送数据
          out.collect((userid, eventTime, eventType, productID))
        }


      }
    )
    // 4  加载用户配置的mysql数据流 自定义实现读取mysql的source
    val mysqlSource: DataStream[(String, (String, Int))] = env.addSource(new MysqlSource).uid("")
    // 5 需要把mysqlsource 广播出去，作为broadcaststate来使用 ,广播流直接使用broadcaststate广播，需要提供一个mapstatedescriptor
    val broadcastStateDesc: MapStateDescriptor[String, (String, Int)] =
      new MapStateDescriptor[String, (String, Int)]("broadcastState", classOf[String], classOf[(String, Int)])
    val broadcastStream: BroadcastStream[(String, (String, Int))] = mysqlSource.broadcast(broadcastStateDesc)
    // 6 双流的connect  合并流，原来流中数据依然是独立存在
    val connectStream: BroadcastConnectedStream[(String, String, String, Int), (String, (String, Int))] = tupleDs.connect(broadcastStream)
    //7 使用processfunction处理connectstream,自定义BroadcastStateProcessFunction实现在处理事件流数据时获取到广播流中的数据，借助于state实现
    val resDs: DataStream[(String, String, String, Int, String, Int)] = connectStream.process(new MyProcessFunction)
    // 8 打印结果
    resDs.print()
    // 9 启动
    env.execute()

  }
}

//7 使用processfunction处理connectstream,自定义BroadcastStateProcessFunction实现在处理事件流数据时获取到广播流中的数据，借助于state实现
/*
@param <IN1> The input type of the non-broadcast side.事件流中数据类型  (userid,eventime,type,productId)
 * @param <IN2> The input type of the broadcast side. 广播流中数据类型  (userid,(username,userage))
 * @param <OUT> The output type of the operator.  输出的数据类型   (userid,eventime,type,productId,username,userage)
 */

class MyProcessFunction extends BroadcastProcessFunction[(String, String, String, Int), (String, (String, Int)),
  (String, String, String, Int, String, Int)] {
  //broadcaststate描述器
  val broadcastStateDesc: MapStateDescriptor[String, (String, Int)] =
    new MapStateDescriptor[String, (String, Int)]("broadcastState", classOf[String], classOf[(String, Int)])


  //处理事件流中数据的方法  对于广播流数据是只读，不能修改的
  override def processElement(value: (String, String, String, Int), ctx: BroadcastProcessFunction[(String, String, String, Int),
    (String, (String, Int)), (String, String, String, Int, String, Int)]#ReadOnlyContext,
                              out: Collector[(String, String, String, Int, String, Int)]): Unit = {
    //处理事件流中数据如何获取到广播流中的数据呢？借助于state，需要在processBroadCastelement中把广播流数据存入state中，在这个方法中获取数据
    val readOnlyState: ReadOnlyBroadcastState[String, (String, Int)] = ctx.getBroadcastState(broadcastStateDesc)
    //根据userid去state中取出其它数据 ,state中有可能存储该userid数据有可能没有
    val bool = readOnlyState.contains(value._1)
    if (bool) {
      //username,userage
      val tuple: (String, Int) = readOnlyState.get(value._1)
      //补全事件流中的数据
      out.collect((value._1, value._2, value._3, value._4, tuple._1, tuple._2))
    } else {
      //可以丢弃，也可以补null值
    }
  }

  //处理广播流中数据的方法
  override def processBroadcastElement(value: (String, (String, Int)),
                                       ctx: BroadcastProcessFunction[(String, String, String, Int), (String, (String, Int)),
                                         (String, String, String, Int, String, Int)]#Context,
                                       out: Collector[(String, String, String, Int, String, Int)]): Unit = {
    //把广播流中的数据存入state
    //根据mapstate描述器获取broadcaststate数据
    val broadCastState =
    ctx.getBroadcastState(broadcastStateDesc)
    //需要把广播流中的数据存入mapstate中
    broadCastState.put(value._1, value._2)
  }
}

// 4  加载用户配置的mysql数据流 自定义实现读取mysql的source  读取mysql返回数据类型：为了后续根据userid查询方便我们设置读取返回类型：
// (userId,(username,userage))
class MysqlSource extends RichSourceFunction[(String, (String, Int))] {
  var conn: Connection = null
  var ps: PreparedStatement = null
  var flag = true

  //打开mysql连接
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager
      .getConnection("jdbc:mysql://node1:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "123456")
    //准备preparestatement
    var sqlString = "select * from user_info"
    ps = conn.prepareStatement(sqlString)
  }

  override def close(): Unit = {
    if (conn != null) {
      conn.close()
    }
    if (ps != null) {
      ps.close()
    }
  }

  //读取数据并发送出去
  override def run(ctx: SourceFunction.SourceContext[(String, (String, Int))]): Unit = {
    while (flag) {
      val result: ResultSet = ps.executeQuery()
      while (result.next()) {
        val userId: String = result.getString("userID")
        val userName: String = result.getString("userName")
        val userAge: Int = result.getInt("userAge")
        // 收集器发送数据
        ctx.collect((userId, (userName, userAge)))
      }
      //休眠1秒
      TimeUnit.SECONDS.sleep(1)
    }
  }

  //取消方法
  override def cancel(): Unit = {
    flag = false
  }
}