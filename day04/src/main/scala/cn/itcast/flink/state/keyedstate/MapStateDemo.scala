package cn.itcast.flink.state.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/*
 使用MapState保存中间结果计算分组的和
 */

object MapStateDemo {
  def main(args: Array[String]): Unit = {
    /*
     * 使用MapState保存中间结果对下面数据进行分组求和
      * 1.获取流处理执行环境
      * 2.加载数据源
      * 3.数据分组
      * 4.数据转换，定义MapState,保存中间结果
      * 5.数据打印
      * 6.触发执行
     */
    //1 创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载数据
    val collectionDs: DataStream[(String, Int)] = env.fromCollection(List(
      ("java", 1),
      ("python", 3),
      ("java", 2),
      ("scala", 2),
      ("python", 1),
      ("java", 1),
      ("scala", 2)))
    // 3 对数据分组
    val keyStream: KeyedStream[(String, Int), Tuple] = collectionDs.keyBy(0)
    // 4 转换 使用mapstate
    // 4.1 mapstate获取与value一样需要上下文，所以我们的转换还的是一个richfunciton
    val mapStateDs: DataStream[(String, Int)] = keyStream.map(
      new RichMapFunction[(String, Int), (String, Int)] {
        // mapstate描述器
        var sumMapState: MapState[String, Int] = null

        // 4.1.1 获取mapstate
        override def open(parameters: Configuration): Unit = {
          // 1 定义一个mapstate描述器
          val mapStateDesc: MapStateDescriptor[String, Int] = new MapStateDescriptor[String, Int]("sumMap",
            //定义typeinformation类型来包装kv数据类型
            TypeInformation.of(classOf[String]), //key的infomation
            TypeInformation.of(classOf[Int])
          )
          // 2 根据描述器获取mapstate
          sumMapState = getRuntimeContext.getMapState(mapStateDesc)
        }

        // 4.1.2 使用mapstate获取历史结果 求和
        override def map(value: (String, Int)): (String, Int) = {
          // 1 获取到新数据的key
          val key: String = value._1
          //2 根据key去获取mapstate中的历史结果,stateValue就是之前数据的累加结果
          val stateValue: Int = sumMapState.get(key)
          sumMapState.put(key, stateValue + value._2)
          // 3 返回结果， 取出mapstate中的value值
          (key, sumMapState.get(key))
        }
      }
    )
    // 4 打印结果
    mapStateDs.print()
    // 5 启动
    env.execute()
  }
}
