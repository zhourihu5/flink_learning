package cn.itcast.batch.transformation

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem

import scala.collection.mutable
import scala.util.Random

/*
演示flink dataset的分区策略
 */


object PartitionDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 演示效果明显，设置并行度为2，设置全局并行度为2
    env.setParallelism(2)
    //2 source 加载数据
    val datas = new mutable.MutableList[(Int, Long, String)]
    datas.+=((1, 1L, "Hello"))
    datas.+=((2, 2L, "Hello"))
    datas.+=((3, 2L, "Hello"))
    datas.+=((4, 3L, "Hello"))
    datas.+=((5, 3L, "Hello"))
    datas.+=((6, 3L, "hehe"))
    datas.+=((7, 4L, "hehe"))
    datas.+=((8, 4L, "hehe"))
    datas.+=((9, 4L, "hehe"))
    datas.+=((10, 4L, "hehe"))
    datas.+=((11, 5L, "hehe"))
    datas.+=((12, 5L, "hehe"))
    datas.+=((13, 5L, "hehe"))
    datas.+=((14, 5L, "hehe"))
    datas.+=((15, 5L, "hehe"))
    datas.+=((16, 6L, "hehe"))
    datas.+=((17, 6L, "hehe"))
    datas.+=((18, 6L, "hehe"))
    datas.+=((19, 6L, "hehe"))
    datas.+=((20, 6L, "hehe"))
    datas.+=((21, 6L, "hehe"))
    val orginalDs: DataSet[(Int, Long, String)] = env.fromCollection(Random.shuffle(datas))

    // 3 转换 使用分区规则进行分区
    //3.1 hash分区
    val hashDs: DataSet[(Int, Long, String)] = orginalDs.partitionByHash(_._3)
    //3.2 range 分区
    val rangeDs: DataSet[(Int, Long, String)] = orginalDs.partitionByRange(_._1)
    //对分区进行排序 sortpartition
    val sortDs: DataSet[(Int, Long, String)] = hashDs.sortPartition(_._2, Order.ASCENDING)

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    hashDs.writeAsText("e:/data/partitionbyhash/", FileSystem.WriteMode.OVERWRITE)
    rangeDs.writeAsText("e:/data/partitionbyrange", FileSystem.WriteMode.OVERWRITE)

    sortDs.writeAsText("e:/data/sortpartition", FileSystem.WriteMode.OVERWRITE)
    // 5 执行
    env.execute()
  }
}
