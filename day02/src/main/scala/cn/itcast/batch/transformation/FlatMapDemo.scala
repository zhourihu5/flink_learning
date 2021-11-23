package cn.itcast.batch.transformation

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/*
演示flink flatMap的operator
 */
/*
需求：
示例
分别将以下数据，转换成国家、省份、城市三个维度的数据。
将以下数据
    张三,中国,江西省,南昌市
    李四,中国,河北省,石家庄市
转换为
    (张三,中国)
    (张三,中国,江西省)
    (张三,中国,江西省,南昌市)
    (李四,中国)
    (李四,中国,河北省)
    (李四,中国,河北省,石家庄市)
 */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val sourceDs: DataSet[String] = env.fromCollection(List(
      "张三,中国,江西省,南昌市",
      "李四,中国,河北省,石家庄市"
    ))

    // 3 转换操作 使用flatMap:分为map和flat,先执行map操作针对每个数据切分，切分后按照要求组成所谓元组数据（3）装入list中，最后执行flat操作去掉
    //list的外壳
    val faltMapDs: DataSet[Product with Serializable] = sourceDs.flatMap(
      line => {
        // 3.1对数据进行切分操作
        val arr: Array[String] = line.split(",")
        // 3.2 组装数据装入list中
        List(
          //(张三,中国)
          (arr(0), arr(1)),
          // (张三,中国,江西省)
          (arr(0), arr(1), arr(2)),
          // (张三,中国,江西省,南昌市)
          (arr(0), arr(1), arr(2), arr(3))
        )
      }
    )


    // 4 输出 (如果直接打印无需进行启动，执行execute)
    faltMapDs.print()
    // 5 执行
  }
}
