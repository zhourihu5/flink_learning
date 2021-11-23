package cn.itcast.batch.transformation

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink RightOuterJoin的operator
 */
/*
需求：用户dataset左关联城市数据
示例
请将以下元组数据(用户id,用户姓名)
(1, "zhangsan") , (2, "lisi") ,(3 , "wangwu")
元组数据(用户id,所在城市)
(1, "beijing"), (2, "shanghai"), (4, "guangzhou")
返回如下数据：
(3,wangwu,null)
(1,zhangsan,beijing)
(2,lisi,shanghai)
 */


object RighterOuterJoinDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    //2.1用户数据
    val userDs: DataSet[(Int, String)] = env.fromCollection(List((1, "zhangsan"), (2, "lisi"), (3, "wangwu")))
    //2.2城市数据
    val cityDs: DataSet[(Int, String)] = env.fromCollection(List((1, "beijing"), (2, "shanghai"), (4, "guangzhou")))
    // 3 转换 使用userDs leftouterjoin cityDs
    /*

      OPTIMIZER_CHOOSES：将选择权交予Flink优化器；
      BROADCAST_HASH_FIRST：广播第一个输入端，同时基于它构建一个哈希表，而第二个输入端作为探索端，选择这种策略的场景是第一个输入端规模很小；
      BROADCAST_HASH_SECOND：广播第二个输入端并基于它构建哈希表，第一个输入端作为探索端，选择这种策略的场景是第二个输入端的规模很小；
      REPARTITION_HASH_FIRST：该策略会导致两个输入端都会被重分区，但会基于第一个输入端构建哈希表。该策略适用于第一个输入端数据量小于
      第二个输入端的数据量，但这两个输入端的规模仍然很大，优化器也是当没有办法估算大小，没有已存在的分区以及排序顺序可被使用时系统默认采用的策略；
      REPARTITION_HASH_SECOND：该策略会导致两个输入端都会被重分区，但会基于第二个输入端构建哈希表。
      该策略适用于两个输入端的规模都很大，但第二个输入端的数据量小于第一个输入端的情况；
      REPARTITION_SORT_MERGE：输入端被以流的形式进行连接并合并成排过序的输入。该策略适用于一个或两个输入端都已排过序的情况；

     */
    //我们一般如果不明确数据集情况，就使用OPTIMIZER_CHOOSES
    val leftJoinAssigner: JoinFunctionAssigner[(Int, String), (Int, String)] = userDs.rightOuterJoin(cityDs,
      JoinHint.OPTIMIZER_CHOOSES).where(0).equalTo(0)
    //3.1 使用apply方法解析JoinFunctionAssigner中的数据处理逻辑
    val resultDs: DataSet[(Int, String, String)] = leftJoinAssigner.apply(
      (left, right) => {
        if (left == null) { //cityds中结果为null
          //返回的数据
          (right._1, right._2, "null")
        } else {
          //返回的数据
          (right._1, right._2, left._2)
        }
      }
    )

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resultDs.print()
    // 5 执行
  }
}
