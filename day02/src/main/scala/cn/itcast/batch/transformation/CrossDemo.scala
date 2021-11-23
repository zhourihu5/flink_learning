package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable
import scala.util.Random

/*
演示flink cross笛卡尔积
 */
/*

 */

object CrossDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    val students = new mutable.MutableList[(Int, String)]
    //学生
    students.+=((1, "张三"))
    students.+=((2, "李四"))
    students.+=((3, "王五"))
    students.+=((4, "赵六"))

    val subjects = new mutable.MutableList[(Int, String)]
    //课程
    subjects.+=((1, "Java"))
    subjects.+=((2, "Python"))
    subjects.+=((3, "前端"))
    subjects.+=((4, "大数据"))
    val stuDs = env.fromCollection(Random.shuffle(students))
    val subjectDs = env.fromCollection(Random.shuffle(subjects))
    // 3 转换 进行笛卡尔积操作,类似偏函数（scala）
    val resDs: DataSet[(Int, String, Int, String)] = stuDs.cross(subjectDs) {
      (item1, item2) => {
        (item1._1, item1._2, item2._1, item2._2)
      }
    }
    val crossDs = stuDs.cross(subjectDs)

   

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resDs.print()
    // 5 执行
  }
}
