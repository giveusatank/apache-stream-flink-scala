package com.pep.flink.other

import org.apache.flink.api.scala._

object FlinkSyllabus01 {
  def main(args: Array[String]): Unit = {
    val path = FlinkSyllabus01.getClass.getClassLoader.getResource("hello.txt").getPath
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.readTextFile(path,"UTF-8")
    val dataSetSum = dataSet.
      flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    dataSetSum.printOnTaskManager("test").setParallelism(1)
    env.execute("job1")
  }
}

