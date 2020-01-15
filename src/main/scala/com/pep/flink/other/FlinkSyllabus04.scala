package com.pep.flink.other

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object FlinkSyllabus04 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = FlinkSyllabus03.getClass.getClassLoader.getResource("name.txt").getPath
    val dataStream_1 = env.readTextFile(path)

    /*val ds = dataStream_1.flatMap(new MyFlatMapFunction)
    ds.print()*/
    env.execute("flink test")
  }
}

class MyMapFunction extends MapFunction[String, String] {
  override def map(t: String): String = {
    val index = "666"
    s"${index}~${t}"
  }
}

/*
class MyFlatMapFunction extends FlatMapFunction[String,String]{
  override def flatMap(t: String, collector: Collector[String]): Unit = {
    val list = new ListCollector[String](new util.ArrayList[String]())
    val array: Array[String] = t.split(",")
    for(record <- array){
      list.collect(record)
    }
    list
  }
}*/
