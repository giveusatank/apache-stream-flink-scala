package com.pep.flink.other

import org.apache.flink.streaming.api.scala._

case class Person(name:String,address:String,id:Int,timeStamp:Long)
object FlinkSyllabus03 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val path = FlinkSyllabus03.getClass.getClassLoader.getResource("name.txt").getPath

    val dataStream_1 = env.readTextFile(path)

    val personStream = dataStream_1.map( x => {
      val array = x.split(",")
      Person(array(0).trim,array(1).trim,array(2).trim.toInt,array(3).trim.toLong)
    })

    val splitStream: SplitStream[Person] = personStream.split(person => {
      if(person.name.length > 4) Seq("longName")
      else Seq("lowName")
    })

    val lowStream: DataStream[Person] = splitStream.select("lowName")
    val highStream: DataStream[Person] = splitStream.select("longName")

    val connectStream = lowStream.connect(highStream)

    val resStream: DataStream[(String, String)] = connectStream.map(
      low => (low.name,"low"),
      high => (high.name,"high")
    )

    resStream.print()
    env.execute("flink test")
  }
}
