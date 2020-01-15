package com.pep.flink.other

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

case class ProductModel(productId: String, userName: String)

object FlinkSyllabus02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties
    prop.setProperty("bootstrap.servers", "172.30.0.7:9092")
    prop.setProperty("group.id", "test")
    prop.setProperty("auto.offset.reset", "latest")

    val kafkaConsumer011 = new FlinkKafkaConsumer011[String]("", new SimpleStringSchema(), prop)
    kafkaConsumer011.setCommitOffsetsOnCheckpoints(true)
    kafkaConsumer011.setStartFromLatest()

    val dataStream01: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer011[String]("flink_1", new SimpleStringSchema(), prop))

    val uvDataStream: DataStream[(String, Int)] = dataStream01
      .filter(x => {
        x.split(",").length == 2
      })
      .map(x => {
        val array = x.split(",")
        ProductModel(array(0).trim, array(1).trim)
      }).keyBy(_.productId).timeWindow(Time.seconds(10), Time.seconds(5))
      .process(new MyProcessFunciton)

    val uvDataStream_2: DataStream[(String, Long)] = dataStream01.filter(x => x.split(",").length == 2)
      .map(x => {
        val array = x.split(",")
        (array(0), 1L)
      }).keyBy(0).reduce((x, y) => (x._1, x._2 + y._2))


    uvDataStream.print("flink stream 111")

    uvDataStream_2.print("flink stream 222")

    env.execute("start job")
  }
}

class MyProcessFunciton extends ProcessWindowFunction[ProductModel, (String, Int), String, TimeWindow] {
  var hashSet = Set[String]()

  override def process(key: String, context: Context, elements: Iterable[ProductModel],
                       out: Collector[(String, Int)]): Unit = {
    for (in <- elements) {
      hashSet += in.userName
    }
    out.collect((key, hashSet.size))
  }
}