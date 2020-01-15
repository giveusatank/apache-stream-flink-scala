package com.pep.flink.other


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector


case class Tempature(id: Int, timeStamp: Long, tempature: Double)

object FlinkSyllabus06 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(3)

    /*val prop = new java.util.Properties()
    prop.setProperty("group.id", "test_02")
    prop.setProperty("bootstrap.servers", "172.30.0.7:9092")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")*/


    /*val sourceDs: DataStream[String] = env.addSource[String](
      new FlinkKafkaConsumer011[String]("flink_1", new SimpleStringSchema(), prop))*/

    val ss = env.socketTextStream("192.168.186.30", 9933)

    /*val tempatureDs = ss
      .filter(x => !x.equals(""))
      .map(x => {
        val array = x.split(",")
        Tempature(array(0).trim.toInt, array(1).trim.toLong, array(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Tempature](
        Time.milliseconds(1000L)
      ) {
        override def extractTimestamp(element: Tempature): Long = {
          element.timeStamp * 1000L
        }
      })
      .keyBy(_.id)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce((x, y) => Tempature(x.id, x.timeStamp, x.tempature + y.tempature))


    tempatureDs.print("tempature: ")*/

    val ooo = ss
      .filter(x => !x.equals(""))
      .map(x => {
        val array = x.split(",")
        Tempature(array(0).trim.toInt, array(1).trim.toLong, array(2).trim.toDouble)
      })
      .keyBy(_.id)
      .process[String](new MyKeyedProcessFunction)

    ooo.print("test~~")

    env.execute("flink_syllabus_06")
  }
}

class MyKeyedProcessFunction extends KeyedProcessFunction[Int, Tempature, String] {

  lazy val lastTimeStamp: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTimeStamp", classOf[Long]))

  lazy val lastTempature: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTempature", classOf[Double]))


  override def processElement(value: Tempature,
                              ctx: KeyedProcessFunction[Int, Tempature, String]#Context,
                              out: Collector[String]): Unit = {
    //第一条
    if (lastTempature.value() == 0.0) {
      lastTempature.update(value.tempature)
    } else {
      //没有定时任务并且温度上升
      if (lastTimeStamp.value() == 0 && lastTempature.value() < value.tempature) {
        val current = ctx.timerService().currentProcessingTime()
        ctx.timerService().registerProcessingTimeTimer(current + 3000L)
        lastTimeStamp.update(current)
        lastTempature.update(value.tempature)
      } else if (lastTimeStamp.value() != 0 && lastTempature.value() >= value.tempature) {
        ctx.timerService().deleteProcessingTimeTimer(lastTimeStamp.value())
        lastTempature.update(value.tempature)
        lastTimeStamp.clear()
      } else if (lastTimeStamp.value() == 0L) {
        lastTempature.update(value.tempature)
      }
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Int, Tempature, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    out.collect(ctx.getCurrentKey + "连续两次温度上涨了！！")
    ctx.timerService().deleteEventTimeTimer(lastTimeStamp.value())
  }
}


