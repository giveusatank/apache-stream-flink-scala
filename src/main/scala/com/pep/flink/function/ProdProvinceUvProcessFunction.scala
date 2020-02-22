package com.pep.flink.function

import java.util
import java.util.Date

import com.pep.flink.bean._
import com.pep.flink.utils.{DataUtils, HyperLogLog}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable


class ProdProvinceUvProcessFunction extends ProcessWindowFunction[UvProxyKeyModel, UvSeparatedKeyModel, String, TimeWindow] {

  override def process(key: String, context: Context,
                       elements: Iterable[UvProxyKeyModel],
                       out: Collector[UvSeparatedKeyModel]): Unit = {
    val sets = new util.HashSet[String]()
    for (element <- elements) {
      sets.add(element.realUserId)
    }
    val arr = key.split("~")
    val iter = sets.iterator()
    while (iter.hasNext) {
      out.collect(UvSeparatedKeyModel(arr(0).trim, arr(1).trim, iter.next()))
    }
  }
}

class ProdProvinceUvIndexProcessFunction extends ProcessWindowFunction[UvProxyKeyModel, ProvinceIndexModel, String, TimeWindow] {

  override def process(key: String,
                       context: Context,
                       elements: Iterable[UvProxyKeyModel],
                       out: Collector[ProvinceIndexModel]): Unit = {

    val item: UvProxyKeyModel = elements.iterator.next()
    val product = item.proxyKey.split("~")(0)
    val province = item.proxyKey.split("~")(1)
    out.collect(ProvinceIndexModel(product, province))
  }
}

class ProductUvProcessFunction extends ProcessWindowFunction[ProductUvModel, ProductUv, String, TimeWindow] {

  /*lazy val lastTimeStamp: ValueState[String] = getRuntimeContext.getState[String](
    new ValueStateDescriptor[String]("lastTimeStamp", classOf[String])
  )*/
  override def process(key: String, context: Context,
                       elements: Iterable[ProductUvModel],
                       out: Collector[ProductUv]): Unit = {
    val hyperLogLog = new HyperLogLog(0.1325); //64个桶
    for (ele <- elements) {
      hyperLogLog.offer(ele.realUserId)
    }
    /* var withTime: Long = context.currentProcessingTime
     if (lastTimeStamp.value() == null) {
       withTime = DataUtils.queryTargetedBatchTimeStamp(context.currentProcessingTime, "5s")
     } else {
       if (Math.abs(
         DataUtils.queryTargetedBatchTimeStamp(context.currentProcessingTime, "5s") -
           lastTimeStamp.value().toLong) > 10) {
         withTime = DataUtils.queryTargetedBatchTimeStamp(context.currentProcessingTime, "5s")
       } else {
         withTime = lastTimeStamp.value().toLong
       }
     }
     lastTimeStamp.update((withTime + 5).toString)*/
    out.collect(ProductUv(key.trim, hyperLogLog.cardinality()))
  }
}

class ProductUvTodayProcessFunciton extends ProcessWindowFunction[ProductUvModel, ProductUvPiplineModel, String, TimeWindow] {
  override def process(key: String, context: Context,
                       elements: Iterable[ProductUvModel],
                       out: Collector[ProductUvPiplineModel]): Unit = {

    val sets = mutable.Set[String]()
    val javaList = new util.ArrayList[String]()
    for (ele <- elements) {
      sets += ele.realUserId
    }
    for (item <- sets) {
      javaList.add(item)
    }
    out.collect(ProductUvPiplineModel(key.trim, javaList))
  }
}

class ProductPvProcessFunction extends ProcessWindowFunction[ProductUvModel, PvValueState, String, TimeWindow] {

  lazy val state: ValueState[PvValueState] = getRuntimeContext.getState[PvValueState](
    new ValueStateDescriptor[PvValueState]("pvState", classOf[PvValueState]))

  lazy val todayTS: ValueState[String] = getRuntimeContext.getState[String](
    new ValueStateDescriptor[String]("todayTS", classOf[String]))

  override def process(key: String, context: Context, elements: Iterable[ProductUvModel], out: Collector[PvValueState]): Unit = {

    val pv_ : Long = elements.size
    val currentTS: String = DataUtils.queryTodayTimeStamp
    val todayTS_ : String = todayTS.value()
    var valueState: PvValueState = null
    if (state.value() == null) {
      //第一次执行
      valueState = PvValueState(key.trim, pv_)
    } else {
      val lastState: PvValueState = state.value()
      //不是第一次执行，要判断是否跨天
      val flag = currentTS.equals(todayTS_)
      if (flag) {
        valueState = PvValueState(lastState.productId.trim, lastState.currentPv + pv_)
      } else {
        valueState = PvValueState(lastState.productId.trim, pv_)
      }
    }
    state.update(valueState)
    todayTS.update(currentTS)
    out.collect(valueState)
  }
}

class ProductPvAggregateFunction extends AggregateFunction[ProductUvModel, Long, Long] {

  override def add(value: ProductUvModel, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class ProductPvWindowFunction extends WindowFunction[Long, PvValueState, String, TimeWindow] {


  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[PvValueState]): Unit = {
    out.collect(PvValueState(key.trim, input.iterator.next()))
  }
}

class ProductPvKeyedProcessFunciton extends KeyedProcessFunction[String, PvValueState, PvValueState] {

  lazy val stateConfig = StateTtlConfig
    .newBuilder(Time.seconds(15))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .build()

  val pvStateDesc = new ValueStateDescriptor[PvValueState]("pvState", classOf[PvValueState])
  pvStateDesc.enableTimeToLive(stateConfig)

  val tsStateDesc = new ValueStateDescriptor[String]("todayTS", classOf[String])
  tsStateDesc.enableTimeToLive(stateConfig)

  lazy val state: ValueState[PvValueState] = getRuntimeContext.getState[PvValueState](
    pvStateDesc)
  lazy val todayTS: ValueState[String] = getRuntimeContext.getState[String](
    tsStateDesc)

  override def processElement(value: PvValueState,
                              ctx: KeyedProcessFunction[String, PvValueState, PvValueState]#Context,
                              out: Collector[PvValueState]): Unit = {

    var currentPv = value.currentPv
    val lastState = state.value()
    var pvValueState: PvValueState = null
    val currentTS: String = DataUtils.queryTodayTimeStamp
    if (lastState == null) {
      //当前key第一条
    } else {
      //当前不是第一条，那么就一定有 todayTS_
      val todayTS_ : String = todayTS.value()
      //判断是否跨天
      if (todayTS_.equals(currentTS)) {
        //没有跨天
        currentPv = currentPv + lastState.currentPv
      }
    }
    pvValueState = PvValueState(value.productId.trim, currentPv)
    todayTS.update(currentTS)
    state.update(pvValueState)
    out.collect(pvValueState)
  }
}

class Product1HourUvProcessFunction extends ProcessWindowFunction[ProductUvModel, ProductUvPipline, String, TimeWindow] {

  override def process(key: String, context: Context,
                       elements: Iterable[ProductUvModel],
                       out: Collector[ProductUvPipline]): Unit = {

    val javaMap = new util.HashMap[java.lang.String, java.lang.Double]()
    val currentTs = DataUtils.queryTargetedBatchTimeStamp(new Date().getTime, "5s")
    for (ele <- elements) {
      javaMap.put(ele.realUserId, java.lang.Double.valueOf(currentTs))
    }
    out.collect(ProductUvPipline(key.trim, javaMap))
  }
}