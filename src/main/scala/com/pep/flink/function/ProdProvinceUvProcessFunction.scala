package com.pep.flink.function

import java.{lang, util}

import com.pep.flink.bean._
import com.pep.flink.utils.DataUtils
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class ProdProvinceUvProcessFunction extends ProcessWindowFunction[UvProxyKeyModel,UvSeparatedKeyModel,String,TimeWindow]{

  override def process(key: String, context: Context,
                       elements: Iterable[UvProxyKeyModel],
                       out: Collector[UvSeparatedKeyModel]): Unit = {
    val sets = new util.HashSet[String]()
    for(element <- elements){
      sets.add(element.realUserId)
    }
    val arr = key.split("~")
    val iter = sets.iterator()
    while (iter.hasNext){
      out.collect(UvSeparatedKeyModel(arr(0).trim,arr(1).trim,iter.next()))
    }
  }
}

class ProdProvinceUvIndexProcessFunction extends ProcessWindowFunction[ProvinceIndexModel,ProvinceIndexModel,String,TimeWindow]{

  override def process(key: String, context: Context, elements: Iterable[ProvinceIndexModel],
                       out: Collector[ProvinceIndexModel]): Unit = {
    val sets = new util.HashSet[String]()
    for(ele <- elements){
     sets.add(ele.province)
    }
    val iter = sets.iterator
    while(iter.hasNext){
      out.collect(ProvinceIndexModel(key.trim,iter.next().trim))
    }
  }
}

class ProductUvProcessFunction extends ProcessWindowFunction[ProductUvModel,ProductUvModel,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[ProductUvModel],
                       out: Collector[ProductUvModel]): Unit = {
    val sets = new util.HashSet[String]()
    for(ele <- elements){
      sets.add(ele.realUserId)
    }
    val iter = sets.iterator()
    while (iter.hasNext){
      out.collect(ProductUvModel(key.trim,iter.next().trim))
    }
  }
}

class ProductPvProcessFunction extends ProcessWindowFunction[ProductUvModel,PvValueState,String,TimeWindow]{

  lazy val state:ValueState[PvValueState] = getRuntimeContext.getState[PvValueState](
    new ValueStateDescriptor[PvValueState]("pvState",classOf[PvValueState]))

  override def process(key: String, context: Context, elements: Iterable[ProductUvModel], out: Collector[PvValueState]): Unit = {
    val processTime: Long = context.currentProcessingTime
    val lists = new util.ArrayList[String]()
    for(ele <- elements){
      lists.add(ele.realUserId)
    }
    val pv_ : Int = lists.size()
    val current:PvValueState = state.value() match {
      case null => PvValueState(key.trim,pv_,processTime)
      case PvValueState(productId,pv,timeStamp) => {
        val flag = DataUtils.judgeTimeStampIsOneDay(processTime,timeStamp)
        if(flag) PvValueState(productId,pv.toInt+pv_,processTime)
        else PvValueState(productId,pv_,processTime)
      }
    }
    state.update(current)
    out.collect(current)
  }
}
