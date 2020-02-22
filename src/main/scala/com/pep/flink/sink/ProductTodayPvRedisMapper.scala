package com.pep.flink.sink

import java.{lang, util}

import com.pep.flink.bean.PvValueState
import com.pep.flink.utils.{DataUtils, RedisPropertyUtils}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


class ProductTodayPvRedisMapper(expire: Int) extends RedisMapper[PvValueState] {
  /**
    * Returns descriptor which defines data type.
    *
    * @return data type descriptor
    */
  override def getCommandDescription: RedisCommandDescription = {
    val rediscommand = RedisCommand.SET
    new RedisCommandDescription(rediscommand)
  }

  /**
    * Extracts key from data.
    *
    * @param data source data
    * @return key
    */
  override def getKeyFromData(data: PvValueState, context: SinkFunction.Context[_]): String = {
    val todayTS = DataUtils.queryTodayTimeStamp
    val prefix = RedisPropertyUtils.getRedisProperty.getProperty("product_today_pv_day_5s_prefix")
     s"${data.productId}:${prefix}:${todayTS}"
  }

  /**
    * Extracts value from data.
    *
    * @param data source data
    * @return value
    */
  override def getValueFromData(data: PvValueState, context: SinkFunction.Context[_]): String = data.currentPv.toString

  /**
    * 添加一个设置过期时间的方法，单位为秒
    */
  override def getRedisKeyExpireTime: Int = expire

  /**
    * 添加Pipeline获取数据方法一
    */
  override def getPipelineArrayListFromData(data: PvValueState, context: SinkFunction.Context[_]): util.ArrayList[String] = null

  /**
    * 添加Pipeline获取数据方法二
    */
  override def getPipelineHashMapFromData(data: PvValueState, context: SinkFunction.Context[_]): util.HashMap[String, lang.Double] = null
}
